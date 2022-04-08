package client;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.util.CloseException;
import de.hhu.bsinfo.infinileap.util.ResourcePool;
import exceptions.DuplicateKeyException;
import exceptions.NotFoundException;
import jdk.incubator.foreign.ResourceScope;
import lombok.extern.slf4j.Slf4j;
import model.PlasmaEntry;
import org.apache.commons.lang3.SerializationException;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.commons.lang3.SerializationUtils.serialize;
import static utils.CommunicationUtils.*;
import static utils.HashUtils.getResponsibleServerID;

@Slf4j
public class DPwRClient {
    private final ResourcePool resources = new ResourcePool();
    private final Map<Integer, InetSocketAddress> serverMap = new HashMap<>();
    private Context context;
    private Worker worker;

    private static final ContextParameters.Feature[] FEATURE_SET = {ContextParameters.Feature.TAG, ContextParameters.Feature.RMA};

    public DPwRClient(final String serverHostAddress, final Integer serverPort) {
        this.serverMap.put(0, new InetSocketAddress(serverHostAddress, serverPort));
        NativeLogger.enable();
        log.info("Using UCX version {}", Context.getVersion());

        try {
            // Initialize UCP context
            log.info("Initializing context");
            final ContextParameters contextParameters = new ContextParameters().setFeatures(FEATURE_SET);
            this.context = Context.initialize(contextParameters, null);

            // Create a worker
            log.info("Creating worker");
            final WorkerParameters workerParameters = new WorkerParameters().setThreadMode(ThreadMode.SINGLE);
            this.worker = context.createWorker(workerParameters);
        } catch (final ControlException e) {
            e.printStackTrace();
        }
    }

    public void put(final String key, final byte[] value, final int timeoutMs, final int maxAttempts) throws CloseException, ControlException, DuplicateKeyException, TimeoutException {
        try (resources) {
            final Endpoint endpoint = createEndpoint(key);
            putOperation(key, value, timeoutMs, endpoint);
        } catch (final CloseException | ControlException | TimeoutException e) {
            if (maxAttempts == 1) {
                throw e;
            }
            resetWorker();
            put(key, value, timeoutMs, maxAttempts - 1);
        }
    }

    public byte[] get(final String key, final int timeoutMs, final int maxAttempts) throws CloseException, NotFoundException, ControlException, TimeoutException, SerializationException {
        try (resources) {
            final Endpoint endpoint = createEndpoint(key);
            return getOperation(key, timeoutMs, endpoint);
        } catch (final CloseException | ControlException | TimeoutException | SerializationException e) {
            if (maxAttempts == 1) {
                throw e;
            }
            resetWorker();
            return get(key, timeoutMs, maxAttempts - 1);
        }
    }

    public void del(final String key, final int timeoutMs, final int maxAttempts) throws CloseException, NotFoundException, ControlException, TimeoutException {
        try (resources) {
            final Endpoint endpoint = createEndpoint(key);
            delOperation(key, timeoutMs, endpoint);
        } catch (final CloseException | ControlException | TimeoutException e) {
            if (maxAttempts == 1) {
                throw e;
            }
            resetWorker();
            del(key, timeoutMs, maxAttempts - 1);
        }
    }

    public void putOperation(final String key, final byte[] value, final int timeoutMs, final Endpoint endpoint) throws SerializationException, ControlException, DuplicateKeyException, TimeoutException {
        log.info("Starting PUT operation");
        final int tagID = receiveTagID(worker, timeoutMs);
        final byte[] entryBytes = serialize(new PlasmaEntry(key, value, new byte[20]));

        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final ArrayList<Long> requests = new ArrayList<>();
            requests.add(prepareToSendString(tagID, "PUT", endpoint, scope));
            requests.addAll(prepareToSendKey(tagID, key, endpoint, scope));
            requests.add(prepareToSendInteger(tagID, entryBytes.length, endpoint, scope));

            sendData(requests, worker, timeoutMs);
        }

        final String statusCode = receiveStatusCode(tagID, worker, timeoutMs);

        if ("200".equals(statusCode)) {
            sendEntryPerRDMA(tagID, entryBytes, worker, endpoint, timeoutMs);
            sendStatusCode(tagID, "200", endpoint, worker, timeoutMs);
        } else if ("409".equals(statusCode)) {
            throw new DuplicateKeyException("An object with that key was already in the plasma store");
        }
        log.info("Put completed\n");
    }


    private byte[] getOperation(final String key, final int timeoutMs, final Endpoint endpoint) throws ControlException, NotFoundException, TimeoutException, SerializationException {
        log.info("Starting GET operation");
        final int tagID = receiveTagID(worker, timeoutMs);
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final ArrayList<Long> requests = new ArrayList<>();
            requests.add(prepareToSendString(tagID, "GET", endpoint, scope));
            requests.addAll(prepareToSendKey(tagID, key, endpoint, scope));

            sendData(requests, worker, timeoutMs);
        }

        final String statusCode = receiveStatusCode(tagID, worker, timeoutMs);

        byte[] value = new byte[0];
        if ("200".equals(statusCode)) {
            value = receiveValuePerRDMA(tagID, endpoint, worker, timeoutMs);
            sendStatusCode(tagID, "200", endpoint, worker, timeoutMs);

        } else if ("404".equals(statusCode)) {
            throw new NotFoundException("An object with the key \"" + key + "\" was not found by the server.");
        }
        log.info("Get completed\n");
        return value;
    }

    private void delOperation(final String key, final int timeoutMs, final Endpoint endpoint) throws NotFoundException, TimeoutException {
        log.info("Starting DEL operation");
        final int tagID = receiveTagID(worker, timeoutMs);
        final ArrayList<Long> requests = new ArrayList<>();

        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            requests.add(prepareToSendString(tagID, "DEL", endpoint, scope));
            requests.addAll(prepareToSendKey(tagID, key, endpoint, scope));

            sendData(requests, worker, timeoutMs);
        }

        final String statusCode = receiveStatusCode(tagID, worker, timeoutMs);

        if ("404".equals(statusCode)) {
            throw new NotFoundException("An object with the key \"" + key + "\" was not found by the server.");
        }
        log.info("Del completed\n");
    }

    protected <T extends AutoCloseable> T pushResource(final T resource) {
        resources.push(resource);
        return resource;
    }

    private Endpoint createEndpoint(final String key) throws ControlException {
        // Determining responsible server
        final Integer responsibleServerID = getResponsibleServerID(key, this.serverMap.size());

        // Creating Endpoint
        log.info("Connecting to {}", this.serverMap.get(responsibleServerID));
        final EndpointParameters endpointParams = new EndpointParameters().setRemoteAddress(this.serverMap.get(responsibleServerID)).setPeerErrorHandlingMode();
        return pushResource(this.worker.createEndpoint(endpointParams));
    }

    private void resetWorker() {
        this.worker.close();
        final WorkerParameters workerParameters = new WorkerParameters().setThreadMode(ThreadMode.SINGLE);
        try {
            this.worker = context.createWorker(workerParameters);
        } catch (final ControlException e) {
            e.printStackTrace();
        }
    }
}
