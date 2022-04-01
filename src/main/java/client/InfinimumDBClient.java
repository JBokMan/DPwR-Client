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
import org.apache.commons.lang3.SerializationUtils;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.commons.lang3.SerializationUtils.serialize;
import static utils.CommunicationUtils.*;

@Slf4j
public class InfinimumDBClient {
    private transient final ResourcePool resources = new ResourcePool();
    private transient final Map<Integer, InetSocketAddress> serverMap = new HashMap<>();
    private transient Worker worker;
    private transient Endpoint endpoint;

    private static final ContextParameters.Feature[] FEATURE_SET = {ContextParameters.Feature.TAG, ContextParameters.Feature.RMA};

    public InfinimumDBClient(final String serverHostAddress, final Integer serverPort) {
        this.serverMap.put(0, new InetSocketAddress(serverHostAddress, serverPort));
        setupServerConnection(serverHostAddress, serverPort);
        try {
            testServerConnection();
        } catch (final ConnectException e) {
            log.error("InfinimumDB-Server could not be reached");
        }
    }

    private void testServerConnection() throws ConnectException {
        //TODO implement
    }

    private void setupServerConnection(final String serverHostAddress, final Integer serverPort) {
        //TODO implement
    }

    public void put(final String key, final byte[] value, final int timeoutMs, final int maxAttempts) throws CloseException, ControlException, DuplicateKeyException, TimeoutException {
        try (resources) {
            initialize(key);
            putOperation(key, value, timeoutMs);
        } catch (final CloseException | ControlException | TimeoutException e) {
            if (maxAttempts == 1) {
                throw e;
            }
            put(key, value, timeoutMs, maxAttempts - 1);
        }
    }

    public byte[] get(final String key, final int timeoutMs, final int maxAttempts) throws CloseException, NotFoundException, ControlException, TimeoutException, SerializationException {
        try (resources) {
            initialize(key);
            return getOperation(key, timeoutMs);
        } catch (final CloseException | ControlException | TimeoutException | SerializationException e) {
            if (maxAttempts == 1) {
                throw e;
            }
            return get(key, timeoutMs, maxAttempts - 1);
        }
    }

    public void del(final String key, final int timeoutMs, final int maxAttempts) throws CloseException, NotFoundException, ControlException, TimeoutException {
        try (resources) {
            initialize(key);
            delOperation(key, timeoutMs);
        } catch (final CloseException | ControlException | TimeoutException e) {
            if (maxAttempts == 1) {
                throw e;
            }
            del(key, timeoutMs, maxAttempts - 1);
        }
    }

    public void putOperation(final String key, final byte[] value, final int timeoutMs) throws SerializationException, ControlException, DuplicateKeyException, TimeoutException {
        log.info("Starting PUT operation");
        final int tagID = receiveTagID(worker, timeoutMs);
        final byte[] entryBytes = serialize(new PlasmaEntry(key, value, new byte[20]));
        final byte[] entrySizeBytes = getLengthAsBytes(entryBytes);

        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final ArrayList<Long> requests = new ArrayList<>();
            requests.add(prepareToSendData(tagID, serialize("PUT"), endpoint, scope));
            requests.addAll(prepareToSendKey(tagID, key, endpoint, scope));
            requests.add(prepareToSendData(tagID, entrySizeBytes, endpoint, scope));

            sendData(requests, worker, timeoutMs);
        }

        final String statusCode = SerializationUtils.deserialize(receiveData(tagID, 10, worker, timeoutMs));
        log.info("Received status code: \"{}\"", statusCode);

        if ("200".equals(statusCode)) {
            putEntry(tagID, entryBytes, worker, endpoint, timeoutMs);
            sendSingleMessage(tagID, serialize("200"), endpoint, worker, timeoutMs);
        } else if ("409".equals(statusCode)) {
            throw new DuplicateKeyException("An object with that key was already in the plasma store");
        }
        log.info("Put completed\n");
    }


    private byte[] getOperation(final String key, final int timeoutMs) throws ControlException, NotFoundException, TimeoutException, SerializationException {
        log.info("Starting GET operation");
        final int tagID = receiveTagID(worker, timeoutMs);
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final ArrayList<Long> requests = new ArrayList<>();
            requests.add(prepareToSendData(tagID, serialize("GET"), endpoint, scope));
            requests.addAll(prepareToSendKey(tagID, key, endpoint, scope));

            sendData(requests, worker, timeoutMs);
        }

        final String statusCode = SerializationUtils.deserialize(receiveData(tagID, 10, worker, timeoutMs));
        log.info("Received status code: \"{}\"", statusCode);

        byte[] value = new byte[0];
        if ("200".equals(statusCode)) {
            value = receiveValue(tagID, endpoint, worker, timeoutMs);
            sendSingleMessage(tagID, serialize("200"), endpoint, worker, timeoutMs);

        } else if ("404".equals(statusCode)) {
            throw new NotFoundException("An object with the key \"" + key + "\" was not found by the server.");
        }
        log.info("Get completed\n");
        return value;
    }

    private void delOperation(final String key, final int timeoutMs) throws NotFoundException, TimeoutException {
        log.info("Starting DEL operation");
        final int tagID = receiveTagID(worker, timeoutMs);
        final ArrayList<Long> requests = new ArrayList<>();

        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            requests.add(prepareToSendData(tagID, serialize("DEL"), endpoint, scope));
            requests.addAll(prepareToSendKey(tagID, key, endpoint, scope));

            sendData(requests, worker, timeoutMs);
        }

        final String statusCode = SerializationUtils.deserialize(receiveData(tagID, 10, worker, timeoutMs));
        log.info("Received status code: \"{}\"", statusCode);

        if ("404".equals(statusCode)) {
            throw new NotFoundException("An object with the key \"" + key + "\" was not found by the server.");
        }
        log.info("Del completed\n");
    }

    private void initialize(final String key) throws ControlException {
        NativeLogger.enable();
        log.info("Using UCX version {}", Context.getVersion());

        // Initialize UCP context
        log.info("Initializing context");
        final ContextParameters contextParameters = new ContextParameters().setFeatures(FEATURE_SET);
        final Context context = pushResource(Context.initialize(contextParameters, null));

        // Create a worker
        log.info("Creating worker");
        final WorkerParameters workerParameters = new WorkerParameters().setThreadMode(ThreadMode.SINGLE);
        this.worker = pushResource(context.createWorker(workerParameters));

        // Determining responsible server
        final Integer responsibleServerID = getResponsibleServerID(key, this.serverMap.size());
        final EndpointParameters endpointParams = new EndpointParameters().setRemoteAddress(this.serverMap.get(responsibleServerID)).setPeerErrorHandlingMode();

        // Creating Endpoint
        log.info("Connecting to {}", this.serverMap.get(responsibleServerID));
        this.endpoint = pushResource(worker.createEndpoint(endpointParams));
    }

    protected <T extends AutoCloseable> T pushResource(final T resource) {
        resources.push(resource);
        return resource;
    }
}
