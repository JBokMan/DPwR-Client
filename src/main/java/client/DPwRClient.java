package client;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.util.CloseException;
import exceptions.DuplicateKeyException;
import exceptions.NotFoundException;
import jdk.incubator.foreign.ResourceScope;
import lombok.extern.slf4j.Slf4j;
import model.PlasmaEntry;
import org.apache.commons.lang3.SerializationException;
import utils.DPwRErrorHandler;

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
    private static final ContextParameters.Feature[] FEATURE_SET = {ContextParameters.Feature.TAG, ContextParameters.Feature.RMA};
    private final Map<Integer, InetSocketAddress> serverMap = new HashMap<>();
    private final ErrorHandler errorHandler = new DPwRErrorHandler();
    private Context context;
    private Worker worker;

    public DPwRClient(final String serverHostAddress, final Integer serverPort) throws CloseException, ControlException, TimeoutException {
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
        getNetworkInformation(serverHostAddress, serverPort, 5);
    }

    public void put(final String key, final byte[] value, final int timeoutMs, final int maxAttempts) throws CloseException, ControlException, DuplicateKeyException, TimeoutException {
        boolean retry = false;
        final InetSocketAddress responsibleServer = this.serverMap.get(getResponsibleServerID(key, this.serverMap.size()));
        final ResourceScope scope = ResourceScope.newConfinedScope();
        final Endpoint endpoint = this.worker.createEndpoint(new EndpointParameters(scope).setRemoteAddress(responsibleServer).setErrorHandler(errorHandler));
        try {
            putOperation(key, value, timeoutMs, endpoint);
        } catch (final ControlException | TimeoutException | SerializationException e) {
            if (maxAttempts > 1) {
                retry = true;
            } else {
                throw e;
            }
        } finally {
            tearDownEndpoint(endpoint, worker, timeoutMs);
            scope.close();
        }
        if (retry) {
            resetWorker();
            put(key, value, timeoutMs, maxAttempts - 1);
        }
    }

    public byte[] get(final String key, final int timeoutMs, final int maxAttempts) throws CloseException, NotFoundException, ControlException, TimeoutException, SerializationException {
        boolean retry = false;
        final InetSocketAddress responsibleServer = this.serverMap.get(getResponsibleServerID(key, this.serverMap.size()));
        final ResourceScope scope = ResourceScope.newConfinedScope();
        final Endpoint endpoint = this.worker.createEndpoint(new EndpointParameters(scope).setRemoteAddress(responsibleServer).setErrorHandler(errorHandler));
        byte[] result = new byte[0];
        try {
            result = getOperation(key, timeoutMs, endpoint);
        } catch (final ControlException | TimeoutException | SerializationException e) {
            if (maxAttempts > 1) {
                retry = true;
            } else {
                throw e;
            }
        } finally {
            tearDownEndpoint(endpoint, worker, timeoutMs);
            scope.close();
        }
        if (retry) {
            resetWorker();
            return get(key, timeoutMs, maxAttempts - 1);
        } else {
            return result;
        }
    }

    public void del(final String key, final int timeoutMs, final int maxAttempts) throws CloseException, NotFoundException, ControlException, TimeoutException {
        boolean retry = false;
        final InetSocketAddress responsibleServer = this.serverMap.get(getResponsibleServerID(key, this.serverMap.size()));
        final ResourceScope scope = ResourceScope.newConfinedScope();
        final Endpoint endpoint = this.worker.createEndpoint(new EndpointParameters(scope).setRemoteAddress(responsibleServer).setErrorHandler(errorHandler));
        try {
            delOperation(key, timeoutMs, endpoint);
        } catch (final TimeoutException e) {
            if (maxAttempts > 1) {
                retry = true;
            } else {
                throw e;
            }
        } finally {
            tearDownEndpoint(endpoint, worker, timeoutMs);
            scope.close();
        }
        if (retry) {
            resetWorker();
            del(key, timeoutMs, maxAttempts - 1);
        }
    }

    private void getNetworkInformation(final String serverHostAddress, final Integer serverPort, final int maxAttempts) throws TimeoutException, ControlException {
        boolean retry = false;
        final ResourceScope scope = ResourceScope.newConfinedScope();
        final Endpoint endpoint = this.worker.createEndpoint(new EndpointParameters(scope).setRemoteAddress(new InetSocketAddress(serverHostAddress, serverPort)).setErrorHandler(this.errorHandler));
        try {
            infOperation(500, endpoint);
        } catch (final TimeoutException e) {
            if (maxAttempts > 1) {
                retry = true;
            } else {
                throw e;
            }
        } finally {
            tearDownEndpoint(endpoint, worker, 500);
            scope.close();
        }
        if (retry) {
            resetWorker();
            getNetworkInformation(serverHostAddress, serverPort, maxAttempts - 1);
        }
    }

    private void infOperation(final int timeoutMs, final Endpoint endpoint) throws TimeoutException {
        final int tagID = receiveTagID(worker, timeoutMs);
        sendStatusCode(tagID, "INF", endpoint, worker, timeoutMs);
        receiveStatusCode(tagID, worker, timeoutMs);
        final int serverCount = receiveTagID(worker, timeoutMs);
        for (int i = 0; i < serverCount; i++) {
            final InetSocketAddress serverAddress = receiveAddress(tagID, worker, timeoutMs);
            this.serverMap.put(i, serverAddress);
        }
        sendStatusCode(tagID, "200", endpoint, worker, timeoutMs);
        log.info(String.valueOf(this.serverMap));
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

            awaitRequests(requests, worker, timeoutMs);
        }

        final String statusCode = receiveStatusCode(tagID, worker, timeoutMs);

        if ("200".equals(statusCode)) {
            sendEntryPerRDMA(tagID, entryBytes, worker, endpoint, timeoutMs);
            sendStatusCode(tagID, "200", endpoint, worker, timeoutMs);
        } else if ("409".equals(statusCode)) {
            throw new DuplicateKeyException("An object with that key was already in the plasma store");
        }
        log.info("Put completed");
    }

    private byte[] getOperation(final String key, final int timeoutMs, final Endpoint endpoint) throws ControlException, NotFoundException, TimeoutException, SerializationException {
        log.info("Starting GET operation");
        final int tagID = receiveTagID(worker, timeoutMs);
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final ArrayList<Long> requests = new ArrayList<>();
            requests.add(prepareToSendString(tagID, "GET", endpoint, scope));
            requests.addAll(prepareToSendKey(tagID, key, endpoint, scope));

            awaitRequests(requests, worker, timeoutMs);
        }

        final String statusCode = receiveStatusCode(tagID, worker, timeoutMs);

        byte[] value = new byte[0];
        if ("200".equals(statusCode)) {
            value = receiveValuePerRDMA(tagID, endpoint, worker, timeoutMs);
            sendStatusCode(tagID, "200", endpoint, worker, timeoutMs);
        } else if ("404".equals(statusCode)) {
            throw new NotFoundException("An object with the key \"" + key + "\" was not found by the server.");
        }
        log.info("Get completed");
        return value;
    }

    private void delOperation(final String key, final int timeoutMs, final Endpoint endpoint) throws NotFoundException, TimeoutException {
        log.info("Starting DEL operation");
        final int tagID = receiveTagID(worker, timeoutMs);
        final ArrayList<Long> requests = new ArrayList<>();

        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            requests.add(prepareToSendString(tagID, "DEL", endpoint, scope));
            requests.addAll(prepareToSendKey(tagID, key, endpoint, scope));

            awaitRequests(requests, worker, timeoutMs);
        }

        final String statusCode = receiveStatusCode(tagID, worker, timeoutMs);

        if ("404".equals(statusCode)) {
            throw new NotFoundException("An object with the key \"" + key + "\" was not found by the server.");
        }
        log.info("Del completed");
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
