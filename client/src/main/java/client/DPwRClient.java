package client;

import de.hhu.bsinfo.infinileap.binding.Context;
import de.hhu.bsinfo.infinileap.binding.ContextParameters;
import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.binding.Endpoint;
import de.hhu.bsinfo.infinileap.binding.EndpointParameters;
import de.hhu.bsinfo.infinileap.binding.ErrorHandler;
import de.hhu.bsinfo.infinileap.binding.NativeLogger;
import de.hhu.bsinfo.infinileap.binding.ThreadMode;
import de.hhu.bsinfo.infinileap.binding.Worker;
import de.hhu.bsinfo.infinileap.binding.WorkerParameters;
import de.hhu.bsinfo.infinileap.util.CloseException;
import exceptions.DuplicateKeyException;
import exceptions.KeyNotFoundException;
import exceptions.NetworkException;
import jdk.incubator.foreign.ResourceScope;
import lombok.extern.slf4j.Slf4j;
import model.PlasmaEntry;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.SerializationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import utils.DPwRErrorHandler;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.commons.lang3.SerializationUtils.serialize;
import static org.apache.logging.log4j.Level.INFO;
import static org.apache.logging.log4j.Level.OFF;
import static utils.CommunicationUtils.awaitRequests;
import static utils.CommunicationUtils.prepareToSendInteger;
import static utils.CommunicationUtils.prepareToSendKey;
import static utils.CommunicationUtils.prepareToSendString;
import static utils.CommunicationUtils.receiveAddress;
import static utils.CommunicationUtils.receiveCount;
import static utils.CommunicationUtils.receiveHash;
import static utils.CommunicationUtils.receiveObjectPerRDMA;
import static utils.CommunicationUtils.receiveStatusCode;
import static utils.CommunicationUtils.receiveTagID;
import static utils.CommunicationUtils.receiveValuePerRDMA;
import static utils.CommunicationUtils.sendEntryPerRDMA;
import static utils.CommunicationUtils.sendStatusCode;
import static utils.CommunicationUtils.tearDownEndpoint;
import static utils.HashUtils.getResponsibleServerID;

@Slf4j
public class DPwRClient {
    private static final ContextParameters.Feature[] FEATURE_SET = {ContextParameters.Feature.TAG, ContextParameters.Feature.RMA};
    private final Map<Integer, InetSocketAddress> serverMap = new HashMap<>();
    private final ErrorHandler errorHandler = new DPwRErrorHandler();
    private InetSocketAddress serverAddress = null;
    private Integer serverTimeout = null;
    private Boolean verbose = null;
    private Context context;
    private Worker worker;

    public DPwRClient() {

    }

    public DPwRClient(final InetSocketAddress serverAddress, final int serverTimeout, final boolean verbose) throws NetworkException {
        this.serverAddress = serverAddress;
        this.serverTimeout = serverTimeout;
        this.verbose = verbose;
        initialize();
    }

    public void setServerAddress(final InetSocketAddress serverAddress) {
        this.serverAddress = serverAddress;
    }

    public void setServerTimeout(final int serverTimeout) {
        this.serverTimeout = serverTimeout;
    }

    public void setVerbose(final boolean verbose) {
        this.verbose = verbose;
    }

    public void initialize() throws NetworkException {
        if (ObjectUtils.isEmpty(this.serverAddress) || ObjectUtils.isEmpty(this.serverAddress) || ObjectUtils.isEmpty(this.serverAddress)) {
            throw new IllegalStateException("Client is not properly set up, either the server address, server timeout or verbose state is missing");
        }
        NativeLogger.enable();
        if (verbose) {
            setLogLevel(INFO);
        } else {
            setLogLevel(OFF);
        }
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
            log.error(e.getMessage());
        }
        getNetworkInformation(serverAddress, 5);
    }

    private void setLogLevel(final org.apache.logging.log4j.Level level) {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
        final LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(level);
        ctx.updateLoggers();
    }

    private void getNetworkInformation(final InetSocketAddress serverAddress, final int maxAttempts) throws NetworkException {
        final ResourceScope scope = ResourceScope.newConfinedScope();
        final Endpoint endpoint;
        try {
            endpoint = this.worker.createEndpoint(new EndpointParameters(scope).setRemoteAddress(serverAddress).setErrorHandler(this.errorHandler));
        } catch (final ControlException e) {
            throw new NetworkException(e.getMessage());
        }
        boolean retry = false;
        try {
            infOperation(endpoint);
        } catch (final TimeoutException e) {
            if (maxAttempts > 1) {
                retry = true;
            } else {
                throw new NetworkException(e.getMessage());
            }
        } finally {
            tearDownEndpoint(endpoint, worker, 500);
            scope.close();
        }
        if (retry) {
            resetWorker();
            getNetworkInformation(serverAddress, maxAttempts - 1);
        }
    }

    private void infOperation(final Endpoint endpoint) throws TimeoutException {
        log.info("Starting INF operation");
        final int tagID = receiveTagID(worker, serverTimeout);
        sendStatusCode(tagID, "INF", endpoint, worker, serverTimeout);
        final int serverCount = receiveCount(tagID, worker, serverTimeout);
        for (int i = 0; i < serverCount; i++) {
            final InetSocketAddress serverAddress = receiveAddress(tagID, worker, serverTimeout);
            this.serverMap.put(i, serverAddress);
        }
        log.info(this.serverMap.entrySet().toString());
        log.info("INF completed");
    }

    public void put(final String key, final byte[] value, final int maxAttempts) throws NetworkException, DuplicateKeyException {
        try {
            processRequest("PUT", key, value, maxAttempts);
        } catch (final KeyNotFoundException | ControlException | TimeoutException e) {
            throw new NetworkException(e.getMessage());
        }
    }

    public byte[] get(final String key, final int maxAttempts) throws NetworkException, KeyNotFoundException {
        final byte[] result;
        try {
            result = processRequest("GET", key, new byte[0], maxAttempts);
        } catch (final DuplicateKeyException | KeyNotFoundException | ControlException | TimeoutException e) {
            throw new NetworkException(e.getMessage());
        }
        return result;
    }

    public void del(final String key, final int maxAttempts) throws NetworkException, KeyNotFoundException {
        try {
            processRequest("DEL", key, new byte[0], maxAttempts);
        } catch (final DuplicateKeyException | ControlException | TimeoutException e) {
            throw new NetworkException(e.getMessage());
        }
    }

    public boolean contains(final String key, final int maxAttempts) throws ControlException, TimeoutException {
        boolean contains = false;
        try {
            final byte[] result = processRequest("CNT", key, new byte[0], maxAttempts);
            contains = Arrays.equals(result, new byte[1]);
        } catch (final DuplicateKeyException | KeyNotFoundException e) {
            log.error(e.getMessage());
        }
        return contains;
    }

    public byte[] hash(final String key, final int maxAttempts) throws KeyNotFoundException, ControlException, TimeoutException {
        byte[] result = new byte[0];
        try {
            result = processRequest("HSH", key, new byte[0], maxAttempts);
        } catch (final DuplicateKeyException e) {
            log.error(e.getMessage());
        }
        return result;
    }

    public List<byte[]> list(final int maxAttempts) throws ControlException, TimeoutException {
        List<byte[]> result = List.of(new byte[0]);
        try {
            result = processListRequest(maxAttempts);
        } catch (final DuplicateKeyException | KeyNotFoundException e) {
            log.error(e.getMessage());
        }
        return result;
    }

    public byte[] processRequest(final String operationName, final String key, final byte[] value, final int maxAttempts) throws KeyNotFoundException, ControlException, TimeoutException, DuplicateKeyException {
        final InetSocketAddress responsibleServer = this.serverMap.get(getResponsibleServerID(key, this.serverMap.size()));
        log.info("Responsible server: {}", responsibleServer);
        final ResourceScope scope = ResourceScope.newConfinedScope();
        final Endpoint endpoint = this.worker.createEndpoint(new EndpointParameters(scope).setRemoteAddress(responsibleServer).setErrorHandler(errorHandler));
        boolean retry = false;
        byte[] result = new byte[0];
        try {
            switch (operationName) {
                case "PUT" -> putOperation(key, value, endpoint);
                case "GET" -> result = getOperation(key, endpoint);
                case "DEL" -> deleteOperation(key, endpoint);
                case "CNT" -> result = containsOperation(key, endpoint);
                case "HSH" -> result = hashOperation(key, endpoint);
            }
        } catch (final TimeoutException | SerializationException e) {
            if (maxAttempts > 1) {
                retry = true;
            } else {
                throw e;
            }
        } finally {
            tearDownEndpoint(endpoint, worker, serverTimeout);
            scope.close();
        }
        if (retry) {
            resetWorker();
            return processRequest(operationName, key, value, maxAttempts - 1);
        }
        return result;
    }

    @SuppressWarnings("TryFinallyCanBeTryWithResources")
    public List<byte[]> processListRequest(int maxAttempts) throws KeyNotFoundException, ControlException, TimeoutException, DuplicateKeyException {
        final ArrayList<byte[]> result = new ArrayList<>();
        for (final InetSocketAddress server : this.serverMap.values()) {
            boolean retry = true;
            while (retry && maxAttempts >= 1) {
                retry = false;
                final ResourceScope scope = ResourceScope.newConfinedScope();
                final Endpoint endpoint = this.worker.createEndpoint(new EndpointParameters(scope).setRemoteAddress(server).setErrorHandler(errorHandler));

                try {
                    result.addAll(listOperation(endpoint));
                } catch (final TimeoutException e) {
                    if (maxAttempts > 1) {
                        retry = true;
                    } else {
                        throw e;
                    }
                } finally {
                    tearDownEndpoint(endpoint, worker, serverTimeout);
                    scope.close();
                }
                if (retry) {
                    resetWorker();
                    maxAttempts = maxAttempts - 1;
                }
            }
        }

        return result;
    }

    public void putOperation(final String key, final byte[] value, final Endpoint endpoint) throws SerializationException, ControlException, DuplicateKeyException, TimeoutException {
        log.info("Starting PUT operation");
        final int tagID = receiveTagID(worker, serverTimeout);
        final byte[] entryBytes = serialize(new PlasmaEntry(key, value, new byte[20]));

        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final ArrayList<Long> requests = new ArrayList<>();
            requests.add(prepareToSendString(tagID, "PUT", endpoint, scope));
            requests.addAll(prepareToSendKey(tagID, key, endpoint, scope));
            requests.add(prepareToSendInteger(tagID, entryBytes.length, endpoint, scope));

            awaitRequests(requests, worker, serverTimeout);
        }

        final String statusCode = receiveStatusCode(tagID, worker, serverTimeout);

        if ("200".equals(statusCode)) {
            sendEntryPerRDMA(tagID, entryBytes, worker, endpoint, serverTimeout);
            sendStatusCode(tagID, "200", endpoint, worker, serverTimeout);
        } else if ("409".equals(statusCode)) {
            throw new DuplicateKeyException("An object with that key was already in the plasma store");
        }
        log.info("Put completed");
    }

    private byte[] getOperation(final String key, final Endpoint endpoint) throws ControlException, KeyNotFoundException, TimeoutException, SerializationException {
        log.info("Starting GET operation");
        final int tagID = receiveTagID(worker, serverTimeout);
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final ArrayList<Long> requests = new ArrayList<>();
            requests.add(prepareToSendString(tagID, "GET", endpoint, scope));
            requests.addAll(prepareToSendKey(tagID, key, endpoint, scope));

            awaitRequests(requests, worker, serverTimeout);
        }

        final String statusCode = receiveStatusCode(tagID, worker, serverTimeout);

        byte[] value = new byte[0];
        if ("200".equals(statusCode)) {
            value = receiveValuePerRDMA(tagID, endpoint, worker, serverTimeout);
            sendStatusCode(tagID, "200", endpoint, worker, serverTimeout);
        } else if ("404".equals(statusCode)) {
            throw new KeyNotFoundException("An object with the key \"" + key + "\" was not found by the server.");
        }
        log.info("Get completed");
        return value;
    }

    private void deleteOperation(final String key, final Endpoint endpoint) throws KeyNotFoundException, TimeoutException {
        log.info("Starting DEL operation");
        final int tagID = receiveTagID(worker, serverTimeout);
        final ArrayList<Long> requests = new ArrayList<>();

        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            requests.add(prepareToSendString(tagID, "DEL", endpoint, scope));
            requests.addAll(prepareToSendKey(tagID, key, endpoint, scope));

            awaitRequests(requests, worker, serverTimeout);
        }

        final String statusCode = receiveStatusCode(tagID, worker, serverTimeout);

        if ("404".equals(statusCode)) {
            throw new KeyNotFoundException("An object with the key \"" + key + "\" was not found by the server.");
        }
        log.info("Del completed");
    }

    private byte[] containsOperation(final String key, final Endpoint endpoint) throws TimeoutException {
        log.info("Starting CNT operation");
        byte[] result = new byte[0];
        final int tagID = receiveTagID(worker, serverTimeout);
        final ArrayList<Long> requests = new ArrayList<>();

        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            requests.add(prepareToSendString(tagID, "CNT", endpoint, scope));
            requests.addAll(prepareToSendKey(tagID, key, endpoint, scope));

            awaitRequests(requests, worker, serverTimeout);
        }

        final String statusCode = receiveStatusCode(tagID, worker, serverTimeout);

        if ("200".equals(statusCode)) {
            result = new byte[1];
        }
        log.info("CNT completed");
        return result;
    }

    private byte[] hashOperation(final String key, final Endpoint endpoint) throws KeyNotFoundException, TimeoutException {
        log.info("Starting HSH operation");
        final byte[] result;
        final int tagID = receiveTagID(worker, serverTimeout);
        final ArrayList<Long> requests = new ArrayList<>();

        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            requests.add(prepareToSendString(tagID, "HSH", endpoint, scope));
            requests.addAll(prepareToSendKey(tagID, key, endpoint, scope));

            awaitRequests(requests, worker, serverTimeout);
        }

        final String statusCode = receiveStatusCode(tagID, worker, serverTimeout);

        if ("404".equals(statusCode)) {
            throw new KeyNotFoundException("An object with the key \"" + key + "\" was not found by the server.");
        }
        result = receiveHash(tagID, worker, serverTimeout);
        log.info("HSH completed");
        return result;
    }

    private List<byte[]> listOperation(final Endpoint endpoint) throws TimeoutException, ControlException {
        log.info("Starting LST operation");
        final int tagID = receiveTagID(worker, serverTimeout);
        final ArrayList<Long> requests = new ArrayList<>();

        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            requests.add(prepareToSendString(tagID, "LST", endpoint, scope));
            awaitRequests(requests, worker, serverTimeout);
        }

        final ArrayList<byte[]> result = new ArrayList<>();
        final int count = receiveCount(tagID, worker, serverTimeout);
        for (int i = 0; i < count; i++) {
            result.add(receiveObjectPerRDMA(tagID, endpoint, worker, serverTimeout));
            sendStatusCode(tagID, "200", endpoint, worker, serverTimeout);
        }

        log.info("LST completed");
        return result;
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
