package client;

import de.hhu.bsinfo.infinileap.binding.Context;
import de.hhu.bsinfo.infinileap.binding.ContextParameters;
import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.binding.DataType;
import de.hhu.bsinfo.infinileap.binding.Endpoint;
import de.hhu.bsinfo.infinileap.binding.EndpointParameters;
import de.hhu.bsinfo.infinileap.binding.ErrorHandler;
import de.hhu.bsinfo.infinileap.binding.NativeLogger;
import de.hhu.bsinfo.infinileap.binding.RequestParameters;
import de.hhu.bsinfo.infinileap.binding.ThreadMode;
import de.hhu.bsinfo.infinileap.binding.Worker;
import de.hhu.bsinfo.infinileap.binding.WorkerParameters;
import de.hhu.bsinfo.infinileap.primitive.NativeInteger;
import de.hhu.bsinfo.infinileap.primitive.NativeLong;
import exceptions.DuplicateKeyException;
import exceptions.KeyNotFoundException;
import exceptions.NetworkException;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import lombok.extern.slf4j.Slf4j;
import model.PlasmaEntry;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.SerializationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import utils.DPwRErrorHandler;
import utils.PlasmaUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.logging.log4j.Level.INFO;
import static org.apache.logging.log4j.Level.OFF;
import static utils.CommunicationUtils.awaitRequests;
import static utils.CommunicationUtils.prepareToSendInteger;
import static utils.CommunicationUtils.prepareToSendKey;
import static utils.CommunicationUtils.prepareToSendStatusString;
import static utils.CommunicationUtils.receiveAddress;
import static utils.CommunicationUtils.receiveCount;
import static utils.CommunicationUtils.receiveHash;
import static utils.CommunicationUtils.receiveObjectPerRDMA;
import static utils.CommunicationUtils.receiveStatusCode;
import static utils.CommunicationUtils.receiveTagID;
import static utils.CommunicationUtils.receiveValuePerRDMA;
import static utils.CommunicationUtils.sendEntryPerRDMA;
import static utils.CommunicationUtils.sendStatusCode;
import static utils.CommunicationUtils.streamTagID;
import static utils.CommunicationUtils.tearDownEndpoint;
import static utils.HashUtils.getResponsibleServerID;

@Slf4j
public class DPwRClient {
    private static final ContextParameters.Feature[] FEATURE_SET = {ContextParameters.Feature.TAG, ContextParameters.Feature.RMA, ContextParameters.Feature.STREAM};
    private final Map<Integer, InetSocketAddress> serverMap = new HashMap<>();
    private final ErrorHandler errorHandler = new DPwRErrorHandler();
    private InetSocketAddress serverAddress = null;
    private Integer serverTimeout = null;
    private Boolean verbose = null;
    private Context context;
    private Worker worker;
    private Endpoint endpoint;
    private int tagID;

    public DPwRClient() {

    }

    public DPwRClient(final InetSocketAddress serverAddress, final int serverTimeout, final boolean verbose) throws NetworkException {
        this.serverAddress = serverAddress;
        this.serverTimeout = serverTimeout;
        this.verbose = verbose;
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
        initialize(99);
    }

    public void initialize(final int maxAttempts) throws NetworkException {
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

        // Initialize UCP context
        log.info("Initializing context");
        final ContextParameters contextParameters = new ContextParameters().setFeatures(FEATURE_SET);
        try {
            this.context = Context.initialize(contextParameters, null);

            // Create a worker
            log.info("Creating worker");
            final WorkerParameters workerParameters = new WorkerParameters().setThreadMode(ThreadMode.SINGLE);
            this.worker = context.createWorker(workerParameters);
        } catch (final ControlException e) {
            throw new NetworkException(e.getMessage());
        }

        try {
            establishConnection(5);
        } catch (final ControlException | TimeoutException e) {
            throw new NetworkException(e.getMessage());
        }
        getNetworkInformation(maxAttempts);
    }

    private void establishConnection(final int attempts) throws ControlException, TimeoutException {
        // Create an endpoint
        log.info("Creating Endpoint");
        final EndpointParameters endpointParameters = new EndpointParameters()
                .setRemoteAddress(serverAddress)
                .setErrorHandler(this.errorHandler)
                .enableClientIdentifier();

        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            this.endpoint = this.worker.createEndpoint(endpointParameters);
            this.tagID = receiveTagIDAsStream(scope);
            //this.tagID = receiveTagID(worker, serverTimeout, scope);
        } catch (final ControlException | TimeoutException e) {
            log.error(e.getMessage());
            if (attempts > 0) {
                establishConnection(attempts - 1);
            } else {
                throw e;
            }
        }
    }

    private int receiveTagIDAsStream(final ResourceScope scope) throws TimeoutException {
        final var buffer = MemorySegment.allocateNative(Integer.BYTES, scope);
        final var length = new NativeLong();

        log.info("Receiving stream");
        final var request = endpoint.receiveStream(buffer, 1, length, new RequestParameters()
                .setDataType(DataType.CONTIGUOUS_32_BIT)
                .setFlags(RequestParameters.Flag.STREAM_WAIT));

        awaitRequests(new long[]{request}, worker, serverTimeout);

        final var first = NativeInteger.map(buffer, 0L);
        return first.get();
    }

    private void setLogLevel(final org.apache.logging.log4j.Level level) {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
        final LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(level);
        ctx.updateLoggers();
    }

    private void getNetworkInformation(final int maxAttempts) throws NetworkException {
        boolean retry = false;
        try {
            infOperation();
        } catch (final TimeoutException e) {
            if (maxAttempts > 1) {
                retry = true;
            } else {
                throw new NetworkException(e.getMessage());
            }
        }
        if (retry) {
            getNetworkInformation(maxAttempts - 1);
        }
    }

    private void infOperation() throws TimeoutException {
        log.info("[{}] Starting INF operation", tagID);
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            requestNewTagID(scope);

            sendStatusCode(tagID, "INF", endpoint, worker, serverTimeout, scope);
            final int serverCount = receiveCount(tagID, worker, serverTimeout, scope);
            for (int i = 0; i < serverCount; i++) {
                final InetSocketAddress serverAddress = receiveAddress(tagID, worker, serverTimeout, scope);
                this.serverMap.put(i, serverAddress);
            }
        }
        log.info(this.serverMap.entrySet().toString());
        log.info("[{}] INF completed", tagID);
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
        } catch (final DuplicateKeyException | ControlException | TimeoutException e) {
            e.printStackTrace();
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

    public void closeConnection() {
        try {
            processRequest("BYE", "", new byte[0], 1);
        } catch (final DuplicateKeyException | ControlException | TimeoutException | KeyNotFoundException e) {
            log.warn(e.getMessage());
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

    public List<byte[]> list(final int maxAttempts) throws ControlException, TimeoutException, NetworkException {
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
        // lookup in server endpoint map
        boolean retry = false;
        byte[] result = new byte[0];
        try {
            switch (operationName) {
                case "PUT" -> putOperation(key, value);
                case "GET" -> result = getOperation(key);
                case "DEL" -> deleteOperation(key);
                case "CNT" -> result = containsOperation(key);
                case "HSH" -> result = hashOperation(key);
                case "BYE" -> closeConnectionOperation();
            }
        } catch (final TimeoutException | SerializationException | IOException | ClassNotFoundException e) {
            log.warn(e.getMessage());
            if (maxAttempts > 1) {
                retry = true;
            } else {
                throw new TimeoutException(e.getMessage());
            }
        }
        if (retry) {
            log.warn("Retry " + operationName);
            try {
                initialize();
            } catch (final NetworkException e) {
                log.error(e.getMessage());
                return result;
            }
            return processRequest(operationName, key, value, maxAttempts - 1);
        }
        return result;
    }

    @SuppressWarnings("TryFinallyCanBeTryWithResources")
    public List<byte[]> processListRequest(int maxAttempts) throws KeyNotFoundException, ControlException, TimeoutException, DuplicateKeyException, NetworkException {
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
            if (maxAttempts == 0) {
                throw new NetworkException("Connection lost");
            }
        }

        return result;
    }

    public void putOperation(final String key, final byte[] value) throws SerializationException, ControlException, DuplicateKeyException, TimeoutException, IOException {
        log.info("[{}] Starting PUT operation", tagID);
        final byte[] entryBytes = PlasmaUtils.serializePlasmaEntry(new PlasmaEntry(key, value, new byte[20]));

        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            requestNewTagID(scope);

            final long[] requests = new long[4];
            requests[0] = prepareToSendStatusString(tagID, "PUT", endpoint, scope);
            final long[] requests_tmp = prepareToSendKey(tagID, key, endpoint, scope);
            requests[1] = requests_tmp[0];
            requests[2] = requests_tmp[1];
            requests[3] = prepareToSendInteger(tagID, entryBytes.length, endpoint, scope);

            awaitRequests(requests, worker, serverTimeout);

            final String statusCode = receiveStatusCode(tagID, worker, serverTimeout, scope);

            switch (statusCode) {
                case "200" -> {
                    sendEntryPerRDMA(tagID, entryBytes, worker, endpoint, serverTimeout, scope);
                    sendStatusCode(tagID, "201", endpoint, worker, serverTimeout, scope);
                    final String resultStatusCode = receiveStatusCode(tagID, worker, serverTimeout, scope);
                    switch (resultStatusCode) {
                        case "202" -> log.info("Success");
                        case "401", "402" -> throw new TimeoutException("Something went wrong");
                        default -> throw new TimeoutException("Wrong status code: " + statusCode);
                    }
                }
                case "400" ->
                        throw new DuplicateKeyException("An object with that key was already in the plasma store");
                default -> throw new TimeoutException("Wrong status code: " + statusCode);
            }
        }
        log.info("[{}] Put completed", tagID);
    }

    private void requestNewTagID(final ResourceScope scope) throws TimeoutException {
        streamTagID(tagID, endpoint, worker, serverTimeout, scope);
        this.tagID = receiveTagIDAsStream(scope);
    }

    private byte[] getOperation(final String key) throws ControlException, KeyNotFoundException, TimeoutException, SerializationException, IOException, ClassNotFoundException {
        log.info("Starting GET operation");
        final byte[] value;
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            requestNewTagID(scope);

            final long[] requests = new long[3];
            requests[0] = prepareToSendStatusString(tagID, "GET", endpoint, scope);
            final long[] requests_tmp = prepareToSendKey(tagID, key, endpoint, scope);
            requests[1] = requests_tmp[0];
            requests[2] = requests_tmp[1];

            awaitRequests(requests, worker, serverTimeout);

            final String statusCode = receiveStatusCode(tagID, worker, serverTimeout, scope);
            switch (statusCode) {
                case "211" -> {
                    value = receiveValuePerRDMA(tagID, endpoint, worker, serverTimeout, scope);
                    sendStatusCode(tagID, "212", endpoint, worker, serverTimeout, scope);
                }
                case "411" ->
                        throw new KeyNotFoundException("An object with the key \"" + key + "\" was not found by the server.");
                default -> throw new TimeoutException("Wrong status code: " + statusCode);
            }

            final String resultStatusCode = receiveStatusCode(tagID, worker, serverTimeout, scope);
            switch (resultStatusCode) {
                case "213" -> log.info("Success");
                case "412" -> throw new TimeoutException("Something went wrong");
                default -> throw new TimeoutException("Wrong status code: " + statusCode);
            }
        }
        log.info("Get completed");
        return value;
    }

    private void deleteOperation(final String key) throws KeyNotFoundException, TimeoutException {
        log.info("Starting DEL operation");
        final long[] requests = new long[3];

        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            requestNewTagID(scope);

            requests[0] = prepareToSendStatusString(tagID, "DEL", endpoint, scope);
            final long[] requests_tmp = prepareToSendKey(tagID, key, endpoint, scope);
            requests[1] = requests_tmp[0];
            requests[2] = requests_tmp[1];

            awaitRequests(requests, worker, serverTimeout);

            final String statusCode = receiveStatusCode(tagID, worker, serverTimeout, scope);
            switch (statusCode) {
                case "221" -> log.info("Success");
                case "421" ->
                        throw new KeyNotFoundException("An object with the key \"" + key + "\" was not found by the server.");
                default -> throw new TimeoutException("Wrong status code: " + statusCode);
            }
        }
        log.info("Del completed");
    }

    private byte[] containsOperation(final String key) throws TimeoutException {
        log.info("Starting CNT operation");
        final byte[] result;
        final long[] requests = new long[3];

        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            requestNewTagID(scope);

            requests[0] = prepareToSendStatusString(tagID, "CNT", endpoint, scope);
            final long[] requests_tmp = prepareToSendKey(tagID, key, endpoint, scope);
            requests[1] = requests_tmp[0];
            requests[2] = requests_tmp[1];

            awaitRequests(requests, worker, serverTimeout);

            final String statusCode = receiveStatusCode(tagID, worker, serverTimeout, scope);

            switch (statusCode) {
                case ("231") -> result = new byte[1];
                case ("431") -> result = new byte[0];
                default -> throw new TimeoutException("Wrong status code: " + statusCode);
            }
        }
        log.info("CNT completed");
        return result;
    }

    private byte[] hashOperation(final String key) throws KeyNotFoundException, TimeoutException {
        log.info("Starting HSH operation");
        final byte[] result;
        final long[] requests = new long[3];

        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            requestNewTagID(scope);

            requests[0] = prepareToSendStatusString(tagID, "HSH", endpoint, scope);
            final long[] requests_tmp = prepareToSendKey(tagID, key, endpoint, scope);
            requests[1] = requests_tmp[0];
            requests[2] = requests_tmp[1];

            awaitRequests(requests, worker, serverTimeout);

            final String statusCode = receiveStatusCode(tagID, worker, serverTimeout, scope);

            switch (statusCode) {
                case ("241") -> log.info("Success");
                case ("441") ->
                        throw new KeyNotFoundException("An object with the key \"" + key + "\" was not found by the server.");
                default -> throw new TimeoutException("Wrong status code: " + statusCode);
            }

            result = receiveHash(tagID, worker, serverTimeout, scope);
            final String resultStatusCode = receiveStatusCode(tagID, worker, serverTimeout, scope);

            if ("242".equals(resultStatusCode)) {
                log.info("Success");
            } else {
                throw new TimeoutException("Wrong status code: " + statusCode);
            }
        }
        log.info("HSH completed");
        return result;
    }

    private void closeConnectionOperation() throws TimeoutException {
        log.info("Starting BYE operation");
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            requestNewTagID(scope);

            final long request = prepareToSendStatusString(tagID, "BYE", endpoint, scope);
            awaitRequests(new long[]{request}, worker, serverTimeout);
            try {
                final long request_2 = endpoint.closeNonBlocking();
                awaitRequests(new long[]{request_2}, worker, 1000);
            } catch (final TimeoutException e) {
                endpoint.close();
            }
        }
        log.info("BYE completed");
    }

    private List<byte[]> listOperation(final Endpoint endpoint) throws TimeoutException, ControlException {
        log.info("Starting LST operation");
        final ArrayList<byte[]> result;
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            requestNewTagID(scope);

            final int tagID = receiveTagID(worker, serverTimeout, scope);

            final long request = prepareToSendStatusString(tagID, "LST", endpoint, scope);
            awaitRequests(new long[]{request}, worker, serverTimeout);

            result = new ArrayList<>();
            final int count = receiveCount(tagID, worker, serverTimeout, scope);
            for (int i = 0; i < count; i++) {
                result.add(receiveObjectPerRDMA(tagID, endpoint, worker, serverTimeout, scope));
                sendStatusCode(tagID, "251", endpoint, worker, serverTimeout, scope);
            }
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
