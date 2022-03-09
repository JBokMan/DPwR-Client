package client;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.util.CloseException;
import de.hhu.bsinfo.infinileap.util.ResourcePool;
import exceptions.DuplicateKeyException;
import exceptions.NotFoundException;
import lombok.extern.slf4j.Slf4j;
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
    private transient Context context;
    private transient final ResourcePool resources = new ResourcePool();
    private transient final Map<Integer, InetSocketAddress> serverMap = new HashMap<>();
    private transient Worker worker;
    private transient Endpoint endpoint;

    private static final long DEFAULT_REQUEST_SIZE = 1024;
    private static final ContextParameters.Feature[] FEATURE_SET = {ContextParameters.Feature.TAG, ContextParameters.Feature.RMA, ContextParameters.Feature.WAKEUP, ContextParameters.Feature.AM, ContextParameters.Feature.ATOMIC_32, ContextParameters.Feature.ATOMIC_64, ContextParameters.Feature.STREAM};

    public InfinimumDBClient(final String serverHostAddress, final Integer serverPort) {
        this.serverMap.put(0, new InetSocketAddress(serverHostAddress, serverPort));
        setupServerConnection(serverHostAddress, serverPort);
        try {
            testServerConnection();
        } catch (ConnectException e) {
            log.error("InfinimumDB-Server could not be reached");
        }
    }

    private void testServerConnection() throws ConnectException {
        //TODO implement
    }

    private void setupServerConnection(final String serverHostAddress, final Integer serverPort) {
        //TODO implement
    }

    public void put(final String key, final byte[] value, final int timeoutMs) throws CloseException, ControlException, DuplicateKeyException, TimeoutException {
        try (resources) {
            initialize(key);
            putOperation(key, value, context, timeoutMs);
        }
    }

    public byte[] get(final String key, final int timeoutMs) throws CloseException, NotFoundException, ControlException, TimeoutException {
        try (resources) {
            initialize(key);
            return getOperation(key, timeoutMs);
        }
    }

    public void del(final String key, final int timeoutMs) throws CloseException, NotFoundException, ControlException, TimeoutException {
        try (resources) {
            initialize(key);
            delOperation(key, timeoutMs);
        }
    }

    public void putOperation(final String key, final byte[] value, final Context context, int timeoutMs) throws SerializationException, ControlException, DuplicateKeyException, TimeoutException {
        log.info("Starting PUT operation");
        final ArrayList<Long> requests = new ArrayList<>();

        requests.add(prepareToSendData(serialize("PUT"), 0L, endpoint));
        requests.addAll(prepareToSendKey(key, endpoint));
        requests.add(prepareToSendRemoteKey(value, endpoint, context));

        sendData(requests, worker, timeoutMs);

        final String statusCode = SerializationUtils.deserialize(receiveData(10, 0L, worker, timeoutMs));
        log.info("Received status code: \"{}\"", statusCode);

        if ("409".equals(statusCode)) {
            throw new DuplicateKeyException("An object with that key was already in the plasma store");
        }

        log.info("Put completed\n");
    }

    private byte[] getOperation(final String key, int timeoutMs) throws ControlException, NotFoundException, TimeoutException {
        log.info("Starting GET operation");
        final ArrayList<Long> requests = new ArrayList<>();

        requests.add(prepareToSendData(serialize("GET"), 0L, endpoint));
        requests.addAll(prepareToSendKey(key, endpoint));

        sendData(requests, worker, timeoutMs);

        final String statusCode = SerializationUtils.deserialize(receiveData(10, 0L, worker, timeoutMs));
        log.info("Received status code: \"{}\"", statusCode);

        byte[] value = new byte[0];
        if ("200".equals(statusCode)) {
            value = receiveValue(endpoint, worker, timeoutMs);
            sendSingleMessage(serialize("200"), 0L, endpoint, worker, timeoutMs);

        } else if ("404".equals(statusCode)) {
            throw new NotFoundException("An object with the key \"" + key + "\" was not found by the server.");
        }
        log.info("Get completed\n");
        return value;
    }

    private void delOperation(String key, int timeoutMs) throws NotFoundException, TimeoutException {
        log.info("Starting DEL operation");
        final ArrayList<Long> requests = new ArrayList<>();

        requests.add(prepareToSendData(serialize("DEL"), 0L, endpoint));
        requests.addAll(prepareToSendKey(key, endpoint));

        sendData(requests, worker, timeoutMs);

        final String statusCode = SerializationUtils.deserialize(receiveData(10, 0L, worker, timeoutMs));
        log.info("Received status code: \"{}\"", statusCode);

        if ("404".equals(statusCode)) {
            throw new NotFoundException("An object with the key \"" + key + "\" was not found by the server.");
        }

        log.info("Del completed\n");
    }

    private void initialize(final String key) throws ControlException {
        NativeLogger.enable();
        log.info("Using UCX version {}", Context.getVersion());

        // Create context parameters
        final ContextParameters contextParameters = new ContextParameters().setFeatures(FEATURE_SET).setRequestSize(DEFAULT_REQUEST_SIZE);

        // Read configuration (Environment Variables)
        final Configuration configuration = pushResource(Configuration.read());

        // Initialize UCP context
        log.info("Initializing context");
        this.context = pushResource(Context.initialize(contextParameters, configuration));
        final WorkerParameters workerParameters = new WorkerParameters().setThreadMode(ThreadMode.SINGLE);

        // Create a worker
        log.info("Creating worker");
        this.worker = pushResource(context.createWorker(workerParameters));

        // Determining responsible server
        final Integer responsibleServerID = getResponsibleServerID(key, this.serverMap.size());
        final EndpointParameters endpointParams = new EndpointParameters().setRemoteAddress(this.serverMap.get(responsibleServerID)).setPeerErrorHandlingMode();

        // Creating Endpoint
        log.info("Connecting to {}", this.serverMap.get(responsibleServerID));
        this.endpoint = worker.createEndpoint(endpointParams);
    }

    protected <T extends AutoCloseable> T pushResource(final T resource) {
        resources.push(resource);
        return resource;
    }
}
