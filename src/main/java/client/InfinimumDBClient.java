package client;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.example.util.CommunicationBarrier;
import de.hhu.bsinfo.infinileap.example.util.Requests;
import de.hhu.bsinfo.infinileap.util.CloseException;
import de.hhu.bsinfo.infinileap.util.ResourcePool;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.Serializable;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import static client.CommunicationUtils.*;

@Slf4j
public class InfinimumDBClient {

    private final JedisPool jedisClient;
    private Context context;
    private final ResourceScope scope = ResourceScope.newSharedScope();
    private final ResourcePool resources = new ResourcePool();
    private final InetSocketAddress serverAddress;
    private Worker worker;
    private Endpoint endpoint;
    private final CommunicationBarrier barrier = new CommunicationBarrier();

    private static final long DEFAULT_REQUEST_SIZE = 1024;
    private static final ContextParameters.Feature[] FEATURE_SET = {
            ContextParameters.Feature.TAG, ContextParameters.Feature.RMA, ContextParameters.Feature.WAKEUP, ContextParameters.Feature.AM,
            ContextParameters.Feature.ATOMIC_32, ContextParameters.Feature.ATOMIC_64, ContextParameters.Feature.STREAM
    };

    public InfinimumDBClient(String serverHostAddress, Integer serverPort, String redisHostAddress, Integer redisPort) {
        this.serverAddress = new InetSocketAddress(serverHostAddress, serverPort);
        setupServerConnection(serverHostAddress, serverPort);
        try {
            testServerConnection();
        } catch (ConnectException e) {
            System.err.println("InfinimumDB-Server could not be reached");
        }

        this.jedisClient = new JedisPool(redisHostAddress, redisPort);
        try {
            this.jedisClient.getResource();
        } catch (JedisConnectionException e) {
            System.err.println("Redis could not be reached");
        }
    }

    private void testServerConnection() throws ConnectException {
        //TODO implement
    }

    private void setupServerConnection(String serverHostAddress, Integer serverPort) {
        //TODO implement
    }

    public void put(String key, Serializable object) {
        NativeLogger.enable();
        log.info("Using UCX version {}", Context.getVersion());
        try (resources) {
            initialize();
            putOperation(key, object, context);
        } catch (ControlException e) {
            log.error("Native operation failed", e);
        } catch (CloseException e) {
            log.error("Closing resource failed", e);
        } catch (InterruptedException e) {
            log.error("Unexpected interrupt occurred", e);
        }

        // Release resource scope
        scope.close();
    }

    private void initialize() throws ControlException, InterruptedException {
        // Create context parameters
        var contextParameters = new ContextParameters()
                .setFeatures(FEATURE_SET)
                .setRequestSize(DEFAULT_REQUEST_SIZE);

        // Read configuration (Environment Variables)
        var configuration = pushResource(
                Configuration.read()
        );

        log.info("Initializing context");

        // Initialize UCP context
        this.context = pushResource(
                Context.initialize(contextParameters, configuration)
        );

        var workerParameters = new WorkerParameters()
                .setThreadMode(ThreadMode.SINGLE);

        log.info("Creating worker");

        // Create a worker
        this.worker = pushResource(
                context.createWorker(workerParameters)
        );

        var endpointParameters = new EndpointParameters()
                .setRemoteAddress(this.serverAddress);

        log.info("Connecting to {}", this.serverAddress);
        this.endpoint = worker.createEndpoint(endpointParameters);
    }

    public void putOperation(String key, Serializable object, Context context) {
        log.info("Starting PUT operation");
        ArrayList<Long> requests = new ArrayList<>();

        //check if object is serializable
        byte[] objectBytes = SerializationUtils.serialize(object);
        if (objectBytes == null) {
            log.warn("Object was not serializable, aborting PUT operation");
            return;
        }

        byte[] messageBytes = SerializationUtils.serialize("PUT");
        requests.add(prepareToSendData(messageBytes, endpoint, barrier, scope));

        final var descriptor = getMemoryDescriptorOfBytes(objectBytes, context);

        byte[] objectID = getMD5Hash(key);
        requests.add(prepareToSendData(objectID, endpoint, barrier, scope));

        requests.add(prepareToSendRemoteKey(descriptor, endpoint, barrier));

        sendData(requests, worker, barrier);

        waitUntilRemoteSignalsCompletion();

        log.info("Put completed");
    }

    private void waitUntilRemoteSignalsCompletion() {
        log.info("Wait until remote signals completion");
        final var completion = MemorySegment.allocateNative(Byte.BYTES, scope);

        long request = worker.receiveTagged(completion, Tag.of(0L), new RequestParameters()
                .setReceiveCallback(barrier::release));

        try {
            Requests.await(worker, barrier);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Requests.release(request);
    }

    protected <T extends AutoCloseable> T pushResource(T resource) {
        resources.push(resource);
        return resource;
    }

    public Object get(String key) {
        //TODO implement
        return null;
    }
}
