package client;


import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.example.util.CommunicationBarrier;
import de.hhu.bsinfo.infinileap.util.CloseException;
import de.hhu.bsinfo.infinileap.util.ResourcePool;
import jdk.incubator.foreign.ResourceScope;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.Serializable;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import static client.CommunicationUtils.*;

@Slf4j
public class InfinimumDBClient {

    private transient final JedisPool jedisClient;
    private transient Context context;
    private transient final ResourceScope scope = ResourceScope.newSharedScope();
    private transient final ResourcePool resources = new ResourcePool();
    private transient final InetSocketAddress serverAddress;
    private transient Worker worker;
    private transient Endpoint endpoint;
    private transient final CommunicationBarrier barrier = new CommunicationBarrier();

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
            log.error("InfinimumDB-Server could not be reached");
        }

        this.jedisClient = new JedisPool(redisHostAddress, redisPort);
        try {
            this.jedisClient.getResource();
        } catch (JedisConnectionException e) {
            log.error("Redis could not be reached");
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
        if (log.isInfoEnabled()) {
            log.info("Using UCX version {}", Context.getVersion());
        }
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

        byte[] objectBytes = serializeObject(object);
        if (objectBytes.length == 0) {
            log.warn("Object was not serializable or empty, aborting PUT operation");
            return;
        }

        byte[] objectID = getMD5Hash(key);
        if (objectID.length == 0) {
            log.warn("An exception occurred while hashing the key, aborting PUT operation");
            return;
        }

        final MemoryDescriptor objectAddress = getMemoryDescriptorOfBytes(objectBytes, context);
        if (objectAddress == null) {
            log.warn("An exception occurred getting the objects memory address, aborting PUT operation");
            return;
        }

        ArrayList<Long> requests = new ArrayList<>();
        requests.add(prepareToSendData(SerializationUtils.serialize("PUT"), 0L, endpoint, barrier, scope));
        requests.add(prepareToSendData(objectID, 0L, endpoint, barrier, scope));
        requests.add(prepareToSendRemoteKey(objectAddress, endpoint, barrier));

        sendData(requests, worker, barrier);

        String statusCode = SerializationUtils.deserialize(receiveData(10, 0L, worker, barrier, scope));
        log.info("Received \"{}\"", statusCode);

        if ("200".equals(statusCode)) {
            Integer serverID = SerializationUtils.deserialize(receiveData(81, 0L, worker, barrier, scope));
            log.info("Received \"{}\"", serverID);
            jedisClient.getResource().set(key.getBytes(StandardCharsets.UTF_8), new byte[]{serverID.byteValue()});
            log.info("Put completed");
        }
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
