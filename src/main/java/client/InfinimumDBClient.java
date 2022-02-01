package client;


import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.util.CloseException;
import de.hhu.bsinfo.infinileap.util.ResourcePool;
import jdk.incubator.foreign.ResourceScope;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import redis.clients.jedis.JedisPool;

import java.io.Serializable;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
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

    private static final long DEFAULT_REQUEST_SIZE = 1024;
    private static final ContextParameters.Feature[] FEATURE_SET = {
            ContextParameters.Feature.TAG, ContextParameters.Feature.RMA, ContextParameters.Feature.WAKEUP, ContextParameters.Feature.AM,
            ContextParameters.Feature.ATOMIC_32, ContextParameters.Feature.ATOMIC_64, ContextParameters.Feature.STREAM
    };

    public InfinimumDBClient(final String serverHostAddress, final Integer serverPort, final String redisHostAddress, final Integer redisPort) {
        this.serverAddress = new InetSocketAddress(serverHostAddress, serverPort);
        setupServerConnection(serverHostAddress, serverPort);
        try {
            testServerConnection();
        } catch (ConnectException e) {
            log.error("InfinimumDB-Server could not be reached");
        }

        this.jedisClient = new JedisPool(redisHostAddress, redisPort);
        this.jedisClient.getResource();
    }

    private void testServerConnection() throws ConnectException {
        //TODO implement
    }

    private void setupServerConnection(final String serverHostAddress, final Integer serverPort) {
        //TODO implement
    }

    public void put(final String key, final Serializable object) {
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
        } catch (NoSuchAlgorithmException e) {
            log.error("Hashing algorithm was not found", e);
        }

        // Release resource scope
        scope.close();
    }

    private void initialize() throws ControlException, InterruptedException {
        // Create context parameters
        final ContextParameters contextParameters = new ContextParameters()
                .setFeatures(FEATURE_SET)
                .setRequestSize(DEFAULT_REQUEST_SIZE);

        // Read configuration (Environment Variables)
        final Configuration configuration = pushResource(
                Configuration.read()
        );

        log.info("Initializing context");

        // Initialize UCP context
        this.context = pushResource(
                Context.initialize(contextParameters, configuration)
        );

        final WorkerParameters workerParameters = new WorkerParameters()
                .setThreadMode(ThreadMode.SINGLE);

        log.info("Creating worker");

        // Create a worker
        this.worker = pushResource(
                context.createWorker(workerParameters)
        );

        final EndpointParameters endpointParams = new EndpointParameters()
                .setRemoteAddress(this.serverAddress);

        log.info("Connecting to {}", this.serverAddress);
        this.endpoint = worker.createEndpoint(endpointParams);
    }

    public void putOperation(final String key, final Serializable object, final Context context) throws SerializationException, NoSuchAlgorithmException, ControlException {
        if (log.isInfoEnabled()) {
            log.info("Starting PUT operation");
        }


        final byte[] objectBytes;
        try {
            objectBytes = serializeObject(object);
        } catch (SerializationException e) {
            if (log.isErrorEnabled()) {
                log.error("An exception occurred while serializing the object, aborting PUT operation");
            }
            throw e;
        }

        final byte[] objectID;
        try {
            objectID = getMD5Hash(key);
        } catch (NoSuchAlgorithmException e) {
            if (log.isErrorEnabled()) {
                log.error("An exception occurred while hashing the key, aborting PUT operation");
            }
            throw e;
        }

        final MemoryDescriptor objectAddress;
        try {
            objectAddress = getMemoryDescriptorOfBytes(objectBytes, context);
        } catch (ControlException e) {
            if (log.isErrorEnabled()) {
                log.error("An exception occurred getting the objects memory address, aborting PUT operation");
            }
            throw e;
        }

        final ArrayList<Long> requests = new ArrayList<>();
        requests.add(prepareToSendData(SerializationUtils.serialize("PUT"), 0L, endpoint, scope));
        requests.add(prepareToSendData(objectID, 0L, endpoint, scope));
        requests.add(prepareToSendRemoteKey(objectAddress, endpoint));

        sendData(requests, worker);

        final String statusCode = SerializationUtils.deserialize(receiveData(10, 0L, worker, scope));
        if (log.isInfoEnabled()) {
            log.info("Received \"{}\"", statusCode);
        }

        if ("200".equals(statusCode)) {
            final Integer serverID = SerializationUtils.deserialize(receiveData(81, 0L, worker, scope));
            if (log.isInfoEnabled()) {
                log.info("Received \"{}\"", serverID);
            }
            jedisClient.getResource().set(key.getBytes(StandardCharsets.UTF_8), new byte[]{serverID.byteValue()});
            if (log.isInfoEnabled()) {
                log.info("Put completed");
            }
        }
    }

    protected <T extends AutoCloseable> T pushResource(final T resource) {
        resources.push(resource);
        return resource;
    }

    public Object get(final String key) {
        //TODO implement
        return null;
    }
}
