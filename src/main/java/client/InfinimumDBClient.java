package client;


import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.util.CloseException;
import de.hhu.bsinfo.infinileap.util.ResourcePool;
import exceptions.NotFoundException;
import jdk.incubator.foreign.ResourceScope;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.math.NumberUtils;
import redis.clients.jedis.JedisPool;

import java.io.Serializable;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static client.CommunicationUtils.*;

@Slf4j
public class InfinimumDBClient {

    private transient final JedisPool jedisClient;
    private transient Context context;
    private transient final ResourcePool resources = new ResourcePool();
    private transient final Map<Long, InetSocketAddress> serverMap = new HashMap<>();
    private transient Worker worker;
    private transient Endpoint endpoint;

    private static final long DEFAULT_REQUEST_SIZE = 1024;
    private static final ContextParameters.Feature[] FEATURE_SET = {
            ContextParameters.Feature.TAG, ContextParameters.Feature.RMA, ContextParameters.Feature.WAKEUP, ContextParameters.Feature.AM,
            ContextParameters.Feature.ATOMIC_32, ContextParameters.Feature.ATOMIC_64, ContextParameters.Feature.STREAM
    };

    public InfinimumDBClient(final String serverHostAddress, final Integer serverPort, final String redisHostAddress, final Integer redisPort) {
        this.serverMap.put(0L, new InetSocketAddress(serverHostAddress, serverPort));
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

    public void put(final String key, final Serializable object) throws NoSuchAlgorithmException, InterruptedException, CloseException, ControlException {

        try (ResourceScope scope = ResourceScope.newSharedScope(); resources) {
            NativeLogger.enable();
            if (log.isInfoEnabled()) {
                log.info("Starting PUT operation");
            }
            if (log.isInfoEnabled()) {
                log.info("Using UCX version {}", Context.getVersion());
            }
            initialize(key);
            putOperation(key, object, context, scope);
        } catch (ControlException e) {
            log.error("Native operation failed", e);
            throw e;
        } catch (CloseException e) {
            log.error("Closing resource failed", e);
            throw e;
        } catch (InterruptedException e) {
            log.error("Unexpected interrupt occurred", e);
            throw e;
        } catch (NoSuchAlgorithmException e) {
            log.error("Hash algorithm was not found", e);
            throw e;
        }
    }

    public byte[] get(final String key) throws CloseException, NotFoundException, ControlException, InterruptedException, NoSuchAlgorithmException {
        final ResourceScope scope = ResourceScope.newSharedScope();
        NativeLogger.enable();
        if (log.isInfoEnabled()) {
            log.info("Starting GET operation");
        }
        if (log.isInfoEnabled()) {
            log.info("Using UCX version {}", Context.getVersion());
        }

        byte[] object;
        try (resources) {
            initialize(key);
            object = getOperation(key, scope);
        } catch (ControlException e) {
            log.error("Native operation failed", e);
            throw e;
        } catch (CloseException e) {
            log.error("Closing resource failed", e);
            throw e;
        } catch (InterruptedException e) {
            log.error("Unexpected interrupt occurred", e);
            throw e;
        } catch (NotFoundException e) {
            log.error("Object was not found", e);
            throw e;
        } catch (NoSuchAlgorithmException e) {
            log.error("Hash algorithm was not found", e);
            throw e;
        } finally {
            scope.close();
            resources.close();
        }

        return object;
    }

    private void initialize(final String key) throws ControlException, InterruptedException, NoSuchAlgorithmException {
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

        Long responsibleServerID = getResponsibleServerID(key, this.serverMap.size());

        final EndpointParameters endpointParams = new EndpointParameters()
                .setRemoteAddress(this.serverMap.get(responsibleServerID));

        log.info("Connecting to {}", this.serverMap.get(responsibleServerID));
        this.endpoint = worker.createEndpoint(endpointParams);
    }

    public void putOperation(final String key, final Serializable object, final Context context, final ResourceScope scope) throws SerializationException, ControlException {
        final byte[] objectBytes;
        try {
            objectBytes = serializeObject(object);
        } catch (SerializationException e) {
            if (log.isErrorEnabled()) {
                log.error("An exception occurred while serializing the object, aborting PUT operation");
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

        final HashMap<String, String> metadata = new HashMap<>();
        metadata.put("key", key);

        final byte[] metadataBytes;
        try {
            metadataBytes = SerializationUtils.serialize(metadata);
        } catch (SerializationException e) {
            if (log.isErrorEnabled()) {
                log.error("An exception occurred while serializing metadata map, aborting PUT operation");
            }
            throw e;
        }

        final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        byteBuffer.putInt(metadataBytes.length);
        final byte[] metadataSizeBytes = byteBuffer.array();

        final ArrayList<Long> requests = new ArrayList<>();
        requests.add(prepareToSendData(SerializationUtils.serialize("PUT"), 0L, endpoint, scope));
        requests.add(prepareToSendRemoteKey(objectAddress, endpoint));
        requests.add(prepareToSendData(metadataSizeBytes, 0L, endpoint, scope));
        requests.add(prepareToSendData(metadataBytes, 0L, endpoint, scope));

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
            jedisClient.getResource().hset(key, "serverID", serverID.toString());
            jedisClient.getResource().hsetnx(key, "hash_count", "1");
            if (log.isInfoEnabled()) {
                log.info("Put completed\n");
            }
        }
    }

    protected <T extends AutoCloseable> T pushResource(final T resource) {
        resources.push(resource);
        return resource;
    }


    private byte[] getOperation(final String key, final ResourceScope scope) throws ControlException, NotFoundException {
        int hashCount = 0;
        final String response = jedisClient.getResource().hget(key, "hash_count");
        if (response == null) {
            if (log.isWarnEnabled()) {
                log.warn("Key was not found in Redis, defaulting to hash count 1");
            }
        } else if (!NumberUtils.isCreatable(response)) {
            if (log.isWarnEnabled()) {
                log.warn("ServerID value was not a number in Redis, defaulting to hash count 1");
            }
        } else {
            hashCount = Integer.parseInt(response);
        }
        if (log.isInfoEnabled()) {
            log.info("Using hash count: {}", hashCount);
        }

        final HashMap<String, String> getData = new HashMap<>();
        getData.put("key", key);
        getData.put("hash_count", String.valueOf(hashCount));

        final byte[] getDataBytes;
        try {
            getDataBytes = SerializationUtils.serialize(getData);
        } catch (SerializationException e) {
            if (log.isErrorEnabled()) {
                log.error("An exception occurred while the data map, aborting GET operation");
            }
            throw e;
        }

        final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        byteBuffer.putInt(getDataBytes.length);
        final byte[] getDataSizeBytes = byteBuffer.array();

        final ArrayList<Long> requests = new ArrayList<>();
        requests.add(prepareToSendData(SerializationUtils.serialize("GET"), 0L, endpoint, scope));
        requests.add(prepareToSendData(getDataSizeBytes, 0L, endpoint, scope));
        requests.add(prepareToSendData(getDataBytes, 0L, endpoint, scope));
        sendData(requests, worker);

        final String statusCode = SerializationUtils.deserialize(receiveData(10, 0L, worker, scope));
        if (log.isInfoEnabled()) {
            log.info("Received \"{}\"", statusCode);
        }

        byte[] output = new byte[0];
        if ("200".equals(statusCode)) {
            final MemoryDescriptor descriptor = receiveMemoryDescriptor(0L, worker);
            output = receiveRemoteObject(descriptor, endpoint, worker, scope, resources);
            if (log.isInfoEnabled()) {
                log.info("Read \"{}\" from remote buffer", SerializationUtils.deserialize(output).toString());
            }
            requests.clear();
            requests.add(prepareToSendData(SerializationUtils.serialize("200"), 0L, endpoint, scope));
            sendData(requests, worker);
            if (log.isInfoEnabled()) {
                log.info("Get completed\n");
            }
        } else if ("404".equals(statusCode)) {
            throw new NotFoundException("An object with the key \"" + key + "\" was not found by the server.");
        }
        return output;
    }
}
