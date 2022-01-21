package client;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.example.util.CommunicationBarrier;
import de.hhu.bsinfo.infinileap.example.util.Requests;
import de.hhu.bsinfo.infinileap.util.CloseException;
import de.hhu.bsinfo.infinileap.util.ResourcePool;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.SerializationUtils;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import utils.PutData;

import java.io.Serializable;
import java.net.ConnectException;
import java.net.InetSocketAddress;

@Slf4j
public class InfinimumDBClient {

    private final JedisPool jedisClient;
    private Context context;
    private final ResourceScope scope = ResourceScope.newSharedScope();
    private final ResourcePool resources = new ResourcePool();
    private final CommunicationBarrier barrier = new CommunicationBarrier();
    private InetSocketAddress serverAddress;
    private Worker worker;
    private Endpoint endpoint;

    private static final Identifier IDENTIFIER = new Identifier(0x01);
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
            sendPutMessage(key, object);
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

    public void sendPutMessage(String key, Serializable object) throws ControlException, InterruptedException {
        byte[] objectBytes = SerializationUtils.serialize(object);
        assert objectBytes != null;
        int dataSize = objectBytes.length;

        // Create memory segment and fill it with data
        final var source = MemorySegment.ofArray(objectBytes);
        final var memoryRegion = this.context.allocateMemory(dataSize);
        memoryRegion.segment().copyFrom(source);
        String dataAddress = memoryRegion.descriptor().remoteAddress().toString();

        String operationName = "PUT";

        int sizeOfOperationName = operationName.getBytes().length + 1;
        String dataString = new PutData(dataSize, dataAddress).toString();
        int sizeOfDataString = dataString.getBytes().length + 1;

        // Create header and data segments
        final var header = MemorySegment.allocateNative(sizeOfOperationName, scope);
        final var data = MemorySegment.allocateNative(sizeOfDataString, scope);

        // Set data within segments
        header.setUtf8String(0L, operationName);
        data.setUtf8String(0L, dataString);

        // Invoke remote handler
        Requests.await(this.worker, this.endpoint.sendActive(IDENTIFIER, header, data, new RequestParameters()
                .setDataType(DataType.CONTIGUOUS_8_BIT)));

        // Wait until remote signals completion
        final var completion = MemorySegment.allocateNative(Byte.BYTES, scope);


        var request = worker.receiveTagged(completion, Tag.of(0L), new RequestParameters()
                .setReceiveCallback(barrier::release));

        Requests.await(worker, barrier);
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
