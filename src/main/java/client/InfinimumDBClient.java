package client;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.util.CloseException;
import de.hhu.bsinfo.infinileap.util.ResourcePool;
import exceptions.DuplicateKeyException;
import exceptions.NotFoundException;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import lombok.extern.slf4j.Slf4j;
import model.PlasmaEntry;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import utils.HashUtils;

import java.lang.ref.Cleaner;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
        } catch (Exception e) {
            log.error(e.getMessage());
            throw e;
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

    public void putOperation(final String key, final byte[] value, final Context context, final int timeoutMs) throws SerializationException, ControlException, DuplicateKeyException, TimeoutException, CloseException {
        log.info("Starting PUT operation");
        final ArrayList<Long> requests = new ArrayList<>();

        PlasmaEntry entry = new PlasmaEntry(key, value, new byte[20]);
        final byte[] entryBytes;
        try {
            entryBytes = serialize(entry);
        } catch (SerializationException e) {
            log.error(e.getMessage());
            throw e;
        }
        final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES).putInt(entryBytes.length);
        final byte[] entrySizeBytes = byteBuffer.array();

        try (final ResourceScope scope = ResourceScope.newConfinedScope(Cleaner.create())) {
            requests.add(prepareToSendData(serialize("PUT"), endpoint, scope));
            requests.addAll(prepareToSendKey(key, endpoint, scope));
            requests.add(prepareToSendData(entrySizeBytes, endpoint, scope));
            sendData(requests, worker, timeoutMs);
        }

        final String statusCode = SerializationUtils.deserialize(receiveData(10, worker, timeoutMs));
        log.info("Received status code: \"{}\"", statusCode);

        if ("200".equals(statusCode)) {
            log.info("Receiving Remote Key");
            try (final ResourceScope scope = ResourceScope.newConfinedScope(Cleaner.create())) {
                final MemoryDescriptor descriptor = new MemoryDescriptor(scope);
                final long request = worker.receiveTagged(descriptor, Tag.of(0L), new RequestParameters(scope));
                awaitRequestIfNecessary(request, worker, timeoutMs);

                log.info(String.valueOf(descriptor.remoteSize()));
                final MemorySegment sourceBuffer = MemorySegment.allocateNative(entryBytes.length, scope);
                sourceBuffer.asByteBuffer().put(entryBytes);
                try (final RemoteKey remoteKey = endpoint.unpack(descriptor)) {
                    log.info(remoteKey.toString());
                    final long request2 = endpoint.put(sourceBuffer, descriptor.remoteAddress(), remoteKey);
                    awaitRequestIfNecessary(request2, worker, timeoutMs);
                }
                sendSingleMessage(serialize("200"), endpoint, worker, timeoutMs);
            }
        } else if ("409".equals(statusCode)) {
            throw new DuplicateKeyException("An object with that key was already in the plasma store");
        }
        log.info("Put completed\n");
    }

    private PlasmaEntry plasmaEntryOf(String key, byte[] value, byte[] idTailEndBytes) {
        return new PlasmaEntry(key, value, HashUtils.generateID(key, idTailEndBytes));
    }

    private byte[] getOperation(final String key, final int timeoutMs) throws ControlException, NotFoundException, TimeoutException {
        log.info("Starting GET operation");
        final ArrayList<Long> requests = new ArrayList<>();

        try (final ResourceScope scope = ResourceScope.newConfinedScope(Cleaner.create())) {
            requests.add(prepareToSendData(serialize("GET"), endpoint, scope));
            requests.addAll(prepareToSendKey(key, endpoint, scope));

            sendData(requests, worker, timeoutMs);
        }

        final String statusCode = SerializationUtils.deserialize(receiveData(10, worker, timeoutMs));
        log.info("Received status code: \"{}\"", statusCode);

        byte[] value = new byte[0];
        if ("200".equals(statusCode)) {
            value = receiveValue(endpoint, worker, timeoutMs);
            sendSingleMessage(serialize("200"), endpoint, worker, timeoutMs);

        } else if ("404".equals(statusCode)) {
            throw new NotFoundException("An object with the key \"" + key + "\" was not found by the server.");
        }
        log.info("Get completed\n");
        return value;
    }

    private void delOperation(final String key, final int timeoutMs) throws NotFoundException, TimeoutException {
        log.info("Starting DEL operation");
        final ArrayList<Long> requests = new ArrayList<>();

        try (final ResourceScope scope = ResourceScope.newConfinedScope(Cleaner.create())) {
            requests.add(prepareToSendData(serialize("DEL"), endpoint, scope));
            requests.addAll(prepareToSendKey(key, endpoint, scope));

            sendData(requests, worker, timeoutMs);
        }

        final String statusCode = SerializationUtils.deserialize(receiveData(10, worker, timeoutMs));
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
