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
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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

    public void put(final String key, final byte[] value) throws NoSuchAlgorithmException, InterruptedException, CloseException, ControlException, DuplicateKeyException {
        try (resources) {
            NativeLogger.enable();
            log.info("Starting PUT operation");
            log.info("Using UCX version {}", Context.getVersion());
            initialize(key);
            putOperation(key, value, context);
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
        try (resources) {
            NativeLogger.enable();
            log.info("Starting GET operation");
            log.info("Using UCX version {}", Context.getVersion());
            initialize(key);
            return getOperation(key);
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
        }
    }

    public void del(final String key) throws CloseException, NotFoundException, ControlException, InterruptedException, NoSuchAlgorithmException {
        try (resources) {
            NativeLogger.enable();
            log.info("Starting DEL operation");
            log.info("Using UCX version {}", Context.getVersion());
            initialize(key);
            delOperation(key);
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
        }
    }

    public void putOperation(final String key, final byte[] value, final Context context) throws SerializationException, ControlException, DuplicateKeyException {
        final ArrayList<Long> requests = new ArrayList<>();

        requests.add(prepareToSendData(serialize("PUT"), 0L, endpoint));
        requests.addAll(prepareToSendKey(key, endpoint));
        requests.add(prepareToSendRemoteKey(value, endpoint, context));

        sendData(requests, worker);

        final String statusCode = SerializationUtils.deserialize(receiveData(10, 0L, worker));
        log.info("Received status code: \"{}\"", statusCode);

        if ("409".equals(statusCode)) {
            throw new DuplicateKeyException("An object with that key was already in the plasma store");
        }

        log.info("Put completed\n");
    }

    private byte[] getOperation(final String key) throws ControlException, NotFoundException {
        final ArrayList<Long> requests = new ArrayList<>();

        requests.add(prepareToSendData(serialize("GET"), 0L, endpoint));
        requests.addAll(prepareToSendKey(key, endpoint));

        sendData(requests, worker);

        final String statusCode = SerializationUtils.deserialize(receiveData(10, 0L, worker));
        log.info("Received status code: \"{}\"", statusCode);

        byte[] result = new byte[0];
        if ("200".equals(statusCode)) {
            result = receiveRemoteObject(endpoint, worker);
            log.info("Read \"{}\" from remote buffer", SerializationUtils.deserialize(result).toString());
            sendSingleMessage(serialize("200"), 0L, endpoint, worker);

        } else if ("404".equals(statusCode)) {
            throw new NotFoundException("An object with the key \"" + key + "\" was not found by the server.");
        }
        log.info("Get completed\n");
        return result;
    }

    private void delOperation(String key) throws ControlException, NotFoundException {
        final ArrayList<Long> requests = new ArrayList<>();

        requests.add(prepareToSendData(serialize("DEL"), 0L, endpoint));
        requests.addAll(prepareToSendKey(key, endpoint));

        sendData(requests, worker);

        final String statusCode = SerializationUtils.deserialize(receiveData(10, 0L, worker));
        log.info("Received status code: \"{}\"", statusCode);

        if ("404".equals(statusCode)) {
            throw new NotFoundException("An object with the key \"" + key + "\" was not found by the server.");
        }

        log.info("Del completed\n");
    }

    private void initialize(final String key) throws ControlException, InterruptedException, NoSuchAlgorithmException {
        // Create context parameters
        final ContextParameters contextParameters = new ContextParameters().setFeatures(FEATURE_SET).setRequestSize(DEFAULT_REQUEST_SIZE);

        // Read configuration (Environment Variables)
        final Configuration configuration = pushResource(Configuration.read());
        log.info("Initializing context");

        // Initialize UCP context
        this.context = pushResource(Context.initialize(contextParameters, configuration));
        final WorkerParameters workerParameters = new WorkerParameters().setThreadMode(ThreadMode.SINGLE);
        log.info("Creating worker");

        // Create a worker
        this.worker = pushResource(context.createWorker(workerParameters));
        final Integer responsibleServerID = getResponsibleServerID(key, this.serverMap.size());
        final EndpointParameters endpointParams = new EndpointParameters().setRemoteAddress(this.serverMap.get(responsibleServerID));
        log.info("Connecting to {}", this.serverMap.get(responsibleServerID));
        this.endpoint = worker.createEndpoint(endpointParams);
    }

    protected <T extends AutoCloseable> T pushResource(final T resource) {
        resources.push(resource);
        return resource;
    }
}
