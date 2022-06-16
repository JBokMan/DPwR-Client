package binding;

import base.KeyValueStore;
import client.DPwRClient;
import exceptions.DuplicateKeyException;
import exceptions.KeyNotFoundException;
import exceptions.NetworkException;
import site.ycsb.Status;

import java.net.InetSocketAddress;

public class DPwRStoreBinding extends KeyValueStore {

    /**
     * The client instance used for all operations.
     */
    private final DPwRClient client = new DPwRClient();

    /**
     * Initializes this binding instance and connects to the remote server.
     *
     * @param serverAddress The server instance's address.
     */
    @Override
    public void initialize(final InetSocketAddress serverAddress) throws NetworkException {
        client.setServerAddress(serverAddress);
        client.setServerTimeout(500);
        client.setVerbose(false);
        client.initialize();
    }

    /**
     * Retrieves an object.
     *
     * @param key The key, under which the object is stored.
     * @return The object.
     * @throws NetworkException     If the network connection fails.
     * @throws KeyNotFoundException If the specified key does not exist.
     */
    @Override
    public byte[] get(final String key) throws NetworkException, KeyNotFoundException {
        return client.get(key, 5);
    }

    /**
     * Stores an object.
     *
     * @param key   The key, under which the object shall be stored.
     * @param value The object to store.
     * @return A YCSB {@link Status} code.
     * @throws NetworkException If the network connection fails.
     */
    @Override
    public Status put(final String key, final byte[] value) throws NetworkException {
        try {
            client.put(key, value, 5);
        } catch (final DuplicateKeyException e) {
            throw new NetworkException(e.getMessage());
        }
        return new Status("201 Created", "The key was saved successfully");
    }

    /**
     * Deletes the given key.
     *
     * @param key The key to be deleted.
     * @return A YCSB {@link Status} code.
     * @throws NetworkException     If the network connection fails.
     * @throws KeyNotFoundException If the specified key does not exist.
     */
    @Override
    public Status delete(final String key) throws NetworkException, KeyNotFoundException {
        client.del(key, 5);
        return new Status("204 No Content", "Value was successfully deleted");
    }
}
