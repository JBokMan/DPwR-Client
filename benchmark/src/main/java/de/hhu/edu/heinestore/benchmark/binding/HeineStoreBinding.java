package de.hhu.edu.heinestore.benchmark.binding;

import de.hhu.edu.heinestore.benchmark.base.KeyValueStore;
import de.hhu.edu.heinestore.client.base.KeyValueClient;
import de.hhu.edu.heinestore.common.exception.KeyNotFoundException;
import de.hhu.edu.heinestore.common.exception.NetworkException;
import site.ycsb.Status;

import java.net.InetSocketAddress;

public class HeineStoreBinding extends KeyValueStore {

    /**
     * The client instance used for all operations.
     */
    private final KeyValueClient client = KeyValueClient.newInstance();

    /**
     * Initializes this binding instance and connects to the remote server.
     *
     * @param serverAddress The server instance's address.
     */
    @Override
    public void initialize(InetSocketAddress serverAddress) throws NetworkException {
        throw new UnsupportedOperationException("Not implemented.");
    }

    /**
     * Retrieves an object.
     *
     * @param key The key, under which the object is stored.
     *
     * @return The object.
     * @throws NetworkException If the network connection fails.
     * @throws KeyNotFoundException If the specified key does not exist.
     */
    @Override
    public byte[] get(String key) throws NetworkException, KeyNotFoundException {
        throw new UnsupportedOperationException("Not implemented.");
    }

    /**
     * Stores an object.
     *
     * @param key The key, under which the object shall be stored.
     * @param value The object to store.
     *
     * @return A YCSB {@link Status} code.
     * @throws NetworkException If the network connection fails.
     */
    @Override
    public Status put(String key, byte[] value) throws NetworkException {
        throw new UnsupportedOperationException("Not implemented.");
    }

    /**
     * Deletes the given key.
     *
     * @param key The key to be deleted.
     *
     * @return A YCSB {@link Status} code.
     * @throws NetworkException If the network connection fails.
     * @throws KeyNotFoundException If the specified key does not exist.
     */
    @Override
    public Status delete(String key) throws NetworkException, KeyNotFoundException {
        throw new UnsupportedOperationException("Not implemented.");
    }
}
