package de.hhu.edu.heinestore.benchmark.base;

import de.hhu.edu.heinestore.benchmark.util.InetSocketAddressConverter;
import de.hhu.edu.heinestore.common.exception.KeyNotFoundException;
import de.hhu.edu.heinestore.common.exception.NetworkException;
import site.ycsb.*;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public abstract class KeyValueStore extends DB {

    private static final String NAMESPACE_SEPARATOR = ".";

    public static final String ADDRESS_KEY = "de.hhu.edu.heinestore.benchmark.server";

    @Override
    public void init() throws DBException {
        var address = getProperties().getProperty(ADDRESS_KEY);
        if (address == null) {
            throw new RuntimeException("The server address is not set. Please set it using the " + ADDRESS_KEY + " property.");
        }

        try {
            initialize(InetSocketAddressConverter.from(address));
        } catch (NetworkException e) {
            throw new DBException("Initializing client connection failed.", e);
        }
    }

    /**
     * Initializes this binding instance and connects to the remote server.
     *
     * @param serverAddress The server instance's address.
     * @throws NetworkException If the network connection fails.
     */
    public abstract void initialize(InetSocketAddress serverAddress) throws NetworkException;

    /**
     * Retrieves an object.
     *
     * @param key The key, under which the object is stored.
     *
     * @return The object.
     * @throws NetworkException If the network connection fails.
     * @throws KeyNotFoundException If the specified key does not exist.
     */
    public abstract byte[] get(String key) throws NetworkException, KeyNotFoundException;

    /**
     * Stores an object.
     *
     * @param key The key, under which the object shall be stored.
     * @param value The object to store.
     *
     * @return A YCSB {@link Status} code.
     * @throws NetworkException If the network connection fails.
     */
    public abstract Status put(String key, byte[] value) throws NetworkException;

    /**
     * Deletes the given key.
     *
     * @param key The key to be deleted.
     *
     * @return A YCSB {@link Status} code.
     * @throws NetworkException If the network connection fails.
     * @throws KeyNotFoundException If the specified key does not exist.
     */
    public abstract Status delete(String key) throws NetworkException, KeyNotFoundException;

    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        if (fields != null && fields.size() != 1) {
            System.err.println("Field counts other than 1 are not supported!");
            return Status.BAD_REQUEST;
        }

        try {
            var value = get(generateKey(table, key));

            if (fields == null) {
                result.put("field0", new ByteArrayByteIterator(value));
            } else {
                result.put(fields.iterator().next(), new ByteArrayByteIterator(value));
            }

            return Status.OK;
        } catch (NetworkException e) {
            return Status.SERVICE_UNAVAILABLE;
        } catch (KeyNotFoundException e) {
            return Status.NOT_FOUND;
        }
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                       Vector<HashMap<String, ByteIterator>> result) {
        return Status.NOT_IMPLEMENTED;
    }

    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
        if (values.size() != 1) {
            System.err.println("Field counts other than 1 are not supported!");
            return Status.BAD_REQUEST;
        }

        try {
            return put(generateKey(table, key), values.values().iterator().next().toArray());
        } catch (NetworkException e) {
            return Status.SERVICE_UNAVAILABLE;
        }
    }

    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        if (values.size() != 1) {
            System.err.println("Field counts other than 1 are not supported!");
            return Status.BAD_REQUEST;
        }

        return update(table, key, values);
    }

    @Override
    public Status delete(String table, String key) {
        try {
            return delete(generateKey(table, key));
        } catch (NetworkException e) {
            return Status.SERVICE_UNAVAILABLE;
        } catch (KeyNotFoundException e) {
            return Status.NOT_FOUND;
        }
    }

    private static String generateKey(String table, String key) {
        return table.concat(NAMESPACE_SEPARATOR).concat(key);
    }
}

