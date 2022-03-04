package main;

import client.InfinimumDBClient;
import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.util.CloseException;
import exceptions.DuplicateKeyException;
import exceptions.NotFoundException;
import lombok.extern.slf4j.Slf4j;

import static org.apache.commons.lang3.SerializationUtils.serialize;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Slf4j
public final class Application {

    private static final String serverHostAddress = "localhost";
    private static final Integer serverPort = 2998;

    public static void main(final String... args) throws CloseException, NotFoundException, ControlException, DuplicateKeyException {
        testCanPutAndGetObject();
        testTwoKeyValuesWork();
        testTwoKeysWithCollidingHash();
        testThreeKeysWithCollidingHash();
        testThreeKeysWithCollidingHashDeletingInOrder1();
        testThreeKeysWithCollidingHashDeletingInOrder2();
        testThreeKeysWithCollidingHashDeletingInOrder3();
        testPutKeyThatAlreadyExistsFails();
        test1000KeyValues();
    }

    private static void testCanPutAndGetObject() throws CloseException, ControlException, NotFoundException, DuplicateKeyException {
        log.debug("Start testCanPutAndGetObject:");
        InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        String key = "This is a key";
        byte[] value = serialize("This is a value");

        client.put(key, value);
        byte[] response = client.get(key);

        assertArrayEquals(value, response);

        client.del(key);
        log.debug("End testCanPutAndGetObject:");
    }

    private static void testTwoKeyValuesWork() throws CloseException, ControlException, NotFoundException, DuplicateKeyException {
        log.debug("Start testTwoKeyValuesWork:");
        InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        String key = "This is a key";
        byte[] value = serialize("This is a value");
        String key2 = "This is a key2";
        byte[] value2 = serialize("This is a value2");

        client.put(key, value);
        client.put(key2, value2);
        byte[] response = client.get(key);
        byte[] response2 = client.get(key2);

        assertArrayEquals(value, response);
        assertArrayEquals(value2, response2);

        client.del(key);
        client.del(key2);
        log.debug("End testTwoKeyValuesWork:");
    }

    private static void testTwoKeysWithCollidingHash() throws CloseException, ControlException, NotFoundException, DuplicateKeyException {
        log.debug("Start testTwoKeysWithCollidingHash:");
        InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        String key = "hash_collision_test_1";
        byte[] value = serialize("This is a value");
        String key2 = "hash_collision_test_2";
        byte[] value2 = serialize("This is a value2");

        client.put(key, value);
        client.put(key2, value2);
        byte[] response = client.get(key);
        byte[] response2 = client.get(key2);

        assertArrayEquals(value, response);
        assertArrayEquals(value2, response2);

        client.del(key);
        client.del(key2);
        log.debug("End testTwoKeysWithCollidingHashCanBePut:");
    }

    private static void testThreeKeysWithCollidingHash() throws CloseException, ControlException, NotFoundException, DuplicateKeyException {
        log.debug("Start testThreeKeysWithCollidingHash:");
        InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        String key = "hash_collision_test_1";
        byte[] value = serialize("This is a value");
        String key2 = "hash_collision_test_2";
        byte[] value2 = serialize("This is a value2");
        String key3 = "hash_collision_test_3";
        byte[] value3 = serialize("This is a value3");

        client.put(key, value);
        client.put(key2, value2);
        client.put(key3, value3);
        byte[] response = client.get(key);
        byte[] response2 = client.get(key2);
        byte[] response3 = client.get(key3);

        assertArrayEquals(value, response);
        assertArrayEquals(value2, response2);
        assertArrayEquals(value3, response3);

        client.del(key);
        client.del(key2);
        client.del(key3);
        log.debug("End testTwoKeysWithCollidingHashCanBePut:");
    }

    private static void testThreeKeysWithCollidingHashDeletingInOrder1() throws CloseException, ControlException, NotFoundException, DuplicateKeyException {
        log.debug("Start testThreeKeysWithCollidingHash:");
        InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        String key = "hash_collision_test_1";
        byte[] value = serialize("This is a value");
        String key2 = "hash_collision_test_2";
        byte[] value2 = serialize("This is a value2");
        String key3 = "hash_collision_test_3";
        byte[] value3 = serialize("This is a value3");

        client.put(key, value);
        client.put(key2, value2);
        client.put(key3, value3);

        client.del(key);

        byte[] response2 = client.get(key2);
        byte[] response3 = client.get(key3);
        assertArrayEquals(value2, response2);
        assertArrayEquals(value3, response3);

        client.del(key2);

        response3 = client.get(key3);
        assertArrayEquals(value3, response3);

        client.del(key3);
        log.debug("End testTwoKeysWithCollidingHashCanBePut:");
    }

    private static void testThreeKeysWithCollidingHashDeletingInOrder2() throws CloseException, ControlException, NotFoundException, DuplicateKeyException {
        log.debug("Start testThreeKeysWithCollidingHashDeletingInOrder2:");
        InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        String key = "hash_collision_test_1";
        byte[] value = serialize("This is a value");
        String key2 = "hash_collision_test_2";
        byte[] value2 = serialize("This is a value2");
        String key3 = "hash_collision_test_3";
        byte[] value3 = serialize("This is a value3");

        client.put(key, value);
        client.put(key2, value2);
        client.put(key3, value3);

        client.del(key3);

        byte[] response = client.get(key);
        byte[] response2 = client.get(key2);
        assertArrayEquals(value, response);
        assertArrayEquals(value2, response2);

        client.del(key2);

        response = client.get(key);
        assertArrayEquals(value, response);

        client.del(key);
        log.debug("End testThreeKeysWithCollidingHashDeletingInOrder2:");
    }

    private static void testThreeKeysWithCollidingHashDeletingInOrder3() throws CloseException, ControlException, NotFoundException, DuplicateKeyException {
        log.debug("Start testThreeKeysWithCollidingHashDeletingInOrder3:");
        InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        String key = "hash_collision_test_1";
        byte[] value = serialize("This is a value");
        String key2 = "hash_collision_test_2";
        byte[] value2 = serialize("This is a value2");
        String key3 = "hash_collision_test_3";
        byte[] value3 = serialize("This is a value3");

        client.put(key, value);
        client.put(key2, value2);
        client.put(key3, value3);

        client.del(key2);

        byte[] response = client.get(key);
        byte[] response3 = client.get(key3);
        assertArrayEquals(value, response);
        assertArrayEquals(value3, response3);

        client.del(key3);

        response = client.get(key);
        assertArrayEquals(value, response);

        client.del(key);
        log.debug("End testThreeKeysWithCollidingHashDeletingInOrder3:");
    }

    private static void testPutKeyThatAlreadyExistsFails() throws CloseException, ControlException, DuplicateKeyException, NotFoundException {
        log.debug("Start testPutKeyThatAlreadyExistsFails:");
        InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        String key = "hash_collision_test_1";
        byte[] value = serialize("This is a value");
        String key2 = "hash_collision_test_2";
        byte[] value2 = serialize("This is a value2");
        byte[] value3 = serialize("This is a value3");

        client.put(key, value);
        client.put(key2, value2);
        assertThrows(DuplicateKeyException.class, () -> client.put(key2, value3));

        client.del(key);
        client.del(key2);
        log.debug("End testPutKeyThatAlreadyExistsFails:");
    }

    private static void test1000KeyValues() throws CloseException, NotFoundException, ControlException, DuplicateKeyException {
        testCanPut1000Times();
        testCanGet1000Times();
        testCanDelete1000Times();
    }

    private static void testCanPut1000Times() throws CloseException, ControlException, DuplicateKeyException {
        log.debug("Start testCanPut1000Times:");
        InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);

        for (int i = 0; i < 1000; i++) {
            String key = "This is a key" + i;
            byte[] value = serialize("This is a value" + i);
            client.put(key, value);
        }
        log.debug("End testCanPut1000Times:");
    }

    private static void testCanGet1000Times() throws CloseException, ControlException, NotFoundException {
        log.debug("Start testCanGet1000Times:");
        InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);

        for (int i = 0; i < 1000; i++) {
            String key = "This is a key" + i;
            byte[] value = serialize("This is a value" + i);
            byte[] response = client.get(key);
            assertArrayEquals(value, response);
        }
        log.debug("End testCanGet1000Times:");
    }

    private static void testCanDelete1000Times() throws CloseException, ControlException, NotFoundException {
        log.debug("Start testCanDelete1000Times:");
        InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);

        for (int i = 0; i < 1000; i++) {
            String key = "This is a key" + i;
            client.del(key);
        }
        log.debug("End testCanDelete1000Times:");
    }
}
