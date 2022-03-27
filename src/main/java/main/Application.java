package main;

import client.InfinimumDBClient;
import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.util.CloseException;
import exceptions.DuplicateKeyException;
import exceptions.NotFoundException;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.commons.lang3.SerializationUtils.serialize;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Slf4j
public final class Application {

    private static final String serverHostAddress = "localhost";
    private static final Integer serverPort = 2998;
    private static final Integer timeoutMs = 500;
    private static final Integer putAttempts = 5;
    private static final Integer getAttempts = 5;
    private static final Integer delAttempts = 5;

    public static void main(final String... args) throws CloseException, NotFoundException, ControlException, DuplicateKeyException, TimeoutException {
        testCanPutAndGetObject();
        testTwoKeyValuesWork();
        testTwoKeysWithCollidingHash();
        testThreeKeysWithCollidingHash();
        testThreeKeysWithCollidingHashDeletingInOrder1();
        testThreeKeysWithCollidingHashDeletingInOrder2();
        testThreeKeysWithCollidingHashDeletingInOrder3();
        testPutKeyThatAlreadyExistsFails();
        timeoutTest();
        testCanPutMultipleTimes(10000, 0);
        testMultipleConnections();
    }

    private static void testCanPutAndGetObject() throws CloseException, ControlException, NotFoundException, DuplicateKeyException, TimeoutException {
        log.debug("Start testCanPutAndGetObject:");
        final InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        final String key = "This is a key";
        final byte[] value = serialize("This is a value");

        client.put(key, value, timeoutMs, putAttempts);
        final byte[] response = client.get(key, timeoutMs, getAttempts);

        assertArrayEquals(value, response);

        client.del(key, timeoutMs, delAttempts);
        log.debug("End testCanPutAndGetObject:");
    }

    private static void testTwoKeyValuesWork() throws CloseException, ControlException, NotFoundException, DuplicateKeyException, TimeoutException {
        log.debug("Start testTwoKeyValuesWork:");
        final InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        final String key = "This is a key";
        final byte[] value = serialize("This is a value");
        final String key2 = "This is a key2";
        final byte[] value2 = serialize("This is a value2");

        client.put(key, value, timeoutMs, putAttempts);
        client.put(key2, value2, timeoutMs, putAttempts);
        final byte[] response = client.get(key, timeoutMs, getAttempts);
        final byte[] response2 = client.get(key2, timeoutMs, getAttempts);

        assertArrayEquals(value, response);
        assertArrayEquals(value2, response2);

        client.del(key, timeoutMs, delAttempts);
        client.del(key2, timeoutMs, delAttempts);
        log.debug("End testTwoKeyValuesWork:");
    }

    private static void testTwoKeysWithCollidingHash() throws CloseException, ControlException, NotFoundException, DuplicateKeyException, TimeoutException {
        log.debug("Start testTwoKeysWithCollidingHash:");
        final InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        final String key = "hash_collision_test_1";
        final byte[] value = serialize("This is a value");
        final String key2 = "hash_collision_test_2";
        final byte[] value2 = serialize("This is a value2");

        client.put(key, value, timeoutMs, putAttempts);
        client.put(key2, value2, timeoutMs, putAttempts);
        final byte[] response = client.get(key, timeoutMs, getAttempts);
        final byte[] response2 = client.get(key2, timeoutMs, getAttempts);

        assertArrayEquals(value, response);
        assertArrayEquals(value2, response2);

        client.del(key, timeoutMs, delAttempts);
        client.del(key2, timeoutMs, delAttempts);
        log.debug("End testTwoKeysWithCollidingHashCanBePut:");
    }

    private static void testThreeKeysWithCollidingHash() throws CloseException, ControlException, NotFoundException, DuplicateKeyException, TimeoutException {
        log.debug("Start testThreeKeysWithCollidingHash:");
        final InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        final String key = "hash_collision_test_1";
        final byte[] value = serialize("This is a value");
        final String key2 = "hash_collision_test_2";
        final byte[] value2 = serialize("This is a value2");
        final String key3 = "hash_collision_test_3";
        final byte[] value3 = serialize("This is a value3");

        client.put(key, value, timeoutMs, putAttempts);
        client.put(key2, value2, timeoutMs, putAttempts);
        client.put(key3, value3, timeoutMs, putAttempts);
        final byte[] response = client.get(key, timeoutMs, getAttempts);
        final byte[] response2 = client.get(key2, timeoutMs, getAttempts);
        final byte[] response3 = client.get(key3, timeoutMs, getAttempts);

        assertArrayEquals(value, response);
        assertArrayEquals(value2, response2);
        assertArrayEquals(value3, response3);

        client.del(key, timeoutMs, delAttempts);
        client.del(key2, timeoutMs, delAttempts);
        client.del(key3, timeoutMs, delAttempts);
        log.debug("End testTwoKeysWithCollidingHashCanBePut:");
    }

    private static void testThreeKeysWithCollidingHashDeletingInOrder1() throws CloseException, ControlException, NotFoundException, DuplicateKeyException, TimeoutException {
        log.debug("Start testThreeKeysWithCollidingHash:");
        final InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        final String key = "hash_collision_test_1";
        final byte[] value = serialize("This is a value");
        final String key2 = "hash_collision_test_2";
        final byte[] value2 = serialize("This is a value2");
        final String key3 = "hash_collision_test_3";
        final byte[] value3 = serialize("This is a value3");

        client.put(key, value, timeoutMs, putAttempts);
        client.put(key2, value2, timeoutMs, putAttempts);
        client.put(key3, value3, timeoutMs, putAttempts);

        client.del(key, timeoutMs, delAttempts);

        final byte[] response2 = client.get(key2, timeoutMs, getAttempts);
        byte[] response3 = client.get(key3, timeoutMs, getAttempts);
        assertArrayEquals(value2, response2);
        assertArrayEquals(value3, response3);

        client.del(key2, timeoutMs, delAttempts);

        response3 = client.get(key3, timeoutMs, getAttempts);
        assertArrayEquals(value3, response3);

        client.del(key3, timeoutMs, delAttempts);
        log.debug("End testTwoKeysWithCollidingHashCanBePut:");
    }

    private static void testThreeKeysWithCollidingHashDeletingInOrder2() throws CloseException, ControlException, NotFoundException, DuplicateKeyException, TimeoutException {
        log.debug("Start testThreeKeysWithCollidingHashDeletingInOrder2:");
        final InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        final String key = "hash_collision_test_1";
        final byte[] value = serialize("This is a value");
        final String key2 = "hash_collision_test_2";
        final byte[] value2 = serialize("This is a value2");
        final String key3 = "hash_collision_test_3";
        final byte[] value3 = serialize("This is a value3");

        client.put(key, value, timeoutMs, putAttempts);
        client.put(key2, value2, timeoutMs, putAttempts);
        client.put(key3, value3, timeoutMs, putAttempts);

        client.del(key3, timeoutMs, delAttempts);

        byte[] response = client.get(key, timeoutMs, getAttempts);
        final byte[] response2 = client.get(key2, timeoutMs, getAttempts);
        assertArrayEquals(value, response);
        assertArrayEquals(value2, response2);

        client.del(key2, timeoutMs, delAttempts);

        response = client.get(key, timeoutMs, getAttempts);
        assertArrayEquals(value, response);

        client.del(key, timeoutMs, delAttempts);
        log.debug("End testThreeKeysWithCollidingHashDeletingInOrder2:");
    }

    private static void testThreeKeysWithCollidingHashDeletingInOrder3() throws CloseException, ControlException, NotFoundException, DuplicateKeyException, TimeoutException {
        log.debug("Start testThreeKeysWithCollidingHashDeletingInOrder3:");
        final InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        final String key = "hash_collision_test_1";
        final byte[] value = serialize("This is a value");
        final String key2 = "hash_collision_test_2";
        final byte[] value2 = serialize("This is a value2");
        final String key3 = "hash_collision_test_3";
        final byte[] value3 = serialize("This is a value3");

        client.put(key, value, timeoutMs, putAttempts);
        client.put(key2, value2, timeoutMs, putAttempts);
        client.put(key3, value3, timeoutMs, putAttempts);

        client.del(key2, timeoutMs, delAttempts);

        byte[] response = client.get(key, timeoutMs, getAttempts);
        final byte[] response3 = client.get(key3, timeoutMs, getAttempts);
        assertArrayEquals(value, response);
        assertArrayEquals(value3, response3);

        client.del(key3, timeoutMs, delAttempts);

        response = client.get(key, timeoutMs, getAttempts);
        assertArrayEquals(value, response);

        client.del(key, timeoutMs, delAttempts);
        log.debug("End testThreeKeysWithCollidingHashDeletingInOrder3:");
    }

    private static void testPutKeyThatAlreadyExistsFails() throws CloseException, ControlException, DuplicateKeyException, NotFoundException, TimeoutException {
        log.debug("Start testPutKeyThatAlreadyExistsFails:");
        final InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        final String key = "hash_collision_test_1";
        final byte[] value = serialize("This is a value");
        final String key2 = "hash_collision_test_2";
        final byte[] value2 = serialize("This is a value2");
        final byte[] value3 = serialize("This is a value3");

        client.put(key, value, timeoutMs, putAttempts);
        client.put(key2, value2, timeoutMs, putAttempts);
        assertThrows(DuplicateKeyException.class, () -> client.put(key2, value3, timeoutMs, putAttempts));

        client.del(key, timeoutMs, delAttempts);
        client.del(key2, timeoutMs, delAttempts);
        log.debug("End testPutKeyThatAlreadyExistsFails:");
    }

    private static void timeoutTest() {
        log.debug("Start timeoutTest:");
        final InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        final String key = "timeout_test";
        final byte[] value = serialize("This is a value");

        assertThrows(TimeoutException.class, () -> client.put(key, value, 100, putAttempts));

        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (final InterruptedException e) {
            log.error(e.getMessage());
        }

        assertThrows(NotFoundException.class, () -> client.del(key, timeoutMs + 2000, delAttempts));
        log.debug("End timeoutTest:");
    }

    private static void testMultipleKeyValues(final int count, final int startIndex) throws CloseException, NotFoundException, ControlException, DuplicateKeyException, ExecutionException, InterruptedException, TimeoutException {
        testCanPutMultipleTimes(count, startIndex);
        testCanGetMultipleTimes(count, startIndex);
        testCanDeleteMultipleTimes(count, startIndex);
    }

    private static void testCanPutMultipleTimes(final int count, final int startIndex) throws CloseException, ControlException, DuplicateKeyException, TimeoutException {
        log.debug("Start testCanPutMultipleTimes:");
        final InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);

        for (int i = startIndex; i < count; i++) {
            final String key = "This is a key" + i;
            final byte[] value = serialize("This is a value" + i);
            client.put(key, value, timeoutMs, putAttempts);
        }
        log.debug("End testCanPutMultipleTimes:");
    }

    private static void testCanGetMultipleTimes(final int count, final int startIndex) throws CloseException, ControlException, NotFoundException, TimeoutException {
        log.debug("Start testCanGetMultipleTimes:");
        final InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);

        for (int j = 0; j < 10; j++) {
            for (int i = startIndex; i < count; i++) {
                final String key = "This is a key" + i;
                final byte[] value = serialize("This is a value" + i);
                final byte[] response = client.get(key, timeoutMs, getAttempts);
                assertArrayEquals(value, response);
            }
        }
        log.debug("End testCanGetMultipleTimes:");
    }

    private static void testCanDeleteMultipleTimes(final int count, final int startIndex) throws CloseException, ControlException, NotFoundException, TimeoutException, DuplicateKeyException {
        log.debug("Start testCanDelete1000Times:");
        final InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        for (int j = 0; j < 10; j++) {
            testCanPutMultipleTimes(0, startIndex);
            for (int i = startIndex; i < count; i++) {
                final String key = "This is a key" + i;
                client.del(key, timeoutMs, delAttempts);
            }
        }

        log.debug("End testCanDelete1000Times:");
    }

    private static void testMultipleConnections() {
        log.debug("Start testMultipleConnections:");

        final Thread thread1 = new Thread(() -> {
            try {
                testMultipleKeyValues(10000, 10000);
            } catch (final CloseException | NotFoundException | ControlException | DuplicateKeyException | ExecutionException | InterruptedException | TimeoutException e) {
                e.printStackTrace();
            }
        });

        final Thread thread2 = new Thread(() -> {
            try {
                testMultipleKeyValues(10000, 20000);
            } catch (final CloseException | NotFoundException | ControlException | DuplicateKeyException | ExecutionException | InterruptedException | TimeoutException e) {
                e.printStackTrace();
            }
        });

        thread1.start();
        thread2.start();

        log.debug("End testMultipleConnections:");
    }
}
