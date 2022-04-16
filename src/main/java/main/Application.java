package main;

import client.DPwRClient;
import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.util.CloseException;
import exceptions.DuplicateKeyException;
import exceptions.NotFoundException;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static org.apache.commons.lang3.SerializationUtils.serialize;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public final class Application {

    private static final String serverHostAddress = "localhost";
    private static final Integer serverPort = 2998;
    private static final Integer timeoutMs = 500;
    private static final Integer putAttempts = 5;
    private static final Integer getAttempts = 5;
    private static final Integer delAttempts = 5;

    public static void main(final String... args) throws CloseException, ControlException, TimeoutException {
        testCanConnectToServer();
        testSuccessfulPut();
        testUnsuccessfulPut();
        testSuccessfulGet();
        testUnsuccessfulGet();
        testSuccessfulDelete();
        testUnsuccessfulDelete();
        testCanPutGetAndDeleteObject();
        testTwoKeyValues();
        testTwoKeyValuesWithCollidingHash();
        testThreeKeyValuesWithCollidingHash();
        testThreeKeysWithCollidingHashDeletingInOrder1();
        testThreeKeysWithCollidingHashDeletingInOrder2();
        testThreeKeysWithCollidingHashDeletingInOrder3();
        testPutTimeout();
        testGetTimeout();
        testDeleteTimeout();
        testMultipleKeyValues(1000, 0);
        testTwoClientsMultipleKeyValues();
        testThreeClientsMultipleKeyValues();
        testStress();
    }

    private static void testCanConnectToServer() {
        log.debug("Start testCanConnectToServer:");
        assertDoesNotThrow(() -> new DPwRClient(serverHostAddress, serverPort));
        log.debug("End testCanConnectToServer:");
    }

    private static void testSuccessfulPut() throws CloseException, ControlException, TimeoutException {
        log.debug("Start testSuccessfulPut:");
        final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
        final String key = "This is a key";
        final byte[] value = serialize("This is a value");

        assertDoesNotThrow(() -> client.put(key, value, timeoutMs, putAttempts));

        log.debug("End testSuccessfulPut:");
    }

    private static void testUnsuccessfulPut() throws CloseException, ControlException, TimeoutException {
        log.debug("Start testUnsuccessfulPut:");
        final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
        final String key = "This is a key";
        final byte[] value = serialize("This is a value");

        assertThrows(DuplicateKeyException.class, () -> client.put(key, value, timeoutMs, putAttempts));

        log.debug("End testUnsuccessfulPut:");
    }

    private static void testSuccessfulGet() throws CloseException, ControlException, TimeoutException {
        log.debug("Start testSuccessfulGet:");
        final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
        final String key = "This is a key";
        final byte[] value = serialize("This is a value");

        assertDoesNotThrow(() -> {
            final byte[] response = client.get(key, timeoutMs, getAttempts);
            assertArrayEquals(value, response);
        });

        log.debug("End testSuccessfulGet:");
    }

    private static void testUnsuccessfulGet() throws CloseException, ControlException, TimeoutException {
        log.debug("Start testSuccessfulGet:");
        final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
        final String key = "This is a key1";

        assertThrows(NotFoundException.class, () -> client.get(key, timeoutMs, getAttempts));

        log.debug("End testSuccessfulGet:");
    }

    private static void testSuccessfulDelete() throws CloseException, ControlException, TimeoutException {
        log.debug("Start testSuccessfulDelete:");
        final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
        final String key = "This is a key";

        assertDoesNotThrow(() -> client.del(key, timeoutMs, delAttempts));

        log.debug("End testSuccessfulDelete:");
    }

    private static void testUnsuccessfulDelete() throws CloseException, ControlException, TimeoutException {
        log.debug("Start testUnsuccessfulDelete:");
        final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
        final String key = "This is a key";

        assertThrows(NotFoundException.class, () -> client.del(key, timeoutMs, delAttempts));

        log.debug("End testUnsuccessfulDelete:");
    }


    private static void testCanPutGetAndDeleteObject() throws CloseException, ControlException, TimeoutException {
        log.debug("Start testCanPutGetAndDeleteObject:");
        final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
        final String key = "This is a key";
        final byte[] value = serialize("This is a value");

        assertDoesNotThrow(() -> {
            client.put(key, value, timeoutMs, putAttempts);
            final byte[] response = client.get(key, timeoutMs, getAttempts);
            assertArrayEquals(value, response);
            client.del(key, timeoutMs, delAttempts);
        });

        log.debug("End testCanPutGetAndDeleteObject:");
    }

    private static void testTwoKeyValues() throws CloseException, ControlException, TimeoutException {
        log.debug("Start testTwoKeyValues:");
        final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
        final String key = "This is a key";
        final byte[] value = serialize("This is a value");
        final String key2 = "This is a key2";
        final byte[] value2 = serialize("This is a value2");

        assertDoesNotThrow(() -> {
            client.put(key, value, timeoutMs, putAttempts);
            client.put(key2, value2, timeoutMs, putAttempts);
            final byte[] response = client.get(key, timeoutMs, getAttempts);
            final byte[] response2 = client.get(key2, timeoutMs, getAttempts);

            assertArrayEquals(value, response);
            assertArrayEquals(value2, response2);

            client.del(key, timeoutMs, delAttempts);
            client.del(key2, timeoutMs, delAttempts);
        });

        log.debug("End testTwoKeyValues:");
    }

    private static void testTwoKeyValuesWithCollidingHash() throws CloseException, ControlException, TimeoutException {
        log.debug("Start testTwoKeyValuesWithCollidingHash:");
        final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
        final String key = "hash_collision_test_1";
        final byte[] value = serialize("This is a value");
        final String key2 = "hash_collision_test_2";
        final byte[] value2 = serialize("This is a value2");

        assertDoesNotThrow(() -> {
            client.put(key, value, timeoutMs, putAttempts);
            client.put(key2, value2, timeoutMs, putAttempts);
            final byte[] response = client.get(key, timeoutMs, getAttempts);
            final byte[] response2 = client.get(key2, timeoutMs, getAttempts);

            assertArrayEquals(value, response);
            assertArrayEquals(value2, response2);

            client.del(key, timeoutMs, delAttempts);
            client.del(key2, timeoutMs, delAttempts);
        });

        log.debug("End testTwoKeyValuesWithCollidingHash:");
    }

    private static void testThreeKeyValuesWithCollidingHash() throws CloseException, ControlException, TimeoutException {
        log.debug("Start testThreeKeyValuesWithCollidingHash:");
        final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
        final String key = "hash_collision_test_1";
        final byte[] value = serialize("This is a value");
        final String key2 = "hash_collision_test_2";
        final byte[] value2 = serialize("This is a value2");
        final String key3 = "hash_collision_test_3";
        final byte[] value3 = serialize("This is a value3");

        assertDoesNotThrow(() -> {
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
        });

        log.debug("End testThreeKeyValuesWithCollidingHash:");
    }

    private static void testThreeKeysWithCollidingHashDeletingInOrder1() throws CloseException, ControlException, TimeoutException {
        log.debug("Start testThreeKeysWithCollidingHashDeletingInOrder1:");
        final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
        final String key = "hash_collision_test_1";
        final byte[] value = serialize("This is a value");
        final String key2 = "hash_collision_test_2";
        final byte[] value2 = serialize("This is a value2");
        final String key3 = "hash_collision_test_3";
        final byte[] value3 = serialize("This is a value3");

        assertDoesNotThrow(() -> {
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
        });

        log.debug("End testThreeKeysWithCollidingHashDeletingInOrder1:");
    }

    private static void testThreeKeysWithCollidingHashDeletingInOrder2() throws CloseException, ControlException, TimeoutException {
        log.debug("Start testThreeKeysWithCollidingHashDeletingInOrder2:");
        final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
        final String key = "hash_collision_test_1";
        final byte[] value = serialize("This is a value");
        final String key2 = "hash_collision_test_2";
        final byte[] value2 = serialize("This is a value2");
        final String key3 = "hash_collision_test_3";
        final byte[] value3 = serialize("This is a value3");

        assertDoesNotThrow(() -> {
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
        });

        log.debug("End testThreeKeysWithCollidingHashDeletingInOrder2:");
    }

    private static void testThreeKeysWithCollidingHashDeletingInOrder3() throws CloseException, ControlException, TimeoutException {
        log.debug("Start testThreeKeysWithCollidingHashDeletingInOrder3:");
        final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
        final String key = "hash_collision_test_1";
        final byte[] value = serialize("This is a value");
        final String key2 = "hash_collision_test_2";
        final byte[] value2 = serialize("This is a value2");
        final String key3 = "hash_collision_test_3";
        final byte[] value3 = serialize("This is a value3");

        assertDoesNotThrow(() -> {
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
        });

        log.debug("End testThreeKeysWithCollidingHashDeletingInOrder3:");
    }

    private static void testPutTimeout() throws CloseException, ControlException, TimeoutException {
        log.debug("Start testPutTimeout:");
        final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
        final String key = "timeout_test";
        final byte[] value = serialize("This is a value");

        assertThrows(TimeoutException.class, () -> client.put(key, value, 100, putAttempts));

        log.debug("End testPutTimeout:");
    }

    private static void testGetTimeout() throws CloseException, ControlException, TimeoutException {
        log.debug("Start testGetTimeout:");
        final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
        final String key = "timeout_test";

        assertThrows(TimeoutException.class, () -> client.get(key, 100, getAttempts));

        log.debug("End testGetTimeout:");
    }

    private static void testDeleteTimeout() throws CloseException, ControlException, TimeoutException {
        log.debug("Start testDeleteTimeout:");
        final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
        final String key = "timeout_test";

        assertThrows(TimeoutException.class, () -> client.del(key, 100, delAttempts));

        log.debug("End testDeleteTimeout:");
    }

    private static void testMultipleKeyValues(final int count, final int startIndex) throws CloseException, ControlException, TimeoutException {
        testCanPutMultipleTimes(count, startIndex);
        testCanGetMultipleTimes(count, startIndex);
        testCanDeleteMultipleTimes(count, startIndex);
    }

    private static void testCanPutMultipleTimes(final int count, final int startIndex) throws CloseException, ControlException, TimeoutException {
        log.debug("Start testCanPutMultipleTimes:");
        final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);

        for (int i = startIndex; i < startIndex + count; i++) {
            final String key = "This is a key" + i;
            final byte[] value = serialize("This is a value" + i);
            assertDoesNotThrow(() -> client.put(key, value, timeoutMs, putAttempts));
        }

        log.debug("End testCanPutMultipleTimes:");
    }

    private static void testCanGetMultipleTimes(final int count, final int startIndex) throws CloseException, ControlException, TimeoutException {
        log.debug("Start testCanGetMultipleTimes:");
        final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);

        for (int i = startIndex; i < startIndex + count; i++) {
            final String key = "This is a key" + i;
            final byte[] value = serialize("This is a value" + i);
            assertDoesNotThrow(() -> {
                final byte[] response = client.get(key, timeoutMs, getAttempts);
                assertArrayEquals(value, response);
            });
        }

        log.debug("End testCanGetMultipleTimes:");
    }

    private static void testCanDeleteMultipleTimes(final int count, final int startIndex) throws CloseException, ControlException, TimeoutException {
        log.debug("Start testCanDeleteMultipleTimes:");
        final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);

        for (int i = startIndex; i < startIndex + count; i++) {
            final String key = "This is a key" + i;
            assertDoesNotThrow(() -> client.del(key, timeoutMs, delAttempts));
        }

        log.debug("End testCanDeleteMultipleTimes:");
    }

    private static void testTwoClientsMultipleKeyValues() {
        log.debug("Start testTwoClientsMultipleKeyValues:");
        final CountDownLatch latch = new CountDownLatch(2);

        final Thread thread1 = new Thread(() -> {
            try {
                testMultipleKeyValues(1000, 1000);
            } catch (final CloseException | ControlException | TimeoutException e) {
                e.printStackTrace();
            }
            latch.countDown();
        });
        final Thread thread2 = new Thread(() -> {
            try {
                testMultipleKeyValues(1000, 2000);
            } catch (final CloseException | ControlException | TimeoutException e) {
                e.printStackTrace();
            }
            latch.countDown();
        });

        thread1.start();
        thread2.start();

        assertDoesNotThrow(() -> latch.await());

        log.debug("End testTwoClientsMultipleKeyValues:");
    }

    private static void testThreeClientsMultipleKeyValues() {
        log.debug("Start testThreeClientsMultipleKeyValues:");
        final CountDownLatch latch = new CountDownLatch(3);

        final Thread thread1 = new Thread(() -> {
            try {
                testMultipleKeyValues(1000, 3000);
            } catch (final CloseException | ControlException | TimeoutException e) {
                e.printStackTrace();
            }
            latch.countDown();
        });
        final Thread thread2 = new Thread(() -> {
            try {
                testMultipleKeyValues(1000, 4000);
            } catch (final CloseException | ControlException | TimeoutException e) {
                e.printStackTrace();
            }
            latch.countDown();
        });
        final Thread thread3 = new Thread(() -> {
            try {
                testMultipleKeyValues(1000, 5000);
            } catch (final CloseException | ControlException | TimeoutException e) {
                e.printStackTrace();
            }
            latch.countDown();
        });

        thread1.start();
        thread2.start();
        thread3.start();

        assertDoesNotThrow(() -> latch.await());

        log.debug("End testThreeClientsMultipleKeyValues:");
    }

    private static void testStress() {
        log.debug("Start testStress:");
        final CountDownLatch latch = new CountDownLatch(4);

        final Thread thread1 = new Thread(() -> {
            try {
                stress(50, 1000, latch);
            } catch (final CloseException | ControlException | TimeoutException e) {
                e.printStackTrace();
            }
        });
        final Thread thread2 = new Thread(() -> {
            try {
                stress(50, 1000, latch);
            } catch (final CloseException | ControlException | TimeoutException e) {
                e.printStackTrace();
            }
        });
        final Thread thread3 = new Thread(() -> {
            try {
                stress(50, 1000, latch);
            } catch (final CloseException | ControlException | TimeoutException e) {
                e.printStackTrace();
            }
        });
        final Thread thread4 = new Thread(() -> {
            try {
                stress(50, 1000, latch);
            } catch (final CloseException | ControlException | TimeoutException e) {
                e.printStackTrace();
            }
        });

        thread1.start();
        thread2.start();
        thread3.start();
        thread4.start();

        assertDoesNotThrow(() -> latch.await());

        log.debug("End testStress:");
    }

    private static void stress(final int range, final int count, final CountDownLatch latch) throws CloseException, ControlException, TimeoutException {
        final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
        final Random random = new Random();
        for (int i = 0; i < count; i++) {
            final int index = random.ints(0, range).findFirst().getAsInt();
            final String key = "This is a key" + index;
            final byte[] value = serialize("This is a value" + index);
            final int operationNumber = random.ints(0, 3).findFirst().getAsInt();
            try {
                switch (operationNumber) {
                    case 0 -> client.put(key, value, timeoutMs, putAttempts);
                    case 1 -> client.get(key, timeoutMs, getAttempts);
                    case 2 -> client.del(key, timeoutMs, delAttempts);
                }
            } catch (final CloseException | NotFoundException | ControlException | DuplicateKeyException | TimeoutException e) {
                log.error(e.getMessage());
            }
        }
        latch.countDown();
    }

}
