package client;

import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.util.CloseException;
import exceptions.DuplicateKeyException;
import exceptions.NotFoundException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static org.apache.commons.lang3.SerializationUtils.serialize;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DPwRClientTest {

    final String serverHostAddress = "localhost";
    final Integer serverPort = 2998;
    final Integer timeoutMs = 500;
    final Integer putAttempts = 5;
    final Integer getAttempts = 5;
    DPwRClient client;
    Integer delAttempts = 5;

    @BeforeAll
    void setup() throws CloseException, ControlException, TimeoutException {
        client = new DPwRClient(serverHostAddress, serverPort);
    }

    @Test
    @Order(1)
    void successfulPut() {
        final String key = "This is a key";
        final byte[] value = serialize("This is a value");
        assertDoesNotThrow(() -> client.put(key, value, timeoutMs, putAttempts));
    }

    @Test
    @Order(2)
    void unsuccessfulPut() {
        final String key = "This is a key";
        final byte[] value = serialize("This is a value");
        assertThrows(DuplicateKeyException.class, () -> client.put(key, value, timeoutMs, putAttempts));
    }

    @Test
    @Order(3)
    void testSuccessfulGet() {
        final String key = "This is a key";
        final byte[] value = serialize("This is a value");
        assertDoesNotThrow(() -> {
            final byte[] response = client.get(key, timeoutMs, getAttempts);
            assertArrayEquals(value, response);
        });
    }

    @Test
    @Order(4)
    void testUnsuccessfulGet() {
        final String key = "This is a key1";
        assertThrows(NotFoundException.class, () -> client.get(key, timeoutMs, getAttempts));
    }

    @Test
    @Order(5)
    void testSuccessfulDelete() {
        final String key = "This is a key";
        assertDoesNotThrow(() -> client.del(key, timeoutMs, delAttempts));
    }

    @Test
    @Order(6)
    void testUnsuccessfulDelete() {
        final String key = "This is a key";
        assertThrows(NotFoundException.class, () -> client.del(key, timeoutMs, delAttempts));
    }

    @Test
    @Order(7)
    void testCanPutGetAndDeleteObject() {
        final String key = "This is a key";
        final byte[] value = serialize("This is a value");
        assertDoesNotThrow(() -> {
            client.put(key, value, timeoutMs, putAttempts);
            final byte[] response = client.get(key, timeoutMs, getAttempts);
            assertArrayEquals(value, response);
            client.del(key, timeoutMs, delAttempts);
        });
    }

    @Test
    @Order(8)
    void testTwoKeyValues() {
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
    }

    @Test
    @Order(9)
    void testTwoKeyValuesWithCollidingHash() {
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
    }

    @Test
    @Order(10)
    void testThreeKeyValuesWithCollidingHash() {
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
    }

    @Test
    @Order(11)
    void testThreeKeysWithCollidingHashDeletingInOrder1() {
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
    }

    @Test
    @Order(12)
    void testThreeKeysWithCollidingHashDeletingInOrder2() {
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
    }

    @Test
    @Order(13)
    void testThreeKeysWithCollidingHashDeletingInOrder3() {
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
    }

    @Test
    @Order(14)
    void testThreeKeysWithCollidingHashDeleteFromStart() {
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
            client.put(key, value, timeoutMs, putAttempts);

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
    }

    @Test
    @Order(15)
    void testThreeKeysWithCollidingHashDeleteFromMiddle() {
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
            client.put(key2, value2, timeoutMs, putAttempts);

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
    }

    @Test
    @Order(15)
    void testThreeKeysWithCollidingHashDeleteFromEnd() {
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
    }

    @Test
    @Order(16)
    void testPutTimeout() {
        final String key = "timeout_test";
        final byte[] value = serialize("This is a value");
        assertThrows(TimeoutException.class, () -> client.put(key, value, 100, putAttempts));
    }

    @Test
    @Order(17)
    void testGetTimeout() {
        final String key = "timeout_test";
        assertThrows(TimeoutException.class, () -> client.get(key, 100, getAttempts));
    }

    @Test
    @Order(18)
    void testDeleteTimeout() {
        final String key = "timeout_test";
        assertThrows(TimeoutException.class, () -> client.del(key, 100, delAttempts));
    }

    @Test
    @Order(19)
    void testCanPutMultipleTimes() {
        assertDoesNotThrow(() -> canPutMultipleTimes(1000, 0, client));
    }

    @Test
    @Order(20)
    void testCanGetMultipleTimes() {
        assertDoesNotThrow(() -> canGetMultipleTimes(1000, 0, client));
    }

    @Test
    @Order(21)
    void testCanDeleteMultipleTimes() {
        assertDoesNotThrow(() -> canDeleteMultipleTimes(1000, 0, client));
    }

    @Test
    @Timeout(60)
    void testTwoClientsMultipleKeyValues() {
        final CountDownLatch latch = new CountDownLatch(2);

        final Thread thread1 = new Thread(() -> {
            try {
                final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
                assertDoesNotThrow(() -> testMultipleKeyValues(500, 1000, client));
            } catch (final CloseException | ControlException | TimeoutException e) {
                e.printStackTrace();
            }
            latch.countDown();
        });
        final Thread thread2 = new Thread(() -> {
            try {
                final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
                assertDoesNotThrow(() -> testMultipleKeyValues(500, 2000, client));
            } catch (final CloseException | ControlException | TimeoutException e) {
                e.printStackTrace();
            }
            latch.countDown();
        });

        thread1.start();
        thread2.start();

        assertDoesNotThrow(() -> latch.await());
    }

    @Test
    @Timeout(60)
    void testThreeClientsMultipleKeyValues() {
        final CountDownLatch latch = new CountDownLatch(3);

        final Thread thread1 = new Thread(() -> {
            try {
                final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
                assertDoesNotThrow(() -> testMultipleKeyValues(500, 3000, client));
            } catch (final CloseException | ControlException | TimeoutException e) {
                e.printStackTrace();
            }
            latch.countDown();
        });
        final Thread thread2 = new Thread(() -> {
            try {
                final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
                assertDoesNotThrow(() -> testMultipleKeyValues(500, 4000, client));
            } catch (final CloseException | ControlException | TimeoutException e) {
                e.printStackTrace();
            }
            latch.countDown();
        });
        final Thread thread3 = new Thread(() -> {
            try {
                final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
                assertDoesNotThrow(() -> testMultipleKeyValues(500, 5000, client));
            } catch (final CloseException | ControlException | TimeoutException e) {
                e.printStackTrace();
            }
            latch.countDown();
        });

        thread1.start();
        thread2.start();
        thread3.start();

        assertDoesNotThrow(() -> latch.await());
    }

    @Test
    @Timeout(60)
    void testStress() {
        final CountDownLatch latch = new CountDownLatch(4);

        final Thread thread1 = new Thread(() -> {
            try {
                final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
                stress(50, 500, latch, client);
            } catch (final CloseException | ControlException | TimeoutException e) {
                e.printStackTrace();
            }
        });
        final Thread thread2 = new Thread(() -> {
            try {
                final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
                stress(50, 500, latch, client);
            } catch (final CloseException | ControlException | TimeoutException e) {
                e.printStackTrace();
            }
        });
        final Thread thread3 = new Thread(() -> {
            try {
                final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
                stress(50, 500, latch, client);
            } catch (final CloseException | ControlException | TimeoutException e) {
                e.printStackTrace();
            }
        });
        final Thread thread4 = new Thread(() -> {
            try {
                final DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
                stress(50, 500, latch, client);
            } catch (final CloseException | ControlException | TimeoutException e) {
                e.printStackTrace();
            }
        });

        thread1.start();
        thread2.start();
        thread3.start();
        thread4.start();

        assertDoesNotThrow(() -> latch.await());
    }

    private void stress(final int range, final int count, final CountDownLatch latch, final DPwRClient client) {
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
            } catch (final CloseException | NotFoundException | ControlException | DuplicateKeyException |
                           TimeoutException e) {
                System.out.println(e.getMessage());
            }
        }
        latch.countDown();
    }

    private void testMultipleKeyValues(final int count, final int startIndex, final DPwRClient client) throws Exception {
        canPutMultipleTimes(count, startIndex, client);
        canGetMultipleTimes(count, startIndex, client);
        canDeleteMultipleTimes(count, startIndex, client);
    }

    private void canPutMultipleTimes(final int count, final int startIndex, final DPwRClient client) throws CloseException, ControlException, DuplicateKeyException, TimeoutException {
        for (int i = startIndex; i < startIndex + count; i++) {
            final String key = "This is a key" + i;
            final byte[] value = serialize("This is a value" + i);
            client.put(key, value, timeoutMs, putAttempts);
        }
    }

    private void canGetMultipleTimes(final int count, final int startIndex, final DPwRClient client) throws Exception {
        for (int i = startIndex; i < startIndex + count; i++) {
            final String key = "This is a key" + i;
            final byte[] value = serialize("This is a value" + i);
            final byte[] response = client.get(key, timeoutMs, getAttempts);
            if (!Arrays.equals(value, response)) {
                throw new Exception();
            }
        }
    }

    private void canDeleteMultipleTimes(final int count, final int startIndex, final DPwRClient client) throws CloseException, NotFoundException, ControlException, TimeoutException {
        for (int i = startIndex; i < startIndex + count; i++) {
            final String key = "This is a key" + i;
            client.del(key, timeoutMs, delAttempts);
        }
    }
}
