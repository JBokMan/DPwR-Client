package client;

import exceptions.DuplicateKeyException;
import exceptions.KeyNotFoundException;
import exceptions.NetworkException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.apache.commons.lang3.SerializationUtils.serialize;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DPwRClientTest {

    final InetSocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 2998);
    final Integer timeoutMs = 2500;
    final Integer putAttempts = 5;
    final Integer getAttempts = 5;
    DPwRClient client;
    final Integer delAttempts = 5;
    final boolean verbose = true;

    @BeforeAll
    void setup() {
        client = new DPwRClient(serverAddress, timeoutMs, verbose);
    }

    private void canPutMultipleTimes(final int count, final int startIndex, final DPwRClient client) throws NetworkException, DuplicateKeyException {
        for (int i = startIndex; i < startIndex + count; i++) {
            final String key = "This is a key" + i;
            final byte[] value = serialize("This is a value" + i);
            client.put(key, value, putAttempts);
        }
    }

    private void canGetMultipleTimes(final int count, final int startIndex, final DPwRClient client) throws Exception {
        for (int i = startIndex; i < startIndex + count; i++) {
            final String key = "This is a key" + i;
            final byte[] value = serialize("This is a value" + i);
            final byte[] response = client.get(key, getAttempts);
            if (!Arrays.equals(value, response)) {
                throw new Exception();
            }
        }
    }

    private void canDeleteMultipleTimes(final int count, final int startIndex, final DPwRClient client) throws NetworkException, KeyNotFoundException {
        for (int i = startIndex; i < startIndex + count; i++) {
            final String key = "This is a key" + i;
            client.del(key, delAttempts);
        }
    }

    @Order(1)
    @Nested
    class BaseFunctionality {
        @BeforeEach
        public void setup() throws NetworkException {
            client.initialize();
        }

        @AfterEach
        public void cleanUp() {
            final String key = "This is a key";
            final String key1 = "This is a key1";
            final String key2 = "This is a key2";

            final String key3 = "hash_collision_test_1";
            final String key4 = "hash_collision_test_2";
            final String key5 = "hash_collision_test_3";

            try {
                client.del(key, delAttempts);
            } catch (final KeyNotFoundException | NetworkException e) {
                System.err.println(e.getMessage());
            }
            try {
                client.del(key1, delAttempts);
            } catch (final KeyNotFoundException | NetworkException e) {
                System.err.println(e.getMessage());
            }
            try {
                client.del(key2, delAttempts);
            } catch (final KeyNotFoundException | NetworkException e) {
                System.err.println(e.getMessage());
            }
            try {
                client.del(key3, delAttempts);
            } catch (final KeyNotFoundException | NetworkException e) {
                System.err.println(e.getMessage());
            }
            try {
                client.del(key4, delAttempts);
            } catch (final KeyNotFoundException | NetworkException e) {
                System.err.println(e.getMessage());
            }
            try {
                client.del(key5, delAttempts);
            } catch (final KeyNotFoundException | NetworkException e) {
                System.err.println(e.getMessage());
            }

            client.closeConnection();
        }

        @Test
        void successfulPut() {
            final String key = "This is a key";
            final byte[] value = serialize("This is a value");
            assertDoesNotThrow(() -> client.put(key, value, putAttempts));
        }

        @Test
        void unsuccessfulPut() {
            final String key = "This is a key";
            final byte[] value = serialize("This is a value");
            assertDoesNotThrow(() -> client.put(key, value, putAttempts));
            assertThrows(DuplicateKeyException.class, () -> client.put(key, value, putAttempts));
        }

        @Test
        void testSuccessfulGet() {
            final String key = "This is a key";
            final byte[] value = serialize("This is a value");
            assertDoesNotThrow(() -> {
                client.put(key, value, putAttempts);
                final byte[] response = client.get(key, getAttempts);
                assertArrayEquals(value, response);
            });
        }

        @Test
        void testUnsuccessfulGet() {
            final String key = "This is a key";
            assertThrows(KeyNotFoundException.class, () -> client.get(key, getAttempts));
        }

        @Test
        void testSuccessfulDelete() {
            final String key = "This is a key";
            final byte[] value = serialize("This is a value");
            assertDoesNotThrow(() -> client.put(key, value, putAttempts));
            assertDoesNotThrow(() -> client.del(key, delAttempts));
        }

        @Test
        void testUnsuccessfulDelete() {
            final String key = "This is a key";
            assertThrows(KeyNotFoundException.class, () -> client.del(key, delAttempts));
        }

        @Test
        void testSuccessfulContains() {
            final String key = "This is a key";
            final byte[] value = serialize("This is a value");
            assertDoesNotThrow(() -> client.put(key, value, putAttempts));
            assertDoesNotThrow(() -> {
                final boolean response = client.contains(key, delAttempts);
                assertTrue(response);
            });
        }

        @Test
        void testUnsuccessfulContains() {
            final String key = "This is a key";
            assertDoesNotThrow(() -> {
                final boolean response = client.contains(key, delAttempts);
                assertFalse(response);
            });
        }

        @Test
        void testSuccessfulHash() {
            final String key = "This is a key";
            final byte[] value = serialize("This is a value");
            final byte[] expected = HexFormat.of().parseHex("718c1e1f3e5d824a");
            assertDoesNotThrow(() -> client.put(key, value, putAttempts));
            assertDoesNotThrow(() -> {
                final byte[] response = client.hash(key, getAttempts);
                System.out.println(Arrays.toString(response));
                assertArrayEquals(expected, response);
            });
        }

        @Test
        void testUnsuccessfulHash() {
            final String key = "This is a key";
            assertThrows(KeyNotFoundException.class, () -> client.hash(key, getAttempts));
        }

        @Test
        void testEmptyList() {
            assertDoesNotThrow(() -> {
                final List<byte[]> response = client.list(delAttempts);
                assertEquals(0, response.size());
            });
        }

        @Test
        void testFilledList() {
            final String key = "This is a key";
            final byte[] value = serialize("This is a value");
            final String key1 = "This is a key1";
            final byte[] value1 = serialize("This is a value1");
            final String key2 = "This is a key2";
            final byte[] value2 = serialize("This is a value2");
            assertDoesNotThrow(() -> {
                client.put(key, value, putAttempts);
                client.put(key1, value1, putAttempts);
                client.put(key2, value2, putAttempts);
            });
            assertDoesNotThrow(() -> {
                final List<byte[]> response = client.list(delAttempts);
                assertEquals(3, response.size());
            });
        }

        @Test
        void testCanPutGetAndDeleteObject() {
            final String key = "This is a key";
            final byte[] value = serialize("This is a value");
            assertDoesNotThrow(() -> {
                client.put(key, value, putAttempts);
                final byte[] response = client.get(key, getAttempts);
                assertArrayEquals(value, response);
                client.del(key, delAttempts);
            });
        }

        @Test
        void testTwoKeyValues() {
            final String key = "This is a key";
            final byte[] value = serialize("This is a value");
            final String key2 = "This is a key2";
            final byte[] value2 = serialize("This is a value2");

            assertDoesNotThrow(() -> {
                client.put(key, value, putAttempts);
                client.put(key2, value2, putAttempts);
                final byte[] response = client.get(key, getAttempts);
                final byte[] response2 = client.get(key2, getAttempts);

                assertArrayEquals(value, response);
                assertArrayEquals(value2, response2);

                client.del(key, delAttempts);
                client.del(key2, delAttempts);
            });
        }

        @Test
        void testTwoKeyValuesWithCollidingHash() {
            final String key = "hash_collision_test_1";
            final byte[] value = serialize("This is a value");
            final String key2 = "hash_collision_test_2";
            final byte[] value2 = serialize("This is a value2");

            assertDoesNotThrow(() -> {
                client.put(key, value, putAttempts);
                client.put(key2, value2, putAttempts);
                final byte[] response = client.get(key, getAttempts);
                final byte[] response2 = client.get(key2, getAttempts);

                assertArrayEquals(value, response);
                assertArrayEquals(value2, response2);

                client.del(key, delAttempts);
                client.del(key2, delAttempts);
            });
        }

        @Test
        void testThreeKeyValuesWithCollidingHash() {
            final String key = "hash_collision_test_1";
            final byte[] value = serialize("This is a value");
            final String key2 = "hash_collision_test_2";
            final byte[] value2 = serialize("This is a value2");
            final String key3 = "hash_collision_test_3";
            final byte[] value3 = serialize("This is a value3");

            assertDoesNotThrow(() -> {
                client.put(key, value, putAttempts);
                client.put(key2, value2, putAttempts);
                client.put(key3, value3, putAttempts);
                final byte[] response = client.get(key, getAttempts);
                final byte[] response2 = client.get(key2, getAttempts);
                final byte[] response3 = client.get(key3, getAttempts);

                assertArrayEquals(value, response);
                assertArrayEquals(value2, response2);
                assertArrayEquals(value3, response3);

                client.del(key, delAttempts);
                client.del(key2, delAttempts);
                client.del(key3, delAttempts);
            });
        }

        @Test
        void testThreeKeysWithCollidingHashDeletingInOrder1() {
            final String key = "hash_collision_test_1";
            final byte[] value = serialize("This is a value");
            final String key2 = "hash_collision_test_2";
            final byte[] value2 = serialize("This is a value2");
            final String key3 = "hash_collision_test_3";
            final byte[] value3 = serialize("This is a value3");

            assertDoesNotThrow(() -> {
                client.put(key, value, putAttempts);
                client.put(key2, value2, putAttempts);
                client.put(key3, value3, putAttempts);

                client.del(key, delAttempts);

                final byte[] response2 = client.get(key2, getAttempts);
                byte[] response3 = client.get(key3, getAttempts);
                assertArrayEquals(value2, response2);
                assertArrayEquals(value3, response3);

                client.del(key2, delAttempts);

                response3 = client.get(key3, getAttempts);
                assertArrayEquals(value3, response3);

                client.del(key3, delAttempts);
            });
        }

        @Test
        void testThreeKeysWithCollidingHashDeletingInOrder2() {
            final String key = "hash_collision_test_1";
            final byte[] value = serialize("This is a value");
            final String key2 = "hash_collision_test_2";
            final byte[] value2 = serialize("This is a value2");
            final String key3 = "hash_collision_test_3";
            final byte[] value3 = serialize("This is a value3");

            assertDoesNotThrow(() -> {
                client.put(key, value, putAttempts);
                client.put(key2, value2, putAttempts);
                client.put(key3, value3, putAttempts);

                client.del(key3, delAttempts);

                byte[] response = client.get(key, getAttempts);
                final byte[] response2 = client.get(key2, getAttempts);
                assertArrayEquals(value, response);
                assertArrayEquals(value2, response2);

                client.del(key2, delAttempts);

                response = client.get(key, getAttempts);
                assertArrayEquals(value, response);

                client.del(key, delAttempts);
            });
        }

        @Test
        void testThreeKeysWithCollidingHashDeletingInOrder3() {
            final String key = "hash_collision_test_1";
            final byte[] value = serialize("This is a value");
            final String key2 = "hash_collision_test_2";
            final byte[] value2 = serialize("This is a value2");
            final String key3 = "hash_collision_test_3";
            final byte[] value3 = serialize("This is a value3");

            assertDoesNotThrow(() -> {
                client.put(key, value, putAttempts);
                client.put(key2, value2, putAttempts);
                client.put(key3, value3, putAttempts);

                client.del(key2, delAttempts);

                byte[] response = client.get(key, getAttempts);
                final byte[] response3 = client.get(key3, getAttempts);
                assertArrayEquals(value, response);
                assertArrayEquals(value3, response3);

                client.del(key3, delAttempts);

                response = client.get(key, getAttempts);
                assertArrayEquals(value, response);

                client.del(key, delAttempts);
            });
        }

        @Test
        void testThreeKeysWithCollidingHashDeleteFromStart() {
            final String key = "hash_collision_test_1";
            final byte[] value = serialize("This is a value");
            final String key2 = "hash_collision_test_2";
            final byte[] value2 = serialize("This is a value2");
            final String key3 = "hash_collision_test_3";
            final byte[] value3 = serialize("This is a value3");

            assertDoesNotThrow(() -> {
                client.put(key, value, putAttempts);
                client.put(key2, value2, putAttempts);
                client.put(key3, value3, putAttempts);

                client.del(key, delAttempts);
                client.put(key, value, putAttempts);

                final byte[] response = client.get(key, getAttempts);
                final byte[] response2 = client.get(key2, getAttempts);
                final byte[] response3 = client.get(key3, getAttempts);
                assertArrayEquals(value, response);
                assertArrayEquals(value2, response2);
                assertArrayEquals(value3, response3);

                client.del(key, delAttempts);
                client.del(key2, delAttempts);
                client.del(key3, delAttempts);
            });
        }

        @Test
        void testThreeKeysWithCollidingHashDeleteFromMiddle() {
            final String key = "hash_collision_test_1";
            final byte[] value = serialize("This is a value");
            final String key2 = "hash_collision_test_2";
            final byte[] value2 = serialize("This is a value2");
            final String key3 = "hash_collision_test_3";
            final byte[] value3 = serialize("This is a value3");

            assertDoesNotThrow(() -> {
                client.put(key, value, putAttempts);
                client.put(key2, value2, putAttempts);
                client.put(key3, value3, putAttempts);

                client.del(key2, delAttempts);
                client.put(key2, value2, putAttempts);

                final byte[] response = client.get(key, getAttempts);
                final byte[] response2 = client.get(key2, getAttempts);
                final byte[] response3 = client.get(key3, getAttempts);
                assertArrayEquals(value, response);
                assertArrayEquals(value2, response2);
                assertArrayEquals(value3, response3);

                client.del(key, delAttempts);
                client.del(key2, delAttempts);
                client.del(key3, delAttempts);
            });
        }

        @Test
        void testThreeKeysWithCollidingHashDeleteFromEnd() {
            final String key = "hash_collision_test_1";
            final byte[] value = serialize("This is a value");
            final String key2 = "hash_collision_test_2";
            final byte[] value2 = serialize("This is a value2");
            final String key3 = "hash_collision_test_3";
            final byte[] value3 = serialize("This is a value3");

            assertDoesNotThrow(() -> {
                client.put(key, value, putAttempts);
                client.put(key2, value2, putAttempts);
                client.put(key3, value3, putAttempts);

                client.del(key3, delAttempts);
                client.put(key3, value3, putAttempts);

                final byte[] response = client.get(key, getAttempts);
                final byte[] response2 = client.get(key2, getAttempts);
                final byte[] response3 = client.get(key3, getAttempts);
                assertArrayEquals(value, response);
                assertArrayEquals(value2, response2);
                assertArrayEquals(value3, response3);

                client.del(key, delAttempts);
                client.del(key2, delAttempts);
                client.del(key3, delAttempts);
            });
        }

        @Test
        void testPutTimeout() {
            final String key = "timeout_test";
            final byte[] value = serialize("This is a value");
            assertThrows(NetworkException.class, () -> client.put(key, value, 1));
        }

        @Test
        void testGetTimeout() {
            final String key = "timeout_test";
            assertThrows(NetworkException.class, () -> client.get(key, 1));
        }

        @Test
        void testDeleteTimeout() {
            final String key = "timeout_test";
            assertThrows(NetworkException.class, () -> client.del(key, 1));
        }
    }

    @Order(2)
    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class StorageTests {

        @BeforeEach
        public void setup() throws NetworkException {
            client.initialize();
        }

        @AfterEach
        public void tearDown() {
            client.closeConnection();
        }

        @Test
        @Order(1)
        void testCanPutMultipleTimes() {
            assertDoesNotThrow(() -> canPutMultipleTimes(10000, 0, client));
        }

        @Test
        @Order(2)
        void testCanGetMultipleTimes() {
            assertDoesNotThrow(() -> canGetMultipleTimes(10000, 0, client));
        }

        @Test
        @Order(3)
        void testCanDeleteMultipleTimes() {
            assertDoesNotThrow(() -> canDeleteMultipleTimes(10000, 0, client));
        }
    }

    @Order(3)
    @Nested
    class Concurrency {
        @Test
        @Timeout(60)
        void testTwoClientsMultipleKeyValues() {
            final CountDownLatch latch = new CountDownLatch(2);

            final Thread thread1 = new Thread(() -> {
                try {
                    final DPwRClient client = new DPwRClient(serverAddress, timeoutMs, verbose);
                    client.initialize();
                    assertDoesNotThrow(() -> testMultipleKeyValues(1000, client, 1000));
                    client.closeConnection();
                } catch (final NetworkException e) {
                    throw new RuntimeException(e);
                }
                latch.countDown();
            });
            final Thread thread2 = new Thread(() -> {
                try {
                    final DPwRClient client = new DPwRClient(serverAddress, timeoutMs, verbose);
                    client.initialize();
                    assertDoesNotThrow(() -> testMultipleKeyValues(2000, client, 1000));
                    client.closeConnection();
                } catch (final NetworkException e) {
                    throw new RuntimeException(e);
                }
                latch.countDown();
            });

            thread1.start();
            thread2.start();

            assertDoesNotThrow(() -> latch.await());
        }

        @Test
        @Timeout(90)
        void testThreeClientsMultipleKeyValues() {
            final CountDownLatch latch = new CountDownLatch(3);

            final Thread thread1 = new Thread(() -> {
                try {
                    final DPwRClient client = new DPwRClient(serverAddress, timeoutMs, verbose);
                    client.initialize();
                    assertDoesNotThrow(() -> testMultipleKeyValues(3000, client, 1000));
                    client.closeConnection();
                } catch (final NetworkException e) {
                    throw new RuntimeException(e);
                }
                latch.countDown();
            });
            final Thread thread2 = new Thread(() -> {
                try {
                    final DPwRClient client = new DPwRClient(serverAddress, timeoutMs, verbose);
                    client.initialize();
                    assertDoesNotThrow(() -> testMultipleKeyValues(4000, client, 1000));
                    client.closeConnection();
                } catch (final NetworkException e) {
                    throw new RuntimeException(e);
                }
                latch.countDown();
            });
            final Thread thread3 = new Thread(() -> {
                try {
                    final DPwRClient client = new DPwRClient(serverAddress, timeoutMs, verbose);
                    client.initialize();
                    assertDoesNotThrow(() -> testMultipleKeyValues(5000, client, 1000));
                    client.closeConnection();
                } catch (final NetworkException e) {
                    throw new RuntimeException(e);
                }
                latch.countDown();
            });

            thread1.start();
            thread2.start();
            thread3.start();

            assertDoesNotThrow(() -> latch.await());
        }

        private void testMultipleKeyValues(final int startIndex, final DPwRClient client, final int count) throws Exception {
            canPutMultipleTimes(count, startIndex, client);
            canGetMultipleTimes(count, startIndex, client);
            canDeleteMultipleTimes(count, startIndex, client);
        }
    }

    @Order(4)
    @Nested
    class StressTests {
        @AfterEach
        public void cleanUp() {
            try {
                client.initialize();
            } catch (final NetworkException e) {
                throw new RuntimeException(e);
            }
            for (int i = 0; i < 50; i++) {
                try {
                    client.del("This is a key" + i, timeoutMs);
                } catch (final KeyNotFoundException | NetworkException e) {
                    System.err.println(e.getMessage());
                }
            }
        }

        @Test
        @Timeout(120)
        void testStress() {
            final CountDownLatch latch = new CountDownLatch(4);

            final Thread thread1 = new Thread(() -> {
                try {
                    final DPwRClient client = new DPwRClient(serverAddress, timeoutMs, verbose);
                    client.initialize();
                    stress(client);
                    client.closeConnection();
                    latch.countDown();
                } catch (final NetworkException e) {
                    throw new RuntimeException(e);
                }
            });
            final Thread thread2 = new Thread(() -> {
                try {
                    final DPwRClient client = new DPwRClient(serverAddress, timeoutMs, verbose);
                    client.initialize();
                    stress(client);
                    client.closeConnection();
                    latch.countDown();
                } catch (final NetworkException e) {
                    throw new RuntimeException(e);
                }
            });
            final Thread thread3 = new Thread(() -> {
                try {
                    final DPwRClient client = new DPwRClient(serverAddress, timeoutMs, verbose);
                    client.initialize();
                    stress(client);
                    client.closeConnection();
                    latch.countDown();
                } catch (final NetworkException e) {
                    throw new RuntimeException(e);
                }
            });
            final Thread thread4 = new Thread(() -> {
                try {
                    final DPwRClient client = new DPwRClient(serverAddress, timeoutMs, verbose);
                    client.initialize();
                    stress(client);
                    client.closeConnection();
                    latch.countDown();
                } catch (final NetworkException e) {
                    throw new RuntimeException(e);
                }
            });

            thread1.start();
            thread2.start();
            thread3.start();
            thread4.start();

            assertDoesNotThrow(() -> latch.await());
        }

        private void stress(final DPwRClient client) {
            final Random random = new Random();
            for (int i = 0; i < 500; i++) {
                final int index = random.ints(0, 50).findFirst().orElse(0);
                final String key = "This is a key" + index;
                final byte[] value = serialize("This is a value" + index);
                final int operationNumber = random.ints(0, 3).findFirst().orElse(0);
                try {
                    switch (operationNumber) {
                        case 0 -> client.put(key, value, putAttempts);
                        case 1 -> client.get(key, getAttempts);
                        case 2 -> client.del(key, delAttempts);
                    }
                } catch (final KeyNotFoundException | DuplicateKeyException | NetworkException e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }
}
