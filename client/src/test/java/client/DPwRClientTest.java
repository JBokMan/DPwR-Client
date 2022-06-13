package client;

import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.util.CloseException;
import exceptions.DuplicateKeyException;
import exceptions.NotFoundException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static org.apache.commons.lang3.SerializationUtils.serialize;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DPwRClientTest {

    final InetSocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 2998);
    final Integer timeoutMs = 500;
    final Integer putAttempts = 5;
    final Integer getAttempts = 5;
    DPwRClient client;
    Integer delAttempts = 5;

    @BeforeAll
    void setup() throws CloseException, ControlException, TimeoutException {
        client = new DPwRClient(serverAddress, timeoutMs, true);
    }

    private void canPutMultipleTimes(final int count, final int startIndex, final DPwRClient client) throws CloseException, ControlException, DuplicateKeyException, TimeoutException {
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

    private void canDeleteMultipleTimes(final int count, final int startIndex, final DPwRClient client) throws CloseException, NotFoundException, ControlException, TimeoutException {
        for (int i = startIndex; i < startIndex + count; i++) {
            final String key = "This is a key" + i;
            client.del(key, delAttempts);
        }
    }

    @Order(1)
    @Nested
    class BaseFunctionality {
        @AfterEach
        public void cleanUp() {
            final String key = "This is a key";
            final String key1 = "This is a key1";
            final String key2 = "This is a key2";
            try {
                client.del(key, delAttempts);
            } catch (final CloseException | TimeoutException | ControlException | NotFoundException e) {
                System.err.println(e.getMessage());
            }
            try {
                client.del(key1, delAttempts);
            } catch (final CloseException | TimeoutException | ControlException | NotFoundException e) {
                System.err.println(e.getMessage());
            }
            try {
                client.del(key2, delAttempts);
            } catch (final CloseException | TimeoutException | ControlException | NotFoundException e) {
                System.err.println(e.getMessage());
            }
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
            assertThrows(NotFoundException.class, () -> client.get(key, getAttempts));
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
            assertThrows(NotFoundException.class, () -> client.del(key, delAttempts));
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
            final byte[] expected = HexFormat.of().parseHex("64fa7530c9b20949");
            assertDoesNotThrow(() -> client.put(key, value, putAttempts));
            assertDoesNotThrow(() -> {
                final byte[] response = client.hash(key, getAttempts);
                assertArrayEquals(expected, response);
            });
        }

        @Test
        void testUnsuccessfulHash() {
            final String key = "This is a key";
            assertThrows(NotFoundException.class, () -> client.hash(key, getAttempts));
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
                final byte[] response = client.get(key, timeoutMs);
                final byte[] response2 = client.get(key2, timeoutMs);

                assertArrayEquals(value, response);
                assertArrayEquals(value2, response2);

                client.del(key, timeoutMs);
                client.del(key2, timeoutMs);
            });
        }

        @Test
        void testTwoKeyValuesWithCollidingHash() {
            final String key = "hash_collision_test_1";
            final byte[] value = serialize("This is a value");
            final String key2 = "hash_collision_test_2";
            final byte[] value2 = serialize("This is a value2");

            assertDoesNotThrow(() -> {
                client.put(key, value, timeoutMs);
                client.put(key2, value2, timeoutMs);
                final byte[] response = client.get(key, timeoutMs);
                final byte[] response2 = client.get(key2, timeoutMs);

                assertArrayEquals(value, response);
                assertArrayEquals(value2, response2);

                client.del(key, timeoutMs);
                client.del(key2, timeoutMs);
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
                client.put(key, value, timeoutMs);
                client.put(key2, value2, timeoutMs);
                client.put(key3, value3, timeoutMs);
                final byte[] response = client.get(key, timeoutMs);
                final byte[] response2 = client.get(key2, timeoutMs);
                final byte[] response3 = client.get(key3, timeoutMs);

                assertArrayEquals(value, response);
                assertArrayEquals(value2, response2);
                assertArrayEquals(value3, response3);

                client.del(key, timeoutMs);
                client.del(key2, timeoutMs);
                client.del(key3, timeoutMs);
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
                client.put(key, value, timeoutMs);
                client.put(key2, value2, timeoutMs);
                client.put(key3, value3, timeoutMs);

                client.del(key, timeoutMs);

                final byte[] response2 = client.get(key2, timeoutMs);
                byte[] response3 = client.get(key3, timeoutMs);
                assertArrayEquals(value2, response2);
                assertArrayEquals(value3, response3);

                client.del(key2, timeoutMs);

                response3 = client.get(key3, timeoutMs);
                assertArrayEquals(value3, response3);

                client.del(key3, timeoutMs);
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
                client.put(key, value, timeoutMs);
                client.put(key2, value2, timeoutMs);
                client.put(key3, value3, timeoutMs);

                client.del(key3, timeoutMs);

                byte[] response = client.get(key, timeoutMs);
                final byte[] response2 = client.get(key2, timeoutMs);
                assertArrayEquals(value, response);
                assertArrayEquals(value2, response2);

                client.del(key2, timeoutMs);

                response = client.get(key, timeoutMs);
                assertArrayEquals(value, response);

                client.del(key, timeoutMs);
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
                client.put(key, value, timeoutMs);
                client.put(key2, value2, timeoutMs);
                client.put(key3, value3, timeoutMs);

                client.del(key2, timeoutMs);

                byte[] response = client.get(key, timeoutMs);
                final byte[] response3 = client.get(key3, timeoutMs);
                assertArrayEquals(value, response);
                assertArrayEquals(value3, response3);

                client.del(key3, timeoutMs);

                response = client.get(key, timeoutMs);
                assertArrayEquals(value, response);

                client.del(key, timeoutMs);
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
                client.put(key, value, timeoutMs);
                client.put(key2, value2, timeoutMs);
                client.put(key3, value3, timeoutMs);

                client.del(key, timeoutMs);
                client.put(key, value, timeoutMs);

                final byte[] response = client.get(key, timeoutMs);
                final byte[] response2 = client.get(key2, timeoutMs);
                final byte[] response3 = client.get(key3, timeoutMs);
                assertArrayEquals(value, response);
                assertArrayEquals(value2, response2);
                assertArrayEquals(value3, response3);

                client.del(key, timeoutMs);
                client.del(key2, timeoutMs);
                client.del(key3, timeoutMs);
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
                client.put(key, value, timeoutMs);
                client.put(key2, value2, timeoutMs);
                client.put(key3, value3, timeoutMs);

                client.del(key2, timeoutMs);
                client.put(key2, value2, timeoutMs);

                final byte[] response = client.get(key, timeoutMs);
                final byte[] response2 = client.get(key2, timeoutMs);
                final byte[] response3 = client.get(key3, timeoutMs);
                assertArrayEquals(value, response);
                assertArrayEquals(value2, response2);
                assertArrayEquals(value3, response3);

                client.del(key, timeoutMs);
                client.del(key2, timeoutMs);
                client.del(key3, timeoutMs);
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
                client.put(key, value, timeoutMs);
                client.put(key2, value2, timeoutMs);
                client.put(key3, value3, timeoutMs);

                client.del(key3, timeoutMs);
                client.put(key3, value3, timeoutMs);

                final byte[] response = client.get(key, timeoutMs);
                final byte[] response2 = client.get(key2, timeoutMs);
                final byte[] response3 = client.get(key3, timeoutMs);
                assertArrayEquals(value, response);
                assertArrayEquals(value2, response2);
                assertArrayEquals(value3, response3);

                client.del(key, timeoutMs);
                client.del(key2, timeoutMs);
                client.del(key3, timeoutMs);
            });
        }

        @Test
        void testPutTimeout() {
            final String key = "timeout_test";
            final byte[] value = serialize("This is a value");
            assertThrows(TimeoutException.class, () -> client.put(key, value, 5));
        }

        @Test
        void testGetTimeout() {
            final String key = "timeout_test";
            assertThrows(TimeoutException.class, () -> client.get(key, 5));
        }

        @Test
        void testDeleteTimeout() {
            final String key = "timeout_test";
            assertThrows(TimeoutException.class, () -> client.del(key, 5));
        }
    }

    @Order(2)
    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class StorageTests {
        @Test
        @Order(1)
        void testCanPutMultipleTimes() {
            assertDoesNotThrow(() -> canPutMultipleTimes(1000, 0, client));
        }

        @Test
        @Order(2)
        void testCanGetMultipleTimes() {
            assertDoesNotThrow(() -> canGetMultipleTimes(1000, 0, client));
        }

        @Test
        @Order(3)
        void testCanDeleteMultipleTimes() {
            assertDoesNotThrow(() -> canDeleteMultipleTimes(1000, 0, client));
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
                    final DPwRClient client = new DPwRClient(serverAddress, timeoutMs, true);
                    assertDoesNotThrow(() -> testMultipleKeyValues(1000, client));
                } catch (final CloseException | ControlException | TimeoutException e) {
                    e.printStackTrace();
                }
                latch.countDown();
            });
            final Thread thread2 = new Thread(() -> {
                try {
                    final DPwRClient client = new DPwRClient(serverAddress, timeoutMs, true);
                    assertDoesNotThrow(() -> testMultipleKeyValues(2000, client));
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
                    final DPwRClient client = new DPwRClient(serverAddress, timeoutMs, true);
                    assertDoesNotThrow(() -> testMultipleKeyValues(3000, client));
                } catch (final CloseException | ControlException | TimeoutException e) {
                    e.printStackTrace();
                }
                latch.countDown();
            });
            final Thread thread2 = new Thread(() -> {
                try {
                    final DPwRClient client = new DPwRClient(serverAddress, timeoutMs, true);
                    assertDoesNotThrow(() -> testMultipleKeyValues(4000, client));
                } catch (final CloseException | ControlException | TimeoutException e) {
                    e.printStackTrace();
                }
                latch.countDown();
            });
            final Thread thread3 = new Thread(() -> {
                try {
                    final DPwRClient client = new DPwRClient(serverAddress, timeoutMs, true);
                    assertDoesNotThrow(() -> testMultipleKeyValues(5000, client));
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

        private void testMultipleKeyValues(final int startIndex, final DPwRClient client) throws Exception {
            canPutMultipleTimes(500, startIndex, client);
            canGetMultipleTimes(500, startIndex, client);
            canDeleteMultipleTimes(500, startIndex, client);
        }
    }

    @Order(4)
    @Nested
    class StressTests {
        @AfterEach
        public void cleanUp() {
            for (int i = 0; i < 50; i++) {
                try {
                    client.del("This is a key" + i, timeoutMs);
                } catch (final CloseException | TimeoutException | ControlException | NotFoundException e) {
                    System.err.println(e.getMessage());
                }
            }
        }

        @Test
        @Timeout(60)
        void testStress() {
            final CountDownLatch latch = new CountDownLatch(4);

            final Thread thread1 = new Thread(() -> {
                try {
                    final DPwRClient client = new DPwRClient(serverAddress, timeoutMs, true);
                    stress(latch, client);
                } catch (final CloseException | ControlException | TimeoutException e) {
                    e.printStackTrace();
                }
            });
            final Thread thread2 = new Thread(() -> {
                try {
                    final DPwRClient client = new DPwRClient(serverAddress, timeoutMs, true);
                    stress(latch, client);
                } catch (final CloseException | ControlException | TimeoutException e) {
                    e.printStackTrace();
                }
            });
            final Thread thread3 = new Thread(() -> {
                try {
                    final DPwRClient client = new DPwRClient(serverAddress, timeoutMs, true);
                    stress(latch, client);
                } catch (final CloseException | ControlException | TimeoutException e) {
                    e.printStackTrace();
                }
            });
            final Thread thread4 = new Thread(() -> {
                try {
                    final DPwRClient client = new DPwRClient(serverAddress, timeoutMs, true);
                    stress(latch, client);
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

        private void stress(final CountDownLatch latch, final DPwRClient client) {
            final Random random = new Random();
            for (int i = 0; i < 500; i++) {
                final int index = random.ints(0, 50).findFirst().orElse(0);
                final String key = "This is a key" + index;
                final byte[] value = serialize("This is a value" + index);
                final int operationNumber = random.ints(0, 3).findFirst().orElse(0);
                try {
                    switch (operationNumber) {
                        case 0 -> client.put(key, value, timeoutMs);
                        case 1 -> client.get(key, timeoutMs);
                        case 2 -> client.del(key, timeoutMs);
                    }
                } catch (final CloseException | NotFoundException | ControlException | DuplicateKeyException |
                               TimeoutException e) {
                    System.out.println(e.getMessage());
                }
            }
            latch.countDown();
        }
    }
}
