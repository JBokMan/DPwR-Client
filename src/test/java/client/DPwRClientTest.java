package client;

import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.util.CloseException;
import exceptions.DuplicateKeyException;
import exceptions.NotFoundException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.TestInstance.Lifecycle;

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

    @Order(1)
    @Nested
    class BaseFunctionality {
        @AfterEach
        public void cleanUp() {
            final String key = "This is a key";
            final String key1 = "This is a key1";
            final String key2 = "This is a key2";
            try {
                client.del(key, timeoutMs, delAttempts);
            } catch (final CloseException | TimeoutException | ControlException | NotFoundException e) {
                System.err.println(e.getMessage());
            }
            try {
                client.del(key1, timeoutMs, delAttempts);
            } catch (final CloseException | TimeoutException | ControlException | NotFoundException e) {
                System.err.println(e.getMessage());
            }
            try {
                client.del(key2, timeoutMs, delAttempts);
            } catch (final CloseException | TimeoutException | ControlException | NotFoundException e) {
                System.err.println(e.getMessage());
            }
        }

        @Test
        void successfulPut() {
            final String key = "This is a key";
            final byte[] value = serialize("This is a value");
            assertDoesNotThrow(() -> client.put(key, value, timeoutMs, putAttempts));
        }

        @Test
        void unsuccessfulPut() {
            final String key = "This is a key";
            final byte[] value = serialize("This is a value");
            assertDoesNotThrow(() -> client.put(key, value, timeoutMs, putAttempts));
            assertThrows(DuplicateKeyException.class, () -> client.put(key, value, timeoutMs, putAttempts));
        }

        @Test
        void testSuccessfulGet() {
            final String key = "This is a key";
            final byte[] value = serialize("This is a value");
            assertDoesNotThrow(() -> {
                client.put(key, value, timeoutMs, putAttempts);
                final byte[] response = client.get(key, timeoutMs, getAttempts);
                assertArrayEquals(value, response);
            });
        }

        @Test
        void testUnsuccessfulGet() {
            final String key = "This is a key";
            assertThrows(NotFoundException.class, () -> client.get(key, timeoutMs, getAttempts));
        }

        @Test
        void testSuccessfulDelete() {
            final String key = "This is a key";
            final byte[] value = serialize("This is a value");
            assertDoesNotThrow(() -> client.put(key, value, timeoutMs, putAttempts));
            assertDoesNotThrow(() -> client.del(key, timeoutMs, delAttempts));
        }

        @Test
        void testUnsuccessfulDelete() {
            final String key = "This is a key";
            assertThrows(NotFoundException.class, () -> client.del(key, timeoutMs, delAttempts));
        }

        @Test
        void testSuccessfulContains() {
            final String key = "This is a key";
            final byte[] value = serialize("This is a value");
            assertDoesNotThrow(() -> client.put(key, value, timeoutMs, putAttempts));
            assertDoesNotThrow(() -> {
                final boolean response = client.contains(key, timeoutMs, delAttempts);
                assertTrue(response);
            });
        }

        @Test
        void testUnsuccessfulContains() {
            final String key = "This is a key";
            assertDoesNotThrow(() -> {
                final boolean response = client.contains(key, timeoutMs, delAttempts);
                assertFalse(response);
            });
        }

        @Test
        void testSuccessfulHash() {
            final String key = "This is a key";
            final byte[] value = serialize("This is a value");
            final byte[] expected = HexFormat.of().parseHex("64fa7530c9b20949");
            assertDoesNotThrow(() -> client.put(key, value, timeoutMs, putAttempts));
            assertDoesNotThrow(() -> {
                final byte[] response = client.hash(key, timeoutMs, getAttempts);
                assertArrayEquals(expected, response);
            });
        }

        @Test
        void testUnsuccessfulHash() {
            final String key = "This is a key";
            assertThrows(NotFoundException.class, () -> client.hash(key, timeoutMs, getAttempts));
        }

        @Test
        void testEmptyList() {
            assertDoesNotThrow(() -> {
                final List<byte[]> response = client.list(timeoutMs, delAttempts);
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
                client.put(key, value, timeoutMs, putAttempts);
                client.put(key1, value1, timeoutMs, putAttempts);
                client.put(key2, value2, timeoutMs, putAttempts);
            });
            assertDoesNotThrow(() -> {
                final List<byte[]> response = client.list(timeoutMs, delAttempts);
                assertEquals(3, response.size());
            });
        }

        @Test
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
        void testPutTimeout() {
            final String key = "timeout_test";
            final byte[] value = serialize("This is a value");
            assertThrows(TimeoutException.class, () -> client.put(key, value, 100, putAttempts));
        }

        @Test
        void testGetTimeout() {
            final String key = "timeout_test";
            assertThrows(TimeoutException.class, () -> client.get(key, 100, getAttempts));
        }

        @Test
        void testDeleteTimeout() {
            final String key = "timeout_test";
            assertThrows(TimeoutException.class, () -> client.del(key, 100, delAttempts));
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

        private void testMultipleKeyValues(final int count, final int startIndex, final DPwRClient client) throws Exception {
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
            for (int i = 0; i < 50; i++) {
                try {
                    client.del("This is a key" + i, timeoutMs, delAttempts);
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
    }
}
