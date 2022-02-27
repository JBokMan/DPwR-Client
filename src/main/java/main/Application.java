package main;

import client.InfinimumDBClient;
import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.util.CloseException;
import exceptions.DuplicateKeyException;
import exceptions.NotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;

import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@Slf4j
public final class Application {

    private static final String serverHostAddress = "localhost";
    private static final Integer serverPort = 2998;

    public static void main(final String... args) throws CloseException, NotFoundException, NoSuchAlgorithmException, ControlException, InterruptedException, DuplicateKeyException {
        //testDelete();
        testCanPutAndGetObject();
        testTwoKeyValuesWork();
        testTwoKeysWithCollidingHashCanBePut();
    }

    private static void testCanPutAndGetObject() throws CloseException, NoSuchAlgorithmException, ControlException, InterruptedException, NotFoundException, DuplicateKeyException {
        log.debug("Start testCanPutAndGetObject:");
        InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        String key = "This is a key";
        byte[] value = SerializationUtils.serialize("This is a value");

        client.put(key, value);
        byte[] response = client.get(key);

        assertArrayEquals(value, response);

        client.del(key);
        log.debug("End testCanPutAndGetObject:");
    }

    private static void testDelete() throws CloseException, NoSuchAlgorithmException, ControlException, InterruptedException, NotFoundException, DuplicateKeyException {
        log.debug("Start testCanPutAndGetObject:");
        InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        String key = "This is a key";
        client.del(key);
        log.debug("End testCanPutAndGetObject:");
    }

    private static void testTwoKeyValuesWork() throws CloseException, NoSuchAlgorithmException, ControlException, InterruptedException, NotFoundException, DuplicateKeyException {
        log.debug("Start testTwoKeyValuesWork:");
        InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        String key = "This is a key";
        byte[] value = SerializationUtils.serialize("This is a value");
        String key2 = "This is a key2";
        byte[] value2 = SerializationUtils.serialize("This is a value2");

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

    private static void testTwoKeysWithCollidingHashCanBePut() throws CloseException, NoSuchAlgorithmException, ControlException, InterruptedException, NotFoundException, DuplicateKeyException {
        log.debug("Start testTwoKeysWithCollidingHashCanBePut:");
        InfinimumDBClient client = new InfinimumDBClient(serverHostAddress, serverPort);
        String key = "d131dd02c5e6eec4693d9a0698aff95c2fcab58712467eab4004583eb8fb7f89" +
                "55ad340609f4b30283e488832571415a085125e8f7cdc99fd91dbdf280373c5b" +
                "d8823e3156348f5bae6dacd436c919c6dd53e2b487da03fd02396306d248cda0" +
                "e99f33420f577ee8ce54b67080a80d1ec69821bcb6a8839396f9652b6ff72a70";
        byte[] value = SerializationUtils.serialize("This is a value");
        String key2 = "d131dd02c5e6eec4693d9a0698aff95c2fcab50712467eab4004583eb8fb7f89" +
                "55ad340609f4b30283e4888325f1415a085125e8f7cdc99fd91dbd7280373c5b" +
                "d8823e3156348f5bae6dacd436c919c6dd53e23487da03fd02396306d248cda0" +
                "e99f33420f577ee8ce54b67080280d1ec69821bcb6a8839396f965ab6ff72a70";
        byte[] value2 = SerializationUtils.serialize("This is a value2");

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
}
