package main;

import client.InfinimumDBClient;
import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.util.CloseException;
import exceptions.NotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;

import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@Slf4j
public final class Application {

    private static final String serverHostAddress = "localhost";
    private static final Integer serverPort = 2998;

    public static void main(final String... args) throws CloseException, NotFoundException, NoSuchAlgorithmException, ControlException, InterruptedException {
        testCanPutAndGetObject();
        tesTwoKeyValuesWork();
    }

    private static void testCanPutAndGetObject() throws CloseException, NoSuchAlgorithmException, ControlException, InterruptedException, NotFoundException {
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

    private static void tesTwoKeyValuesWork() throws CloseException, NoSuchAlgorithmException, ControlException, InterruptedException, NotFoundException {
        log.debug("Start tesTwoKeyValuesWork:");
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
        log.debug("End tesTwoKeyValuesWork:");
    }
}
