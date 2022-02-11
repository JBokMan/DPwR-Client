package main;

import client.InfinimumDBClient;
import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.util.CloseException;
import exceptions.NotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;

import java.security.NoSuchAlgorithmException;

@Slf4j
public final class Application {

    public static void main(final String... args) {
        final InfinimumDBClient client = new InfinimumDBClient("localhost", 2998, "localhost", 6379);

        try {
            client.put("I am the key", "I am the value");
        } catch (NoSuchAlgorithmException | InterruptedException | CloseException | ControlException e) {
            if (log.isErrorEnabled()) {
                log.info(e.getMessage());
            }
        }

        byte[] object = new byte[0];
        try {
            object = client.get("I am the key");
        } catch (CloseException | NotFoundException | ControlException | InterruptedException | NoSuchAlgorithmException e) {
            if (log.isErrorEnabled()) {
                log.info(e.getMessage());
            }
        }
        if (object != null && object.length > 0) {
            if (log.isInfoEnabled()) {
                log.info(SerializationUtils.deserialize(object).toString() + "\n");
            }
        }

        object = new byte[0];
        try {
            object = client.get("I do not exist");
        } catch (CloseException | NotFoundException | ControlException | InterruptedException | NoSuchAlgorithmException e) {
            if (log.isErrorEnabled()) {
                log.info(e.getMessage());
            }
        }
        if (object != null && object.length > 0) {
            if (log.isInfoEnabled()) {
                log.info(SerializationUtils.deserialize(object).toString() + "\n");
            }
        }
    }
}
