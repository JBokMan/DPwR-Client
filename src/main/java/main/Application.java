package main;

import client.InfinimumDBClient;
import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.util.CloseException;
import exceptions.NotFoundException;
import org.apache.commons.lang3.SerializationUtils;

public final class Application {
    public static void main(final String... args) {
        final InfinimumDBClient client = new InfinimumDBClient("localhost", 2998, "localhost", 6379);
        client.put("I am the key", "I am the value");
        byte[] object = new byte[0];
        try {
            object = client.get("I do not exist");
        } catch (CloseException | NotFoundException | ControlException | InterruptedException e) {
            e.printStackTrace();
        }
        if (object.length > 1) {
            System.out.println(SerializationUtils.deserialize(object).toString());
        }
    }
}
