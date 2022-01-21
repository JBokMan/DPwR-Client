import client.InfinimumDBClient;
import de.hhu.bsinfo.infinileap.binding.ControlException;

public final class App {
    public static void main(String... args) throws ControlException {
        InfinimumDBClient client = new InfinimumDBClient("localhost", 2998, "localhost", 4321);
        client.put("Hello World", "Hello World");
    }
}
