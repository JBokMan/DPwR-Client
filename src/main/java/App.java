import client.InfinimumDBClient;

public final class App {
    public static void main(String... args) {
        InfinimumDBClient client = new InfinimumDBClient("localhost", 1234, "localhost", 4321);
    }
}
