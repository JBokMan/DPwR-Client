package main;

import client.InfinimumDBClient;

public final class Application {
    public static void main(final String... args) {
        final InfinimumDBClient client = new InfinimumDBClient("localhost", 2998, "localhost", 6379);
        client.put("I am the key2", "I am the value");
    }
}
