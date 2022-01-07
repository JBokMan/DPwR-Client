package client;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.Serializable;
import java.net.ConnectException;

public class InfinimumDBClient {

    private final JedisPool jedisClient;

    public InfinimumDBClient(String serverHostAddress, Integer serverPort, String redisHostAddress, Integer redisPort) {
        setupServerConnection(serverHostAddress, serverPort);
        try {
            testServerConnection();
        } catch (ConnectException e) {
            System.err.println("InfinimumDB-Server could not be reached");
        }

        this.jedisClient = new JedisPool(redisHostAddress, redisPort);
        try {
            this.jedisClient.getResource();
        } catch (JedisConnectionException e) {
            System.err.println("Redis could not be reached");
        }
    }

    private void testServerConnection() throws ConnectException {
        //TODO implement
    }

    private void setupServerConnection(String serverHostAddress, Integer serverPort) {
        //TODO implement
    }

    public Integer put(String key, Serializable object) {
        //TODO implement
        return -1;
    }

    public Object get(String key) {
        //TODO implement
        return null;
    }
}
