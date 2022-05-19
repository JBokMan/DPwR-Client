package main;

import client.DPwRClient;
import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.util.CloseException;
import exceptions.DuplicateKeyException;
import exceptions.NotFoundException;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static org.apache.commons.lang3.SerializationUtils.serialize;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public final class Application {

    private static final String serverHostAddress = "localhost";
    private static final Integer serverPort = 2998;
    private static final Integer timeoutMs = 500;
    private static final Integer putAttempts = 5;
    private static final Integer getAttempts = 5;
    private static final Integer delAttempts = 5;

    public static void main(final String... args) throws CloseException, ControlException, TimeoutException {
        DPwRClient client = new DPwRClient(serverHostAddress, serverPort);
    }
}
