package main;

import client.DPwRClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import utils.InetSocketAddressConverter;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;

@Command(name = "dpwr_client", mixinStandardHelpOptions = true,
        description = "Starts a DPwR server with the given address and port")
public class Application implements Callable<Integer> {
    @Option(names = {"-c", "--connect"}, description = "The address of the server this client should connect to. Default is 127.0.0.1:2998")
    private final InetSocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 2998);
    @Option(names = {"-t", "--server-timeout"}, description = "The timeout for server operations in milliseconds. Default is 500MS")
    private final int serverTimeout = 500;
    @Option(names = {"-v", "--verbose"}, description = "Whether or not info logs should be displayed. Default is false")
    private final boolean verbose = false;

    public static void main(final String[] args) {
        final int exitCode = new CommandLine(new Application())
                .registerConverter(InetSocketAddress.class, new InetSocketAddressConverter(2998))
                .execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        try {
            final DPwRClient client = new DPwRClient(serverAddress, serverTimeout, verbose);
            //...
            return 0;
        } catch (final Exception e) {
            return -1;
        }
    }
}
