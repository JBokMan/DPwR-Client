package main;

import client.DPwRClient;
import de.hhu.bsinfo.infinileap.example.util.Constants;
import de.hhu.bsinfo.infinileap.example.util.InetSocketAddressConverter;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;

@Command(name = "dpwr_server", mixinStandardHelpOptions = true,
        description = "Starts a DPwR server with the given address and port")
public class Application implements Callable<Integer> {
    @Option(names = {"-c", "--connect"}, description = "The address of the server this client should connect to. Default is 127.0.0.1:2998")
    private final InetSocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 2998);
    @Option(names = {"-t", "--server-timeout"}, description = "The timeout for server operations in milliseconds. Default is 500MS")
    private int serverTimeout = 500;
    @Option(names = {"-v", "--verbose"}, description = "Whether or not info logs should be displayed. Default is false")
    private boolean verbose = false;

    public static void main(final String[] args) {
        final int exitCode = new CommandLine(new Application())
                .registerConverter(InetSocketAddress.class, new InetSocketAddressConverter(Constants.DEFAULT_PORT))
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
