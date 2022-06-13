import runner.BenchmarkRunner;
import util.InetSocketAddressConverter;
import de.hhu.edu.heinestore.common.util.Constants;
import picocli.CommandLine;

import java.net.InetSocketAddress;

public class App {

    public static void main(String[] args) {
        var exitCode = new CommandLine(new BenchmarkRunner())
                .registerConverter(InetSocketAddress.class, new InetSocketAddressConverter(Constants.DEFAULT_PORT))
                .setCaseInsensitiveEnumValuesAllowed(true)
                .execute(args);

        System.exit(exitCode);
    }
}
