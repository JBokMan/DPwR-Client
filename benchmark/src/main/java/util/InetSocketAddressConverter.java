package util;

import de.hhu.edu.heinestore.common.util.Constants;
import picocli.CommandLine;

import java.net.InetSocketAddress;

public class InetSocketAddressConverter implements CommandLine.ITypeConverter<InetSocketAddress> {

    private final int defaultPort;

    public InetSocketAddressConverter(int defaultPort) {
        this.defaultPort = defaultPort;
    }

    @Override
    public InetSocketAddress convert(final String address) throws Exception {
        var splittedAddress = address.split(":");
        if (splittedAddress.length == 0 || splittedAddress.length > 2) {
            throw new CommandLine.TypeConversionException("No connection string specified");
        }

        var hostname = splittedAddress[0];
        var port = defaultPort;
        if (splittedAddress.length > 1) {
            try {
                port = Integer.parseInt(splittedAddress[1]);
            } catch (NumberFormatException e) {
                throw new CommandLine.TypeConversionException("Invalid port specified");
            }
        }

        try {
            return new InetSocketAddress(hostname, port);
        } catch (IllegalArgumentException e) {
            throw new CommandLine.TypeConversionException(e.getMessage());
        }
    }

    private static final InetSocketAddressConverter INSTANCE = new InetSocketAddressConverter(Constants.DEFAULT_PORT);

    public static InetSocketAddress from(String address) {
        try {
            return INSTANCE.convert(address);
        } catch (Exception e) {
            throw new IllegalArgumentException("Converting address failed.", e);
        }
    }
}
