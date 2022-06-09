package de.hhu.edu.heinestore.benchmark.runner;

import de.hhu.edu.heinestore.benchmark.base.KeyValueStore;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import site.ycsb.Client;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;

@Slf4j
@CommandLine.Command(
        name = "benchmark",
        description = "Starts the YCSB benchmark."
)
public class BenchmarkRunner implements Runnable {

    private static final String BINDING_CLASS = "de.hhu.edu.heinestore.benchmark.binding.HeineStoreBinding";
    private static final String JSON_EXPORTER = "site.ycsb.measurements.exporter.JSONMeasurementsExporter";

    @CommandLine.Option(
            names = {"-c", "--connect"},
            description = "The HeineStore server's ip address and port.",
            required = true)
    private InetSocketAddress serverAddress;

    @CommandLine.Option(
            names = {"-p", "--properties"},
            description = "The properties file.",
            required = true)
    private Path properties;

    @CommandLine.Option(
            names = {"-e", "--export"},
            description = "The export file.")
    private Path export;

    @Override
    public void run() {
        Client.main(generateParameters());
    }

    private String[] generateParameters() {
        var parameters = new ArrayList<String>();

        // Run transaction phase
        parameters.add("-t");

        // Set server address
        parameters.add("-p");
        parameters.add(String.format("%s=%s:%d", KeyValueStore.ADDRESS_KEY,
                serverAddress.getHostString(), serverAddress.getPort()));

        // Write results to file if path was set
        if (export != null) {
            parameters.add("-p");
            parameters.add(String.format("exportfile=%s", export.toAbsolutePath().toString()));
            parameters.add("-p");
            parameters.add(String.format("exporter=%s", JSON_EXPORTER));
        }

        // Set properties file
        parameters.add("-P");
        parameters.add(properties.toAbsolutePath().toString());

        // Set binding implementation
        parameters.add("-db");
        parameters.add(BINDING_CLASS);

        return parameters.toArray(String[]::new);
    }
}
