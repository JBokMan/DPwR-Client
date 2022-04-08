package utils;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.example.util.Requests;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import jdk.incubator.foreign.ValueLayout;
import lombok.extern.slf4j.Slf4j;
import model.PlasmaEntry;
import org.apache.commons.lang3.SerializationException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static de.hhu.bsinfo.infinileap.example.util.Requests.state;
import static org.apache.commons.lang3.SerializationUtils.deserialize;
import static org.apache.commons.lang3.SerializationUtils.serialize;

@Slf4j
public class CommunicationUtils {

    final private static TimeUnit timeUnit = TimeUnit.MILLISECONDS;

    private static Long prepareToSendData(final int tagID, final byte[] data, final Endpoint endpoint, final ResourceScope scope) {
        log.info("Prepare to send data");
        final int dataSize = data.length;

        final MemorySegment source = MemorySegment.ofArray(data);
        final MemorySegment buffer = MemorySegment.allocateNative(dataSize, scope);
        buffer.copyFrom(source);

        return endpoint.sendTagged(buffer, Tag.of(tagID), new RequestParameters());
    }

    public static Long prepareToSendString(final int tagID, final String string, final Endpoint endpoint, final ResourceScope scope) {
        return prepareToSendData(tagID, serialize(string), endpoint, scope);
    }

    public static Long prepareToSendInteger(final int tagID, final int integer, final Endpoint endpoint, final ResourceScope scope) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES).putInt(integer);
        return prepareToSendData(tagID, byteBuffer.array(), endpoint, scope);
    }

    public static ArrayList<Long> prepareToSendKey(final int tagID, final String key, final Endpoint endpoint, final ResourceScope scope) {
        final ArrayList<Long> requests = new ArrayList<>();
        final byte[] keyBytes = serialize(key);
        requests.add(prepareToSendInteger(tagID, keyBytes.length, endpoint, scope));
        requests.add(prepareToSendData(tagID, keyBytes, endpoint, scope));
        return requests;
    }

    public static void sendData(final List<Long> requests, final Worker worker, final int timeoutMs) throws TimeoutException {
        log.info("Sending data");
        boolean timeoutHappened = false;
        for (final Long request : requests) {
            if (timeoutHappened) {
                worker.cancelRequest(request);
            } else {
                int counter = 0;
                while (state(request) != Requests.State.COMPLETE && counter < timeoutMs) {
                    worker.progress();
                    try {
                        synchronized (timeUnit) {
                            timeUnit.wait(1);
                        }
                    } catch (final InterruptedException e) {
                        log.error(e.getMessage());
                        worker.cancelRequest(request);
                        timeoutHappened = true;
                        continue;
                    }
                    counter++;
                }
                if (state(request) != Requests.State.COMPLETE) {
                    worker.cancelRequest(request);
                    timeoutHappened = true;
                } else {
                    Requests.release(request);
                }
            }
        }
        if (timeoutHappened) {
            throw new TimeoutException("A timeout occurred while sending data");
        }
    }

    public static void sendStatusCode(final int tagID, final String statusCode, final Endpoint endpoint, final Worker worker, final int timeoutMs) throws TimeoutException {
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final Long request = prepareToSendString(tagID, statusCode, endpoint, scope);
            sendData(List.of(request), worker, timeoutMs);
        }
    }

    private static MemorySegment memorySegmentOfBytes(final byte[] entryBytes, final ResourceScope scope) {
        final MemorySegment sourceBuffer = MemorySegment.allocateNative(entryBytes.length, scope);
        sourceBuffer.asByteBuffer().put(entryBytes);
        return sourceBuffer;
    }

    public static void sendEntryPerRDMA(final int tagID, final byte[] entryBytes, final Worker worker, final Endpoint endpoint, final int timeoutMs) throws TimeoutException, ControlException {
        log.info("Send Entry per RDMA");
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final MemoryDescriptor descriptor = receiveMemoryDescriptor(tagID, worker, timeoutMs, scope);
            final MemorySegment sourceBuffer = memorySegmentOfBytes(entryBytes, scope);
            try (final RemoteKey remoteKey = endpoint.unpack(descriptor)) {
                final long request = endpoint.put(sourceBuffer, descriptor.remoteAddress(), remoteKey);
                awaitRequestIfNecessary(request, worker, timeoutMs);
            }
        }
    }

    private static byte[] receiveData(final int tagID, final int size, final Worker worker, final int timeoutMs) throws TimeoutException {
        log.info("Receiving message");
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final MemorySegment buffer = MemorySegment.allocateNative(size, scope);
            final long request = worker.receiveTagged(buffer, Tag.of(tagID), new RequestParameters(scope));
            awaitRequestIfNecessary(request, worker, timeoutMs);
            return buffer.toArray(ValueLayout.JAVA_BYTE);
        }
    }

    private static int receiveInteger(final Worker worker, final int timeoutMs) throws TimeoutException {
        final byte[] integerBytes = receiveData(0, Integer.BYTES, worker, timeoutMs);
        final ByteBuffer byteBuffer = ByteBuffer.wrap(integerBytes);
        final int number = byteBuffer.getInt();
        log.info("Received \"{}\"", number);
        return number;
    }

    public static int receiveTagID(final Worker worker, final int timeoutMs) throws TimeoutException {
        return receiveInteger(worker, timeoutMs);
    }

    public static String receiveStatusCode(final int tagID, final Worker worker, final int timeoutMs) throws TimeoutException, SerializationException {
        final byte[] statusCodeBytes = receiveData(tagID, 10, worker, timeoutMs);
        final String statusCode = deserialize(statusCodeBytes);
        log.info("Received status code: \"{}\"", statusCode);
        return statusCode;
    }

    private static MemoryDescriptor receiveMemoryDescriptor(final int tagID, final Worker worker, final int timeoutMs, final ResourceScope scope) throws TimeoutException {
        log.info("Receiving Memory Descriptor");
        final MemoryDescriptor descriptor = new MemoryDescriptor(scope);
        final long request = worker.receiveTagged(descriptor, Tag.of(tagID), new RequestParameters(scope));
        awaitRequestIfNecessary(request, worker, timeoutMs);
        return descriptor;
    }

    private static PlasmaEntry getPlasmaEntryFromBuffer(final ByteBuffer objectBuffer) {
        final byte[] data = new byte[objectBuffer.remaining()];
        objectBuffer.get(data);
        return deserialize(data);
    }

    @SuppressWarnings("PMD.DataflowAnomalyAnalysis")
    public static byte[] receiveValuePerRDMA(final int tagID, final Endpoint endpoint, final Worker worker, final int timeoutMs) throws ControlException, TimeoutException, SerializationException {
        log.info("Receiving Remote Key");
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final MemoryDescriptor descriptor = new MemoryDescriptor(scope);
            final long request = worker.receiveTagged(descriptor, Tag.of(tagID), new RequestParameters(scope));
            awaitRequestIfNecessary(request, worker, timeoutMs);

            final MemorySegment targetBuffer = MemorySegment.allocateNative(descriptor.remoteSize(), scope);
            try (final RemoteKey remoteKey = endpoint.unpack(descriptor)) {
                final long request2 = endpoint.get(targetBuffer, descriptor.remoteAddress(), remoteKey, new RequestParameters(scope));
                awaitRequestIfNecessary(request2, worker, timeoutMs);
            }

            final ByteBuffer objectBuffer = targetBuffer.asByteBuffer();
            final PlasmaEntry entry = getPlasmaEntryFromBuffer(objectBuffer);
            log.info("Read \"{}\" from remote buffer", entry);

            return entry.value;
        }
    }

    private static void awaitRequestIfNecessary(final long request, final Worker worker, final int timeoutMs) throws TimeoutException {
        if (Status.isError(request)) {
            log.warn("A request has an error status");
        }
        int counter = 0;
        while (state(request) != Requests.State.COMPLETE && counter < timeoutMs) {
            worker.progress();
            try {
                synchronized (timeUnit) {
                    timeUnit.wait(1);
                }
            } catch (final InterruptedException e) {
                log.error(e.getMessage());
                worker.cancelRequest(request);
                return;
            }
            counter++;
        }
        if (state(request) != Requests.State.COMPLETE) {
            worker.cancelRequest(request);
            throw new TimeoutException("A timeout occurred while receiving data");
        } else {
            Requests.release(request);
        }
    }
}
