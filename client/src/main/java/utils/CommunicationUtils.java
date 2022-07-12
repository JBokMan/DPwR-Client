package utils;

import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.binding.Endpoint;
import de.hhu.bsinfo.infinileap.binding.MemoryDescriptor;
import de.hhu.bsinfo.infinileap.binding.RemoteKey;
import de.hhu.bsinfo.infinileap.binding.RequestParameters;
import de.hhu.bsinfo.infinileap.binding.Tag;
import de.hhu.bsinfo.infinileap.binding.Worker;
import de.hhu.bsinfo.infinileap.util.Requests;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import jdk.incubator.foreign.ValueLayout;
import lombok.extern.slf4j.Slf4j;
import model.PlasmaEntry;
import org.apache.commons.lang3.SerializationException;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;

import static de.hhu.bsinfo.infinileap.util.Requests.State.COMPLETE;
import static de.hhu.bsinfo.infinileap.util.Requests.State.ERROR;
import static de.hhu.bsinfo.infinileap.util.Requests.state;
import static org.apache.commons.lang3.SerializationUtils.deserialize;
import static org.apache.commons.lang3.SerializationUtils.serialize;

@Slf4j
public class CommunicationUtils {

    private static Long prepareToSendData(final int tagID, final byte[] data, final Endpoint endpoint, final ResourceScope scope) {
        log.info("Prepare to send data");
        final int dataSize = data.length;

        final MemorySegment source = MemorySegment.ofArray(data);
        final MemorySegment buffer = MemorySegment.allocateNative(dataSize, scope);
        buffer.copyFrom(source);

        return endpoint.sendTagged(buffer, Tag.of(tagID));
    }

    public static Long prepareToSendString(final int tagID, final String string, final Endpoint endpoint, final ResourceScope scope) {
        return prepareToSendData(tagID, serialize(string), endpoint, scope);
    }

    public static Long prepareToSendInteger(final int tagID, final int integer, final Endpoint endpoint, final ResourceScope scope) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES).putInt(integer);
        return prepareToSendData(tagID, byteBuffer.array(), endpoint, scope);
    }

    public static long[] prepareToSendKey(final int tagID, final String key, final Endpoint endpoint, final ResourceScope scope) {
        final long[] requests = new long[2];
        final byte[] keyBytes = serialize(key);
        requests[0] = prepareToSendInteger(tagID, keyBytes.length, endpoint, scope);
        requests[1] = prepareToSendData(tagID, keyBytes, endpoint, scope);
        return requests;
    }

    private static void awaitRequest(final long request, final Worker worker, final int timeoutMs) throws TimeoutException, InterruptedException {
        final long timeout = 2_000L * timeoutMs;
        int counter = 0;
        Requests.State requestState = state(request);
        while ((requestState != COMPLETE) && (requestState != ERROR) && (counter < timeout)) {
            worker.progress();
            counter++;
            requestState = state(request);
        }
        if (requestState != COMPLETE) {
            worker.cancelRequest(request);
            throw new TimeoutException("A timeout occurred while awaiting a request");
        } else {
            Requests.release(request);
        }
    }

    public static void awaitRequests(final long[] requests, final Worker worker, final int timeoutMs) throws TimeoutException {
        boolean timeoutHappened = false;
        for (int i = 0; i < requests.length; i++) {
            if (timeoutHappened) {
                worker.cancelRequest(requests[i]);
                continue;
            }
            try {
                awaitRequest(requests[i], worker, timeoutMs);
            } catch (final TimeoutException | InterruptedException e) {
                timeoutHappened = true;
                worker.cancelRequest(requests[i]);
            }
        }
        if (timeoutHappened) {
            log.error("A timeout occurred while sending data");
            throw new TimeoutException("A timeout occurred while sending data");
        }
    }

    public static void sendStatusCode(final int tagID, final String statusCode, final Endpoint endpoint, final Worker worker, final int timeoutMs) throws TimeoutException {
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final long request = prepareToSendString(tagID, statusCode, endpoint, scope);
            awaitRequests(new long[]{request}, worker, timeoutMs);
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
                awaitRequests(new long[]{request}, worker, timeoutMs);
            }
        }
    }

    private static byte[] receiveData(final int tagID, final int size, final Worker worker, final int timeoutMs) throws TimeoutException {
        log.info("Receiving message");
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final MemorySegment buffer = MemorySegment.allocateNative(size, scope);
            final long request = worker.receiveTagged(buffer, Tag.of(tagID), new RequestParameters(scope));
            awaitRequests(new long[]{request}, worker, timeoutMs);
            return buffer.toArray(ValueLayout.JAVA_BYTE);
        }
    }

    private static int receiveInteger(final int tagID, final Worker worker, final int timeoutMs) throws TimeoutException {
        final byte[] integerBytes = receiveData(tagID, Integer.BYTES, worker, timeoutMs);
        final ByteBuffer byteBuffer = ByteBuffer.wrap(integerBytes);
        final int number = byteBuffer.getInt();
        log.info("Received \"{}\"", number);
        return number;
    }

    public static int receiveTagID(final Worker worker, final int timeoutMs) throws TimeoutException {
        return receiveInteger(0, worker, timeoutMs);
    }

    public static int receiveCount(final int tagID, final Worker worker, final int timeoutMs) throws TimeoutException {
        return receiveInteger(tagID, worker, timeoutMs);
    }

    public static InetSocketAddress receiveAddress(final int tagID, final Worker worker, final int timeoutMs) throws TimeoutException, SerializationException {
        final int addressSize = receiveInteger(tagID, worker, timeoutMs);
        final byte[] serverAddressBytes = receiveData(tagID, addressSize, worker, timeoutMs);
        return deserialize(serverAddressBytes);
    }

    public static byte[] receiveHash(final int tagID, final Worker worker, final int timeoutMs) throws TimeoutException {
        final int hashSize = receiveInteger(tagID, worker, timeoutMs);
        return receiveData(tagID, hashSize, worker, timeoutMs);
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
        awaitRequests(new long[]{request}, worker, timeoutMs);
        return descriptor;
    }

    private static PlasmaEntry getPlasmaEntryFromBuffer(final ByteBuffer objectBuffer) throws SerializationException {
        final byte[] data = new byte[objectBuffer.remaining()];
        objectBuffer.get(data);
        return deserialize(data);
    }

    private static MemorySegment prepareBufferAndGetBytes(final int tagID, final Endpoint endpoint, final Worker worker, final int timeoutMs, final ResourceScope scope) throws TimeoutException, ControlException {
        final MemoryDescriptor descriptor = receiveMemoryDescriptor(tagID, worker, timeoutMs, scope);

        final MemorySegment targetBuffer = MemorySegment.allocateNative(descriptor.remoteSize(), scope);
        try (final RemoteKey remoteKey = endpoint.unpack(descriptor)) {
            final long request = endpoint.get(targetBuffer, descriptor.remoteAddress(), remoteKey, new RequestParameters(scope));
            awaitRequests(new long[]{request}, worker, timeoutMs);
        }
        return targetBuffer;
    }

    @SuppressWarnings("PMD.DataflowAnomalyAnalysis")
    public static byte[] receiveValuePerRDMA(final int tagID, final Endpoint endpoint, final Worker worker, final int timeoutMs) throws ControlException, TimeoutException, SerializationException {
        log.info("Receiving Remote Key");
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final MemorySegment targetBuffer = prepareBufferAndGetBytes(tagID, endpoint, worker, timeoutMs, scope);

            final ByteBuffer objectBuffer = targetBuffer.asByteBuffer();
            final PlasmaEntry entry = getPlasmaEntryFromBuffer(objectBuffer);
            log.info("Read \"{}\" from remote buffer", entry);

            return entry.value;
        }
    }

    public static byte[] receiveObjectPerRDMA(final int tagID, final Endpoint endpoint, final Worker worker, final int timeoutMs) throws ControlException, TimeoutException, SerializationException {
        log.info("Receiving Remote Key");
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final MemorySegment targetBuffer = prepareBufferAndGetBytes(tagID, endpoint, worker, timeoutMs, scope);

            return targetBuffer.toArray(ValueLayout.JAVA_BYTE);
        }
    }

    public static void tearDownEndpoint(final Endpoint endpoint, final Worker worker, final int timeoutMs) {
        try {
            final long[] requests = new long[2];
            requests[0] = endpoint.flush();
            requests[1] = endpoint.closeNonBlocking();
            awaitRequests(requests, worker, timeoutMs);
        } catch (final TimeoutException e) {
            log.warn(e.getMessage());
        }
    }
}
