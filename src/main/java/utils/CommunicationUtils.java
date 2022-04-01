package utils;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.example.util.Requests;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import jdk.incubator.foreign.ValueLayout;
import lombok.extern.slf4j.Slf4j;
import model.PlasmaEntry;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static de.hhu.bsinfo.infinileap.example.util.Requests.state;
import static org.apache.commons.lang3.SerializationUtils.deserialize;
import static org.apache.commons.lang3.SerializationUtils.serialize;
import static utils.HashUtils.bytesToHex;
import static utils.HashUtils.generateID;

@Slf4j
public class CommunicationUtils {

    public static Long prepareToSendData(final int tagID, final byte[] data, final Endpoint endpoint, final ResourceScope scope) {
        log.info("Prepare to send data");
        final int dataSize = data.length;

        final MemorySegment source = MemorySegment.ofArray(data);
        final MemorySegment buffer = MemorySegment.allocateNative(dataSize, scope);
        buffer.copyFrom(source);

        return endpoint.sendTagged(buffer, Tag.of(tagID), new RequestParameters());
    }

    public static void putEntry(final int tagID, final byte[] entryBytes, final Worker worker, final Endpoint endpoint, final int timeoutMs) throws TimeoutException, ControlException {
        log.info("Put Entry");
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final MemoryDescriptor descriptor = receiveMemoryDescriptor(tagID, worker, timeoutMs, scope);
            final MemorySegment sourceBuffer = memorySegmentOfBytes(entryBytes, scope);
            try (final RemoteKey remoteKey = endpoint.unpack(descriptor)) {
                final long request = endpoint.put(sourceBuffer, descriptor.remoteAddress(), remoteKey);
                awaitRequestIfNecessary(request, worker, timeoutMs);
            }
        }
    }

    public static MemoryDescriptor receiveMemoryDescriptor(final int tagID, final Worker worker, final int timeoutMs, final ResourceScope scope) throws TimeoutException {
        log.info("Receiving Memory Descriptor");
        final MemoryDescriptor descriptor = new MemoryDescriptor(scope);
        final long request = worker.receiveTagged(descriptor, Tag.of(tagID), new RequestParameters(scope));
        awaitRequestIfNecessary(request, worker, timeoutMs);
        return descriptor;
    }

    private static MemorySegment memorySegmentOfBytes(final byte[] entryBytes, final ResourceScope scope) {
        final MemorySegment sourceBuffer = MemorySegment.allocateNative(entryBytes.length, scope);
        sourceBuffer.asByteBuffer().put(entryBytes);
        return sourceBuffer;
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
                        final TimeUnit timeUnit = TimeUnit.MILLISECONDS;
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

    public static void sendSingleMessage(final int tagID, final byte[] data, final Endpoint endpoint, final Worker worker, final int timeoutMs) throws TimeoutException {
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final Long request = prepareToSendData(tagID, data, endpoint, scope);
            sendData(List.of(request), worker, timeoutMs);
        }
    }

    public static byte[] receiveData(final int tagID, final int size, final Worker worker, final int timeoutMs) throws TimeoutException {
        log.info("Receiving message");
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final MemorySegment buffer = MemorySegment.allocateNative(size, scope);
            final long request = worker.receiveTagged(buffer, Tag.of(tagID), new RequestParameters(scope));
            awaitRequestIfNecessary(request, worker, timeoutMs);
            return buffer.toArray(ValueLayout.JAVA_BYTE);
        }
    }

    @SuppressWarnings("PMD.DataflowAnomalyAnalysis")
    public static byte[] receiveValue(final int tagID, final Endpoint endpoint, final Worker worker, final int timeoutMs) throws ControlException, TimeoutException {
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

    public static void awaitRequestIfNecessary(final long request, final Worker worker, final int timeoutMs) throws TimeoutException {
        if (Status.isError(request)) {
            log.warn("A request has an error status");
        }
        int counter = 0;
        while (state(request) != Requests.State.COMPLETE && counter < timeoutMs) {
            worker.progress();
            try {
                final TimeUnit timeUnit = TimeUnit.MILLISECONDS;
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

    public static int receiveTagID(final Worker worker, final int timeoutMs) throws TimeoutException {
        final byte[] tagIDBytes = receiveData(0, Integer.BYTES, worker, timeoutMs);
        final ByteBuffer byteBuffer = ByteBuffer.wrap(tagIDBytes);
        final int tagID = byteBuffer.getInt();
        log.info("Received \"{}\"", tagID);
        return tagID;
    }

    public static ArrayList<Long> prepareToSendKey(final int tagID, final String key, final Endpoint endpoint, final ResourceScope scope) {
        final ArrayList<Long> requests = new ArrayList<>();

        final byte[] keyBytes = serialize(key);
        final byte[] keySizeBytes = getLengthAsBytes(keyBytes);

        requests.add(prepareToSendData(tagID, keySizeBytes, endpoint, scope));
        requests.add(prepareToSendData(tagID, keyBytes, endpoint, scope));

        return requests;
    }

    public static byte[] getLengthAsBytes(final byte[] object) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES).putInt(object.length);
        return byteBuffer.array();
    }

    public static Integer getResponsibleServerID(final String key, final int serverCount) {
        final byte[] id = generateID(key);
        final String idAsHexValues = bytesToHex(id);
        final BigInteger idAsNumber = new BigInteger(idAsHexValues, 16);
        return idAsNumber.remainder(BigInteger.valueOf(serverCount)).intValue();
    }

    public static PlasmaEntry getPlasmaEntryFromBuffer(final ByteBuffer objectBuffer) {
        final byte[] data = new byte[objectBuffer.remaining()];
        objectBuffer.get(data);
        return deserialize(data);
    }
}
