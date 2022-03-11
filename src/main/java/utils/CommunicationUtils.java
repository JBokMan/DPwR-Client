package utils;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.example.util.CommunicationBarrier;
import de.hhu.bsinfo.infinileap.example.util.Requests;
import de.hhu.bsinfo.infinileap.util.CloseException;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import jdk.incubator.foreign.ValueLayout;
import lombok.extern.slf4j.Slf4j;
import model.PlasmaEntry;
import org.apache.commons.lang3.SerializationException;

import java.lang.ref.Cleaner;
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

    public static MemoryDescriptor getMemoryDescriptorOfBytes(final byte[] object, final Context context) throws ControlException, CloseException {
        final MemorySegment source = MemorySegment.ofArray(object);
        try(final MemoryRegion memoryRegion = context.allocateMemory(object.length)) {
            memoryRegion.segment().copyFrom(source);
            return memoryRegion.descriptor();
        }
    }

    public static Long prepareToSendData(final byte[] data, final Endpoint endpoint, final ResourceScope scope) {
        log.info("Prepare to send data");
        final int dataSize = data.length;

        final MemorySegment source = MemorySegment.ofArray(data);
        final MemorySegment buffer = MemorySegment.allocateNative(dataSize, scope);
        buffer.copyFrom(source);

        return endpoint.sendTagged(buffer, Tag.of(0L), new RequestParameters());
    }

    public static Long prepareToSendRemoteKey(final byte[] value, final Endpoint endpoint, final Context context) throws ControlException, CloseException {
        log.info("Prepare to send remote key");
        final MemoryDescriptor objectAddress;
        try {
            objectAddress = getMemoryDescriptorOfBytes(value, context);
        } catch (ControlException | CloseException e) {
            log.error(e.getMessage());
            throw e;
        }
        return endpoint.sendTagged(objectAddress, Tag.of(0L));
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
                        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
                        synchronized (timeUnit) {
                            timeUnit.wait(1);
                        }
                    } catch (InterruptedException e) {
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

    public static void sendSingleMessage(final byte[] data, final Endpoint endpoint, final Worker worker, final int timeoutMs) throws TimeoutException {
        try (final ResourceScope scope = ResourceScope.newConfinedScope(Cleaner.create())) {
            final Long request = prepareToSendData(data, endpoint, scope);
            sendData(List.of(request), worker, timeoutMs);
        }
    }

    public static byte[] receiveData(final int size, final Worker worker, final int timeoutMs) throws TimeoutException {
        log.info("Receiving message");
        try (final ResourceScope scope = ResourceScope.newConfinedScope(Cleaner.create())) {
            final MemorySegment buffer = MemorySegment.allocateNative(size, scope);
            final long request = worker.receiveTagged(buffer, Tag.of(0L), new RequestParameters(scope));
            awaitRequestIfNecessary(request, worker, timeoutMs);
            return buffer.toArray(ValueLayout.JAVA_BYTE);
        }
    }

    @SuppressWarnings("PMD.DataflowAnomalyAnalysis")
    public static byte[] receiveValue(final Endpoint endpoint, final Worker worker, final int timeoutMs) throws ControlException, TimeoutException {
        log.info("Receiving Remote Key");
        try (final ResourceScope scope = ResourceScope.newConfinedScope(Cleaner.create())) {
            final MemoryDescriptor descriptor = new MemoryDescriptor(scope);
            final long request = worker.receiveTagged(descriptor, Tag.of(0L), new RequestParameters(scope));
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
                TimeUnit timeUnit = TimeUnit.MILLISECONDS;
                synchronized (timeUnit) {
                    timeUnit.wait(1);
                }
            } catch (InterruptedException e) {
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

    public static ArrayList<Long> prepareToSendKey(final String key, final Endpoint endpoint, final ResourceScope scope) {
        final ArrayList<Long> requests = new ArrayList<>();

        final byte[] keyBytes;
        try {
            keyBytes = serialize(key);
        } catch (SerializationException e) {
            log.error(e.getMessage());
            throw e;
        }

        final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES).putInt(keyBytes.length);
        final byte[] keySizeBytes = byteBuffer.array();

        requests.add(prepareToSendData(keySizeBytes, endpoint, scope));
        requests.add(prepareToSendData(keyBytes, endpoint, scope));

        return requests;
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
