package client;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.example.util.CommunicationBarrier;
import de.hhu.bsinfo.infinileap.example.util.Requests;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;

@Slf4j
public class CommunicationUtils {
    public static long prepareToSendData(byte[] data, Endpoint endpoint, CommunicationBarrier barrier, ResourceScope scope) {
        log.info("Prepare to send data");
        int dataSize = data.length;

        // Allocate a buffer and write the message
        final var source = MemorySegment.ofArray(data);
        final var buffer = MemorySegment.allocateNative(dataSize, scope);
        buffer.copyFrom(source);

        long request = endpoint.sendTagged(buffer, Tag.of(0L), new RequestParameters()
                .setSendCallback(barrier::release));

        return request;
    }

    public static long prepareToSendRemoteKey(MemoryDescriptor descriptor, Endpoint endpoint, CommunicationBarrier barrier) {
        log.info("Prepare to send remote key");
        long request = endpoint.sendTagged(descriptor, Tag.of(0L), new RequestParameters().setSendCallback(barrier::release));
        return request;
    }

    public static void sendData(ArrayList<Long> requests, Worker worker, CommunicationBarrier barrier) {
        log.info("Sending data");
        try {
            Requests.await(worker, barrier);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (long request : requests) {
            Requests.release(request);
        }
    }
}
