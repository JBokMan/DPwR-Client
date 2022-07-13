package utils;

import model.PlasmaEntry;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

public class PlasmaUtils {
    public static PlasmaEntry deserializePlasmaEntry(final byte[] entryBytes) throws IOException, ClassNotFoundException {
        final PlasmaEntry entry;
        try (final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(entryBytes)) {
            try (final ObjectInput objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
                entry = (PlasmaEntry) objectInputStream.readObject();
            }
        }
        return entry;
    }

    public static byte[] serializePlasmaEntry(final PlasmaEntry entry) throws IOException {
        final byte[] entryBytes;
        try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            try (final ObjectOutput objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
                objectOutputStream.writeObject(entry);
                entryBytes = byteArrayOutputStream.toByteArray();
            }
        }
        return entryBytes;
    }
}
