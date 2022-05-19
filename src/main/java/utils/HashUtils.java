package utils;

import lombok.extern.slf4j.Slf4j;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

@Slf4j
public class HashUtils {
    private static final boolean TEST_MODE = true;

    private static byte[] getMD5Hash(final String text) {
        final MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance("MD5");
        } catch (final NoSuchAlgorithmException e) {
            log.error("The MD5 hash algorithm was not found.", e);
            return new byte[0];
        }
        byte[] id = messageDigest.digest(text.getBytes(StandardCharsets.UTF_8));
        if (TEST_MODE) {
            if (text.contains("hash_collision_test")) {
                id = new byte[16];
            }
        }
        // If all bits are zero there are problems with the next entry id's of the plasma entry's
        if (Arrays.equals(id, new byte[16])) {
            // Set the first bit to 1
            id[0] |= 1 << (0);
        }
        return id;
    }

    private static String bytesToHex(final byte[] bytes) {
        final StringBuilder sb = new StringBuilder();
        for (final byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    public static Integer getResponsibleServerID(final String key, final int serverCount) {
        final byte[] id = getMD5Hash(key);
        final String idAsHexValues = bytesToHex(id);
        final BigInteger idAsNumber = new BigInteger(idAsHexValues, 16);
        log.info(idAsNumber.toString());
        log.info(String.valueOf(serverCount));
        log.info(String.valueOf(idAsNumber.remainder(BigInteger.valueOf(serverCount)).intValue()));
        return idAsNumber.remainder(BigInteger.valueOf(serverCount)).intValue();
    }
}
