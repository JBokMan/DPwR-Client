package utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

@Slf4j
public class HashUtils {
    private static final boolean TEST_MODE = true;

    private static byte[] generateID(final String key, final byte[] idTailEndBytes) {
        // Generate plasma object id
        byte[] id = new byte[0];
        try {
            id = getMD5Hash(key);
        } catch (final NoSuchAlgorithmException e) {
            log.error("The MD5 hash algorithm was not found.", e);
            //ToDo handle exception
        }
        final byte[] fullID = ArrayUtils.addAll(id, idTailEndBytes);
        log.info("FullID: {} of key: {}", fullID, key);
        return fullID;
    }

    private static byte[] generateID(final String key) {
        return generateID(key, new byte[4]);
    }

    private static byte[] getMD5Hash(final String text) throws NoSuchAlgorithmException {
        final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
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
        final byte[] id = generateID(key);
        final String idAsHexValues = bytesToHex(id);
        final BigInteger idAsNumber = new BigInteger(idAsHexValues, 16);
        return idAsNumber.remainder(BigInteger.valueOf(serverCount)).intValue();
    }
}
