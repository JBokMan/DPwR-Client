package utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HashUtilsTest {

    @Test
    void getResponsibleServerIDWithOnlyOneServerReturnsIndexOfFirstServer() {
        final String key = "This is a key";
        final int serverCount = 1;

        final int id = HashUtils.getResponsibleServerID(key, serverCount);

        assertEquals(0, id);
    }

    @Test
    void getResponsibleServerIDWithZeroServersReturnsMinusOne() {
        final String key = "This is a key";
        final int serverCount = 0;

        final int id = HashUtils.getResponsibleServerID(key, serverCount);

        assertEquals(-1, id);
    }

    @Test
    void getResponsibleServerIDWithTwoServersCanReturnZeroAndOne() {
        final String key = "Ths is a key";
        final String key1 = "This is a key1";
        final int serverCount = 2;

        final int id = HashUtils.getResponsibleServerID(key, serverCount);
        final int id1 = HashUtils.getResponsibleServerID(key1, serverCount);

        assertEquals(0, id);
        assertEquals(1, id1);
    }

    @Test
    void getResponsibleServerIDWithThreeServersCanReturnZeroAndOneAndTwo() {
        final String key = "This is a ke";
        final String key1 = "This i a key1";
        final String key2 = "Tis i a ky2";
        final int serverCount = 3;

        final int id = HashUtils.getResponsibleServerID(key, serverCount);
        final int id1 = HashUtils.getResponsibleServerID(key1, serverCount);
        final int id2 = HashUtils.getResponsibleServerID(key2, serverCount);

        assertEquals(0, id);
        assertEquals(1, id1);
        assertEquals(2, id2);
    }

    @Test
    void getResponsibleServerIDWithEmptyKeyReturnsValidId() {
        final String key = "";
        final int serverCount = 1;

        final int id = HashUtils.getResponsibleServerID(key, serverCount);

        assertEquals(0, id);
    }
}