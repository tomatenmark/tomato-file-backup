package de.mherrmann.tomatofilebackup.core.cli;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ProgressHelperTest {

    @Test
    void shouldGetFormattedBytesUnitBytes(){
        long bytes = 978;
        String expected = "    978 Bytes";

        String actual = ProgressHelper.getFormattedBytes(bytes);

        assertEquals(expected, actual);
    }

    @Test
    void shouldGetFormattedBytesUnitKB(){
        long bytes = 2345;
        String expected = "   2,29 KB   ";

        String actual = ProgressHelper.getFormattedBytes(bytes);

        assertEquals(expected, actual);
    }

    @Test
    void shouldGetFormattedBytesUnitMB(){
        long bytes = 1024*2548;
        String expected = "   2,49 MB   ";

        String actual = ProgressHelper.getFormattedBytes(bytes);

        assertEquals(expected, actual);
    }

    @Test
    void shouldGetFormattedBytesUnitGB(){
        long bytes = 1024L*1124*2548;
        String expected = "   2,73 GB   ";

        String actual = ProgressHelper.getFormattedBytes(bytes);

        assertEquals(expected, actual);
    }

    @Test
    void shouldGetFormattedBytesUnitTB(){
        long bytes = 1024L*1234*1124*2548;
        String expected = "   3,29 TB   ";

        String actual = ProgressHelper.getFormattedBytes(bytes);

        assertEquals(expected, actual);
    }

    @Test
    void shouldGetFormattedPercentNotFinishedOneDigit(){
        long done = 4642;
        long total = 55252;
        String expected = "  8";

        String actual = ProgressHelper.getFormattedPercent(done, total);

        assertEquals(expected, actual);
    }

    @Test
    void shouldGetFormattedPercentNotFinishedTwoDigits(){
        long done = 4642*5;
        long total = 55252;
        String expected = " 42";

        String actual = ProgressHelper.getFormattedPercent(done, total);

        assertEquals(expected, actual);
    }

    @Test
    void shouldGetFormattedPercentFinished(){
        long total = 55252;
        String expected = "100";

        String actual = ProgressHelper.getFormattedPercent(total, total);

        assertEquals(expected, actual);
    }

    @Test
    void shouldGetShortPathOriginShort(){
        String path = "/short/path.txt";

        String actual = ProgressHelper.getShortPath(path);

        assertEquals(path, actual);
    }

    @Test
    void shouldGetShortPathOriginMedium(){
        String path = "/medium/path/some/sub/sub/sub/sub/sub/sub/sub/sub/files/text.txt";
        String expected = ".../text.txt";

        String actual = ProgressHelper.getShortPath(path);

        assertEquals(expected, actual);
    }

    @Test
    void shouldGetShortPathOriginLong(){
        String path = "/medium/path/some/sub/sub/sub/sub/sub/sub/sub/sub/files/" +
                "long-file-name-some-lorem-ipsum-dolor-sit-amit-lorem-ipsum-text.txt";
        String expected = ".../long-file-name-some-lorem-...-sit-amit-lorem-ipsum-text.txt";

        String actual = ProgressHelper.getShortPath(path);

        assertEquals(expected, actual);
    }

}
