package de.mherrmann.tomatofilebackup.filetransfer;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

class CompressionEngineTest {

    private final byte[] unzipped = get1024Zeros();
    private final byte[] zipped = getExpectedCompressed();
    private File testFile;

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeEach
    void setUp() throws IOException {
        testFile = new File("./test.gz");
        testFile.createNewFile();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @AfterEach
    void tearDown() {
        testFile = new File("./test.gz");
        testFile.delete();
    }

    @Test
    void shouldCompress() throws IOException {
        CompressionEngine.storeCompressed(unzipped, testFile);

        assertArrayEquals(zipped, Files.readAllBytes(testFile.toPath()));
    }

    @Test
    void shouldDecompress() throws IOException {
        Files.write(testFile.toPath(), zipped);

        byte[] bytesUncompressed = CompressionEngine.unzip(testFile, unzipped.length);

        assertArrayEquals(unzipped, bytesUncompressed);
    }

    private byte[] get1024Zeros(){
        byte[] bytes = new byte[1024];
        Arrays.fill(bytes, (byte) 48); //48 is ascii for 0 (hex: 30)
        return bytes;
    }

    private byte[] getExpectedCompressed() {
        return new byte[]{
                31, -117, 8, 0, 0, 0, 0, 0, 0, 0, 51, 48, 24, 5, -93, 96, 20, -116, 84, 0, 0, -4, 118, -34, -84, 0, 4, 0, 0
        };
    }
}
