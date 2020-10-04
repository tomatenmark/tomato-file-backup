package de.mherrmann.tomatofilebackup.filetransfer;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

public class CompressionEngineTest {

    private final byte[] unzipped = get1024Zeros();
    private final byte[] zipped = getZipped();
    private File testFile;

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeEach
    public void setUp() throws IOException {
        testFile = new File("./test.gz");
        testFile.createNewFile();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @AfterEach
    public void tearDown() {
        testFile = new File("./test.gz");
        testFile.delete();
    }

    @Test
    public void shouldZip() throws IOException {
        FileOutputStream fos = new FileOutputStream(testFile);
        CompressionEngine.zip(unzipped, fos);

        assertArrayEquals(zipped, Files.readAllBytes(testFile.toPath()));
    }

    @Test
    public void shouldUnzip() throws IOException {
        Files.write(testFile.toPath(), zipped);

        byte[] bytesUncompressed = CompressionEngine.unzip(testFile, unzipped.length);

        assertArrayEquals(unzipped, bytesUncompressed);
    }

    private byte[] get1024Zeros(){
        byte[] bytes = new byte[1024];
        Arrays.fill(bytes, (byte) 48); //48 is ascii for 0 (hex: 30)
        return bytes;
    }

    private byte[] getZipped() {
        return new byte[]{
                31, -117, 8, 0, 0, 0, 0, 0, 0, 0, 51, 48, 24, 5, -93, 96, 20, -116, 84, 0, 0, -4, 118, -34, -84, 0, 4, 0, 0
        };
    }
}
