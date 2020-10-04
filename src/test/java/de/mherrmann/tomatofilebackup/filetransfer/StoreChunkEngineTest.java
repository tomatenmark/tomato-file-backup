package de.mherrmann.tomatofilebackup.filetransfer;

import static org.junit.jupiter.api.Assertions.*;

import de.mherrmann.tomatofilebackup.TestUtil;
import de.mherrmann.tomatofilebackup.chunking.ChecksumEngine;
import de.mherrmann.tomatofilebackup.chunking.Chunk;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class StoreChunkEngineTest {

    private File sourceFile;
    private final File targetDirectory = new File("./test");

    @BeforeEach
    public void setUp() throws Exception {
        sourceFile = TestUtil.buildRandomTestFile(5*1024*1024);
        TestUtil.createTestDirectory();
    }

    @AfterEach
    public void tearDown(){
        TestUtil.removeTestFiles();
    }

    @Test
    public void shouldStoreChunkToTestDirectoryUncompressed() throws IOException {
        Chunk chunk = prepareChunk(false);

        StoreChunkEngine.storeChunk(sourceFile, targetDirectory.getAbsolutePath(), chunk);

        assertValidStored(chunk, false);
    }

    @Test
    public void shouldStoreChunkToTestDirectoryCompressed() throws IOException {
        Chunk chunk = prepareChunk(true);

        StoreChunkEngine.storeChunk(sourceFile, targetDirectory.getAbsolutePath(), chunk);

        assertValidStored(chunk, true);
    }

    private void assertValidStored(Chunk chunk, boolean compressed) throws IOException {
        File file = new File(targetDirectory.getAbsolutePath()+"/"+chunk.getChecksum());
        assertTrue(file.exists());
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        byte[] bytes = new byte[(int) randomAccessFile.length()];
        randomAccessFile.read(bytes);
        if(compressed){
            assertNotEquals(bytes.length, chunk.getLength());
            bytes = CompressionEngine.unzip(file, chunk.getLength());
        }
        assertEquals(chunk.getLength(), bytes.length);
        String checksum = ChecksumEngine.getChecksum(bytes, 0, bytes.length);
        assertEquals(chunk.getChecksum(), checksum);
    }

    private Chunk prepareChunk(boolean compress) throws IOException {
        Chunk chunk = new Chunk(368123, 468346);
        chunk.setCompressed(compress);
        byte[] bytes = getChunkBytes(chunk);
        chunk.setChecksum(ChecksumEngine.getChecksum(bytes, 0, bytes.length));
        return chunk;
    }

    private byte[] getChunkBytes(Chunk chunk) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(sourceFile, "r");
        randomAccessFile.seek(chunk.getOffset());
        byte[] bytes = new byte[chunk.getLength()];
        randomAccessFile.read(bytes);
        return bytes;
    }

}
