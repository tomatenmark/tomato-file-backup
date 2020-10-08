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
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

class TransferEngineTest {

    private File sourceFile;
    private final File targetDirectory = new File("./test");

    @BeforeEach
    void setUp() {
        TestUtil.createTestDirectory();
    }

    @AfterEach
    void tearDown(){
        TestUtil.removeTestFiles();
    }

    @Test
    void shouldStoreChunkFromListToTestDirectoryUncompressed() throws Exception {
        sourceFile = TestUtil.buildRandomTestFile(5*1024*1024);
        Chunk chunk = prepareChunk();
        String targetPath = targetDirectory.getAbsolutePath();
        TransferEngine engine = new TransferEngine();
        RandomAccessFile source = new RandomAccessFile(sourceFile, "r");
        FileChannel sourceChannel = source.getChannel();
        List<Chunk> chunks = new ArrayList<>();
        chunks.add(chunk);

        engine.storeChunks(source, sourceChannel, targetPath, chunks, false);

        assertValidStored(chunk, false);
    }

    @Test
    void shouldStoreChunkFromListToTestDirectoryCompressed() throws Exception {
        sourceFile = TestUtil.buildTestFileWithZeroChars(5*1024*1024);
        Chunk chunk = prepareChunk();
        TransferEngine engine = new TransferEngine();
        RandomAccessFile source = new RandomAccessFile(sourceFile, "r");
        FileChannel sourceChannel = source.getChannel();
        List<Chunk> chunks = new ArrayList<>();
        chunks.add(chunk);

        engine.storeChunks(source, sourceChannel, targetDirectory.getAbsolutePath(), chunks, true);

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
            bytes = CompressionEngine.restoreDecompressed(file, chunk.getLength());
        }
        assertEquals(chunk.getLength(), bytes.length);
        String checksum = ChecksumEngine.getChecksum(bytes, 0, bytes.length);
        assertEquals(chunk.getChecksum(), checksum);
    }

    private Chunk prepareChunk() throws IOException {
        Chunk chunk = new Chunk(368123, 468346);
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
