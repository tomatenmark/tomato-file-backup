package de.mherrmann.tomatofilebackup.chunking;

import static org.junit.jupiter.api.Assertions.*;

import de.mherrmann.tomatofilebackup.TestUtil;
import org.junit.jupiter.api.Test;

class FastCDCTest {

    @Test
    void shouldGetFirstChunk() {
        byte[] source = TestUtil.buildBiggerReproducibleRandomTestBytes();
        FastCDC fastCDC = new FastCDC(source, ChunkingEngine.MIN_CHUNK_SIZE, ChunkingEngine.AVG_CHUNK_SIZE, ChunkingEngine.MAX_CHUNK_SIZE);

        Chunk chunk1 = fastCDC.next();
        Chunk chunk2 = fastCDC.next();

        assertEquals(0, chunk1.getOffset());
        assertEquals(769557, chunk1.getLength());
        assertEquals(769557, chunk2.getOffset());
        assertEquals(1239023, chunk2.getLength());
    }

    @Test
    void shouldHaveNext() {
        byte[] source = TestUtil.buildBiggerReproducibleRandomTestBytes();
        FastCDC fastCDC = new FastCDC(source, ChunkingEngine.MIN_CHUNK_SIZE, ChunkingEngine.AVG_CHUNK_SIZE, ChunkingEngine.MAX_CHUNK_SIZE);

        boolean hasNext = fastCDC.hasNext();

        assertTrue(hasNext);
    }

    @Test
    void shouldNotHaveNext() {
        byte[] source = TestUtil.buildBiggerReproducibleRandomTestBytes();
        FastCDC fastCDC = new FastCDC(source, ChunkingEngine.MIN_CHUNK_SIZE, ChunkingEngine.AVG_CHUNK_SIZE, ChunkingEngine.MAX_CHUNK_SIZE);
        fastCDC.next();
        fastCDC.next();

        boolean hasNext = fastCDC.hasNext();

        assertFalse(hasNext);
    }

}
