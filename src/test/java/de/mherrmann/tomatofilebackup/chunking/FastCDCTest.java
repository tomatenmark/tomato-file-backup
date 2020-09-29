package de.mherrmann.tomatofilebackup.chunking;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.util.Random;

public class FastCDCTest {

    @Test
    public void shouldGetFirstChunk() {
        byte[] source = buildReproducibleRandomTestBytes();
        FastCDC fastCDC = new FastCDC(source, ChunkingEngine.MIN_CHUNK_SIZE, ChunkingEngine.AVG_CHUNK_SIZE, ChunkingEngine.MAX_CHUNK_SIZE);

        Chunk chunk1 = fastCDC.next();
        Chunk chunk2 = fastCDC.next();

        assertEquals(0, chunk1.getOffset());
        assertEquals(552563, chunk1.getLength());
        assertEquals(552563, chunk2.getOffset());
        assertEquals(216994, chunk2.getLength());
    }

    @Test
    public void shouldHaveNext() {
        byte[] source = buildReproducibleRandomTestBytes();
        FastCDC fastCDC = new FastCDC(source, ChunkingEngine.MIN_CHUNK_SIZE, ChunkingEngine.AVG_CHUNK_SIZE, ChunkingEngine.MAX_CHUNK_SIZE);

        boolean hasNext = fastCDC.hasNext();

        assertTrue(hasNext);
    }

    @Test
    public void shouldNotHaveNext() {
        byte[] source = buildReproducibleRandomTestBytes();
        FastCDC fastCDC = new FastCDC(source, ChunkingEngine.MIN_CHUNK_SIZE, ChunkingEngine.AVG_CHUNK_SIZE, ChunkingEngine.MAX_CHUNK_SIZE);
        fastCDC.next();
        fastCDC.next();

        boolean hasNext = fastCDC.hasNext();

        assertFalse(hasNext);
    }

    private byte[] buildReproducibleRandomTestBytes() {
        Random random = new Random(42);
        byte[] bytes = new byte[769557];
        random.nextBytes(bytes);
        return bytes;
    }
}
