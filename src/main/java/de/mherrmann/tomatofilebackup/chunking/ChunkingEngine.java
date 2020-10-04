package de.mherrmann.tomatofilebackup.chunking;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ChunkingEngine {

    private static final int KB = 1024;
    private static final int MB = 1024*KB;
    static final int PORTION = 16*MB;
    static final int MIN_CHUNK_SIZE = 512*KB;
    static final int AVG_CHUNK_SIZE = MB;
    static final int MAX_CHUNK_SIZE = 8*MB;

    private RandomAccessFile file;
    private long fileProcessed;
    private long chunkingProcessed;
    private final AtomicLong processedChecksums;
    private final AtomicLong chunkCount;
    private final AtomicBoolean finished;
    private List<Chunk> chunks;

    private byte[] previousPortion;
    private int previousPortionReuse;

    private boolean compress;

    public ChunkingEngine(){
        processedChecksums = new AtomicLong(0);
        chunkCount = new AtomicLong(-1);
        finished = new AtomicBoolean(false);
    }

    public List<Chunk> getChunks(RandomAccessFile file, boolean compress) throws IOException {
        init(file, compress);
        chunks = new ArrayList<>();
        long length = file.length();
        if(length <= MIN_CHUNK_SIZE){
            findChunksForSmallSizeFile((int)length);
        } else if(length <= PORTION){
            findChunksForMediumSizeFile((int)length);
        } else {
            findChunksForLargeSizeFile();
        }
        waitForChecksums();
        return chunks;
    }

    private void init(RandomAccessFile file, boolean compress) {
        this.file = file;
        fileProcessed = 0;
        chunkingProcessed = 0;
        processedChecksums.set(0);
        chunkCount.set(-1);
        finished.set(false);
        previousPortion = new byte[]{};
        previousPortionReuse = 0;
        this.compress = compress;
    }

    private void findChunksForSmallSizeFile(int length) throws IOException {
        Chunk chunk = new Chunk(0, length);
        byte[] source = getNextFilePartArrayForMediumSizeFile(file, length);
        addChunk(chunk, source, true);
    }

    private void findChunksForMediumSizeFile(int length) throws IOException {
        byte[] source = getNextFilePartArrayForMediumSizeFile(file, length);
        processForMediumSizeFile(source);
    }

    private void findChunksForLargeSizeFile() throws IOException {
        boolean lastChunk = false;
        while (!lastChunk){
            lastChunk = processNextPortion();
        }
    }

    private boolean processNextPortion() throws IOException {
        byte[] source = getNextFilePartArrayForLargeSizeFile();
        if(source.length == 0){
            return true;
        }
        boolean lastPortion = file.length() == fileProcessed;
        processForLargeSizeFilePortion(source, lastPortion);
        return lastPortion;
    }

    private void processForMediumSizeFile(byte[] source){
        FastCDC fastCDC = new FastCDC(source, MIN_CHUNK_SIZE, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE);
        while(fastCDC.hasNext()){
            Chunk chunk = fastCDC.next();
            addChunk(chunk, source, true);
        }
    }

    private void processForLargeSizeFilePortion(byte[] source, boolean lastPortion){
        FastCDC fastCDC = new FastCDC(source, MIN_CHUNK_SIZE, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE);
        while(fastCDC.hasNext()){
            Chunk chunk = fastCDC.next();
            long processedFromPortion = chunk.getOffset()+chunk.getLength();
            long remaining = source.length - processedFromPortion;
            addChunk(chunk, source, lastPortion && !fastCDC.hasNext());
            if(remaining < MAX_CHUNK_SIZE && !lastPortion){
                previousPortion = source;
                previousPortionReuse = (int) (source.length-processedFromPortion);
                chunkingProcessed += processedFromPortion;
                break;
            }
        }
    }

    private byte[] getNextFilePartArrayForLargeSizeFile() throws IOException {
        long remaining = file.length() - fileProcessed;
        int nextPortionSize = (int)Math.min(PORTION, remaining);
        byte[] bytes = new byte[nextPortionSize + previousPortionReuse];
        if(previousPortionReuse > 0){
            System.arraycopy(previousPortion, previousPortion.length-previousPortionReuse, bytes, 0, previousPortionReuse);
        }
        file.read(bytes, previousPortionReuse, nextPortionSize);
        fileProcessed += nextPortionSize;
        return bytes;
    }

    private byte[] getNextFilePartArrayForMediumSizeFile(RandomAccessFile file, int length) throws IOException {
        byte[] bytes = new byte[length];
        file.read(bytes);
        return bytes;
    }

    private void addChunk(Chunk chunk, byte[] source, boolean lastChunk){
        chunk.setCompressed(compress);
        chunks.add(chunk);
        if(lastChunk){
            chunkCount.set(chunks.size());
        }
        setChecksum(source, chunk);
        chunk.addProcessedOffset(chunkingProcessed);
    }

    private void setChecksum(byte[] bytes, Chunk chunk){
        int start = (int)chunk.getOffset();
        int length = chunk.getLength();
        Thread thread = new Thread(() -> setChecksumInSubThread(bytes, start, length, chunk));
        thread.start();
    }

    private void setChecksumInSubThread(byte[] bytes, int start, int length, Chunk chunk){
        chunk.setChecksum(ChecksumEngine.getChecksum(bytes, start, length));
        long finishedChecksums = this.processedChecksums.incrementAndGet();
        if(finishedChecksums == chunkCount.get()){
            synchronized (finished){
                finished.set(true);
                finished.notifyAll();
            }
        }
    }

    private void waitForChecksums() {
        try {
            synchronized(finished) {
                while(!finished.get()){
                    finished.wait();
                }
            }
        } catch(InterruptedException ignored){
            Thread.currentThread().interrupt();
        }
    }
}
