package de.mherrmann.tomatofilebackup.chunking;

import org.apache.commons.codec.digest.MurmurHash3;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class ChecksumEngine {

    private final AtomicLong processedChecksums;
    private final AtomicLong chunkCount;
    private final AtomicBoolean finished;

    ChecksumEngine(AtomicLong processedChecksums, AtomicLong chunkCount,
                          AtomicBoolean finished) {
        this.processedChecksums = processedChecksums;
        this.chunkCount = chunkCount;
        this.finished = finished;
    }

    void setChecksum(byte[] bytes, Chunk chunk){
        int start = (int)chunk.getOffset();
        int length = chunk.getLength();
        Thread thread = new Thread(() -> setChecksumInSubThread(bytes, start, length, chunk));
        thread.start();
    }

    static String getChecksum(byte[] bytes, int start, int length){
        long[] result = MurmurHash3.hash128x64(bytes, start, length, 0);
        return String.format("%016X%016X", result[0], result[1]);
    }

    private void setChecksumInSubThread(byte[] bytes, int start, int length, Chunk chunk){
        chunk.setChecksum(getChecksum(bytes, start, length));
        long finishedChecksums = this.processedChecksums.incrementAndGet();
        if(finishedChecksums == chunkCount.get()){
            synchronized (finished){
                finished.set(true);
                finished.notify();
            }
        }
    }

}
