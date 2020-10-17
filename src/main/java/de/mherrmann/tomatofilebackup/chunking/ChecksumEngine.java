package de.mherrmann.tomatofilebackup.chunking;

import org.apache.commons.codec.digest.MurmurHash3;

public class ChecksumEngine {

    private ChecksumEngine(){}

    /**
     * Creates a checksum for chunkBytes using MurMurHash3 (128 bits long, x64 type)
     * @param bytes - the byte array where the chunk bytes are in
     * @param start - start at index <code>start</code>
     * @param length - use <code>length</code> bytes beginning at <code>start</code>
     * @return the MurMurHash3 checksum as upper-case hex-string
     */
    public static String getChunkChecksum(byte[] bytes, int start, int length){
        long[] result = MurmurHash3.hash128x64(bytes, start, length, 0);
        return String.format("%016X%016X", result[0], result[1]);
    }

    /**
     * Creates a checksum for snapshot using MurMurHash3 (64 bits long, x64 type)
     * @param uuid - the uuid of the snapshot
     * @return the MurMurHash3 checksum as upper-case hex-string
     */
    public static String getSnapshotChecksum(String uuid){
        byte[] bytes = uuid.getBytes();
        long[] result = MurmurHash3.hash128x64(bytes, 0, bytes.length, 0);
        return String.format("%016X", result[0]);
    }

}
