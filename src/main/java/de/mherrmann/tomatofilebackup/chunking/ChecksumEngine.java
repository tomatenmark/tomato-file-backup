package de.mherrmann.tomatofilebackup.chunking;

import org.apache.commons.codec.digest.MurmurHash3;

class ChecksumEngine {

    private ChecksumEngine(){}

    /**
     * Creates a checksum using MurMurHash3
     * (link)
     * @param bytes - the bytes to hash
     * @param start - start at index <code>start</code>
     * @param length - use <code>length</code> bytes beginning at <code>start</code>
     * @return the MurMurHash3 checksum as upper-case hex-string
     */
    static String getChecksum(byte[] bytes, int start, int length){
        long[] result = MurmurHash3.hash128x64(bytes, start, length, 0);
        return String.format("%016X%016X", result[0], result[1]);
    }

}
