package de.mherrmann.tomatofilebackup.filetransfer;

import de.mherrmann.tomatofilebackup.chunking.Chunk;

import java.io.*;

public class StoreChunkEngine {

    //private StoreChunkEngine(){}

    public void storeChunk(RandomAccessFile source, String targetDirectoryPath, Chunk chunk) throws IOException {
        File target = new File(targetDirectoryPath, chunk.getChecksum());
        boolean created = target.createNewFile();
        if(!created){
            throw new IOException("Unknown Error - Chunk file was not created. Checksum: " + chunk.getChecksum());
        }
        try (
                RandomAccessFile targetRaf = new RandomAccessFile(target, "rw")
        ){
            byte[] chunkBytes = new byte[chunk.getLength()];
            source.seek(chunk.getOffset());
            source.read(chunkBytes);
            if(chunk.isCompressed()){
                CompressionEngine.storeCompressed(chunkBytes, target);
            } else {
                targetRaf.write(chunkBytes);
            }
        } catch (IOException exception) {
            throw new IOException("Error while storing chunk " + chunk.getChecksum(), exception);
        }
    }

}
