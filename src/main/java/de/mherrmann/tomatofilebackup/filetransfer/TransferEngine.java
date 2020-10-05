package de.mherrmann.tomatofilebackup.filetransfer;

import de.mherrmann.tomatofilebackup.chunking.Chunk;

import java.io.*;

public class TransferEngine {

    public void storeChunk(byte[] chunkBytes, String targetDirectoryPath, Chunk chunk) throws IOException {
        File targetFile = new File(targetDirectoryPath, chunk.getChecksum());
        boolean created = targetFile.createNewFile();
        if(!created){
            throw new IOException("Unknown Error - Chunk file was not created. Checksum: " + chunk.getChecksum());
        }
        try (
                RandomAccessFile targetRandomAccessFile = new RandomAccessFile(targetFile, "rw")
        ){
            if(chunk.isCompressed()){
                CompressionEngine.storeCompressed(chunkBytes, targetFile);
            } else {
                targetRandomAccessFile.write(chunkBytes);
            }
        } catch (IOException exception) {
            throw new IOException("Error while storing chunk " + chunk.getChecksum(), exception);
        }
    }

}
