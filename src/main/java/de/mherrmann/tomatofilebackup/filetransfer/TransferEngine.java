package de.mherrmann.tomatofilebackup.filetransfer;

import de.mherrmann.tomatofilebackup.chunking.Chunk;

import java.io.*;
import java.nio.channels.FileChannel;

public class TransferEngine {

    public void storeChunk(RandomAccessFile source, FileChannel sourceChannel,
                           String targetDirectoryPath, Chunk chunk) throws IOException {
        File targetFile = new File(targetDirectoryPath, chunk.getChecksum());
        boolean created = targetFile.createNewFile();
        if(!created){
            throw new IOException("Unknown Error - Chunk file was not created. Checksum: " + chunk.getChecksum());
        }
        try (
                RandomAccessFile targetRandomAccessFile = new RandomAccessFile(targetFile, "rw");
                FileChannel targetChannel = targetRandomAccessFile.getChannel()
        ){
            if(chunk.isCompressed()){
                byte[] chunkBytes = new byte[chunk.getLength()];
                source.seek(chunk.getOffset());
                source.read(chunkBytes);
                CompressionEngine.storeCompressed(chunkBytes, targetFile);
            } else {
                sourceChannel.transferTo(chunk.getOffset(), chunk.getLength(), targetChannel);
            }
        } catch (IOException exception) {
            throw new IOException("Error while storing chunk " + chunk.getChecksum(), exception);
        }
    }

}
