package de.mherrmann.tomatofilebackup.filetransfer;

import de.mherrmann.tomatofilebackup.chunking.Chunk;

import java.io.*;
import java.nio.channels.FileChannel;

public class StoreChunkEngine {

    private StoreChunkEngine(){}

    public static void storeChunk(File source, String targetDirectoryPath, Chunk chunk) throws IOException {
        File target = new File(targetDirectoryPath+File.separator+chunk.getChecksum());
        boolean created = target.createNewFile();
        if(!created){
            throw new IOException("Unknown Error - Chunk file was not created. Checksum: " + chunk.getChecksum());
        }
        try (
                FileInputStream fileInputStream = new FileInputStream(source);
                FileOutputStream fileOutputStream = new FileOutputStream(target)
        ){
            if(chunk.isCompressed()){
                byte[] chunkBytes = new byte[chunk.getLength()];
                fileInputStream.skip(chunk.getOffset());
                fileInputStream.read(chunkBytes);
                CompressionEngine.zip(chunkBytes, fileOutputStream);
            } else {
                storeChunkWithoutCompression(fileInputStream, fileOutputStream, chunk);
            }
        } catch (IOException exception) {
            throw new IOException("Error while storing chunk " + chunk.getChecksum(), exception);
        }
    }

    private static void storeChunkWithoutCompression(FileInputStream source, FileOutputStream target, Chunk chunk) throws IOException {
        try (
                FileChannel sourceChannel = source.getChannel();
                FileChannel targetChannel = target.getChannel()
        ) {
            sourceChannel.transferTo(chunk.getOffset(), chunk.getLength(), targetChannel);
        } catch (IOException exception) {
            throw new IOException("Error while storing chunk using file channels. Chunk: " + chunk.getChecksum(), exception);
        }
    }

}
