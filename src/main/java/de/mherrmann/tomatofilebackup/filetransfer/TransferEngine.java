package de.mherrmann.tomatofilebackup.filetransfer;

import de.mherrmann.tomatofilebackup.chunking.Chunk;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.List;

public class TransferEngine {

    public void storeChunks(RandomAccessFile source, String targetDirectoryPath,
                            List<Chunk> chunks, boolean compress) throws IOException {
        FileChannel sourceChannel = source.getChannel();
        for (Chunk chunk : chunks) {
            storeChunk(source, sourceChannel, targetDirectoryPath, chunk, compress);
        }
    }

    public void restoreFile(RandomAccessFile target, String sourceDirectoryPath,
                            List<Chunk> chunks, boolean compress) throws IOException {
        FileChannel targetChannel = target.getChannel();
        for (Chunk chunk : chunks) {
            restoreChunk(target, targetChannel, sourceDirectoryPath, chunk, compress);
        }
    }

    private void storeChunk(RandomAccessFile source, FileChannel sourceChannel,
                           String targetDirectoryPath, Chunk chunk, boolean compress) throws IOException {
        File targetFile = new File(targetDirectoryPath, chunk.getChecksum());
        boolean created = targetFile.createNewFile();
        if(!created){
            throw new IOException("Unknown Error - Chunk file was not created. Checksum: " + chunk.getChecksum());
        }
        try (
                RandomAccessFile targetRandomAccessFile = new RandomAccessFile(targetFile, "rw");
                FileChannel targetChannel = targetRandomAccessFile.getChannel()
        ){
            if(compress){
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

    private void restoreChunk(RandomAccessFile target, FileChannel targetChannel,
                              String sourceDirectoryPath, Chunk chunk, boolean compress) throws IOException {
        File sourceFile = new File(sourceDirectoryPath, chunk.getChecksum());
        try (
                RandomAccessFile sourceRandomAccessFile = new RandomAccessFile(sourceFile, "r");
                FileChannel sourceChannel = sourceRandomAccessFile.getChannel()
        ){
            if(compress){
                byte[] chunkBytes = CompressionEngine.restoreDecompressed(sourceFile, chunk.getLength());
                target.write(chunkBytes);
            } else {
                //sourceChannel.transferTo(chunk.getOffset(), chunk.getLength(), targetChannel);
                targetChannel.transferFrom(sourceChannel, chunk.getOffset(), chunk.getLength());
            }
        } catch (IOException exception) {
            throw new IOException("Error while restoring chunk " + chunk.getChecksum(), exception);
        }
    }

}
