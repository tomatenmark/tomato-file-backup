package de.mherrmann.tomatofilebackup.filetransfer;

import de.mherrmann.tomatofilebackup.chunking.Chunk;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.List;

public class TransferEngine {

    public void storeChunks(File sourceFile, File chunksDirectory,
                            List<Chunk> chunks, boolean compress) throws IOException {
        String chunksDirectoryPath = chunksDirectory.getAbsolutePath();
        try (
                RandomAccessFile sourceRandomAccessFile = new RandomAccessFile(sourceFile, "r");
                FileChannel sourceChannel = sourceRandomAccessFile.getChannel()
        ){
            for (Chunk chunk : chunks) {
                storeChunk(sourceRandomAccessFile, sourceChannel, chunksDirectoryPath, chunk, compress);
            }
        } catch(IOException exception){
            throw new IOException("Error: Could not store chunks", exception);
        }
    }

    public void restoreFile(File targetFile, File chunksDirectory,
                            List<Chunk> chunks, boolean compress) throws IOException {
        String chunksDirectoryPath = chunksDirectory.getAbsolutePath();
        if(targetFile.exists()){
            Files.delete(targetFile.toPath());
        }
        boolean created = targetFile.createNewFile();
        if(!created){
            throw new IOException("Unknown Error - File to restore was not created.");
        }
        try (
            RandomAccessFile targetRandomAccessFile = new RandomAccessFile(targetFile, "rw");
            FileChannel targetChannel = targetRandomAccessFile.getChannel()
        ){
            for (Chunk chunk : chunks) {
                restoreChunk(targetRandomAccessFile, targetChannel, chunksDirectoryPath, chunk, compress);
            }
        } catch(IOException exception){
            throw new IOException("Error: Could not restore file", exception);
        }
    }

    private void storeChunk(RandomAccessFile source, FileChannel sourceChannel,
                           String chunksDirectoryPath, Chunk chunk, boolean compress) throws IOException {
        File chunkFile = new File(chunksDirectoryPath, chunk.getChecksum());
        boolean created = chunkFile.createNewFile();
        if(!created){
            throw new IOException("Unknown Error - Chunk file was not created. Checksum: " + chunk.getChecksum());
        }
        try (
                RandomAccessFile chunkRandomAccessFile = new RandomAccessFile(chunkFile, "rw");
                FileChannel chunkChannel = chunkRandomAccessFile.getChannel()
        ){
            if(compress){
                byte[] chunkBytes = new byte[chunk.getLength()];
                source.seek(chunk.getOffset());
                source.read(chunkBytes);
                CompressionEngine.storeCompressed(chunkBytes, chunkFile);
            } else {
                sourceChannel.transferTo(chunk.getOffset(), chunk.getLength(), chunkChannel);
            }
        } catch (IOException exception) {
            throw new IOException("Error while storing chunk " + chunk.getChecksum(), exception);
        }
    }

    private void restoreChunk(RandomAccessFile target, FileChannel targetChannel,
                              String chunksDirectoryPath, Chunk chunk, boolean compress) throws IOException {
        File chunkFile = new File(chunksDirectoryPath, chunk.getChecksum());
        try (
                RandomAccessFile chunkRandomAccessFile = new RandomAccessFile(chunkFile, "r");
                FileChannel chunkChannel = chunkRandomAccessFile.getChannel()
        ){
            if(compress){
                byte[] chunkBytes = CompressionEngine.restoreDecompressed(chunkFile, chunk.getLength());
                target.write(chunkBytes);
            } else {
                targetChannel.transferFrom(chunkChannel, chunk.getOffset(), chunk.getLength());
            }
        } catch (IOException exception) {
            throw new IOException("Error while restoring chunk " + chunk.getChecksum(), exception);
        }
    }

}
