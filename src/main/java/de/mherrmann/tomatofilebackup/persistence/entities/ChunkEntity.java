package de.mherrmann.tomatofilebackup.persistence.entities;

import de.mherrmann.tomatofilebackup.chunking.Chunk;

public class ChunkEntity extends Chunk {
    private String uuid;

    public ChunkEntity(String uuid, Chunk chunk) {
        super(chunk.getOffset(), chunk.getLength());
        setChecksum(chunk.getChecksum());
        this.uuid = uuid;
    }

    public String getUuid() {
        return uuid;
    }
}
