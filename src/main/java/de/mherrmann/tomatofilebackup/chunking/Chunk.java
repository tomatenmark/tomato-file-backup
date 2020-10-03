package de.mherrmann.tomatofilebackup.chunking;

public class Chunk {
    private long offset;
    private final int length;
    private String checksum;
    private boolean compressed;

    public Chunk(long offset, int length) {
        this.offset = offset;
        this.length = length;
    }

    public String getChecksum() {
        return checksum;
    }

    public void setChecksum(String checksum) {
        this.checksum = checksum;
    }

    public long getOffset() {
        return offset;
    }

    public void addProcessedOffset(long processed) {
        this.offset += processed;
    }

    public int getLength() {
        return length;
    }

    public void setCompressed(boolean compressed){
        this.compressed = compressed;
    }

    public boolean isCompressed(){
        return compressed;
    }

    @Override
    public boolean equals(Object other){
        if(!(other instanceof Chunk)){
            return false;
        }
        if(this.checksum == null){
            return false;
        }
        return (this.checksum.equals(((Chunk) other).checksum)) && this.length == ((Chunk) other).length;
    }

    @Override
    public int hashCode(){
        if(checksum == null){
            return -1;
        }
        return checksum.hashCode();
    }
}
