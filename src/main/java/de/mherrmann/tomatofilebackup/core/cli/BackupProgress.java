package de.mherrmann.tomatofilebackup.core.cli;

public class BackupProgress extends Progress {
    private volatile long totalBytes;
    private volatile long totalFiles;
    private volatile long fileBytes;
    private volatile long totalBytesProcessed;
    private volatile long fileBytesChunked;
    private volatile long fileBytesWritten;
    private volatile String currentFileChunking;
    private volatile String currentFileWriting;

    public BackupProgress(){
        super(3);
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    public void setTotalBytes(long totalBytes) {
        this.totalBytes = totalBytes;
    }

    public long getTotalFiles() {
        return totalFiles;
    }

    public void setTotalFiles(long totalFiles) {
        this.totalFiles = totalFiles;
    }

    public long getFileBytes() {
        return fileBytes;
    }

    public void setFileBytes(long fileBytes) {
        this.fileBytes = fileBytes;
    }

    public long getTotalBytesProcessed() {
        return totalBytesProcessed;
    }

    public void setTotalBytesProcessed(long totalBytesProcessed) {
        this.totalBytesProcessed = totalBytesProcessed;
    }

    public long getFileBytesChunked() {
        return fileBytesChunked;
    }

    public void setFileBytesChunked(long fileBytesChunked) {
        this.fileBytesChunked = fileBytesChunked;
    }

    public long getFileBytesWritten() {
        return fileBytesWritten;
    }

    public void setFileBytesWritten(long fileBytesWritten) {
        this.fileBytesWritten = fileBytesWritten;
    }

    public String getCurrentFileChunking() {
        return currentFileChunking;
    }

    public void setCurrentFileChunking(String currentFileChunking) {
        this.currentFileChunking = currentFileChunking;
    }

    public String getCurrentFileWriting() {
        return currentFileWriting;
    }

    public void setCurrentFileWriting(String currentFileWriting) {
        this.currentFileWriting = currentFileWriting;
    }

    public String buildProgressLines() {
        return String.format("Running backup...\n" +
                    "Finished %s / %s (%s %%)\n" +
                    " Chunking %s\n" +
                    "   %s / %s (%s %%)\n" +
                    " Storing %s\n" +
                    "   %s / %s (%s %%)\n",
                    getFormattedBytes(totalBytesProcessed),
                    getFormattedBytes(totalBytes),
                    getPercent(totalBytesProcessed, totalBytes),
                    getShortPath(currentFileChunking),
                    getFormattedBytes(fileBytesChunked),
                    getFormattedBytes(fileBytes),
                    getPercent(fileBytesChunked, fileBytes),
                    getShortPath(currentFileWriting),
                    getFormattedBytes(fileBytesWritten),
                    getFormattedBytes(fileBytes),
                    getPercent(fileBytesWritten, fileBytes)
                );
    }
}
