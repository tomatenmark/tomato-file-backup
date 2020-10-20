package de.mherrmann.tomatofilebackup.persistence;

import de.mherrmann.tomatofilebackup.chunking.Chunk;
import de.mherrmann.tomatofilebackup.persistence.entities.ChunkEntity;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

class ChunkDatabaseEngine {

    private final Connection connection;

    ChunkDatabaseEngine(Connection connection) {
        this.connection = connection;
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    ChunkEntity addChunk(Chunk chunk, String fileUuid) throws SQLException {
        String chunkUuid = UUID.randomUUID().toString();
        String sql = "INSERT INTO chunk(chunk_uuid,checksum,length) VALUES(?,?,?);";
        try {
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, chunkUuid);
            preparedStatement.setString(2, chunk.getChecksum());
            preparedStatement.setInt(3, chunk.getLength());
            preparedStatement.executeUpdate();
            addChunkFileRelation(fileUuid, chunkUuid, chunk.getOffset());
            connection.commit();
        } catch(SQLException exception){
            connection.rollback();
            throw new SQLException("Error: Could not add chunk. checksum: " + chunk.getChecksum(), exception);
        }
        connection.setAutoCommit(true);
        return getChunkByChecksum(chunk.getChecksum()).get();
    }

    void addChunkFileRelation(String fileUuid, String chunkUuid, long offset) throws SQLException {
        String relationUuid = UUID.randomUUID().toString();
        String sql = "INSERT INTO file_chunk_relation(relation_uuid,file_uuid,chunk_uuid,offset) VALUES(?,?,?,?);";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, relationUuid);
        preparedStatement.setString(2, fileUuid);
        preparedStatement.setString(3, chunkUuid);
        preparedStatement.setLong(4, offset);
        preparedStatement.executeUpdate();
    }

    Optional<ChunkEntity> getChunkByChecksum(String checksum) throws SQLException {
        String sql = "SELECT chunk.*, offset FROM chunk " +
                "LEFT JOIN file_chunk_relation USING (chunk_uuid)" +
                "WHERE checksum = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, checksum);
        ResultSet resultSet = preparedStatement.executeQuery();
        if(!resultSet.next()){
            return Optional.empty();
        }
        Chunk chunk = new Chunk(resultSet.getLong("offset"), resultSet.getInt("length"));
        chunk.setChecksum(checksum);
        return Optional.of(new ChunkEntity(resultSet.getString("chunk_uuid"), chunk));
    }

    List<ChunkEntity> getChunksByFileUuid(String fileUuid) throws SQLException {
        List<ChunkEntity> chunks = new ArrayList<>();
        String sql = "SELECT chunk.*, offset From chunk " +
                "LEFT JOIN file_chunk_relation USING(chunk_uuid) " +
                "LEFT JOIN file USING(file_uuid) " +
                "WHERE file_uuid = ? " +
                "ORDER BY offset";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, fileUuid);
        ResultSet resultSet = preparedStatement.executeQuery();
        while(resultSet.next()){
            Chunk chunk = new Chunk(resultSet.getLong("offset"), resultSet.getInt("length"));
            chunk.setChecksum(resultSet.getString("checksum"));
            chunks.add(new ChunkEntity(resultSet.getString("chunk_uuid"), chunk));
        }
        return chunks;
    }

    List<String> removeOrphanedChunks() throws SQLException {
        List<String> checksums = new ArrayList<>();
        String getSql = "SELECT checksum FROM chunk WHERE chunk_uuid IN (" +
                "SELECT chunk.chunk_uuid FROM chunk " +
                "LEFT JOIN file_chunk_relation USING (chunk_uuid) " +
                "WHERE file_uuid IS NULL" +
                ")";
        Statement getStatement = connection.createStatement();
        ResultSet resultSet = getStatement.executeQuery(getSql);
        while(resultSet.next()){
            checksums.add(resultSet.getString("checksum"));
        }
        try {
            String removeSql = "DELETE FROM chunk WHERE chunk_uuid IN (" +
                    "SELECT chunk.chunk_uuid FROM chunk " +
                    "LEFT JOIN file_chunk_relation USING (chunk_uuid) " +
                    "WHERE file_uuid IS NULL" +
                    ")";
            Statement removeStatement = connection.createStatement();
            removeStatement.executeUpdate(removeSql);
        } catch(SQLException exception){
            throw new SQLException("Could not remove orphaned chunks.", exception);
        }

        return checksums;
    }
}
