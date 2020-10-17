package de.mherrmann.tomatofilebackup.persistence;

import de.mherrmann.tomatofilebackup.chunking.Chunk;
import de.mherrmann.tomatofilebackup.persistence.entities.ChunkEntity;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ChunkDatabaseEngine {

    Connection connection;

    public ChunkDatabaseEngine(Connection connection) {
        this.connection = connection;
    }

    public ChunkEntity addChunk(Chunk chunk, String fileUuid, int ordinal) throws SQLException {
        String chunkUuid = UUID.randomUUID().toString();
        String sql = "INSERT INTO chunk(chunk_uuid,checksum,offset,length) VALUES(?,?,?,?);";
        try {
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, chunkUuid);
            preparedStatement.setString(2, chunk.getChecksum());
            preparedStatement.setLong(3, chunk.getOffset());
            preparedStatement.setInt(4, chunk.getLength());
            preparedStatement.executeUpdate();
            addChunkFileRelation(fileUuid, chunkUuid, ordinal);
            connection.commit();
        } catch(SQLException exception){
            connection.rollback();
            throw new SQLException("Error: Could not add chunk. checksum: " + chunk.getChecksum(), exception);
        }
        connection.setAutoCommit(true);
        return getChunkByChecksum(chunk.getChecksum());
    }

    public void addChunkFileRelation(String fileUuid, String chunkUuid, int ordinal) throws SQLException {
        String relationUuid = UUID.randomUUID().toString();
        String sql = "INSERT INTO file_chunk_relation(relation_uuid,file_uuid,chunk_uuid,ordinal) VALUES(?,?,?,?);";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, relationUuid);
        preparedStatement.setString(2, fileUuid);
        preparedStatement.setString(3, chunkUuid);
        preparedStatement.setInt(4, ordinal);
        preparedStatement.executeUpdate();
    }

    public ChunkEntity getChunkByChecksum(String checksum) throws SQLException {
        String sql = "SELECT * FROM chunk WHERE checksum = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, checksum);
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        Chunk chunk = new Chunk(resultSet.getLong("offset"), resultSet.getInt("length"));
        chunk.setChecksum(checksum);
        return new ChunkEntity(resultSet.getString("chunk_uuid"), chunk);
    }

    public List<ChunkEntity> getChunksByFileUuid(String fileUuid) throws SQLException {
        List<ChunkEntity> chunks = new ArrayList<>();
        String sql = "SELECT chunk.* From chunk " +
                "LEFT JOIN file_chunk_relation USING(chunk_uuid) " +
                "LEFT JOIN file USING(file_uuid) " +
                "WHERE file_uuid = ? " +
                "ORDER BY ordinal";
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

    public boolean existsChunkByChecksum(String checksum) throws SQLException {
        String sql = "SELECT * FROM chunk WHERE checksum = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, checksum);
        ResultSet resultSet = preparedStatement.executeQuery();
        return !resultSet.isClosed();
    }
}
