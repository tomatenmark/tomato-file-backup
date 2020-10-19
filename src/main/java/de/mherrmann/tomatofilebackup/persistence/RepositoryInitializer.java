package de.mherrmann.tomatofilebackup.persistence;

import de.mherrmann.tomatofilebackup.Constants;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.*;
import java.util.UUID;

public class RepositoryInitializer {

    private RepositoryInitializer(){}

    public static void initialize(String repositoryPath) throws IOException, SQLException {
        initializeRepositoryDirectory(repositoryPath);
        Connection connection = createDb(repositoryPath);
        createTables(connection);
        putRepositoryData(connection, repositoryPath);
    }

    private static void initializeRepositoryDirectory(String repositoryPath) throws IOException {
        File repositoryDirectory = new File(repositoryPath);
        if(repositoryDirectory.isFile()){
            throw new IOException("Error: Repository path is not directory.");
        }
        if(!repositoryDirectory.exists()){
            Files.createDirectory(repositoryDirectory.toPath());
        }
    }

    private static Connection createDb(String repositoryPath) throws SQLException {
        File dbFile = new File(repositoryPath, Constants.DB_FILENAME);
        String url = "jdbc:sqlite:"+dbFile.getAbsolutePath();
        return DriverManager.getConnection(url);
    }

    private static void createTables(Connection connection) throws SQLException {
        createChunkTable(connection);
        createFileTable(connection);
        createSnapshotTable(connection);
        createRepositoryTable(connection);
        createFileChunkRelationTable(connection);
        createFileSnapshotRelationTable(connection);
    }

    private static void createChunkTable(Connection connection) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS chunk(" +
                  "chunk_uuid text PRIMARY KEY," +
                  "checksum text," +
                  "length integer" +
                ");";
        Statement statement = connection.createStatement();
        statement.execute(sql);
    }

    private static void createFileTable(Connection connection) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS file(" +
                  "file_uuid text PRIMARY KEY," +
                  "path text," +
                  "size integer," +
                  "inode integer, " +
                  "mtime integer," +
                  "compressed integer," +
                  "link integer," +
                  "junction integer," +
                  "directory integer," +
                  "link_path text" +
                ");";
        Statement statement = connection.createStatement();
        statement.execute(sql);
    }

    private static void createSnapshotTable(Connection connection) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS snapshot(" +
                  "snapshot_uuid text PRIMARY KEY," +
                  "hash_id text," +
                  "source text," +
                  "host text," +
                  "ctime integer" +
                ");";
        Statement statement = connection.createStatement();
        statement.execute(sql);
    }

    private static void createRepositoryTable(Connection connection) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS repository(" +
                  "repository_uuid text PRIMARY KEY," +
                  "path text," +
                  "version text" +
                ");";
        Statement statement = connection.createStatement();
        statement.execute(sql);
    }

    private static void createFileChunkRelationTable(Connection connection) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS file_chunk_relation(" +
                  "relation_uuid text PRIMARY KEY," +
                  "file_uuid text REFERENCES file(file_uuid) ON DELETE CASCADE," +
                  "chunk_uuid text REFERENCES chunk(chunk_uuid) ON DELETE CASCADE," +
                  "offset integer" +
                ");";
        Statement statement = connection.createStatement();
        statement.execute(sql);
    }

    private static void createFileSnapshotRelationTable(Connection connection) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS file_snapshot_relation(" +
                  "relation_uuid text PRIMARY KEY," +
                  "file_uuid text REFERENCES file(file_uuid) ON DELETE CASCADE," +
                  "snapshot_uuid text REFERENCES snapshot(snapshot_uuid) ON DELETE CASCADE" +
                ");";
        Statement statement = connection.createStatement();
        statement.execute(sql);
    }

    private static void putRepositoryData(Connection connection, String repositoryPath) throws SQLException {
        String uuid = UUID.randomUUID().toString();
        String sql = "INSERT INTO repository(repository_uuid,path,version) VALUES(?,?,?)";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, uuid);
        preparedStatement.setString(2, repositoryPath);
        preparedStatement.setString(3, Constants.VERSION);
        preparedStatement.executeUpdate();
    }

}
