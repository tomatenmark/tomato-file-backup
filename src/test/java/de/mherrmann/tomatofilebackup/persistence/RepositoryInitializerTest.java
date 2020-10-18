package de.mherrmann.tomatofilebackup.persistence;

import static org.junit.jupiter.api.Assertions.*;

import de.mherrmann.tomatofilebackup.Constants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import de.mherrmann.tomatofilebackup.TestUtil;

import java.io.File;
import java.io.IOException;
import java.sql.*;

public class RepositoryInitializerTest {

    private static final String REPO = "./test/";

    @AfterEach
    void tearDown(){
        TestUtil.removeTestFiles();
    }

    @Test
    void shouldInitializeRepository() throws IOException, SQLException {
        RepositoryInitializer.initialize(new File(REPO).getAbsolutePath());

        assertTrue(new File(REPO, Constants.DB_FILENAME).exists());
        assertValidInitialized();
    }

    private void assertValidInitialized() throws SQLException {
        File dbFile = new File(REPO, Constants.DB_FILENAME);
        String url = "jdbc:sqlite:"+dbFile.getAbsolutePath();
        Connection connection = DriverManager.getConnection(url);
        assertTablesExist(connection);
        assertColumnsExist(connection, "chunk", "chunk_uuid", "checksum", "length");
        assertColumnsExist(connection, "file", "file_uuid", "path", "size", "inode", "mtime", "compressed", "link", "directory", "link_path");
        assertColumnsExist(connection, "snapshot", "snapshot_uuid", "hash_id", "source", "host", "ctime");
        assertColumnsExist(connection, "repository", "repository_uuid", "path", "version");
        assertColumnsExist(connection, "file_chunk_relation", "relation_uuid", "file_uuid", "chunk_uuid", "offset");
        assertColumnsExist(connection, "file_snapshot_relation", "relation_uuid", "file_uuid", "snapshot_uuid");
        assertValidVersion(connection);
    }

    private void assertTablesExist(Connection connection) throws SQLException {
        ResultSet resultSet = showTablesResult(connection);
        String[] expected = {"chunk", "file", "snapshot", "repository", "file_chunk_relation", "file_snapshot_relation"};
        String[] tablesExist = new String[expected.length];
        int i = 0;
        while (resultSet.next()) {
            tablesExist[i++] = resultSet.getString(1);
        }
        assertArrayEquals(expected, tablesExist);
    }

    private void assertColumnsExist(Connection connection, String table, String... expected) throws SQLException {
        String sql = "PRAGMA table_info(" + table + ");";
        Statement statement  = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        String[] columnsExist = new String[expected.length];
        int i = 0;
        while (resultSet.next()) {
            columnsExist[i++] = resultSet.getString(2);
        }
        assertArrayEquals(expected, columnsExist);
    }

    private void assertValidVersion(Connection connection) throws SQLException {
        String sql = "SELECT version FROM repository;";
        Statement statement  = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        resultSet.next();
        assertEquals(Constants.VERSION, resultSet.getString("version"));
    }

    private ResultSet showTablesResult(Connection connection) throws SQLException {
        String sql = "SELECT name FROM sqlite_master WHERE type='table';";
        Statement statement  = connection.createStatement();
        return statement.executeQuery(sql);
    }
}
