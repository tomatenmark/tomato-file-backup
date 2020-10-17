package de.mherrmann.tomatofilebackup.persistence;

import de.mherrmann.tomatofilebackup.TestUtil;
import de.mherrmann.tomatofilebackup.persistence.entities.FileEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class DatabaseEngineFileTest {

    private static final String TEST_REPOSITORY_PATH = "./test/";
    private static final String TEST_FILE_PATH = new File("./test/testFile.txt").getAbsolutePath();
    private static final long TEST_SIZE = 123456;
    private static final long TEST_FILE_INODE = 234654;
    private static final long TEST_SYMLINK_INODE = 456432;
    private static final long TEST_MTIME = 1234567890;
    private static final String TEST_SNAPSHOT_UUID = UUID.randomUUID().toString();
    private static final String TEST_SOURCE_PATH = "/home/max/";
    private static final String TEST_HOST = "pcmax";
    private static final long TEST_CTIME = 1987654321;
    private static final String TEST_SYMLINK_SOURCE = "/somewhere";
    private static final String TEST_SYMLINK_TARGET = "/somewhere/else";

    private DatabaseEngine engine;

    @BeforeEach
    void setUp() throws SQLException, IOException {
        TestUtil.createTestDirectory();
        engine = new DatabaseEngine(TEST_REPOSITORY_PATH);
        engine.initializeRepository();
    }

    @AfterEach
    void tearDown() throws SQLException {
        TestUtil.removeTestFiles();
        engine.destroy();
    }

    @Test
    void shouldAddRegularFile() throws SQLException {
        engine.addRegularFile(TEST_FILE_PATH, TEST_SIZE, TEST_FILE_INODE, TEST_MTIME, false, TEST_SNAPSHOT_UUID);

        assertValidFile(TEST_FILE_PATH, TEST_FILE_INODE);
    }

    @Test
    void shouldAddSymlink() throws SQLException {
        engine.addSymlink(TEST_SYMLINK_SOURCE, TEST_SIZE, TEST_SYMLINK_INODE, TEST_MTIME, false, TEST_SYMLINK_TARGET, TEST_SNAPSHOT_UUID);

        assertValidSymlink();
    }

    @Test
    void shouldGetFiles() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);
        engine.addRegularFile(TEST_FILE_PATH, TEST_SIZE, TEST_FILE_INODE, TEST_MTIME, false, TEST_SNAPSHOT_UUID);
        engine.addRegularFile(TEST_FILE_PATH+2, TEST_SIZE, TEST_FILE_INODE+2, TEST_MTIME, false, TEST_SNAPSHOT_UUID);

        List<FileEntity> files = engine.getFilesBySnapshotUuid(TEST_SNAPSHOT_UUID);

        assertEquals(2, files.size());
        assertEquals(TEST_FILE_PATH, files.get(0).getPath());
        assertEquals(TEST_SIZE, files.get(0).getSize());
        assertEquals(TEST_FILE_INODE, files.get(0).getInode());
        assertEquals(TEST_MTIME, files.get(0).getMtime());
        assertEquals(TEST_FILE_PATH+2, files.get(1).getPath());
        assertEquals(TEST_SIZE, files.get(1).getSize());
        assertEquals(TEST_FILE_INODE+2, files.get(1).getInode());
        assertEquals(TEST_MTIME, files.get(1).getMtime());
    }

    private ResultSet assertValidFile(String filePath, long inode) throws SQLException {
        String sql = "SELECT * FROM file WHERE inode = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setLong(1, inode);
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        assertEquals(filePath, resultSet.getString("path"));
        assertEquals(TEST_SIZE, resultSet.getLong("size"));
        assertEquals(TEST_MTIME, resultSet.getLong("mtime"));
        assertEquals(0, resultSet.getInt("compressed"));
        assertValidSnapshotFileRelation(resultSet.getString("file_uuid"));
        return resultSet;
    }

    private void assertValidSymlink() throws SQLException {
        ResultSet resultSet = assertValidFile(TEST_SYMLINK_SOURCE, TEST_SYMLINK_INODE);
        assertEquals(1, resultSet.getInt("link"));
        assertEquals(TEST_SYMLINK_TARGET, resultSet.getString("link_path"));
    }

    private void assertValidSnapshotFileRelation(String fileUuid) throws SQLException {
        String sql = "SELECT * FROM file_snapshot_relation WHERE file_uuid = ? AND snapshot_uuid = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setString(1, fileUuid);
        preparedStatement.setString(2, TEST_SNAPSHOT_UUID);
        ResultSet resultSet = preparedStatement.executeQuery();
        assertFalse(resultSet.isClosed());
    }
}
