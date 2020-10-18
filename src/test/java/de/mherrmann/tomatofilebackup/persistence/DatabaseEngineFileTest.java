package de.mherrmann.tomatofilebackup.persistence;

import de.mherrmann.tomatofilebackup.TestUtil;
import de.mherrmann.tomatofilebackup.persistence.entities.FileEntity;
import de.mherrmann.tomatofilebackup.persistence.entities.SnapshotEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class DatabaseEngineFileTest {

    private static final String TEST_REPOSITORY_PATH = "./test/";
    private static final String TEST_FILE_PATH = new File("./test/testFile.txt").getAbsolutePath();
    private static final long TEST_SIZE = 123456;
    private static final long TEST_FILE_INODE = 234654;
    private static final long TEST_SYMLINK_INODE = 456432;
    private static final long TEST_DIRECTORY_INODE = 579275;
    private static final long TEST_MTIME = 1234567890;
    private static final String TEST_SOURCE_PATH = "/home/max/";
    private static final String TEST_HOST = "pcmax";
    private static final long TEST_CTIME = 1987654321;
    private static final String TEST_SYMLINK_SOURCE = "/somewhere";
    private static final String TEST_SYMLINK_TARGET = "/somewhere/else";
    private static final String TEST_DIRECTORY_PATH = new File("./test/").getAbsolutePath();

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
        SnapshotEntity snapshotEntity = engine.addSnapshot("test", "test", 1234567890);
        engine.addRegularFile(TEST_FILE_PATH, TEST_SIZE, TEST_FILE_INODE, TEST_MTIME, false, snapshotEntity);

        assertValidRegularFile(snapshotEntity.getUuid());
    }

    @Test
    void shouldAddDirectory() throws SQLException {
        SnapshotEntity snapshotEntity = engine.addSnapshot("test", "test", 1234567890);
        engine.addDirectory(TEST_DIRECTORY_PATH, TEST_DIRECTORY_INODE, TEST_MTIME, snapshotEntity);

        assertValidDirectory(snapshotEntity.getUuid());
    }

    @Test
    void shouldAddSymlink() throws SQLException {
        SnapshotEntity snapshotEntity = engine.addSnapshot("test", "test", 1234567890);
        engine.addSymlink(TEST_SYMLINK_SOURCE, TEST_SYMLINK_INODE, TEST_MTIME, TEST_SYMLINK_TARGET, snapshotEntity);

        assertValidSymlink(snapshotEntity.getUuid());
    }

    @Test
    void shouldAddFileSnapshotRelation() throws SQLException {
        SnapshotEntity snapshotEntity = engine.addSnapshot("test", "test", 1234567890);
        FileEntity fileEntity = engine.addRegularFile(TEST_FILE_PATH, TEST_SIZE, TEST_FILE_INODE,
                TEST_MTIME, false, snapshotEntity);

        engine.addFileSnapshotRelation(fileEntity.getUuid(), snapshotEntity.getUuid());

        assertValidSnapshotFileRelation(fileEntity.getUuid(), snapshotEntity.getUuid());
    }

    @Test
    void shouldGetFilesBySnapshotUuidOrderByInode() throws SQLException {
        SnapshotEntity snapshotEntity = engine.addSnapshot("test", "test", 1234567890);
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);
        engine.addRegularFile(TEST_FILE_PATH+2, TEST_SIZE, TEST_FILE_INODE+2, TEST_MTIME, false, snapshotEntity);
        engine.addRegularFile(TEST_FILE_PATH, TEST_SIZE, TEST_FILE_INODE, TEST_MTIME, false, snapshotEntity);

        List<FileEntity> files = engine.getFilesBySnapshotUuidOrderByInode(snapshotEntity.getUuid());

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

    @Test
    void shouldGetFilesBySnapshotUuidOrderByPath() throws SQLException {
        SnapshotEntity snapshotEntity = engine.addSnapshot("test", "test", 1234567890);
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);
        engine.addRegularFile(TEST_FILE_PATH+2, TEST_SIZE, TEST_FILE_INODE+2, TEST_MTIME, false, snapshotEntity);
        engine.addRegularFile(TEST_FILE_PATH, TEST_SIZE, TEST_FILE_INODE, TEST_MTIME, false, snapshotEntity);

        List<FileEntity> files = engine.getFilesBySnapshotUuidOrderByPath(snapshotEntity.getUuid());

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

    @Test
    void shouldGetFileByInode() throws SQLException {
        SnapshotEntity snapshotEntity = engine.addSnapshot("test", "test", 1234567890);
        engine.addRegularFile(TEST_FILE_PATH, TEST_SIZE, TEST_FILE_INODE, TEST_MTIME, false, snapshotEntity);

        FileEntity file = engine.getFileByInode(TEST_FILE_INODE, snapshotEntity);

        assertEquals(TEST_FILE_PATH, file.getPath());
        assertEquals(TEST_SIZE, file.getSize());
        assertEquals(TEST_MTIME, file.getMtime());
    }

    @Test
    void shouldGetFileByNameAndSizeAndMtime() throws SQLException {
        SnapshotEntity snapshotEntity = engine.addSnapshot("test", "test", 1234567890);
        engine.addRegularFile(TEST_FILE_PATH, TEST_SIZE, TEST_FILE_INODE, TEST_MTIME, false, snapshotEntity);
        String name = TEST_FILE_PATH.replaceAll("/(.*?)$", "$1");

        FileEntity file = engine.getFileByNameAndSizeAndMtime(name, TEST_SIZE, TEST_MTIME, snapshotEntity);

        assertEquals(TEST_FILE_PATH, file.getPath());
        assertEquals(TEST_SIZE, file.getSize());
        assertEquals(TEST_MTIME, file.getMtime());
    }

    @Test
    void shouldRemoveOrphanedFiles() throws SQLException {
        SnapshotEntity snapshotEntity = engine.addSnapshot("test", "test", 1234567890);
        FileEntity fileEntityExpectedToBeRemoved = engine.addRegularFile(TEST_FILE_PATH, TEST_SIZE, TEST_FILE_INODE, TEST_MTIME, false, snapshotEntity);
        FileEntity fileEntityExpectedToBeRemained = engine.addRegularFile(TEST_FILE_PATH+2, TEST_SIZE, TEST_FILE_INODE+2, TEST_MTIME, false, snapshotEntity);
        orphanageFile(fileEntityExpectedToBeRemoved);

        engine.removeOrphanedFiles();

        assertRemoved(fileEntityExpectedToBeRemoved);
        assertRemained(fileEntityExpectedToBeRemained);
    }

    private void assertValidRegularFile(String snapshotUuid) throws SQLException {
        ResultSet resultSet = assertValidFile(TEST_FILE_PATH, TEST_FILE_INODE, TEST_SIZE, snapshotUuid);
        assertFalse(resultSet.getBoolean("link"));
        assertFalse(resultSet.getBoolean("directory"));
    }

    private void assertValidDirectory(String snapshotUuid) throws SQLException {
        ResultSet resultSet = assertValidFile(TEST_DIRECTORY_PATH, TEST_DIRECTORY_INODE, 0, snapshotUuid);
        assertFalse(resultSet.getBoolean("link"));
        assertTrue(resultSet.getBoolean("directory"));
    }

    private void assertValidSymlink(String snapshotUuid) throws SQLException {
        ResultSet resultSet = assertValidFile(TEST_SYMLINK_SOURCE, TEST_SYMLINK_INODE, 0, snapshotUuid);
        assertTrue(resultSet.getBoolean("link"));
        assertFalse(resultSet.getBoolean("directory"));
        assertEquals(TEST_SYMLINK_TARGET, resultSet.getString("link_path"));
    }

    private ResultSet assertValidFile(String filePath, long inode, long size, String snapshotUuid) throws SQLException {
        String sql = "SELECT * FROM file WHERE inode = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setLong(1, inode);
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        assertEquals(filePath, resultSet.getString("path"));
        assertEquals(size, resultSet.getLong("size"));
        assertEquals(TEST_MTIME, resultSet.getLong("mtime"));
        assertEquals(0, resultSet.getInt("compressed"));
        assertValidSnapshotFileRelation(resultSet.getString("file_uuid"), snapshotUuid);
        return resultSet;
    }

    private void assertValidSnapshotFileRelation(String fileUuid, String snapshotUuid) throws SQLException {
        String sql = "SELECT * FROM file_snapshot_relation WHERE file_uuid = ? AND snapshot_uuid = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setString(1, fileUuid);
        preparedStatement.setString(2, snapshotUuid);
        ResultSet resultSet = preparedStatement.executeQuery();
        assertFalse(resultSet.isClosed());
    }

    private void orphanageFile(FileEntity fileEntity) throws SQLException {
        String sql = "DELETE FROM file_snapshot_relation WHERE file_uuid = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setString(1, fileEntity.getUuid());
        preparedStatement.executeUpdate();
    }

    private void assertRemoved(FileEntity fileEntity) throws SQLException {
        String sql = "SELECT * FROM file WHERE file_uuid = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setString(1, fileEntity.getUuid());
        ResultSet resultSet = preparedStatement.executeQuery();
        assertTrue(resultSet.isClosed());
    }

    private void assertRemained(FileEntity fileEntity) throws SQLException {
        String sql = "SELECT * FROM file WHERE file_uuid = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setString(1, fileEntity.getUuid());
        ResultSet resultSet = preparedStatement.executeQuery();
        assertFalse(resultSet.isClosed());
    }
}
