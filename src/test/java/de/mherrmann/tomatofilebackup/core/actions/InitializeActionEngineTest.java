package de.mherrmann.tomatofilebackup.core.actions;

import de.mherrmann.tomatofilebackup.Constants;
import de.mherrmann.tomatofilebackup.TestUtil;
import de.mherrmann.tomatofilebackup.exceptions.IllegalActionCommandException;
import de.mherrmann.tomatofilebackup.persistence.RepositoryInitializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;

public class InitializeActionEngineTest {

    private static final String TEST_REPOSITORY_PATH = "./test/repo/";
    private static final String TEST_REPOSITORY_DB_PATH = "./test/repo/"+ Constants.DB_FILENAME;

    @BeforeEach
    void setUp() {
        TestUtil.createTestDirectory();
    }

    @AfterEach
    void tearDown() {
        TestUtil.removeTestFiles();
    }

    @Test
    void shouldInitializeRepository() throws IOException, SQLException {
        InitializeActionEngine engine = new InitializeActionEngine();

        engine.run(Collections.emptyMap(), Collections.emptyList(), Collections.singletonList(TEST_REPOSITORY_PATH));

        assertTrue(new File(TEST_REPOSITORY_DB_PATH).exists());
    }

    @Test
    void shouldFailInitializeRepositoryCausedByMissingPath() {
        InitializeActionEngine engine = new InitializeActionEngine();
        Exception exception = null;

        try {
            engine.run(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList());
        } catch (Exception ex){
            exception = ex;
        }

        assertFalse(new File(TEST_REPOSITORY_DB_PATH).exists());
        assertNotNull(exception);
        assertEquals(IllegalActionCommandException.class, exception.getClass());
        assertEquals(Constants.ErrorReport.MISSING_PATH.getMessage(), exception.getMessage());
    }

    @Test
    void shouldFailInitializeRepositoryCausedByFilePath() throws IOException {
        InitializeActionEngine engine = new InitializeActionEngine();
        Exception exception = null;
        File file = new File("./test/test");
        file.createNewFile();

        try {
            engine.run(Collections.emptyMap(), Collections.emptyList(), Collections.singletonList(file.getAbsolutePath()));
        } catch (Exception ex){
            exception = ex;
        }

        assertFalse(new File(TEST_REPOSITORY_DB_PATH).exists());
        assertNotNull(exception);
        assertEquals(IOException.class, exception.getClass());
        assertEquals(Constants.ErrorReport.PATH_MUST_BE_DIRECTORY.getMessage(), exception.getMessage());
    }

    @Test
    void shouldFailInitializeRepositoryCausedByAlreadyExistingRepository() throws IOException, SQLException {
        InitializeActionEngine engine = new InitializeActionEngine();
        Exception exception = null;
        RepositoryInitializer.initialize(TEST_REPOSITORY_PATH);

        try {
            engine.run(Collections.emptyMap(), Collections.emptyList(), Collections.singletonList(TEST_REPOSITORY_PATH));
        } catch (Exception ex){
            exception = ex;
        }

        assertNotNull(exception);
        assertEquals(IOException.class, exception.getClass());
        assertEquals(Constants.ErrorReport.REPOSITORY_ALREADY_EXISTS.getMessage(), exception.getMessage());
    }

}
