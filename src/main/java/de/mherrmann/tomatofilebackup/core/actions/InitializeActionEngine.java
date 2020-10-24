package de.mherrmann.tomatofilebackup.core.actions;

import de.mherrmann.tomatofilebackup.Constants;
import de.mherrmann.tomatofilebackup.exceptions.IllegalActionCommandException;
import de.mherrmann.tomatofilebackup.persistence.RepositoryInitializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class InitializeActionEngine extends ActionEngine {

    @Override
    public void run(Map<String, String> properties, List<String> enabledSwitches, List<String> mainValues)
            throws IOException, SQLException {
        if(mainValues.isEmpty()){
            throw new IllegalActionCommandException(Constants.ErrorReport.MISSING_PATH.getMessage());
        }
        String repositoryPath = mainValues.get(0);
        initializeRepository(repositoryPath);
    }

    @Override
    public String getActionHelpMessage() {
        return Constants.TFB_INTRO + "\n" +
                " Help for initialize:\n" +
                "\n" +
                " Usage: tfb initialize PATHTOREPOSITORY\n" +
                "  Example: tfb initialize /mnt/backup/repo/";
    }

    private void initializeRepository(String repositoryPath) throws SQLException, IOException {
        initializeRepositoryDirectory(repositoryPath);
        RepositoryInitializer.initialize(repositoryPath);
    }

    private static void initializeRepositoryDirectory(String repositoryPath) throws IOException {
        File repositoryDirectory = new File(repositoryPath);
        File repositoryDirectoryDBFile = new File(repositoryPath, Constants.DB_FILENAME);
        File repositoryDirectoryParent = repositoryDirectory.getParentFile();
        if(!repositoryDirectoryParent.exists() || !repositoryDirectoryParent.isDirectory() || !repositoryDirectoryParent.canWrite()){
            throw new IOException(Constants.ErrorReport.PARENT_DIRECTORY_PROBLEM.getMessage(repositoryDirectoryParent.getAbsolutePath()));
        }
        if(repositoryDirectoryDBFile.exists()){
            throw new IOException(Constants.ErrorReport.REPOSITORY_ALREADY_EXISTS.getMessage());
        }
        if(repositoryDirectory.isFile()){
            throw new IOException(Constants.ErrorReport.PATH_MUST_BE_DIRECTORY.getMessage());
        }
        if(!repositoryDirectory.exists()){
            Files.createDirectory(repositoryDirectory.toPath());
        }
    }
}
