package de.mherrmann.tomatofilebackup.core.actions;

import de.mherrmann.tomatofilebackup.Constants;
import de.mherrmann.tomatofilebackup.core.Option;
import de.mherrmann.tomatofilebackup.exceptions.IllegalActionCommandException;
import de.mherrmann.tomatofilebackup.persistence.RepositoryInitializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InitializeActionEngine extends ActionEngine {

    private static final List<Option.Property> AVAILABLE_PROPERTIES = Collections.emptyList();
    private static final List<Option.Switch> AVAILABLE_SWITCHES = Collections.emptyList();
    private static final List<Option.Property> MANDATORY_PROPERTIES = Collections.emptyList();
    private static final String ACTION_NAME = "initialize";
    private static final String USAGE = "tfb backup --"+Option.Property.repository.name()+"="+Option.Property.repository.getPlaceholder()+" [OPTIONS] SOURCE_PATH";
    private static final String EXAMPLE = "Example: tfb backup --repository=/mnt/backup/repo/ -p -v /home/max/";

    public InitializeActionEngine(){
        super(AVAILABLE_PROPERTIES, AVAILABLE_SWITCHES, MANDATORY_PROPERTIES, ACTION_NAME, USAGE, EXAMPLE);
    }

    @Override
    public void run(Map<Option.Property, String> properties, List<Option.Switch> enabledSwitches, List<String> mainValues)
            throws IOException, SQLException {
        checkBoundaries(properties, enabledSwitches);
        if(mainValues.isEmpty()){
            throw new IllegalActionCommandException(Constants.ErrorReport.MISSING_PATH.getMessage());
        }
        String repositoryPath = mainValues.get(0);
        initializeRepository(repositoryPath);
    }

    private void initializeRepository(String repositoryPath) throws SQLException, IOException {
        initializeRepositoryDirectory(repositoryPath);
        RepositoryInitializer.initialize(repositoryPath);
    }

    private static void initializeRepositoryDirectory(String repositoryPath) throws IOException {
        File repositoryDirectory = new File(repositoryPath);
        File repositoryDirectoryDBFile = new File(repositoryPath, Constants.DB_FILENAME);
        File chunksDirectory = new File(repositoryPath, Constants.CHUNKS_DIRECTORY_NAME);
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
        Files.createDirectory(chunksDirectory.toPath());
    }
}
