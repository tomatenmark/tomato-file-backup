package de.mherrmann.tomatofilebackup.persistence;

import de.mherrmann.tomatofilebackup.persistence.entities.RepositoryEntity;

import java.io.IOException;
import java.sql.*;

public class RepositoryDatabaseEngine {

    Connection connection;
    private final String repositoryPath;

    public RepositoryDatabaseEngine(String repositoryPath, Connection connection) throws SQLException {
        this.connection = connection;
        this.repositoryPath = repositoryPath;
    }

    public void initializeRepository() throws IOException, SQLException {
        RepositoryInitializer.initialize(repositoryPath);
    }

    public RepositoryEntity getRepository() throws SQLException {
        String sql = "SELECT * FROM repository";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        return new RepositoryEntity(
                resultSet.getString("repository_uuid"),
                resultSet.getString("path"),
                resultSet.getString("version")
        );
    }
}
