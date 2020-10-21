package de.mherrmann.tomatofilebackup.persistence;

import de.mherrmann.tomatofilebackup.persistence.entities.RepositoryEntity;

import java.io.IOException;
import java.sql.*;

class RepositoryDatabaseEngine {

    private final Connection connection;
    private final String repositoryPath;

    RepositoryDatabaseEngine(String repositoryPath, Connection connection) {
        this.connection = connection;
        this.repositoryPath = repositoryPath;
    }

    void initializeRepository() throws IOException, SQLException {
        RepositoryInitializer.initialize(repositoryPath);
    }

    RepositoryEntity getRepository() throws SQLException {
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
