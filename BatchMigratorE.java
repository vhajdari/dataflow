import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.yaml.snakeyaml.Yaml;

public class BatchMigratorE {

    private static final int DEFAULT_BATCH_SIZE = 10000;
    private static final String CONFIG_FILE_ENV_VAR = "CONFIG_FILE_PATH"; // Environment variable name

    // Prepared statement cache
    private Map<String, PreparedStatement> statementCache = new HashMap<>();

    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
        // Get configuration file path from environment variable
        String configFilePath = Optional.ofNullable(System.getenv(CONFIG_FILE_ENV_VAR)).orElse("config.yml");

        // Load configuration from YAML file
        Map<String, Map<String, String>> connections = loadConnectionsFromYaml(configFilePath);

        // Validate required connection details
        validateConnectionDetails(connections);

        // Get source and target connection details (assuming same table name)
        Map<String, String> sourceConfig = connections.get("source");

        // Get details from configuration
        String sourceDbType = sourceConfig.get("dbType");
        String targetDbType = sourceConfig.get("dbType"); // Assuming target is same type as source
        String sourceUrl = sourceConfig.get("url");
        String targetUrl = sourceConfig.get("url");
        String sourceUser = sourceConfig.get("userName");
        String sourcePassword = sourceConfig.get("password");
        String targetUser = sourceConfig.get("userName");
        String targetPassword = sourceConfig.get("password");

        // Get batch size (default: 10000)
        int batchSize = Integer.parseInt(Optional.ofNullable(System.getenv("BATCH_SIZE")).orElse(String.valueOf(DEFAULT_BATCH_SIZE)));

        // Load JDBC drivers based on database types (replace with actual driver loading logic)
        Class<?> sourceDriverClass = loadDriver(sourceDbType);
        Class<?> targetDriverClass = loadDriver(targetDbType);
        Class.forName(sourceDriverClass.getName());
        Class.forName(targetDriverClass.getName());

        // Connect to databases
        Connection sourceConn = DriverManager.getConnection(sourceUrl, sourceUser, sourcePassword);
        Connection targetConn = DriverManager.getConnection(targetUrl, targetUser, targetPassword);

        // Disable autocommit
        sourceConn.setAutoCommit(false);
        targetConn.setAutoCommit(false);

        // Get table name from environment variable
        String sourceTable = Optional.ofNullable(System.getenv("SOURCE_TABLE")).orElse("your_source_table");

        // Retrieve source table metadata
        DatabaseMetaData sourceMetaData = sourceConn.getMetaData();
        ResultSet resultSet = sourceMetaData.getColumns(null, null, sourceTable, null);

        // Build a list of column names
        List<String> columnNames = new ArrayList<>();
        while (resultSet.next()) {
            columnNames.add(resultSet.getString("COLUMN_NAME"));
        }

        // Build dynamic target insert statement using source table name
        StringBuilder targetSql = new StringBuilder("INSERT INTO " + sourceTable + " (");
        for (int i = 0; i < columnNames.size(); i++) {
            targetSql.append("?,");
        }
        targetSql.deleteCharAt(targetSql.length() - 1); // Remove trailing comma
        targetSql.append(") VALUES (");
        for (int i = 0; i < columnNames.size(); i++) {
            targetSql.append("?,");
        }
        targetSql.deleteCharAt(targetSql.length() - 1); // Remove trailing comma
        targetSql.append(")");

        // Prepare statements with caching
        PreparedStatement sourceStmt = getPreparedStatement(sourceConn, "SELECT * FROM " + sourceTable);
        PreparedStatement targetStmt = getPreparedStatement(targetConn, targetSql.toString());

        // Process records in batches
        resultSet = sourceStmt.executeQuery();
        int count = 0;
        while (resultSet.next()) {
            // Set values for target insert statement using column names
            for (int i = 1; i <= columnNames.size(); i++) {
                String columnName = columnNames.get(i - 1);
      


      
