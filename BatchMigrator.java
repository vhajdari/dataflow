import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class BatchMigrator {

    private static final int DEFAULT_BATCH_SIZE = 10000;

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        // Get connection details from environment variables
        String sourceUrl = Optional.ofNullable(System.getenv("SOURCE_URL")).orElse("jdbc:your-source-database:port/your_source_db");
        String sourceUser = Optional.ofNullable(System.getenv("SOURCE_USER")).orElse("source_user");
        String sourcePassword = Optional.ofNullable(System.getenv("SOURCE_PASSWORD")).orElse("source_password");

        String targetUrl = Optional.ofNullable(System.getenv("TARGET_URL")).orElse("jdbc:your-target-database:port/your_target_db");
        String targetUser = Optional.ofNullable(System.getenv("TARGET_USER")).orElse("target_user");
        String targetPassword = Optional.ofNullable(System.getenv("TARGET_PASSWORD")).orElse("target_password");

        // Get batch size (default: 10000)
        int batchSize = Integer.parseInt(Optional.ofNullable(System.getenv("BATCH_SIZE")).orElse(String.valueOf(DEFAULT_BATCH_SIZE)));

        // Load drivers (replace with your specific drivers)
        Class.forName("your.source.database.Driver"); // Replace with actual driver class name
        Class.forName("your.target.database.Driver"); // Replace with actual driver class name

        // Connect to databases
        Connection sourceConn = DriverManager.getConnection(sourceUrl, sourceUser, sourcePassword);
        Connection targetConn = DriverManager.getConnection(targetUrl, targetUser, targetPassword);

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

        // Build dynamic target insert statement
        StringBuilder targetSql = new StringBuilder("INSERT INTO your_target_table (");
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

        // Prepare statements
        PreparedStatement sourceStmt = sourceConn.prepareStatement("SELECT * FROM " + sourceTable);
        PreparedStatement targetStmt = targetConn.prepareStatement(targetSql.toString());

        // Process records in batches
        resultSet = sourceStmt.executeQuery();
        int count = 0;
        while (resultSet.next()) {
            // Set values for target insert statement using column names
            for (int i = 1; i <= columnNames.size(); i++) {
                String columnName = columnNames.get(i - 1);
                Object value = resultSet.getObject(i);
                targetStmt.setObject(i, value);
            }

            targetStmt.addBatch();
            count++;

            if (count % batchSize == 0) {
                targetStmt.executeBatch();
                count = 0; // Reset counter for next batch
            }
        }

        // Handle remaining records in the batch (if any)
        if (count > 0) {
            targetStmt.executeBatch();
        }

        // Close resources
        resultSet.close();
        sourceStmt.close();
        targetStmt.close();
        sourceConn.close();
        targetConn.close();

        System.out.println("Data migration completed successfully!");
    }
}
