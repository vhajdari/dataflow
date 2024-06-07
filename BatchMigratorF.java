import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.yaml.snakeyaml.Yaml;

public class BatchMigratorF {

    private static final int DEFAULT_BATCH_SIZE = 10000;
    private static final String CONFIG_FILE_ENV_VAR = "CONFIG_FILE_PATH"; // Environment variable name

    // Prepared statement cache
    private Map<String, PreparedStatement> statementCache = new HashMap<>();

    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
        // ... (rest of the program logic up to here)

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
                targetConn.commit(); // Commit after each batch
                count = 0; // Reset counter for next batch
            }
        }

        // Handle remaining records in the batch (if any)
        if (count > 0) {
            targetStmt.executeBatch();
            targetConn.commit(); // Final commit for remaining records
        }

        // Close resources
        resultSet.close();
        sourceStmt.close();
        targetStmt.close();
        sourceConn.close();
        targetConn.close();

        System.out.println("Data migration completed successfully.");
    }

    private PreparedStatement getPreparedStatement(Connection conn, String sql) throws SQLException {
        PreparedStatement stmt = statementCache.get(sql);
        if (stmt == null) {
            stmt = conn.prepareStatement(sql);
            statementCache.put(sql, stmt);
        }
        return stmt;
    }

    // ... (other helper methods for loading configuration and validating details)
}
