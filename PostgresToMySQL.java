import java.sql.*;

public class PostgresToMySQL {

    private static final int BATCH_SIZE = 1000; // Number of records to process per batch

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        // Replace with your connection details
        String postgresUrl = "jdbc:postgresql://localhost:5432/postgres_db";
        String postgresUser = "postgres_user";
        String postgresPassword = "postgres_password";

        String mysqlUrl = "jdbc:mysql://localhost:3306/mysql_db";
        String mysqlUser = "mysql_user";
        String mysqlPassword = "mysql_password";

        // Load drivers
        Class.forName("org.postgresql.Driver");
        Class.forName("com.mysql.cj.jdbc.Driver");

        // Connect to databases
        Connection postgresConn = DriverManager.getConnection(postgresUrl, postgresUser, postgresPassword);
        Connection mysqlConn = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);

        // Prepare statements
        PreparedStatement postgresStmt = postgresConn.prepareStatement("SELECT * FROM your_table");
        PreparedStatement mysqlStmt = mysqlConn.prepareStatement("INSERT INTO your_table (column1, column2, ...) VALUES (?, ?, ...)");

        // Process records in batches
        ResultSet resultSet = postgresStmt.executeQuery();
        int count = 0;
        while (resultSet.next()) {
            // Extract data from Postgres result set
            // Replace with your actual column names and data types
            String data1 = resultSet.getString("column1");
            int data2 = resultSet.getInt("column2");
            // ...

            // Set values for MySQL insert statement
            mysqlStmt.setString(1, data1);
            mysqlStmt.setInt(2, data2);
            // ... (set values for all columns)

            mysqlStmt.addBatch();
            count++;

            if (count % BATCH_SIZE == 0) {
                mysqlStmt.executeBatch();
                count = 0; // Reset counter for next batch
            }
        }

        // Handle remaining records in the batch (if any)
        if (count > 0) {
            mysqlStmt.executeBatch();
        }

        // Close resources
        resultSet.close();
        postgresStmt.close();
        mysqlStmt.close();
        postgresConn.close();
        mysqlConn.close();

        System.out.println("Data migration completed successfully!");
    }
}
