import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataTransfer {

    public static class DBConfig {
        public String name;
        public String databaseName;
        public String schemaName;
        public String dbType;
        public String url;
        public String userName;
        public String password;
    }

    public static class Config {
        public List<DBConfig> connections;
    }

    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
        if (args.length < 3) {
            System.out.println("Usage: java DataTransfer <config-file> <batch-size> <table1> <table2> ...");
            return;
        }

        String configFilePath = args[0];
        int batchSize = Integer.parseInt(args[1]);
        String[] tables = new String[args.length - 2];
        System.arraycopy(args, 2, tables, 0, args.length - 2);

        Config config = loadConfig(configFilePath);

        DBConfig mysqlConfig = config.connections.stream().filter(conn -> "mysql".equals(conn.name)).findFirst().orElse(null);
        DBConfig pgConfig = config.connections.stream().filter(conn -> "pg".equals(conn.name)).findFirst().orElse(null);

        if (mysqlConfig == null || pgConfig == null) {
            System.out.println("Both MySQL and PostgreSQL configurations must be provided in the config file.");
            return;
        }

        ExecutorService executorService = Executors.newFixedThreadPool(tables.length);

        try (Connection mysqlConn = DriverManager.getConnection(mysqlConfig.url, mysqlConfig.userName, mysqlConfig.password);
             Connection pgConn = DriverManager.getConnection(pgConfig.url, pgConfig.userName, pgConfig.password)) {

            for (String table : tables) {
                executorService.execute(() -> {
                    try {
                        transferTableData(mysqlConn, pgConn, table, batchSize);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
            }
        } finally {
            executorService.shutdown();
        }
    }

    private static Config loadConfig(String filePath) throws IOException {
        Yaml yaml = new Yaml(new Constructor(Config.class));
        try (FileInputStream inputStream = new FileInputStream(filePath)) {
            return yaml.load(inputStream);
        }
    }

    private static void transferTableData(Connection sourceConn, Connection targetConn, String tableName, int batchSize) throws SQLException {
        String selectSQL = "SELECT * FROM " + tableName;
        String insertSQL = "INSERT INTO " + tableName + " (%s) VALUES (%s)";

        try (Statement stmt = sourceConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = stmt.executeQuery(selectSQL)) {

            ResultSetMetaData rsMetaData = rs.getMetaData();
            int columnCount = rsMetaData.getColumnCount();

            StringBuilder columns = new StringBuilder();
            StringBuilder placeholders = new StringBuilder();
            for (int i = 1; i <= columnCount; i++) {
                columns.append(rsMetaData.getColumnName(i));
                placeholders.append("?");
                if (i < columnCount) {
                    columns.append(",");
                    placeholders.append(",");
                }
            }

            insertSQL = String.format(insertSQL, columns.toString(), placeholders.toString());

            try (PreparedStatement pstmt = targetConn.prepareStatement(insertSQL)) {
                int count = 0;

                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        pstmt.setObject(i, rs.getObject(i));
                    }
                    pstmt.addBatch();

                    if (++count % batchSize == 0) {
                        pstmt.executeBatch();
                    }
                }
                pstmt.executeBatch();
            }
        }
    }
}
