import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseTransfer {

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
        if (args.length < 4) {
            System.out.println("Usage: java DatabaseTransfer <config-file> <batch-size> <source-name> <target-name> <table1> <table2> ...");
            return;
        }

        String configFilePath = args[0];
        int batchSize = Integer.parseInt(args[1]);
        String sourceName = args[2];
        String targetName = args[3];
        String[] tables = new String[args.length - 4];
        System.arraycopy(args, 4, tables, 0, args.length - 4);

        Config config = loadConfig(configFilePath);

        DBConfig sourceConfig = config.connections.stream().filter(conn -> sourceName.equals(conn.name)).findFirst().orElse(null);
        DBConfig targetConfig = config.connections.stream().filter(conn -> targetName.equals(conn.name)).findFirst().orElse(null);

        if (sourceConfig == null || targetConfig == null) {
            System.out.println("Both source and target database configurations must be provided in the config file.");
            return;
        }

        ExecutorService executorService = Executors.newFixedThreadPool(tables.length);

        try (Connection sourceConn = DriverManager.getConnection(sourceConfig.url, sourceConfig.userName, sourceConfig.password);
             Connection targetConn = DriverManager.getConnection(targetConfig.url, targetConfig.userName, targetConfig.password)) {

            for (String table : tables) {
                executorService.execute(() -> {
                    try {
                        transferTableData(sourceConn, targetConn, table, batchSize);
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
