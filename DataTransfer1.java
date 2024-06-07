import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataTransfer1 {

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

        // Load JDBC drivers from environment variable
        loadJdbcDrivers();

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

        try (Connection sourceConn = createConnection(sourceConfig);
             Connection targetConn = createConnection(targetConfig)) {

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

    private static Connection createConnection(DBConfig config) throws SQLException {
        Pattern pattern = Pattern.compile("jdbc:(\\w+):\\/\\/[^?]+\\?user=([^&]+)&password=([^&]+)");
        Matcher matcher = pattern.matcher(config.url);
        if (matcher.find()) {
            String dbType = matcher.group(1);
            String userName = matcher.group(2);
            String password = matcher.group(3);
            return DriverManager.getConnection(config.url.replaceAll("\\?user=[^&]+&password=[^&]+", ""), userName, password);
        } else {
            throw new SQLException("Invalid JDBC URL format");
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

    private static void loadJdbcDrivers() throws IOException, ClassNotFoundException {
        String driversPath = System.getenv("ROSETTA_DRIVERS");
        if (driversPath == null || driversPath.isEmpty()) {
            throw new IOException("Environment variable ROSETTA_DRIVERS is not set or empty.");
        }

        File dir = new File(driversPath);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IOException("ROSETTA_DRIVERS path is not a valid directory.");
        }

        File[] files = dir.listFiles((d, name) -> name.endsWith(".jar"));
        if (files == null || files.length == 0) {
            throw new IOException("No JDBC driver JAR files found in the directory specified by ROSETTA_DRIVERS.");
        }

        URL[] urls = new URL[files.length];
        for (int i = 0; i < files.length; i++) {
            urls[i] = files[i].toURI().toURL();
        }

        URLClassLoader loader = new URLClassLoader(urls, DatabaseTransfer.class.getClassLoader());
        for (URL url : urls) {
            try (URLClassLoader tempLoader = new URLClassLoader(new URL[]{url}, loader)) {
                tempLoader.loadClass("java.sql.Driver");
            } catch (ClassNotFoundException e) {
                System.out.println("Failed to load driver from: " + url);
            }
        }
    }
}
