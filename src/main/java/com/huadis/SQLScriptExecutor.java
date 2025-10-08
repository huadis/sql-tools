package com.huadis;

import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 功能说明：  <br>
 * A universal SQL script executor that supports both MySQL and PostgreSQL databases.
 * It can execute SQL scripts containing multiple statements with proper error handling
 * and execution reporting.
 * 开发人员：@author huadi <br>
 * 开发时间: 2025年10月08日 <br>
 */
public class SQLScriptExecutor {

    // Default SQL statement delimiter
    private static final String DEFAULT_DELIMITER = ";";

    private static final Map<String, String> DEFAULT_DB_DRIVERS = new HashMap<>();

    static {
        DEFAULT_DB_DRIVERS.put("mysql", "com.mysql.cj.jdbc.Driver");
        DEFAULT_DB_DRIVERS.put("mysql5", "com.mysql.jdbc.Driver");
        DEFAULT_DB_DRIVERS.put("mysql8", "com.mysql.cj.jdbc.Driver");
        DEFAULT_DB_DRIVERS.put("pgsql", "org.postgresql.Driver");
    }

    // 获取驱动的方法
    public static String getDriver(String dbType) {
        return DEFAULT_DB_DRIVERS.get(dbType);
    }

    /**
     * Configuration model to hold database connection parameters
     */
    public static class DbConfig {
        private String dbType;
        private String jdbcUrl;
        private String username;
        private String password;
        private String driverClass = getDriver("mysql");
        private String scriptPath;

        // Getters
        public String getDbType() {
            return dbType;
        }

        public String getJdbcUrl() {
            return jdbcUrl;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

        public String getDriverClass() {
            return driverClass;
        }

        public String getScriptPath() {
            return scriptPath;
        }

        // Setters
        public void setDbType(String dbType) {
            this.dbType = dbType;
        }

        public void setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public void setDriverClass(String driverClass) {
            if (driverClass == null || driverClass.trim().isEmpty()) {
                this.driverClass = getDriver(getDbType());
            } else {
                this.driverClass = driverClass;
            }
        }

        public void setScriptPath(String scriptPath) {
            this.scriptPath = scriptPath;
        }

        @Override
        public String toString() {
            return "DbConfig{\n" +
                    "  dbType='" + dbType + "'\n" +
                    "  jdbcUrl='" + jdbcUrl + "'\n" +
                    "  username='" + username + "'\n" +
                    "  password='" + password + "'\n" +
                    "  driverClass='" + driverClass + "'\n" +
                    "  scriptPath='" + scriptPath + "'\n" +
                    "}";
        }
    }

    /**
     * Main method to execute SQL script
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        DbConfig config = null;

        try {
            // Determine configuration source: YAML file or command-line arguments
            if (args.length == 1 && (args[0].endsWith(".yml") || args[0].endsWith(".yaml"))) {
                // Load configuration from YAML file
                System.out.println("Loading configuration from YAML file: " + args[0]);
                config = loadConfigFromYaml(args[0]);
            } else if (args.length == 2 && (args[0].endsWith(".yml") || args[0].endsWith(".yaml"))) {
                // Load configuration from YAML file
                System.out.println("Loading configuration from YAML file: " + args[0]);
                config = loadConfigFromYaml(args[0]);
                config.setScriptPath(args[1]);
            } else if (args.length == 5) {
                // Load configuration from command-line arguments
                System.out.println("Loading configuration from command-line arguments");
                config = createConfigFromArgs(args);
            } else {
                printUsage();
                return;
            }
            System.out.println("SQLConfig: " + config);
            // Validate configuration
            if (!validateConfig(config)) {
                System.err.println("Invalid configuration parameters");
                return;
            }

            // Execute SQL script with the configuration
            executeScript(config);

        } catch (FileNotFoundException e) {
            System.err.println("Configuration file not found: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error during execution: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Creates configuration from command-line arguments
     *
     * @param args Command line arguments
     * @return DbConfig object
     */
    private static DbConfig createConfigFromArgs(String[] args) {
        DbConfig config = new DbConfig();
        config.setDbType(args[0].toLowerCase());
        config.setJdbcUrl(args[1]);
        config.setUsername(args[2]);
        config.setPassword(args[3]);
        config.setScriptPath(args[4]);
        config.setDriverClass(null);
        return config;
    }

    /**
     * Loads configuration from YAML file
     *
     * @param yamlFilePath Path to YAML file
     * @return DbConfig object
     * @throws FileNotFoundException if file not found
     */
    private static DbConfig loadConfigFromYaml(String yamlFilePath) throws IOException {
        Yaml yaml = new Yaml();
        Map<String, Object> yamlData = yaml.load(Files.newInputStream(Paths.get(yamlFilePath)));

        // Extract database configuration section
        Map<String, Object> dbConfigMap = (Map<String, Object>) yamlData.get("datasource");

        DbConfig config = new DbConfig();
        config.setDbType(((String) dbConfigMap.get("dialect")).toLowerCase());
        config.setJdbcUrl((String) dbConfigMap.get("url"));
        config.setUsername((String) dbConfigMap.get("username"));
        config.setPassword((String) dbConfigMap.get("password"));
        config.setDriverClass((String) dbConfigMap.get("driver"));
        config.setScriptPath((String) dbConfigMap.get("scriptPath"));


        return config;
    }

    /**
     * Validates configuration parameters
     *
     * @param config DbConfig object to validate
     * @return true if valid, false otherwise
     */
    private static boolean validateConfig(DbConfig config) {
        if (config == null) return false;

        // Check required fields
        if (config.getDbType() == null || config.getJdbcUrl() == null ||
                config.getUsername() == null || config.getDriverClass() == null ||
                config.getScriptPath() == null) {
            return false;
        }

        // Check supported database types
        return config.getDbType().equals("mysql") || config.getDbType().equals("pgsql");
    }

    /**
     * Executes SQL script using provided configuration
     *
     * @param config Database configuration
     * @throws SQLException           if database error occurs
     * @throws IOException            if file error occurs
     * @throws ClassNotFoundException if driver not found
     */
    private static void executeScript(DbConfig config) throws SQLException, IOException, ClassNotFoundException {
        Connection conn = null;
        Statement stmt = null;

        try {
            // Load database driver
            System.out.println("Loading JDBC driver: " + config.getDriverClass());
            Class.forName(config.getDriverClass());

            // Establish database connection
            System.out.println("Connecting to " + config.getDbType() + " database: " + config.getJdbcUrl());
            conn = DriverManager.getConnection(config.getJdbcUrl(),
                    config.getUsername(),
                    config.getPassword());

            // Create statement object
            stmt = conn.createStatement();

            // Read and parse SQL script
            System.out.println("Reading SQL script: " + config.getScriptPath());
            List<String> sqlCommands = readAndSplitScript(config.getScriptPath(), DEFAULT_DELIMITER);

            // Execute SQL commands
            System.out.println("Starting execution of " + sqlCommands.size() + " SQL commands");
            int successCount = 0;

            for (int i = 0; i < sqlCommands.size(); i++) {
                String sql = sqlCommands.get(i);
                if (!sql.trim().isEmpty()) {
                    try {
                        stmt.execute(sql);
                        successCount++;
                        // Print progress every 10 commands
                        if ((i + 1) % 10 == 0) {
                            System.out.println("Executed " + (i + 1) + " commands, " + successCount + " successful");
                        }
                    } catch (SQLException e) {
                        System.err.println("Error executing command " + (i + 1) + ": " + e.getMessage());
                        System.err.println("Failed SQL: " + sql);
                    }
                }
            }

            // Print final execution summary
            System.out.println("\nExecution complete - Total: " + sqlCommands.size() +
                    ", Successful: " + successCount +
                    ", Failed: " + (sqlCommands.size() - successCount));

        } finally {
            // Clean up resources
            if (stmt != null) stmt.close();
            if (conn != null) {
                conn.close();
                System.out.println("Database connection closed");
            }
        }
    }

    /**
     * Reads SQL script file and splits it into individual commands
     *
     * @param filePath  Path to SQL script file
     * @param delimiter Command delimiter
     * @return List of SQL commands
     * @throws IOException If error occurs while reading file
     */
    private static List<String> readAndSplitScript(String filePath, String delimiter) throws IOException {
        List<String> sqlCommands = new ArrayList<>();
        StringBuilder currentCommand = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmedLine = line.trim();

                // Skip comment lines and empty lines
                if (trimmedLine.isEmpty() || trimmedLine.startsWith("--")) {
                    continue;
                }

                currentCommand.append(line).append("\n");

                // Check if line ends with delimiter
                if (trimmedLine.endsWith(delimiter)) {
                    // Remove delimiter and add to list
                    String sql = currentCommand.toString().replace(delimiter, "").trim();
                    sqlCommands.add(sql);
                    currentCommand.setLength(0); // Reset for next command
                }
            }

            // Handle last command that might not have delimiter
            if (currentCommand.length() > 0) {
                sqlCommands.add(currentCommand.toString().trim());
            }
        }

        return sqlCommands;
    }

    /**
     * Prints usage instructions
     */
    private static void printUsage() {
        System.out.println("Usage 1 (Command-line arguments):");
        System.out.println("java SQLScriptExecutor <dbType> <jdbcUrl> <username> <password> <scriptPath>");
        System.out.println("\nUsage 2 (YAML configuration file):");
        System.out.println("java SQLScriptExecutor <path_to_config.yml>");
        System.out.println("\nExample YAML configuration:");
        System.out.println("database:");
        System.out.println("  type: mysql");
        System.out.println("  url: jdbc:mysql://localhost:3306/mydb?useSSL=false");
        System.out.println("  username: root");
        System.out.println("  password: password");
        System.out.println("  driver: com.mysql.cj.jdbc.Driver");
        System.out.println("  scriptPath: ./script.sql");
    }
}
