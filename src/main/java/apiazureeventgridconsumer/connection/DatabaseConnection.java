package apiazureeventgridconsumer.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import java.io.File;

public class DatabaseConnection {
    private static final Logger logger = Logger.getLogger(DatabaseConnection.class.getName());

    /**
     * Creates a connection to the Oracle database.
     * Uses environment variables for configuration.
     */
    public static Connection getConnection() throws SQLException {
        try {
            // Load the Oracle JDBC driver
            Class.forName("oracle.jdbc.driver.OracleDriver");
            
            // Get credentials from environment variables
            String user = System.getenv("ORACLE_USER");
            String password = System.getenv("ORACLE_PASSWORD");
            String tnsName = System.getenv("ORACLE_TNS_NAME");
            String walletPath = System.getenv("ORACLE_WALLET_PATH");
            
            // Use defaults if environment variables are not set
            if (user == null) user = "ADMIN";
            if (password == null) password = "PassCloud123";
            if (tnsName == null) tnsName = "g82idu9csvrtaymm_high";
            
            // Determine wallet path based on environment
            if (walletPath == null) {
                // Check if we're running in Azure Functions
                if (System.getenv("WEBSITE_SITE_NAME") != null) {
                    // Azure Functions Linux consumption plan
                    walletPath = "/home/site/wwwroot/wallet";
                    
                    // Check if Linux path exists, otherwise try Windows path
                    File f = new File(walletPath);
                    if (!f.exists()) {
                        walletPath = "D:/home/site/wwwroot/wallet";
                    }
                } else {
                    // Local development
                    walletPath = "/Users/pablojavier/Desktop/Wallet_CLOUDS8";
                }
            }
            
            // Log all connection details for debugging
            logger.info("Connection details:");
            logger.info("User: " + user);
            logger.info("TNS Name: " + tnsName);
            logger.info("Wallet Path: " + walletPath);
            
            // Check if wallet directory exists
            File walletDir = new File(walletPath);
            if (!walletDir.exists()) {
                logger.severe("Wallet directory does not exist: " + walletPath);
                logger.info("Current directory: " + new File(".").getAbsolutePath());
                // List files in parent directory for debugging
                File parentDir = walletDir.getParentFile();
                if (parentDir != null && parentDir.exists()) {
                    logger.info("Files in parent directory (" + parentDir.getAbsolutePath() + "):");
                    for (String file : parentDir.list()) {
                        logger.info(" - " + file);
                    }
                }
            } else {
                logger.info("Wallet directory exists. Files in wallet directory:");
                for (String file : walletDir.list()) {
                    logger.info(" - " + file);
                }
            }
            
            // Build connection string with explicit TNS_ADMIN parameter
            String url = "jdbc:oracle:thin:@" + tnsName + "?TNS_ADMIN=" + walletPath;
            logger.info("Connection URL: " + url);
            
            // Set connection properties
            Properties props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", password);
            props.setProperty("oracle.net.wallet_location", "(SOURCE=(METHOD=file)(METHOD_DATA=(DIRECTORY=" + walletPath + ")))");
            
            // Connect to the database
            Connection conn = DriverManager.getConnection(url, props);
            logger.info("Database connection established successfully");
            return conn;
            
        } catch (ClassNotFoundException e) {
            logger.severe("Oracle JDBC driver not found: " + e.getMessage());
            throw new SQLException("Oracle JDBC driver not found", e);
        } catch (SQLException e) {
            logger.severe("Database connection error: " + e.getMessage());
            logger.severe("Error details: " + e.toString());
            if (e.getCause() != null) {
                logger.severe("Caused by: " + e.getCause().toString());
            }
            throw e;
        }
    }
}