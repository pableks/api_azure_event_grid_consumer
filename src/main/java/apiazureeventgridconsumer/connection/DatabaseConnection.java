package apiazureeventgridconsumer.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import com.microsoft.azure.functions.ExecutionContext;

public class DatabaseConnection {
    // Variable estática para almacenar el contexto de ejecución actual
    public static ExecutionContext currentContext;

    // Método para establecer el contexto desde la función invocada
    public static void setExecutionContext(ExecutionContext context) {
        currentContext = context;
    }

    // Método para loggear usando el contexto de Azure si está disponible
    private static void logInfo(String message) {
        if (currentContext != null) {
            currentContext.getLogger().info(message);
        } else {
            // Fallback si no hay contexto (por ejemplo, en pruebas locales)
            System.out.println(message);
        }
    }
    
    private static void logError(String message, Throwable e) {
        if (currentContext != null) {
            currentContext.getLogger().severe(message + ": " + e.getMessage());
        } else {
            System.err.println(message + ": " + e.getMessage());
        }
    }

    public static boolean testConnection() {
        try (Connection conn = getConnection()) {
            boolean isValid = conn.isValid(5);
            logInfo("Conexión a la base de datos probada: " + (isValid ? "Válida" : "Inválida"));
            return isValid;
        } catch (SQLException e) {
            logError("Error al probar la conexión", e);
            return false;
        }
    }

    // Get connection to Oracle database
    public static Connection getConnection() throws SQLException {
        try {
            // Load the Oracle JDBC driver
            Class.forName("oracle.jdbc.driver.OracleDriver");
            
            // Get credentials from environment variables
            String tnsName = System.getenv("ORACLE_TNS_NAME");
            String user = System.getenv("ORACLE_USER");
            String password = System.getenv("ORACLE_PASSWORD");
            String walletPath = System.getenv("ORACLE_WALLET_PATH");
            
            // Use defaults if environment variables are not set (for local testing)
            if (user == null) user = "ADMIN";
            if (password == null) password = "PassCloud123";
            if (tnsName == null) tnsName = "g82idu9csvrtaymm_high";
            if (walletPath == null) walletPath = "/Users/pablojavier/Desktop/Wallet_CLOUDS8";
            
            // Build connection string
            String url = "jdbc:oracle:thin:@" + tnsName + "?TNS_ADMIN=" + walletPath;
            
            // Set connection properties
            Properties props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", password);
            props.setProperty("oracle.net.ssl_version", "1.2");
            props.setProperty("oracle.net.wallet_location", "(SOURCE=(METHOD=file)(METHOD_DATA=(DIRECTORY=" + walletPath + ")))");
            
            // Log connection details
            logInfo("Configurando conexión a la base de datos:");
            logInfo("URL: " + url);
            logInfo("Usuario: " + user);
            logInfo("Ubicación del wallet: " + props.getProperty("oracle.net.wallet_location"));
            
            // Connect to the database
            Connection conn = DriverManager.getConnection(url, props);
            logInfo("Conexión establecida correctamente.");
            return conn;
            
        } catch (ClassNotFoundException e) {
            logError("Oracle JDBC driver no encontrado", e);
            throw new SQLException("Oracle JDBC driver not found", e);
        } catch (SQLException e) {
            logError("Error al establecer la conexión", e);
            throw e;
        } catch (Exception e) {
            logError("Error inesperado al establecer la conexión", e);
            throw new SQLException("Unexpected error establishing database connection", e);
        }
    }
}
