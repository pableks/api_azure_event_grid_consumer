// Consumer function
package apiazureeventgridconsumer;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.EventGridTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;

import apiazureeventgridconsumer.connection.DatabaseConnection;
import apiazureeventgridconsumer.model.User;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * Azure Functions with Event Grid Trigger.
 * This function processes events from Event Grid and performs database operations.
 */
public class Function {
    
    /**
     * This function gets triggered by Event Grid events.
     * It processes user-related events and performs the actual database operations.
     */
    @FunctionName("ProcessUserEvents")
    public void run(
            @EventGridTrigger(name = "eventGridEvent") String content,
            final ExecutionContext context) {
        Logger logger = context.getLogger();
        logger.info("Event Grid trigger function executed.");
        
        try {
            // Parse the event
            Gson gson = new Gson();
            JsonObject eventGridEvent = gson.fromJson(content, JsonObject.class);
            
            logger.info("Event received: " + eventGridEvent.toString());
            
            // Extract event information
            String eventType = eventGridEvent.get("eventType").getAsString();
            String dataStr = eventGridEvent.get("data").toString();
            
            // Parse the user data
            User user = gson.fromJson(dataStr, User.class);
            logger.info("User data: " + user.toString());
            
            // Process different event types
            switch (eventType) {
                case "UserCreated":
                    createUser(user, logger);
                    break;
                case "UserUpdated":
                    updateUser(user, logger);
                    break;
                case "UserDeleted":
                    deleteUser(user.getId(), logger);
                    break;
                default:
                    logger.warning("Unknown event type: " + eventType);
            }
        } catch (Exception e) {
            logger.severe("Error processing event: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Create a user in the database
     */
    private void createUser(User user, Logger logger) {
        String query = "INSERT INTO USUARIOS (EMAIL, PASSWORD, ROL) VALUES (?, ?, ?)";
        
        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query, new String[]{"ID"})) {
            
            stmt.setString(1, user.getEmail());
            stmt.setString(2, user.getPassword());
            stmt.setLong(3, user.getRoleId());
            
            int affectedRows = stmt.executeUpdate();
            
            if (affectedRows > 0) {
                // Get the generated ID
                ResultSet generatedKeys = stmt.getGeneratedKeys();
                if (generatedKeys.next()) {
                    Long userId = generatedKeys.getLong(1);
                    logger.info("User created with ID: " + userId);
                }
            } else {
                logger.warning("Failed to create user");
            }
        } catch (SQLException e) {
            logger.severe("Database error while creating user: " + e.getMessage());
        }
    }
    
    /**
     * Update a user in the database
     */
    private void updateUser(User user, Logger logger) {
        String query = "UPDATE USUARIOS SET EMAIL = ?, PASSWORD = ?, ROL = ? WHERE ID = ?";
        
        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            
            stmt.setString(1, user.getEmail());
            stmt.setString(2, user.getPassword());
            stmt.setLong(3, user.getRoleId());
            stmt.setLong(4, user.getId());
            
            int rowsUpdated = stmt.executeUpdate();
            if (rowsUpdated > 0) {
                logger.info("User updated successfully: ID = " + user.getId());
            } else {
                logger.warning("Failed to update user: ID = " + user.getId() + " (user may not exist)");
            }
        } catch (SQLException e) {
            logger.severe("Database error while updating user: " + e.getMessage());
        }
    }
    
    /**
     * Delete a user from the database
     */
    private void deleteUser(Long id, Logger logger) {
        String query = "DELETE FROM USUARIOS WHERE ID = ?";
        
        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            
            stmt.setLong(1, id);
            
            int rowsDeleted = stmt.executeUpdate();
            if (rowsDeleted > 0) {
                logger.info("User deleted successfully: ID = " + id);
            } else {
                logger.warning("Failed to delete user: ID = " + id + " (user may not exist)");
            }
        } catch (SQLException e) {
            logger.severe("Database error while deleting user: " + e.getMessage());
        }
    }
}