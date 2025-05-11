// Consumer function
package apiazureeventgridconsumer;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonElement;
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
    
    // Event type constants
    private static final String EVENT_USER_CREATED = "UserCreated";
    private static final String EVENT_USER_UPDATED = "UserUpdated";
    private static final String EVENT_USER_DELETED = "UserDeleted";
    private static final String EVENT_ROLE_DELETED = "RoleDeleted";
    
    // Default role ID (same as defined in the producer)
    private static final Long DEFAULT_ROLE_ID = 2L;
    
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
        logger.info("Raw content received: " + content);
        
        try {
            Gson gson = new Gson();
            JsonObject eventData = gson.fromJson(content, JsonObject.class);
            
            // Extract event type and data with additional logging and error handling
            if (!eventData.has("eventType")) {
                logger.warning("Event data missing 'eventType' field: " + eventData.toString());
                return;
            }
            
            String eventType = eventData.get("eventType").getAsString();
            logger.info("Processing event type: " + eventType);
            
            // Check if data field exists and extract it safely
            if (!eventData.has("data")) {
                logger.warning("Event data missing 'data' field: " + eventData.toString());
                return;
            }
            
            // Handle different possible formats of the data field
            JsonElement dataElement = eventData.get("data");
            String dataJson;
            
            if (dataElement.isJsonPrimitive()) {
                // If data is a string (common in some Event Grid formats)
                dataJson = dataElement.getAsString();
            } else if (dataElement.isJsonObject()) {
                // If data is directly a JSON object
                dataJson = dataElement.toString();
            } else {
                logger.warning("Unexpected data format: " + dataElement.toString());
                return;
            }
            
            logger.info("Event data: " + dataJson);
            
            switch (eventType) {
                case EVENT_USER_CREATED:
                    processUserCreated(dataJson, logger);
                    break;
                    
                case EVENT_ROLE_DELETED:
                    processRoleDeleted(dataJson, logger);
                    break;
                    
                case EVENT_USER_UPDATED:
                case EVENT_USER_DELETED:
                    // Handle other events
                    logger.info("Processing event: " + eventType);
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
     * Process a user created event
     * This method now checks if the role is null and assigns the default role
     */
    private void processUserCreated(String dataJson, Logger logger) {
        logger.info("Processing user created event");
        
        try {
            Gson gson = new Gson();
            JsonObject userData = gson.fromJson(dataJson, JsonObject.class);
            
            long userId = userData.get("id").getAsLong();
            String email = userData.get("email").getAsString();
            
            // Check if roleId is null or not present
            JsonElement roleIdElement = userData.get("roleId");
            boolean needsDefaultRole = roleIdElement == null || roleIdElement.isJsonNull();
            
            if (needsDefaultRole) {
                logger.info("User " + userId + " (" + email + ") has no role assigned. Assigning default role: " + DEFAULT_ROLE_ID);
                
                // Assign the default role
                assignDefaultRole(userId, email, logger);
            } else {
                logger.info("User " + userId + " (" + email + ") already has role: " + roleIdElement.getAsLong());
            }
            
        } catch (Exception e) {
            logger.severe("Error processing user created event: " + e.getMessage());
        }
    }
    
    /**
     * Assign the default role to a user
     */
    private void assignDefaultRole(long userId, String email, Logger logger) {
        String query = "UPDATE USUARIOS SET ROL = ? WHERE ID = ?";
        
        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            
            stmt.setLong(1, DEFAULT_ROLE_ID);
            stmt.setLong(2, userId);
            
            int rowsUpdated = stmt.executeUpdate();
            
            if (rowsUpdated > 0) {
                logger.info("Default role " + DEFAULT_ROLE_ID + " assigned to user " + email + " (ID: " + userId + ")");
            } else {
                logger.warning("Failed to assign default role to user: " + userId);
            }
            
        } catch (SQLException e) {
            logger.severe("Database error while assigning default role: " + e.getMessage());
        }
    }
    
    /**
     * Process role deleted event - update all users with this role
     */
    private void processRoleDeleted(String dataJson, Logger logger) {
        logger.info("Processing role deleted event");
        
        try {
            Gson gson = new Gson();
            JsonObject data = gson.fromJson(dataJson, JsonObject.class);
            
            long roleId = data.get("roleId").getAsLong();
            
            // Update all users with this role - set their role to NULL
            updateUsersAfterRoleDeletion(roleId, logger);
            
        } catch (Exception e) {
            logger.severe("Error processing role deletion: " + e.getMessage());
        }
    }
    
    /**
     * Update all users belonging to a deleted role
     */
    private void updateUsersAfterRoleDeletion(long roleId, Logger logger) {
        String query = "UPDATE USUARIOS SET ROL = NULL WHERE ROL = ?";
        
        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            
            stmt.setLong(1, roleId);
            
            int updatedRows = stmt.executeUpdate();
            logger.info("Updated " + updatedRows + " users after role deletion (roleId: " + roleId + ")");
            
        } catch (SQLException e) {
            logger.severe("Database error updating users after role deletion: " + e.getMessage());
        }
    }
}