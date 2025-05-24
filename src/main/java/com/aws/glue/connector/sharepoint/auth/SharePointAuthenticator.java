package com.aws.glue.connector.sharepoint.auth;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.microsoft.graph.authentication.TokenCredentialAuthProvider;
import com.microsoft.graph.requests.GraphServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * SharePoint Authentication Handler
 * 
 * Handles OAuth2 Client Credentials flow authentication
 * with Microsoft Graph API for SharePoint access.
 */
public class SharePointAuthenticator {
    
    private static final Logger logger = LoggerFactory.getLogger(SharePointAuthenticator.class);
    
    private static final String GRAPH_SCOPE = "https://graph.microsoft.com/.default";
    
    private final String clientId;
    private final String clientSecret;
    private final String tenantId;
    private GraphServiceClient<okhttp3.Request> graphClient;
    
    public SharePointAuthenticator(String clientId, String clientSecret, String tenantId) {
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.tenantId = tenantId;
        
        initializeGraphClient();
    }
    
    /**
     * Initialize Microsoft Graph Service Client with Client Credentials authentication
     */
    private void initializeGraphClient() {
        try {
            logger.info("Initializing Microsoft Graph client for tenant: {}", tenantId);
            
            // Create Client Secret Credential
            ClientSecretCredential credential = new ClientSecretCredentialBuilder()
                    .clientId(clientId)
                    .clientSecret(clientSecret)
                    .tenantId(tenantId)
                    .build();
            
            // Create Token Credential Auth Provider
            TokenCredentialAuthProvider authProvider = new TokenCredentialAuthProvider(
                    Collections.singletonList(GRAPH_SCOPE),
                    credential
            );
            
            // Build Graph Service Client
            graphClient = GraphServiceClient
                    .builder()
                    .authenticationProvider(authProvider)
                    .buildClient();
            
            logger.info("Microsoft Graph client initialized successfully");
            
        } catch (Exception e) {
            logger.error("Failed to initialize Microsoft Graph client", e);
            throw new RuntimeException("SharePoint authentication failed", e);
        }
    }
    
    /**
     * Get authenticated Graph Service Client
     * 
     * @return Authenticated GraphServiceClient instance
     */
    public GraphServiceClient<okhttp3.Request> getGraphClient() {
        if (graphClient == null) {
            throw new IllegalStateException("Graph client not initialized");
        }
        return graphClient;
    }
    
    /**
     * Test authentication by making a simple API call
     * 
     * @return true if authentication is successful, false otherwise
     */
    public boolean testAuthentication() {
        try {
            logger.debug("Testing SharePoint authentication");
            
            // Simple test call to validate authentication
            graphClient.me().buildRequest().get();
            
            logger.info("SharePoint authentication test successful");
            return true;
            
        } catch (Exception e) {
            logger.warn("SharePoint authentication test failed", e);
            return false;
        }
    }
    
    /**
     * Get client information for logging purposes
     */
    public String getClientInfo() {
        return String.format("ClientId: %s, TenantId: %s", 
                           maskClientId(clientId), tenantId);
    }
    
    private String maskClientId(String clientId) {
        if (clientId == null || clientId.length() < 8) {
            return "***";
        }
        return clientId.substring(0, 4) + "***" + clientId.substring(clientId.length() - 4);
    }
}
