package com.aws.glue.connector.sharepoint.auth;

import com.azure.identity.ClientSecretCredential;
import com.microsoft.graph.requests.GraphServiceClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SharePointAuthenticator
 */
class SharePointAuthenticatorTest {
    
    private static final String TEST_CLIENT_ID = "test-client-id";
    private static final String TEST_CLIENT_SECRET = "test-client-secret";
    private static final String TEST_TENANT_ID = "test-tenant-id";
    
    @Mock
    private GraphServiceClient<okhttp3.Request> mockGraphClient;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }
    
    @Test
    void constructor_WithValidCredentials_ShouldCreateInstance() {
        // When & Then
        assertDoesNotThrow(() -> {
            SharePointAuthenticator authenticator = new SharePointAuthenticator(
                TEST_CLIENT_ID, TEST_CLIENT_SECRET, TEST_TENANT_ID
            );
            assertNotNull(authenticator);
        });
    }
    
    @Test
    void constructor_WithNullClientId_ShouldThrowException() {
        // When & Then
        assertThrows(RuntimeException.class, () -> {
            new SharePointAuthenticator(null, TEST_CLIENT_SECRET, TEST_TENANT_ID);
        });
    }
    
    @Test
    void constructor_WithNullClientSecret_ShouldThrowException() {
        // When & Then
        assertThrows(RuntimeException.class, () -> {
            new SharePointAuthenticator(TEST_CLIENT_ID, null, TEST_TENANT_ID);
        });
    }
    
    @Test
    void constructor_WithNullTenantId_ShouldThrowException() {
        // When & Then
        assertThrows(RuntimeException.class, () -> {
            new SharePointAuthenticator(TEST_CLIENT_ID, TEST_CLIENT_SECRET, null);
        });
    }
    
    @Test
    void getGraphClient_AfterSuccessfulInitialization_ShouldReturnClient() {
        // Given
        SharePointAuthenticator authenticator = new SharePointAuthenticator(
            TEST_CLIENT_ID, TEST_CLIENT_SECRET, TEST_TENANT_ID
        );
        
        // When
        GraphServiceClient<okhttp3.Request> client = authenticator.getGraphClient();
        
        // Then
        assertNotNull(client);
    }
    
    @Test
    void getClientInfo_ShouldMaskClientId() {
        // Given
        SharePointAuthenticator authenticator = new SharePointAuthenticator(
            "12345678-1234-1234-1234-123456789012", TEST_CLIENT_SECRET, TEST_TENANT_ID
        );
        
        // When
        String clientInfo = authenticator.getClientInfo();
        
        // Then
        assertTrue(clientInfo.contains("1234***9012"));
        assertTrue(clientInfo.contains(TEST_TENANT_ID));
    }
    
    @Test
    void getClientInfo_WithShortClientId_ShouldMaskCompletely() {
        // Given
        SharePointAuthenticator authenticator = new SharePointAuthenticator(
            "short", TEST_CLIENT_SECRET, TEST_TENANT_ID
        );
        
        // When
        String clientInfo = authenticator.getClientInfo();
        
        // Then
        assertTrue(clientInfo.contains("***"));
        assertTrue(clientInfo.contains(TEST_TENANT_ID));
    }
}