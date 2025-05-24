package com.aws.glue.connector.sharepoint.client;

import com.aws.glue.connector.sharepoint.auth.SharePointAuthenticator;
import com.aws.glue.connector.sharepoint.util.SharePointFile;
import com.microsoft.graph.models.DriveItem;
import com.microsoft.graph.models.File;
import com.microsoft.graph.requests.GraphServiceClient;
import com.microsoft.graph.requests.DriveItemRequest;
import com.microsoft.graph.requests.DriveItemRequestBuilder;
import com.microsoft.graph.requests.DriveRequestBuilder;
import com.microsoft.graph.requests.SiteRequestBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.OffsetDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SharePointClient file path functionality
 */
class SharePointClientFilePathTest {
    
    @Mock
    private SharePointAuthenticator mockAuthenticator;
    
    @Mock
    private GraphServiceClient<okhttp3.Request> mockGraphClient;
    
    @Mock
    private SiteRequestBuilder mockSiteBuilder;
    
    @Mock
    private DriveRequestBuilder mockDriveBuilder;
    
    @Mock
    private DriveItemRequestBuilder mockRootBuilder;
    
    @Mock
    private DriveItemRequestBuilder mockItemBuilder;
    
    @Mock
    private DriveItemRequest mockItemRequest;
    
    private SharePointClient client;
    private final String testSiteId = "test-site-id";
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        
        // Setup mock chain
        when(mockAuthenticator.getGraphClient()).thenReturn(mockGraphClient);
        when(mockGraphClient.sites(testSiteId)).thenReturn(mockSiteBuilder);
        when(mockSiteBuilder.drive()).thenReturn(mockDriveBuilder);
        when(mockDriveBuilder.root()).thenReturn(mockRootBuilder);
        when(mockRootBuilder.itemWithPath(anyString())).thenReturn(mockItemBuilder);
        when(mockItemBuilder.buildRequest()).thenReturn(mockItemRequest);
        
        client = new SharePointClient(mockAuthenticator, testSiteId);
    }
    
    @Test
    void getFileByPath_WithValidCsvFile_ShouldReturnFile() {
        // Given
        String filePath = "data/test.csv";
        DriveItem mockItem = new DriveItem();
        mockItem.id = "file-id-123";
        mockItem.name = "test.csv";
        mockItem.size = 1024L;
        mockItem.lastModifiedDateTime = OffsetDateTime.now();
        mockItem.file = new File();
        
        when(mockItemRequest.get()).thenReturn(mockItem);
        
        // When
        SharePointFile result = client.getFileByPath(filePath);
        
        // Then
        assertNotNull(result);
        assertEquals("file-id-123", result.getId());
        assertEquals("test.csv", result.getName());
        assertEquals(1024L, result.getSize());
        assertTrue(result.isCsvFile());
        
        verify(mockRootBuilder).itemWithPath("data/test.csv");
    }
    
    @Test
    void getFileByPath_WithValidExcelFile_ShouldReturnFile() {
        // Given
        String filePath = "reports/data.xlsx";
        DriveItem mockItem = new DriveItem();
        mockItem.id = "file-id-456";
        mockItem.name = "data.xlsx";
        mockItem.size = 2048L;
        mockItem.lastModifiedDateTime = OffsetDateTime.now();
        mockItem.file = new File();
        
        when(mockItemRequest.get()).thenReturn(mockItem);
        
        // When
        SharePointFile result = client.getFileByPath(filePath);
        
        // Then
        assertNotNull(result);
        assertEquals("file-id-456", result.getId());
        assertEquals("data.xlsx", result.getName());
        assertEquals(2048L, result.getSize());
        assertTrue(result.isExcelFile());
        
        verify(mockRootBuilder).itemWithPath("reports/data.xlsx");
    }
    
    @Test
    void getFileByPath_WithLeadingSlash_ShouldRemoveSlash() {
        // Given
        String filePath = "/folder/test.csv";
        DriveItem mockItem = new DriveItem();
        mockItem.id = "file-id-789";
        mockItem.name = "test.csv";
        mockItem.size = 512L;
        mockItem.lastModifiedDateTime = OffsetDateTime.now();
        mockItem.file = new File();
        
        when(mockItemRequest.get()).thenReturn(mockItem);
        
        // When
        SharePointFile result = client.getFileByPath(filePath);
        
        // Then
        assertNotNull(result);
        verify(mockRootBuilder).itemWithPath("folder/test.csv");
    }
    
    @Test
    void getFileByPath_WithUnsupportedFileType_ShouldThrowException() {
        // Given
        String filePath = "documents/test.txt";
        DriveItem mockItem = new DriveItem();
        mockItem.id = "file-id-unsupported";
        mockItem.name = "test.txt";
        mockItem.size = 256L;
        mockItem.lastModifiedDateTime = OffsetDateTime.now();
        mockItem.file = new File();
        
        when(mockItemRequest.get()).thenReturn(mockItem);
        
        // When & Then
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            client.getFileByPath(filePath);
        });
        
        assertTrue(exception.getMessage().contains("Unsupported file type"));
        assertTrue(exception.getMessage().contains("test.txt"));
    }
    
    @Test
    void getFileByPath_WithNonExistentFile_ShouldReturnNull() {
        // Given
        String filePath = "nonexistent/file.csv";
        when(mockItemRequest.get()).thenReturn(null);
        
        // When
        SharePointFile result = client.getFileByPath(filePath);
        
        // Then
        assertNull(result);
        verify(mockRootBuilder).itemWithPath("nonexistent/file.csv");
    }
    
    @Test
    void getFileByPath_WithDirectoryItem_ShouldReturnNull() {
        // Given
        String filePath = "folder";
        DriveItem mockItem = new DriveItem();
        mockItem.id = "folder-id";
        mockItem.name = "folder";
        mockItem.file = null; // This indicates it's a folder, not a file
        
        when(mockItemRequest.get()).thenReturn(mockItem);
        
        // When
        SharePointFile result = client.getFileByPath(filePath);
        
        // Then
        assertNull(result);
    }
    
    @Test
    void getFileByPathAsList_WithValidFile_ShouldReturnSingleFileList() {
        // Given
        String filePath = "test.csv";
        DriveItem mockItem = new DriveItem();
        mockItem.id = "file-id-list";
        mockItem.name = "test.csv";
        mockItem.size = 1024L;
        mockItem.lastModifiedDateTime = OffsetDateTime.now();
        mockItem.file = new File();
        
        when(mockItemRequest.get()).thenReturn(mockItem);
        
        // When
        List<SharePointFile> result = client.getFileByPathAsList(filePath);
        
        // Then
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("file-id-list", result.get(0).getId());
        assertEquals("test.csv", result.get(0).getName());
    }
    
    @Test
    void getFileByPathAsList_WithNonExistentFile_ShouldReturnEmptyList() {
        // Given
        String filePath = "nonexistent.csv";
        when(mockItemRequest.get()).thenReturn(null);
        
        // When
        List<SharePointFile> result = client.getFileByPathAsList(filePath);
        
        // Then
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }
    
    @Test
    void getFileByPath_WithApiException_ShouldThrowRuntimeException() {
        // Given
        String filePath = "error/file.csv";
        when(mockItemRequest.get()).thenThrow(new RuntimeException("Graph API error"));
        
        // When & Then
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            client.getFileByPath(filePath);
        });
        
        assertTrue(exception.getMessage().contains("Failed to get SharePoint file at path"));
        assertTrue(exception.getMessage().contains("error/file.csv"));
    }
}
