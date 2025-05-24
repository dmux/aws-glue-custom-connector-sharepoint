package com.aws.glue.connector.sharepoint;

import com.aws.glue.connector.sharepoint.client.SharePointClient;
import com.aws.glue.connector.sharepoint.util.SharePointFile;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SharePointDataSourceReader
 */
class SharePointDataSourceReaderTest {
    
    @Mock
    private SharePointClient mockSharePointClient;
    
    private Map<String, String> validOptions;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        
        validOptions = new HashMap<>();
        validOptions.put("sharepoint.clientId", "test-client-id");
        validOptions.put("sharepoint.clientSecret", "test-client-secret");
        validOptions.put("sharepoint.tenantId", "test-tenant-id");
        validOptions.put("sharepoint.siteId", "test-site-id");
    }
    
    @Test
    void readSchema_WithMockedClient_ShouldReturnStructType() {
        // Given
        CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(validOptions);
        SharePointDataSourceReader reader = new SharePointDataSourceReader(options, mockSharePointClient);
        
        // Mock a CSV file
        SharePointFile mockFile = new SharePointFile(
            "file1", "test.csv", 1024L, OffsetDateTime.now()
        );
        List<SharePointFile> mockFiles = Arrays.asList(mockFile);
        
        // Mock CSV content as InputStream
        String csvContent = "Name,Age,City\nJohn,30,New York\nJane,25,London";
        InputStream csvInputStream = new ByteArrayInputStream(csvContent.getBytes());
        
        when(mockSharePointClient.listFiles()).thenReturn(mockFiles);
        when(mockSharePointClient.downloadFile("file1")).thenReturn(csvInputStream);
        
        // When
        StructType schema = reader.readSchema();
        
        // Then
        assertNotNull(schema);
        assertEquals(3, schema.fields().length);
        assertEquals("Name", schema.fields()[0].name());
        assertEquals("Age", schema.fields()[1].name());
        assertEquals("City", schema.fields()[2].name());
        
        // Verify interactions
        verify(mockSharePointClient).listFiles();
        verify(mockSharePointClient).downloadFile("file1");
    }
    
    @Test
    void readSchema_WithNoFiles_ShouldThrowException() {
        // Given
        CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(validOptions);
        SharePointDataSourceReader reader = new SharePointDataSourceReader(options, mockSharePointClient);
        
        when(mockSharePointClient.listFiles()).thenReturn(Arrays.asList());
        
        // When & Then
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            reader.readSchema();
        });
        
        assertEquals("No files found in SharePoint library", exception.getMessage());
        verify(mockSharePointClient).listFiles();
    }
    
    @Test
    void getFiles_ShouldReturnFilesFromClient() {
        // Given
        CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(validOptions);
        SharePointDataSourceReader reader = new SharePointDataSourceReader(options, mockSharePointClient);
        
        SharePointFile mockFile = new SharePointFile(
            "file1", "test.csv", 1024L, OffsetDateTime.now()
        );
        List<SharePointFile> expectedFiles = Arrays.asList(mockFile);
        
        when(mockSharePointClient.listFiles()).thenReturn(expectedFiles);
        
        // When
        List<SharePointFile> actualFiles = reader.getFiles();
        
        // Then
        assertEquals(expectedFiles, actualFiles);
        verify(mockSharePointClient).listFiles();
    }
    
    @Test
    void name_ShouldReturnSharePoint() {
        // Given
        CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(validOptions);
        SharePointDataSourceReader reader = new SharePointDataSourceReader(options, mockSharePointClient);
        
        // When & Then
        assertEquals("SharePoint", reader.name());
    }
}
