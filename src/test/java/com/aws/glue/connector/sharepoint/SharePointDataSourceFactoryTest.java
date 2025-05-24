package com.aws.glue.connector.sharepoint;

import com.aws.glue.connector.sharepoint.client.SharePointClient;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SharePointDataSourceFactory
 */
class SharePointDataSourceFactoryTest {
    
    @Mock
    private SharePointDataSourceReader mockReader;
    
    private TestableSharePointDataSourceFactory factory;
    private Map<String, String> validOptions;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        factory = new TestableSharePointDataSourceFactory();
        
        validOptions = new HashMap<>();
        validOptions.put("sharepoint.clientId", "test-client-id");
        validOptions.put("sharepoint.clientSecret", "test-client-secret");
        validOptions.put("sharepoint.tenantId", "test-tenant-id");
        validOptions.put("sharepoint.siteId", "test-site-id");
    }
    
    @Test
    void inferSchema_WithValidOptions_ShouldReturnStructType() {
        // Given
        CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(validOptions);
        
        StructType expectedSchema = new StructType(new StructField[]{
            DataTypes.createStructField("Name", DataTypes.StringType, true),
            DataTypes.createStructField("Age", DataTypes.StringType, true),
            DataTypes.createStructField("City", DataTypes.StringType, true)
        });
        
        factory.setMockReader(mockReader);
        when(mockReader.readSchema()).thenReturn(expectedSchema);
        
        // When
        StructType actualSchema = factory.inferSchema(options);
        
        // Then
        assertNotNull(actualSchema);
        assertEquals(expectedSchema, actualSchema);
        verify(mockReader).readSchema();
    }
    
    @Test
    void getTable_WithValidOptions_ShouldReturnTable() {
        // Given
        CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(validOptions);
        StructType schema = new StructType();
        Transform[] partitioning = new Transform[0];
        
        factory.setMockReader(mockReader);
        
        // When
        Table table = factory.getTable(schema, partitioning, validOptions);
        
        // Then
        assertNotNull(table);
        assertEquals(mockReader, table);
    }
    
    @Test
    void inferSchema_WithMissingClientId_ShouldThrowException() {
        // Given
        validOptions.remove("sharepoint.clientId");
        CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(validOptions);
        
        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            factory.inferSchema(options);
        });
    }
    
    @Test
    void inferSchema_WithMissingClientSecret_ShouldThrowException() {
        // Given
        validOptions.remove("sharepoint.clientSecret");
        CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(validOptions);
        
        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            factory.inferSchema(options);
        });
    }
    
    @Test
    void inferSchema_WithMissingTenantId_ShouldThrowException() {
        // Given
        validOptions.remove("sharepoint.tenantId");
        CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(validOptions);
        
        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            factory.inferSchema(options);
        });
    }
    
    @Test
    void inferSchema_WithMissingSiteId_ShouldThrowException() {
        // Given
        validOptions.remove("sharepoint.siteId");
        CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(validOptions);
        
        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            factory.inferSchema(options);
        });
    }
    
    @Test
    void inferSchema_WithEmptyOptions_ShouldThrowException() {
        // Given
        CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(new HashMap<>());
        
        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            factory.inferSchema(options);
        });
    }
    
    /**
     * Testable version of SharePointDataSourceFactory that allows injecting mock readers
     */
    private static class TestableSharePointDataSourceFactory extends SharePointDataSourceFactory {
        private SharePointDataSourceReader mockReader;
        
        public void setMockReader(SharePointDataSourceReader mockReader) {
            this.mockReader = mockReader;
        }
        
        @Override
        protected SharePointDataSourceReader createReader(CaseInsensitiveStringMap options) {
            if (mockReader != null) {
                return mockReader;
            }
            return super.createReader(options);
        }
    }
}