package com.aws.glue.connector.sharepoint.parser;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for CsvFileParser
 */
class CsvFileParserTest {
    
    private CsvFileParser parser;
    
    @BeforeEach
    void setUp() {
        parser = new CsvFileParser();
    }
    
    @Test
    void supports_WithCsvExtension_ShouldReturnTrue() {
        // When & Then
        assertTrue(parser.supports(".csv"));
    }
    
    @Test
    void supports_WithExcelExtension_ShouldReturnFalse() {
        // When & Then
        assertFalse(parser.supports(".xlsx"));
        assertFalse(parser.supports(".xls"));
    }
    
    @Test
    void supports_WithOtherExtension_ShouldReturnFalse() {
        // When & Then
        assertFalse(parser.supports(".txt"));
        assertFalse(parser.supports(".pdf"));
    }
    
    @Test
    void inferSchema_WithValidCsvHeaders_ShouldReturnCorrectSchema() {
        // Given
        String csvContent = "Name,Age,City\nJohn,30,New York\nJane,25,London";
        InputStream inputStream = new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8));
        
        // When
        StructType schema = parser.inferSchema(inputStream);
        
        // Then
        assertNotNull(schema);
        assertEquals(3, schema.fields().length);
        assertEquals("Name", schema.fields()[0].name());
        assertEquals("Age", schema.fields()[1].name());
        assertEquals("City", schema.fields()[2].name());
    }
    
    @Test
    void inferSchema_WithEmptyFile_ShouldThrowException() {
        // Given
        String csvContent = "";
        InputStream inputStream = new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8));
        
        // When & Then
        assertThrows(RuntimeException.class, () -> {
            parser.inferSchema(inputStream);
        });
    }
    
    @Test
    void inferSchema_WithHeadersOnly_ShouldReturnCorrectSchema() {
        // Given
        String csvContent = "Name,Age,City";
        InputStream inputStream = new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8));
        
        // When
        StructType schema = parser.inferSchema(inputStream);
        
        // Then
        assertNotNull(schema);
        assertEquals(3, schema.fields().length);
    }
    
    @Test
    void parseFile_WithValidCsvData_ShouldReturnCorrectRows() {
        // Given
        String csvContent = "Name,Age,City\nJohn,30,New York\nJane,25,London";
        InputStream inputStream = new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8));
        
        // First infer schema
        InputStream schemaStream = new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8));
        StructType schema = parser.inferSchema(schemaStream);
        
        // When
        List<InternalRow> rows = parser.parseFile(inputStream, schema);
        
        // Then
        assertNotNull(rows);
        assertEquals(2, rows.size());
        
        // Check first row (handling both UTF8String and String fallback)
        InternalRow firstRow = rows.get(0);
        assertRowValue(firstRow, 0, "John");
        assertRowValue(firstRow, 1, "30");
        assertRowValue(firstRow, 2, "New York");
        
        // Check second row
        InternalRow secondRow = rows.get(1);
        assertRowValue(secondRow, 0, "Jane");
        assertRowValue(secondRow, 1, "25");
        assertRowValue(secondRow, 2, "London");
    }
    
    @Test
    void parseFile_WithMissingValues_ShouldHandleGracefully() {
        // Given
        String csvContent = "Name,Age,City\nJohn,30,\nJane,,London";
        InputStream inputStream = new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8));
        
        // First infer schema
        InputStream schemaStream = new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8));
        StructType schema = parser.inferSchema(schemaStream);
        
        // When
        List<InternalRow> rows = parser.parseFile(inputStream, schema);
        
        // Then
        assertNotNull(rows);
        assertEquals(2, rows.size());
        
        // Check first row with missing city
        InternalRow firstRow = rows.get(0);
        assertRowValue(firstRow, 2, "");
        
        // Check second row with missing age
        InternalRow secondRow = rows.get(1);
        assertRowValue(secondRow, 1, "");
    }
    
    @Test
    void parseFile_WithQuotedValues_ShouldParseCorrectly() {
        // Given
        String csvContent = "Name,Description\n\"John Doe\",\"A person with, commas in description\"\n\"Jane Smith\",\"Another person\"";
        InputStream inputStream = new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8));
        
        // First infer schema
        InputStream schemaStream = new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8));
        StructType schema = parser.inferSchema(schemaStream);
        
        // When
        List<InternalRow> rows = parser.parseFile(inputStream, schema);
        
        // Then
        assertNotNull(rows);
        assertEquals(2, rows.size());
        
        // Check first row
        InternalRow firstRow = rows.get(0);
        assertRowValue(firstRow, 0, "John Doe");
        assertRowValue(firstRow, 1, "A person with, commas in description");
    }
    
    /**
     * Helper method to assert row values, handling both UTF8String and String types
     */
    private void assertRowValue(InternalRow row, int index, String expectedValue) {
        Object actualValue = row.get(index, org.apache.spark.sql.types.DataTypes.StringType);
        if (actualValue instanceof UTF8String) {
            assertEquals(UTF8String.fromString(expectedValue), actualValue);
        } else if (actualValue instanceof String) {
            assertEquals(expectedValue, actualValue);
        } else {
            fail("Unexpected value type: " + (actualValue != null ? actualValue.getClass() : "null"));
        }
    }
}