package com.aws.glue.connector.sharepoint.util;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SharePointFile
 */
class SharePointFileTest {
    
    @Test
    void constructor_WithValidParameters_ShouldCreateInstance() {
        // Given
        String id = "test-id";
        String name = "test.csv";
        Long size = 1024L;
        OffsetDateTime lastModified = OffsetDateTime.now();
        
        // When
        SharePointFile file = new SharePointFile(id, name, size, lastModified);
        
        // Then
        assertEquals(id, file.getId());
        assertEquals(name, file.getName());
        assertEquals(size, file.getSize());
        assertEquals(lastModified, file.getLastModified());
    }
    
    @Test
    void getExtension_WithCsvFile_ShouldReturnCsvExtension() {
        // Given
        SharePointFile file = new SharePointFile("id", "test.csv", 1024L, OffsetDateTime.now());
        
        // When
        String extension = file.getExtension();
        
        // Then
        assertEquals(".csv", extension);
    }
    
    @Test
    void getExtension_WithExcelFile_ShouldReturnExcelExtension() {
        // Given
        SharePointFile xlsFile = new SharePointFile("id1", "test.xls", 1024L, OffsetDateTime.now());
        SharePointFile xlsxFile = new SharePointFile("id2", "test.xlsx", 1024L, OffsetDateTime.now());
        
        // When & Then
        assertEquals(".xls", xlsFile.getExtension());
        assertEquals(".xlsx", xlsxFile.getExtension());
    }
    
    @Test
    void getExtension_WithUppercaseExtension_ShouldReturnLowercase() {
        // Given
        SharePointFile file = new SharePointFile("id", "TEST.CSV", 1024L, OffsetDateTime.now());
        
        // When
        String extension = file.getExtension();
        
        // Then
        assertEquals(".csv", extension);
    }
    
    @Test
    void getExtension_WithNoExtension_ShouldReturnEmptyString() {
        // Given
        SharePointFile file = new SharePointFile("id", "testfile", 1024L, OffsetDateTime.now());
        
        // When
        String extension = file.getExtension();
        
        // Then
        assertEquals("", extension);
    }
    
    @Test
    void getExtension_WithNullName_ShouldReturnEmptyString() {
        // Given
        SharePointFile file = new SharePointFile("id", null, 1024L, OffsetDateTime.now());
        
        // When
        String extension = file.getExtension();
        
        // Then
        assertEquals("", extension);
    }
    
    @Test
    void isCsvFile_WithCsvExtension_ShouldReturnTrue() {
        // Given
        SharePointFile file = new SharePointFile("id", "test.csv", 1024L, OffsetDateTime.now());
        
        // When & Then
        assertTrue(file.isCsvFile());
    }
    
    @Test
    void isCsvFile_WithExcelExtension_ShouldReturnFalse() {
        // Given
        SharePointFile file = new SharePointFile("id", "test.xlsx", 1024L, OffsetDateTime.now());
        
        // When & Then
        assertFalse(file.isCsvFile());
    }
    
    @Test
    void isExcelFile_WithExcelExtensions_ShouldReturnTrue() {
        // Given
        SharePointFile xlsFile = new SharePointFile("id1", "test.xls", 1024L, OffsetDateTime.now());
        SharePointFile xlsxFile = new SharePointFile("id2", "test.xlsx", 1024L, OffsetDateTime.now());
        
        // When & Then
        assertTrue(xlsFile.isExcelFile());
        assertTrue(xlsxFile.isExcelFile());
    }
    
    @Test
    void isExcelFile_WithCsvExtension_ShouldReturnFalse() {
        // Given
        SharePointFile file = new SharePointFile("id", "test.csv", 1024L, OffsetDateTime.now());
        
        // When & Then
        assertFalse(file.isExcelFile());
    }
    
    @Test
    void equals_WithSameId_ShouldReturnTrue() {
        // Given
        String id = "same-id";
        SharePointFile file1 = new SharePointFile(id, "file1.csv", 1024L, OffsetDateTime.now());
        SharePointFile file2 = new SharePointFile(id, "file2.xlsx", 2048L, OffsetDateTime.now());
        
        // When & Then
        assertEquals(file1, file2);
    }
    
    @Test
    void equals_WithDifferentId_ShouldReturnFalse() {
        // Given
        SharePointFile file1 = new SharePointFile("id1", "file.csv", 1024L, OffsetDateTime.now());
        SharePointFile file2 = new SharePointFile("id2", "file.csv", 1024L, OffsetDateTime.now());
        
        // When & Then
        assertNotEquals(file1, file2);
    }
    
    @Test
    void equals_WithNull_ShouldReturnFalse() {
        // Given
        SharePointFile file = new SharePointFile("id", "file.csv", 1024L, OffsetDateTime.now());
        
        // When & Then
        assertNotEquals(file, null);
    }
    
    @Test
    void hashCode_WithSameId_ShouldReturnSameHash() {
        // Given
        String id = "same-id";
        SharePointFile file1 = new SharePointFile(id, "file1.csv", 1024L, OffsetDateTime.now());
        SharePointFile file2 = new SharePointFile(id, "file2.xlsx", 2048L, OffsetDateTime.now());
        
        // When & Then
        assertEquals(file1.hashCode(), file2.hashCode());
    }
    
    @Test
    void toString_ShouldContainAllFields() {
        // Given
        String id = "test-id";
        String name = "test.csv";
        Long size = 1024L;
        OffsetDateTime lastModified = OffsetDateTime.now();
        SharePointFile file = new SharePointFile(id, name, size, lastModified);
        
        // When
        String toString = file.toString();
        
        // Then
        assertTrue(toString.contains(id));
        assertTrue(toString.contains(name));
        assertTrue(toString.contains(size.toString()));
    }
}