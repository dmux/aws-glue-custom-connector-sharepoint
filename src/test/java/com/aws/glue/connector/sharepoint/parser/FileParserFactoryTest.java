package com.aws.glue.connector.sharepoint.parser;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for FileParserFactory
 */
class FileParserFactoryTest {
    
    @Test
    void getParser_WithCsvFile_ShouldReturnCsvParser() {
        // When
        FileParser parser = FileParserFactory.getParser("test.csv");
        
        // Then
        assertNotNull(parser);
        assertInstanceOf(CsvFileParser.class, parser);
        assertTrue(parser.supports(".csv"));
    }
    
    @Test
    void getParser_WithXlsFile_ShouldReturnExcelParser() {
        // When
        FileParser parser = FileParserFactory.getParser("test.xls");
        
        // Then
        assertNotNull(parser);
        assertInstanceOf(ExcelFileParser.class, parser);
        assertTrue(parser.supports(".xls"));
    }
    
    @Test
    void getParser_WithXlsxFile_ShouldReturnExcelParser() {
        // When
        FileParser parser = FileParserFactory.getParser("test.xlsx");
        
        // Then
        assertNotNull(parser);
        assertInstanceOf(ExcelFileParser.class, parser);
        assertTrue(parser.supports(".xlsx"));
    }
    
    @Test
    void getParser_WithUppercaseExtension_ShouldWork() {
        // When
        FileParser csvParser = FileParserFactory.getParser("TEST.CSV");
        FileParser xlsParser = FileParserFactory.getParser("TEST.XLS");
        FileParser xlsxParser = FileParserFactory.getParser("TEST.XLSX");
        
        // Then
        assertInstanceOf(CsvFileParser.class, csvParser);
        assertInstanceOf(ExcelFileParser.class, xlsParser);
        assertInstanceOf(ExcelFileParser.class, xlsxParser);
    }
    
    @Test
    void getParser_WithMixedCaseExtension_ShouldWork() {
        // When
        FileParser csvParser = FileParserFactory.getParser("test.CsV");
        FileParser xlsParser = FileParserFactory.getParser("test.XlS");
        FileParser xlsxParser = FileParserFactory.getParser("test.XlSx");
        
        // Then
        assertInstanceOf(CsvFileParser.class, csvParser);
        assertInstanceOf(ExcelFileParser.class, xlsParser);
        assertInstanceOf(ExcelFileParser.class, xlsxParser);
    }
    
    @Test
    void getParser_WithUnsupportedExtension_ShouldThrowException() {
        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            FileParserFactory.getParser("test.txt");
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            FileParserFactory.getParser("test.pdf");
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            FileParserFactory.getParser("test.doc");
        });
    }
    
    @Test
    void getParser_WithNoExtension_ShouldThrowException() {
        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            FileParserFactory.getParser("testfile");
        });
    }
    
    @Test
    void getParser_WithNullFileName_ShouldThrowException() {
        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            FileParserFactory.getParser(null);
        });
    }
    
    @Test
    void getParser_WithEmptyFileName_ShouldThrowException() {
        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            FileParserFactory.getParser("");
        });
    }
    
    @Test
    void isSupported_WithSupportedExtensions_ShouldReturnTrue() {
        // When & Then
        assertTrue(FileParserFactory.isSupported("test.csv"));
        assertTrue(FileParserFactory.isSupported("test.xls"));
        assertTrue(FileParserFactory.isSupported("test.xlsx"));
        assertTrue(FileParserFactory.isSupported("TEST.CSV"));
        assertTrue(FileParserFactory.isSupported("test.XlSx"));
    }
    
    @Test
    void isSupported_WithUnsupportedExtensions_ShouldReturnFalse() {
        // When & Then
        assertFalse(FileParserFactory.isSupported("test.txt"));
        assertFalse(FileParserFactory.isSupported("test.pdf"));
        assertFalse(FileParserFactory.isSupported("test.doc"));
        assertFalse(FileParserFactory.isSupported("testfile"));
    }
    
    @Test
    void isSupported_WithNullFileName_ShouldReturnFalse() {
        // When & Then
        assertFalse(FileParserFactory.isSupported(null));
    }
    
    @Test
    void isSupported_WithEmptyFileName_ShouldReturnFalse() {
        // When & Then
        assertFalse(FileParserFactory.isSupported(""));
    }
}