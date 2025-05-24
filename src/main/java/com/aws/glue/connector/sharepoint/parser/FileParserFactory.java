package com.aws.glue.connector.sharepoint.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating appropriate file parsers
 * 
 * Returns the correct parser implementation based on file extension.
 */
public class FileParserFactory {
    
    private static final Logger logger = LoggerFactory.getLogger(FileParserFactory.class);
    
    /**
     * Get appropriate parser for the given file name
     * 
     * @param fileName Name of the file to parse
     * @return FileParser implementation for the file type
     * @throws IllegalArgumentException if file type is not supported
     */
    public static FileParser getParser(String fileName) {
        if (fileName == null) {
            throw new IllegalArgumentException("File name cannot be null");
        }
        
        String extension = getFileExtension(fileName);
        
        switch (extension) {
            case ".csv":
                logger.debug("Creating CSV parser for file: {}", fileName);
                return new CsvFileParser();
                
            case ".xls":
            case ".xlsx":
                logger.debug("Creating Excel parser for file: {}", fileName);
                return new ExcelFileParser();
                
            default:
                throw new IllegalArgumentException(
                    "Unsupported file type: " + extension + 
                    ". Supported types: .csv, .xls, .xlsx"
                );
        }
    }
    
    /**
     * Check if the file type is supported
     * 
     * @param fileName Name of the file
     * @return true if supported, false otherwise
     */
    public static boolean isSupported(String fileName) {
        if (fileName == null) {
            return false;
        }
        
        String extension = getFileExtension(fileName);
        return ".csv".equals(extension) || 
               ".xls".equals(extension) || 
               ".xlsx".equals(extension);
    }
    
    /**
     * Extract file extension from file name
     */
    private static String getFileExtension(String fileName) {
        int lastDot = fileName.lastIndexOf('.');
        if (lastDot == -1) {
            return "";
        }
        return fileName.substring(lastDot).toLowerCase();
    }
}
