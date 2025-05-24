package com.aws.glue.connector.sharepoint.parser;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.io.InputStream;
import java.util.List;

/**
 * File Parser interface for different file types
 * 
 * Defines the contract for parsing CSV and Excel files
 * into Spark DataFrames.
 */
public interface FileParser {
    
    /**
     * Infer schema from file content
     * 
     * @param inputStream File content stream
     * @return Spark StructType representing the schema
     */
    StructType inferSchema(InputStream inputStream);
    
    /**
     * Parse file content into rows
     * 
     * @param inputStream File content stream
     * @param schema Expected schema for the data
     * @return List of InternalRow objects
     */
    List<InternalRow> parseFile(InputStream inputStream, StructType schema);
    
    /**
     * Check if this parser supports the given file extension
     * 
     * @param fileExtension File extension (e.g., ".csv", ".xlsx")
     * @return true if supported, false otherwise
     */
    boolean supports(String fileExtension);
}
