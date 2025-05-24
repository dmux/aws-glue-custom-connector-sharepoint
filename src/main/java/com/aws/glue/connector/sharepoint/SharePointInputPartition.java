package com.aws.glue.connector.sharepoint;

import com.aws.glue.connector.sharepoint.client.SharePointClient;
import com.aws.glue.connector.sharepoint.parser.FileParser;
import com.aws.glue.connector.sharepoint.parser.FileParserFactory;
import com.aws.glue.connector.sharepoint.util.SharePointFile;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Iterator;

/**
 * SharePoint Input Partition for parallel file processing
 * 
 * Each partition handles one SharePoint file, allowing Spark
 * to process multiple files in parallel.
 */
public class SharePointInputPartition implements InputPartition {
    
    private static final Logger logger = LoggerFactory.getLogger(SharePointInputPartition.class);
    
    private final SharePointFile file;
    private final SharePointClient sharePointClient;
    private final StructType schema;
    
    public SharePointInputPartition(SharePointFile file, 
                                  SharePointClient sharePointClient, 
                                  StructType schema) {
        this.file = file;
        this.sharePointClient = sharePointClient;
        this.schema = schema;
    }
    
    public SharePointFile getFile() {
        return file;
    }
    
    public SharePointClient getSharePointClient() {
        return sharePointClient;
    }
    
    public StructType getSchema() {
        return schema;
    }
    
    /**
     * Reader implementation for a single SharePoint file partition
     */
    public static class SharePointInputPartitionReader implements PartitionReader<InternalRow> {
        
        private final SharePointFile file;
        private final SharePointClient sharePointClient;
        private final StructType schema;
        private Iterator<InternalRow> rowIterator;
        private boolean isInitialized = false;
        
        public SharePointInputPartitionReader(SharePointFile file, 
                                            SharePointClient sharePointClient, 
                                            StructType schema) {
            this.file = file;
            this.sharePointClient = sharePointClient;
            this.schema = schema;
        }
        
        @Override
        public boolean next() {
            if (!isInitialized) {
                initialize();
            }
            return rowIterator.hasNext();
        }
        
        @Override
        public InternalRow get() {
            if (!isInitialized) {
                initialize();
            }
            return rowIterator.next();
        }
        
        @Override
        public void close() {
            // Clean up resources if needed
            logger.debug("Closing partition reader for file: {}", file.getName());
        }
        
        private void initialize() {
            try {
                logger.info("Initializing reader for file: {}", file.getName());
                
                InputStream fileStream = sharePointClient.downloadFile(file.getId());
                FileParser parser = FileParserFactory.getParser(file.getName());
                
                rowIterator = parser.parseFile(fileStream, schema).iterator();
                isInitialized = true;
                
                logger.info("Successfully initialized reader for file: {}", file.getName());
                
            } catch (Exception e) {
                logger.error("Failed to initialize reader for file: {}", file.getName(), e);
                throw new RuntimeException("Failed to read SharePoint file: " + file.getName(), e);
            }
        }
    }
}
