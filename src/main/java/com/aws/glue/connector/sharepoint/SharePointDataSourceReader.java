package com.aws.glue.connector.sharepoint;

import com.aws.glue.connector.sharepoint.auth.SharePointAuthenticator;
import com.aws.glue.connector.sharepoint.client.SharePointClient;
import com.aws.glue.connector.sharepoint.parser.FileParserFactory;
import com.aws.glue.connector.sharepoint.util.SharePointFile;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * SharePoint DataSourceReader implementation
 * 
 * Handles reading data from SharePoint libraries by:
 * 1. Authenticating with SharePoint via Microsoft Graph
 * 2. Listing files in the specified library
 * 3. Creating partitions for parallel processing
 */
public class SharePointDataSourceReader implements Table, SupportsRead {
    
    private static final Logger logger = LoggerFactory.getLogger(SharePointDataSourceReader.class);
    
    private final CaseInsensitiveStringMap options;
    private final SharePointClient sharePointClient;
    private StructType schema;
    
    public SharePointDataSourceReader(CaseInsensitiveStringMap options) {
        this.options = options;
        validateOptions();
        
        // Initialize SharePoint client with authentication
        SharePointAuthenticator authenticator = new SharePointAuthenticator(
            this.options.get("sharepoint.clientId"),
            this.options.get("sharepoint.clientSecret"),
            this.options.get("sharepoint.tenantId")
        );
        
        this.sharePointClient = new SharePointClient(
            authenticator,
            this.options.get("sharepoint.siteId")
        );
        
        logger.info("SharePoint DataSourceReader initialized for site: {}", 
                   this.options.get("sharepoint.siteId"));
    }
    
    // Constructor for testing with dependency injection
    public SharePointDataSourceReader(CaseInsensitiveStringMap options, SharePointClient sharePointClient) {
        this.options = options;
        this.sharePointClient = sharePointClient;
        
        logger.info("SharePoint DataSourceReader initialized with injected client");
    }
    
    private void validateOptions() {
        String clientId = options.get("sharepoint.clientId");
        String clientSecret = options.get("sharepoint.clientSecret");
        String tenantId = options.get("sharepoint.tenantId");
        String siteId = options.get("sharepoint.siteId");
        
        // Check for missing or empty values
        if (isNullOrEmpty(clientId) || isNullOrEmpty(clientSecret) || 
            isNullOrEmpty(tenantId) || isNullOrEmpty(siteId)) {
            throw new IllegalArgumentException(
                "SharePoint authentication parameters not configured. " +
                "Required: sharepoint.clientId, sharepoint.clientSecret, " +
                "sharepoint.tenantId, sharepoint.siteId"
            );
        }
        
        logger.debug("SharePoint connection parameters validated");
    }
    
    private boolean isNullOrEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }
    
    // Table interface methods
    @Override
    public String name() {
        return "SharePoint";
    }
    
    @Override
    public StructType schema() {
        return readSchema();
    }
    
    @Override
    public Set<TableCapability> capabilities() {
        return Set.of(TableCapability.BATCH_READ);
    }
    
    // SupportsRead interface methods
    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new SharePointScanBuilder(this);
    }
    
    public StructType readSchema() {
        if (schema == null) {
            // Get first file to determine schema
            List<SharePointFile> files = sharePointClient.listFiles();
            if (files.isEmpty()) {
                throw new RuntimeException("No files found in SharePoint library");
            }
            
            SharePointFile firstFile = files.get(0);
            schema = FileParserFactory.getParser(firstFile.getName())
                    .inferSchema(sharePointClient.downloadFile(firstFile.getId()));
            
            logger.info("Inferred schema from file: {} with {} fields", 
                       firstFile.getName(), schema.fields().length);
        }
        
        return schema;
    }
    
    public List<SharePointFile> getFiles() {
        return sharePointClient.listFiles();
    }
    
    public SharePointClient getSharePointClient() {
        return sharePointClient;
    }
    
    /**
     * Inner class to implement ScanBuilder
     */
    public static class SharePointScanBuilder implements ScanBuilder {
        private final SharePointDataSourceReader reader;
        
        public SharePointScanBuilder(SharePointDataSourceReader reader) {
            this.reader = reader;
        }
        
        @Override
        public Scan build() {
            return new SharePointScan(reader);
        }
    }
    
    /**
     * Inner class to implement Scan
     */
    public static class SharePointScan implements Scan {
        private final SharePointDataSourceReader reader;
        
        public SharePointScan(SharePointDataSourceReader reader) {
            this.reader = reader;
        }
        
        @Override
        public StructType readSchema() {
            return reader.readSchema();
        }
        
        @Override
        public Batch toBatch() {
            return new SharePointBatch(reader);
        }
    }
    
    /**
     * Inner class to implement Batch
     */
    public static class SharePointBatch implements Batch {
        private final SharePointDataSourceReader reader;
        
        public SharePointBatch(SharePointDataSourceReader reader) {
            this.reader = reader;
        }
        
        @Override
        public InputPartition[] planInputPartitions() {
            List<SharePointFile> files = reader.getFiles();
            InputPartition[] partitions = new InputPartition[files.size()];
            
            // Create one partition per file for parallel processing
            for (int i = 0; i < files.size(); i++) {
                partitions[i] = new SharePointInputPartition(
                    files.get(i), 
                    reader.getSharePointClient(), 
                    reader.readSchema()
                );
            }
            
            LoggerFactory.getLogger(SharePointBatch.class).info(
                "Created {} partitions for {} files", partitions.length, files.size());
            return partitions;
        }
        
        @Override
        public PartitionReaderFactory createReaderFactory() {
            return new SharePointPartitionReaderFactory();
        }
    }
    
    /**
     * Factory to create partition readers for SharePoint files
     */
    public static class SharePointPartitionReaderFactory implements PartitionReaderFactory {
        
        @Override
        public PartitionReader<InternalRow> createReader(InputPartition partition) {
            SharePointInputPartition sharePointPartition = (SharePointInputPartition) partition;
            
            return new SharePointInputPartition.SharePointInputPartitionReader(
                sharePointPartition.getFile(),
                sharePointPartition.getSharePointClient(),
                sharePointPartition.getSchema()
            );
        }
    }
}
