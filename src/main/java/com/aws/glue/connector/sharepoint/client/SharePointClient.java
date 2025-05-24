package com.aws.glue.connector.sharepoint.client;

import com.aws.glue.connector.sharepoint.auth.SharePointAuthenticator;
import com.aws.glue.connector.sharepoint.util.SharePointFile;
import com.microsoft.graph.models.DriveItem;
import com.microsoft.graph.requests.DriveItemCollectionPage;
import com.microsoft.graph.requests.GraphServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * SharePoint Client for file operations
 * 
 * Provides methods to list and download files from SharePoint
 * document libraries using Microsoft Graph API.
 */
public class SharePointClient {
    
    private static final Logger logger = LoggerFactory.getLogger(SharePointClient.class);
    
    private static final List<String> SUPPORTED_EXTENSIONS = Arrays.asList(
            ".csv", ".xls", ".xlsx"
    );
    
    private final SharePointAuthenticator authenticator;
    private final String siteId;
    private final GraphServiceClient<okhttp3.Request> graphClient;
    
    public SharePointClient(SharePointAuthenticator authenticator, String siteId) {
        this.authenticator = authenticator;
        this.siteId = siteId;
        this.graphClient = authenticator.getGraphClient();
        
        logger.info("SharePoint client initialized for site: {}", siteId);
    }
    
    /**
     * List all supported files in the SharePoint document library
     * 
     * @return List of SharePointFile objects for supported file types
     */
    public List<SharePointFile> listFiles() {
        List<SharePointFile> files = new ArrayList<>();
        
        try {
            logger.info("Listing files from SharePoint site: {}", siteId);
            
            DriveItemCollectionPage children = graphClient
                    .sites(siteId)
                    .drive()
                    .root()
                    .children()
                    .buildRequest()
                    .get();
            
            if (children != null && children.getCurrentPage() != null) {
                for (DriveItem item : children.getCurrentPage()) {
                    if (item.file != null && isSupportedFile(item.name)) {
                        SharePointFile spFile = new SharePointFile(
                                item.id,
                                item.name,
                                item.size,
                                item.lastModifiedDateTime
                        );
                        files.add(spFile);
                        
                        logger.debug("Found supported file: {} ({})", 
                                   item.name, formatFileSize(item.size));
                    }
                }
            }
            
            logger.info("Found {} supported files in SharePoint library", files.size());
            
        } catch (Exception e) {
            logger.error("Failed to list files from SharePoint", e);
            throw new RuntimeException("Failed to list SharePoint files", e);
        }
        
        return files;
    }
    
    /**
     * Download file content from SharePoint
     * 
     * @param fileId SharePoint file ID
     * @return InputStream containing file content
     */
    public InputStream downloadFile(String fileId) {
        try {
            logger.debug("Downloading file with ID: {}", fileId);
            
            InputStream fileStream = graphClient
                    .sites(siteId)
                    .drive()
                    .items(fileId)
                    .content()
                    .buildRequest()
                    .get();
            
            if (fileStream == null) {
                throw new RuntimeException("Failed to download file: " + fileId);
            }
            
            logger.debug("Successfully downloaded file: {}", fileId);
            return fileStream;
            
        } catch (Exception e) {
            logger.error("Failed to download file: {}", fileId, e);
            throw new RuntimeException("Failed to download SharePoint file: " + fileId, e);
        }
    }
    
    /**
     * Get file metadata by ID
     * 
     * @param fileId SharePoint file ID
     * @return SharePointFile object with metadata
     */
    public SharePointFile getFileMetadata(String fileId) {
        try {
            logger.debug("Getting metadata for file: {}", fileId);
            
            DriveItem item = graphClient
                    .sites(siteId)
                    .drive()
                    .items(fileId)
                    .buildRequest()
                    .get();
            
            if (item == null) {
                throw new RuntimeException("File not found: " + fileId);
            }
            
            return new SharePointFile(
                    item.id,
                    item.name,
                    item.size,
                    item.lastModifiedDateTime
            );
            
        } catch (Exception e) {
            logger.error("Failed to get file metadata: {}", fileId, e);
            throw new RuntimeException("Failed to get SharePoint file metadata: " + fileId, e);
        }
    }
    
    /**
     * Get file by specific path in the SharePoint document library
     * 
     * @param filePath Path to the file relative to the document library root (e.g., "folder/subfolder/file.csv")
     * @return SharePointFile object if found, null otherwise
     */
    public SharePointFile getFileByPath(String filePath) {
        try {
            logger.info("Getting file by path: {} from SharePoint site: {}", filePath, siteId);
            
            // Clean the path - remove leading slash if present
            String cleanPath = filePath.startsWith("/") ? filePath.substring(1) : filePath;
            
            // Use the Graph API to get the file by path
            DriveItem item = graphClient
                    .sites(siteId)
                    .drive()
                    .root()
                    .itemWithPath(cleanPath)
                    .buildRequest()
                    .get();
            
            if (item != null && item.file != null) {
                // Check if file type is supported
                if (!isSupportedFile(item.name)) {
                    throw new RuntimeException("Unsupported file type: " + item.name + 
                                             ". Supported types: " + SUPPORTED_EXTENSIONS);
                }
                
                SharePointFile spFile = new SharePointFile(
                        item.id,
                        item.name,
                        item.size,
                        item.lastModifiedDateTime
                );
                
                logger.info("Found file by path: {} ({})", item.name, formatFileSize(item.size));
                return spFile;
            } else {
                logger.warn("File not found at path: {}", filePath);
                return null;
            }
            
        } catch (RuntimeException e) {
            // Re-throw validation exceptions (like unsupported file type) as-is
            if (e.getMessage() != null && e.getMessage().contains("Unsupported file type")) {
                throw e;
            }
            logger.error("Failed to get file by path: {}", filePath, e);
            throw new RuntimeException("Failed to get SharePoint file at path: " + filePath, e);
        } catch (Exception e) {
            logger.error("Failed to get file by path: {}", filePath, e);
            throw new RuntimeException("Failed to get SharePoint file at path: " + filePath, e);
        }
    }
    
    /**
     * Get single file as list (for compatibility with existing listFiles usage)
     * 
     * @param filePath Path to the specific file
     * @return List containing the single file, or empty list if not found
     */
    public List<SharePointFile> getFileByPathAsList(String filePath) {
        List<SharePointFile> files = new ArrayList<>();
        SharePointFile file = getFileByPath(filePath);
        if (file != null) {
            files.add(file);
        }
        return files;
    }

    /**
     * Check if file extension is supported
     */
    private boolean isSupportedFile(String fileName) {
        if (fileName == null) {
            return false;
        }
        
        String lowerFileName = fileName.toLowerCase();
        return SUPPORTED_EXTENSIONS.stream()
                .anyMatch(lowerFileName::endsWith);
    }
    
    /**
     * Format file size for logging
     */
    private String formatFileSize(Long size) {
        if (size == null) {
            return "unknown size";
        }
        
        if (size < 1024) {
            return size + " B";
        } else if (size < 1024 * 1024) {
            return String.format("%.1f KB", size / 1024.0);
        } else {
            return String.format("%.1f MB", size / (1024.0 * 1024.0));
        }
    }
    
    /**
     * Test connection to SharePoint
     */
    public boolean testConnection() {
        try {
            logger.info("Testing SharePoint connection for site: {}", siteId);
            
            graphClient
                    .sites(siteId)
                    .drive()
                    .buildRequest()
                    .get();
            
            logger.info("SharePoint connection test successful");
            return true;
            
        } catch (Exception e) {
            logger.error("SharePoint connection test failed", e);
            return false;
        }
    }
}
