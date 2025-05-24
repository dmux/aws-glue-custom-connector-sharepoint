package com.aws.glue.connector.sharepoint.util;

import java.time.OffsetDateTime;

/**
 * SharePoint File metadata container
 * 
 * Represents a file in SharePoint with its metadata
 * for processing by the custom connector.
 */
public class SharePointFile {
    
    private final String id;
    private final String name;
    private final Long size;
    private final OffsetDateTime lastModified;
    
    public SharePointFile(String id, String name, Long size, OffsetDateTime lastModified) {
        this.id = id;
        this.name = name;
        this.size = size;
        this.lastModified = lastModified;
    }
    
    public String getId() {
        return id;
    }
    
    public String getName() {
        return name;
    }
    
    public Long getSize() {
        return size;
    }
    
    public OffsetDateTime getLastModified() {
        return lastModified;
    }
    
    /**
     * Get file extension in lowercase
     */
    public String getExtension() {
        if (name == null) {
            return "";
        }
        
        int lastDot = name.lastIndexOf('.');
        if (lastDot == -1) {
            return "";
        }
        
        return name.substring(lastDot).toLowerCase();
    }
    
    /**
     * Check if this is a CSV file
     */
    public boolean isCsvFile() {
        return ".csv".equals(getExtension());
    }
    
    /**
     * Check if this is an Excel file
     */
    public boolean isExcelFile() {
        String ext = getExtension();
        return ".xls".equals(ext) || ".xlsx".equals(ext);
    }
    
    @Override
    public String toString() {
        return String.format("SharePointFile{id='%s', name='%s', size=%d, lastModified=%s}", 
                           id, name, size, lastModified);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        SharePointFile that = (SharePointFile) obj;
        return id != null ? id.equals(that.id) : that.id == null;
    }
    
    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}
