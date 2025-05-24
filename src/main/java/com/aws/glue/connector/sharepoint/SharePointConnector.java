package com.aws.glue.connector.sharepoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SharePoint Connector Main Class
 * 
 * Entry point for the AWS Glue Custom Connector for SharePoint.
 * This class provides information about the connector and serves
 * as the main class referenced in the manifest.
 */
public class SharePointConnector {
    
    private static final Logger logger = LoggerFactory.getLogger(SharePointConnector.class);
    
    private static final String CONNECTOR_NAME = "AWS Glue SharePoint Custom Connector";
    private static final String CONNECTOR_VERSION = "1.0.0";
    private static final String CONNECTOR_DESCRIPTION = 
        "Custom connector for reading CSV and Excel files from SharePoint document libraries";
    
    public static void main(String[] args) {
        logger.info("Starting {}", CONNECTOR_NAME);
        logger.info("Version: {}", CONNECTOR_VERSION);
        logger.info("Description: {}", CONNECTOR_DESCRIPTION);
        
        if (args.length > 0 && "version".equals(args[0])) {
            System.out.println(getVersionInfo());
            return;
        }
        
        if (args.length > 0 && "help".equals(args[0])) {
            System.out.println(getHelpInfo());
            return;
        }
        
        logger.info("SharePoint Connector is ready for use with AWS Glue");
        logger.info("Use {} as the DataSource class in your Glue job", 
                   SharePointDataSourceFactory.class.getName());
    }
    
    /**
     * Get connector version information
     */
    public static String getVersionInfo() {
        return String.format("%s v%s", CONNECTOR_NAME, CONNECTOR_VERSION);
    }
    
    /**
     * Get connector help information
     */
    public static String getHelpInfo() {
        return String.format(
            "%s\n\n" +
            "Description: %s\n\n" +
            "Required Connection Properties:\n" +
            "  - sharepoint.clientId: Azure AD Application ID\n" +
            "  - sharepoint.clientSecret: Azure AD Application Secret\n" +
            "  - sharepoint.tenantId: Azure AD Tenant ID\n" +
            "  - sharepoint.siteId: SharePoint Site ID\n\n" +
            "Supported File Types:\n" +
            "  - CSV (.csv)\n" +
            "  - Excel (.xls, .xlsx)\n\n" +
            "Usage in Glue Job:\n" +
            "  import com.aws.glue.connector.sharepoint.SharePointDataSourceFactory\n" +
            "  \n" +
            "  options = {\n" +
            "    \"sharepoint.clientId\": \"your-client-id\",\n" +
            "    \"sharepoint.clientSecret\": \"your-client-secret\",\n" +
            "    \"sharepoint.tenantId\": \"your-tenant-id\",\n" +
            "    \"sharepoint.siteId\": \"your-site-id\"\n" +
            "  }\n" +
            "  \n" +
            "  df = SharePointDataSourceFactory.getDataSource(options)\n",
            CONNECTOR_NAME,
            CONNECTOR_DESCRIPTION
        );
    }
    
    /**
     * Get connector name
     */
    public static String getConnectorName() {
        return CONNECTOR_NAME;
    }
    
    /**
     * Get connector version
     */
    public static String getConnectorVersion() {
        return CONNECTOR_VERSION;
    }
    
    /**
     * Get connector description
     */
    public static String getConnectorDescription() {
        return CONNECTOR_DESCRIPTION;
    }
}
