package com.aws.glue.connector.sharepoint;

import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SharePoint DataSource Factory for AWS Glue Custom Connector
 * 
 * Implements TableProvider interface to create a custom data source
 * that can read CSV and Excel files from SharePoint libraries.
 */
public class SharePointDataSourceFactory implements TableProvider {
    
    private static final Logger logger = LoggerFactory.getLogger(SharePointDataSourceFactory.class);
    
    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        logger.info("Inferring schema for SharePoint connector with options: {}", 
                   options.keySet());
        
        // Create a reader to infer schema
        SharePointDataSourceReader reader = createReader(options);
        return reader.readSchema();
    }
    
    /**
     * Create a SharePointDataSourceReader instance.
     * Protected for testing purposes.
     */
    protected SharePointDataSourceReader createReader(CaseInsensitiveStringMap options) {
        return new SharePointDataSourceReader(options);
    }
    
    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        logger.info("Creating SharePoint Table with schema");
        
        CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(properties);
        return createReader(options);
    }
    
    /**
     * Entry point for Glue jobs using this connector
     */
    public static org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> getDataSource(java.util.Map<String, String> options) {
        logger.info("Creating SharePoint DataSource from Glue job");
        
        org.apache.spark.sql.SparkSession spark = org.apache.spark.sql.SparkSession.active();
        
        return spark.read()
                .format("com.aws.glue.connector.sharepoint.SharePointDataSourceFactory")
                .options(options)
                .load();
    }
}
