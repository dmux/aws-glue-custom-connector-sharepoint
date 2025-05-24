package com.aws.glue.connector.sharepoint.parser;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Excel File Parser implementation
 * 
 * Parses Excel files (.xls and .xlsx) using Apache POI library
 * and converts the data to Spark InternalRow format.
 */
public class ExcelFileParser implements FileParser {
    
    private static final Logger logger = LoggerFactory.getLogger(ExcelFileParser.class);
    
    @Override
    public StructType inferSchema(InputStream inputStream) {
        try (Workbook workbook = WorkbookFactory.create(inputStream)) {
            
            Sheet sheet = workbook.getSheetAt(0);
            if (sheet == null) {
                throw new RuntimeException("Excel file has no sheets");
            }
            
            Row headerRow = sheet.getRow(0);
            if (headerRow == null) {
                throw new RuntimeException("Excel file has no header row");
            }
            
            List<StructField> fields = new ArrayList<>();
            Iterator<Cell> cellIterator = headerRow.cellIterator();
            
            while (cellIterator.hasNext()) {
                Cell cell = cellIterator.next();
                String columnName = getCellValueAsString(cell);
                
                if (columnName != null && !columnName.trim().isEmpty()) {
                    // For simplicity, treat all columns as strings
                    // In a production system, you might want to infer data types
                    fields.add(DataTypes.createStructField(
                        columnName.trim(), 
                        DataTypes.StringType, 
                        true
                    ));
                }
            }
            
            if (fields.isEmpty()) {
                throw new RuntimeException("Excel file has no valid columns");
            }
            
            StructType schema = DataTypes.createStructType(fields.toArray(new StructField[0]));
            logger.debug("Inferred Excel schema with {} columns", fields.size());
            
            return schema;
            
        } catch (IOException e) {
            logger.error("Failed to infer Excel schema", e);
            throw new RuntimeException("Failed to infer Excel schema", e);
        }
    }
    
    @Override
    public List<InternalRow> parseFile(InputStream inputStream, StructType schema) {
        List<InternalRow> rows = new ArrayList<>();
        
        try (Workbook workbook = WorkbookFactory.create(inputStream)) {
            
            Sheet sheet = workbook.getSheetAt(0);
            if (sheet == null) {
                throw new RuntimeException("Excel file has no sheets");
            }
            
            int rowCount = 0;
            boolean isFirstRow = true;
            
            for (Row row : sheet) {
                // Skip header row
                if (isFirstRow) {
                    isFirstRow = false;
                    continue;
                }
                
                InternalRow internalRow = parseRow(row, schema);
                rows.add(internalRow);
                rowCount++;
                
                if (rowCount % 10000 == 0) {
                    logger.debug("Parsed {} Excel rows", rowCount);
                }
            }
            
            logger.info("Successfully parsed {} rows from Excel file", rowCount);
            
        } catch (IOException e) {
            logger.error("Failed to parse Excel file", e);
            throw new RuntimeException("Failed to parse Excel file", e);
        }
        
        return rows;
    }
    
    @Override
    public boolean supports(String fileExtension) {
        return ".xls".equals(fileExtension) || ".xlsx".equals(fileExtension);
    }
    
    /**
     * Parse a single Excel row into InternalRow
     */
    private InternalRow parseRow(Row row, StructType schema) {
        Object[] rowData = new Object[schema.fields().length];
        
        for (int i = 0; i < schema.fields().length; i++) {
            Cell cell = row.getCell(i, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
            String value = getCellValueAsString(cell);
            
            // Convert to UTF8String for Spark internal representation
            rowData[i] = value != null ? UTF8String.fromString(value.trim()) : null;
        }
        
        return new GenericInternalRow(rowData);
    }
    
    /**
     * Convert Excel cell value to string
     */
    private String getCellValueAsString(Cell cell) {
        if (cell == null) {
            return "";
        }
        
        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getDateCellValue().toString();
                } else {
                    double numericValue = cell.getNumericCellValue();
                    // Check if it's a whole number
                    if (numericValue == Math.floor(numericValue)) {
                        return String.valueOf((long) numericValue);
                    } else {
                        return String.valueOf(numericValue);
                    }
                }
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            case FORMULA:
                // Evaluate formula and get the result
                try {
                    FormulaEvaluator evaluator = cell.getSheet().getWorkbook().getCreationHelper().createFormulaEvaluator();
                    CellValue cellValue = evaluator.evaluate(cell);
                    return getCellValueAsString(cellValue);
                } catch (Exception e) {
                    logger.warn("Failed to evaluate formula in cell, returning formula string", e);
                    return cell.getCellFormula();
                }
            case BLANK:
            case _NONE:
            default:
                return "";
        }
    }
    
    /**
     * Convert CellValue to string (for formula evaluation results)
     */
    private String getCellValueAsString(CellValue cellValue) {
        if (cellValue == null) {
            return "";
        }
        
        switch (cellValue.getCellType()) {
            case STRING:
                return cellValue.getStringValue();
            case NUMERIC:
                double numericValue = cellValue.getNumberValue();
                if (numericValue == Math.floor(numericValue)) {
                    return String.valueOf((long) numericValue);
                } else {
                    return String.valueOf(numericValue);
                }
            case BOOLEAN:
                return String.valueOf(cellValue.getBooleanValue());
            default:
                return "";
        }
    }
}
