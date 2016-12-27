package resultSet;

import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.logging.log4j.Logger;

import logger.Log4j;

public class ResultSetMetaDataa implements java.sql.ResultSetMetaData {
    
    private String tableName;
    private ArrayList<String> columnNames;
    private ArrayList<String> columnTypes;
    static Logger logger;
    //= LogManager.getLogger(ResultSetMetaDataa.class);  
    
    public ResultSetMetaDataa(String tableName, ArrayList<String> columnNames, ArrayList<String> columnTypes) {
        ResultSetMetaDataa.logger = Log4j.getInstance();
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }
    
    @Override
    public int getColumnCount() throws SQLException {
        logger.info("Getting Column Count..");
        return this.columnNames.size();
    }
    
    @Override
    public String getColumnLabel(int column) throws SQLException {
        logger.info("Getting Column Label....");
        if (column < 1 || column > this.columnNames.size()) {
            logger.error("Error getting column label!");
            throw new SQLException();
        }
        return this.columnNames.get(column - 1);
    }
    
    @Override
    public String getColumnName(int column) throws SQLException {
        return this.getColumnLabel(column);
    }
    
    @Override
    public int getColumnType(int column) throws SQLException {
        logger.info("Getting column type..");
        if (column < 1 || column > this.columnTypes.size()) {
            logger.error("Error getting column type");
            throw new SQLException();
        }
        String type = this.columnTypes.get(column - 1);
        SQLType sqlType = new SQLType(type);
        return sqlType.getSQLType();
    }
    
    @Override
    public String getTableName(int column) throws SQLException {
        logger.info("Getting Table Name...");
        return this.tableName;
    }
    
    //////////////////////////////////////////////////////
    /////////////////////// unused/////////////////////////
    ////////////////////////////////////////////////////////
    @Override
    public boolean isWrapperFor(Class<?> iface) throws UnsupportedOperationException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public <T> T unwrap(Class<T> iface) throws UnsupportedOperationException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public String getCatalogName(int column) throws UnsupportedOperationException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public String getColumnClassName(int column) throws UnsupportedOperationException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public int getColumnDisplaySize(int column) throws UnsupportedOperationException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public String getColumnTypeName(int column) throws UnsupportedOperationException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public int getPrecision(int column) throws UnsupportedOperationException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public int getScale(int column) throws UnsupportedOperationException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public String getSchemaName(int column) throws UnsupportedOperationException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public boolean isAutoIncrement(int column) throws UnsupportedOperationException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public boolean isCaseSensitive(int column) throws UnsupportedOperationException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public boolean isCurrency(int column) throws UnsupportedOperationException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public boolean isDefinitelyWritable(int column) throws UnsupportedOperationException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public int isNullable(int column) throws UnsupportedOperationException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public boolean isReadOnly(int column) throws UnsupportedOperationException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public boolean isSearchable(int column) throws UnsupportedOperationException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public boolean isSigned(int column) throws UnsupportedOperationException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public boolean isWritable(int column) throws UnsupportedOperationException {
        // TODO Auto-generated method stub
        return false;
    }
    
}
