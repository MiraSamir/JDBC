package resultSet;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import adapter.Adapter;
import dataTypes.TypeException;
import dataTypes.TypeFactory;
import dbms.CaseInsensitive;
import logger.Log4j;

public class ResultSett implements java.sql.ResultSet {
    
    private Adapter adapter;
    private LinkedHashMap<String, ArrayList<String>> columnTypeAccess;
    
    public LinkedHashMap<String, ArrayList<String>> getColumnTypeAccess() {
        return columnTypeAccess;
    }
    
    private LinkedHashMap<Integer, ArrayList<String>> indexAccess;
    private int rowIndex;
    private int rowSize;
    private int columnSize;
    private Statement statement;
    private TypeFactory typeInvoker;
    private ResultSetMetaDataa resultSetMetaData;
    private CaseInsensitive caseInsensitive;
    
    final static String stmtIsClosed = "Statement is closed!";
    static Logger logger;
    
    public ResultSett(Adapter adapter, Statement statement) {
        ResultSett.logger = Log4j.getInstance();
        this.rowIndex = 0; //before 1    //row 1 based
        this.adapter = adapter;
        this.columnTypeAccess = adapter.getColumnTypeAccess();
        this.indexAccess = adapter.getIndexAccess();
        this.rowSize = adapter.getIndexAccess().get(1).size();
        this.columnSize = adapter.getColumnTypeAccess().size();
        this.statement = statement;
        this.caseInsensitive = new CaseInsensitive();
        this.typeInvoker = new TypeFactory();
        this.resultSetMetaData = new ResultSetMetaDataa(adapter.getTableName(), adapter.getColumnNames(),
                adapter.getTableIdentifierTypes());
    }
    
    @Override
    public boolean absolute(int index) throws SQLException {
        if (isClosed()) {
            logger.error("FATAL ERROR !" + ResultSett.stmtIsClosed);
            throw new SQLException(ResultSett.stmtIsClosed);
        }
        if (index == 0 || index < -(this.rowSize)) {
            beforeFirst();
            return false;
        } else if (index > this.rowSize) {
            afterLast();
            return false;
        } else {
            if (index < 0) {
                this.rowIndex = index + this.rowSize + 1;
            } else {
                this.rowIndex = index;
            }
            return true;
        }
    }
    
    //bara el bounds
    @Override
    public void afterLast() throws SQLException {
        if (isClosed()) {
            logger.error("FATAL ERROR !" + ResultSett.stmtIsClosed);
            throw new SQLException(ResultSett.stmtIsClosed);
        }
        if (this.rowSize != 0) {
            this.rowIndex = this.rowSize + 1;
        }
    }
    
    //bara el bounds
    @Override
    public void beforeFirst() throws SQLException {
        if (isClosed()) {
            logger.error("FATAL ERROR !" + ResultSett.stmtIsClosed);
            throw new SQLException(ResultSett.stmtIsClosed);
        }
        if (this.rowSize != 0) {
            this.rowIndex = 0;
        }
    }
    
    @Override
    public boolean isAfterLast() throws SQLException {
        if (isClosed()) {
            logger.error("FATAL ERROR !" + ResultSett.stmtIsClosed);
            throw new SQLException(ResultSett.stmtIsClosed);
        }
        if (this.rowIndex == (this.rowSize + 1)) {
            return true;
        }
        return false;
    }
    
    @Override
    public boolean isBeforeFirst() throws SQLException {
        if (isClosed()) {
            logger.error("FATAL ERROR !" + ResultSett.stmtIsClosed);
            throw new SQLException(ResultSett.stmtIsClosed);
        }
        if (this.rowIndex == 0) {
            return true;
        }
        return false;
    }
    
    @Override
    public boolean isFirst() throws SQLException {
        if (isClosed()) {
            logger.error("FATAL ERROR !" + ResultSett.stmtIsClosed);
            throw new SQLException(ResultSett.stmtIsClosed);
        }
        if (this.rowIndex == 1) {
            return true;
        }
        return false;
    }
    
    @Override
    public boolean isLast() throws SQLException {
        if (isClosed()) {
            logger.error("FATAL ERROR !" + ResultSett.stmtIsClosed);
            throw new SQLException(ResultSett.stmtIsClosed);
        }
        if (this.rowIndex == this.rowSize) {
            return true;
        }
        return false;
    }
    
    @Override
    public boolean first() throws SQLException { //<====== cursor to index 1
        if (isClosed()) {
            logger.error("FATAL ERROR !" + ResultSett.stmtIsClosed);
            throw new SQLException(ResultSett.stmtIsClosed);
        }
        if (this.rowSize == 0) {
            return false;
        }
        this.rowIndex = 1;
        return true;
    }
    
    @Override
    public boolean last() throws SQLException {
        if (isClosed()) {
            logger.error("FATAL ERROR !" + ResultSett.stmtIsClosed);
            throw new SQLException(ResultSett.stmtIsClosed);
        }
        if (this.rowSize == 0) {
            return false;
        }
        this.rowIndex = this.rowSize;
        return true;
    }
    
    @Override
    public boolean next() throws SQLException {
        if (isClosed()) {
            logger.error("FATAL ERROR !" + ResultSett.stmtIsClosed);
            throw new SQLException(ResultSett.stmtIsClosed);
        }
        if (this.rowIndex < this.rowSize) {
            this.rowIndex++;
            return true;
        }
        afterLast();
        return false;
    }
    
    @Override
    public boolean previous() throws SQLException {
        if (isClosed()) {
            logger.error("FATAL ERROR !" + ResultSett.stmtIsClosed);
            throw new SQLException(ResultSett.stmtIsClosed);
        }
        if (this.rowIndex > 2) {
            this.rowIndex--;
            return true;
        }
        beforeFirst();
        return false;
    }
    
    @Override
    public ResultSetMetaDataa getMetaData() throws SQLException {
        logger.info("Getting Metadata..");
        return this.resultSetMetaData;
    }
    
    @Override
    public Statement getStatement() throws SQLException {
        logger.info("Getting Statement...");
        if (isClosed()) {
            logger.error("FATAL ERROR !" + ResultSett.stmtIsClosed);
            throw new SQLException(ResultSett.stmtIsClosed);
        }
        return this.statement;
    }
    
    @Override
    public Date getDate(int columnIndex) throws SQLException {
        if (isClosed() || isBeforeFirst() || isAfterLast() || columnIndex < 1 || columnIndex > this.columnSize) {
            logger.error("FATAL ERROR !" + "Error getting Date");
            throw new SQLException("Error Getting Date.");
        }
        String value = this.indexAccess.get(columnIndex).get(this.rowIndex - 1);
        if (value.equals("-")) {
            return null; //<================================================================= //undefined return requirement
        }
        try {
            if (typeInvoker.invoke("date").isValid(value)) {
                return (java.sql.Date) typeInvoker.invoke("date").castType(value);
            }
        } catch (Exception e) {
            throw new SQLException();
        }
        throw new SQLException();
    }
    
    @Override
    public Date getDate(String columnName) throws SQLException {
        if (isClosed() || isBeforeFirst() || isAfterLast()
                || !caseInsensitive.containsCaseInsensitive(this.columnTypeAccess, columnName)) {
            logger.error("FATAL ERROR !" + "Error getting Date");
            throw new SQLException("Error Getting Date.");
        }
        String value = this.columnTypeAccess.get(columnName).get(this.rowIndex - 1);
        if (value.equals("-")) {
            return null; //<================================================================= undefined return requirement
        }
        try {
            if (typeInvoker.invoke("date").isValid(value)) {
                return (java.sql.Date) typeInvoker.invoke("date").castType(value);
            }
        } catch (Exception e) {
            throw new SQLException();
        }
        throw new SQLException();
    }
    
    @Override
    public float getFloat(int columnIndex) throws SQLException {
        if (isClosed() || isBeforeFirst() || isAfterLast() || columnIndex < 1 || columnIndex > this.columnSize) {
            logger.error("FATAL ERROR !" + "Error getting Float");
            throw new SQLException("Error Getting Float.");
        }
        String value = this.indexAccess.get(columnIndex).get(this.rowIndex - 1);
        if (value.equals("-")) {
            return 0; //<================================================================= undefined return requirement
        }
        try {
            if (typeInvoker.invoke("float").isValid(value)) {
                return (Float) typeInvoker.invoke("float").castType(value);
            }
        } catch (Exception e) {
            throw new SQLException();
        }
        
        throw new SQLException();
    }
    
    @Override
    public float getFloat(String columnName) throws SQLException {
        if (isClosed() || isBeforeFirst() || isAfterLast()
                || !this.caseInsensitive.containsCaseInsensitive(this.columnTypeAccess, columnName)) {
            logger.error("FATAL ERROR !" + "Error getting Float");
            throw new SQLException("Error Getting Float.");
        }
        String value = this.columnTypeAccess.get(columnName).get(this.rowIndex - 1);
        if (value.equals("-")) {
            return 0; //<================================================================= undefined return requirement
        }
        try {
            if (typeInvoker.invoke("float").isValid(value)) {
                return (Float) typeInvoker.invoke("float").castType(value);
            }
        } catch (Exception e) {
            throw new SQLException();
        }
        
        throw new SQLException();
    }
    
    @Override
    public Object getObject(int columnIndex) throws SQLException {
        if (isClosed() || isBeforeFirst() || isAfterLast() || columnIndex < 1 || columnIndex > this.columnSize) {
            logger.error("FATAL ERROR !" + "Error getting Object");
            throw new SQLException("Error Getting Object.");
        }
        String value = this.indexAccess.get(columnIndex).get(this.rowIndex - 1);
        if (value.equals("-")) {
            return null; //<================================================================= undefined return requirement
        }
        
        try {
            return ((Object) typeInvoker.invoke("object").castType(value,
                    this.adapter.getTableIdentifierTypes().get(columnIndex - 1)));
        } catch (ParseException | TypeException e) {
            logger.error("FATAL ERROR !" + "Error getting Object");
            throw new SQLException();
        }
    }
    
    @Override
    public String getString(int columnIndex) throws SQLException {
        if (isClosed() || isBeforeFirst() || isAfterLast() || columnIndex < 1 || columnIndex > this.columnSize) {
            logger.error("FATAL ERROR !" + "Error getting String");
            throw new SQLException("Error Getting String.");
        }
        String value = this.indexAccess.get(columnIndex).get(this.rowIndex - 1);
        if (value.equals("-")) {
            return null; //<================================================================= undefined return requirement
        }
        try {
            if (typeInvoker.invoke("varchar").isValid(value)) {
                return (String) typeInvoker.invoke("varchar").castType(value);
            }
        } catch (Exception e) {
            logger.error("FATAL ERROR !" + "Error getting Float");
            throw new SQLException();
        }
        
        throw new SQLException();
    }
    
    @Override
    public String getString(String columnName) throws SQLException {
        if (isClosed() || isBeforeFirst() || isAfterLast()
                || !this.caseInsensitive.containsCaseInsensitive(this.columnTypeAccess, columnName)) {
            logger.error("FATAL ERROR !" + "Error getting String");
            throw new SQLException("Error Getting String.");
        }
        String value = this.columnTypeAccess.get(columnName).get(this.rowIndex - 1);
        if (value.equals("-")) {
            return null; //<================================================================= undefined return requirement
        }
        try {
            if (typeInvoker.invoke("varchar").isValid(value)) {
                return (String) typeInvoker.invoke("varchar").castType(value);
            }
        } catch (Exception e) {
            logger.error("FATAL ERROR !" + "Error getting String");
            throw new SQLException();
        }
        
        throw new SQLException();
    }
    
    @Override
    public int getInt(int columnIndex) throws SQLException {
        if (isClosed() || isBeforeFirst() || isAfterLast() || columnIndex < 1 || columnIndex > this.columnSize) {
            logger.error("FATAL ERROR !" + "Error getting Integer");
            throw new SQLException("Error Getting Integer.");
        }
        String value = this.indexAccess.get(columnIndex).get(this.rowIndex - 1);
        if (value.equals("-")) {
            return 0;
        }
        try {
            if (typeInvoker.invoke("int").isValid(value)) {
                return (Integer) typeInvoker.invoke("int").castType(value);
            }
        } catch (Exception e) {
            logger.error("FATAL ERROR !" + "Error getting Integer");
            throw new SQLException();
        }
        throw new SQLException();
    }
    
    @Override
    public int getInt(String columnName) throws SQLException {
        if (isClosed() || isBeforeFirst() || isAfterLast()
                || !this.caseInsensitive.containsCaseInsensitive(this.columnTypeAccess, columnName)) {
            logger.error("FATAL ERROR !" + "Error getting Integer");
            throw new SQLException("Error Getting Integer.");
        }
        String value = this.columnTypeAccess.get(columnName).get(this.rowIndex - 1);
        if (value.equals("-")) {
            return 0; //<================================================================= undefined return requirement
        }
        try {
            if (typeInvoker.invoke("int").isValid(value)) {
                return (Integer) typeInvoker.invoke("int").castType(value);
            }
        } catch (Exception e) {
            logger.error("FATAL ERROR !" + "Error getting Integer");
            throw new SQLException();
        }
        throw new SQLException();
    }
    
    @Override
    public void close() throws SQLException {
        logger.warn("Closing Resultset...");
        this.adapter = null;
        this.statement = null;
    }
    
    @Override
    public boolean isClosed() throws SQLException {
        logger.info("Checking if closed: " + ((this.adapter == null) && (this.statement == null)));
        return (this.adapter == null) && (this.statement == null);
    }
    
    @Override
    public int findColumn(String columnName) throws SQLException {
        if (isClosed() || isBeforeFirst() || isAfterLast()
                || !this.caseInsensitive.containsCaseInsensitive(this.columnTypeAccess, columnName)) {
            logger.error("Error finding Column!");
            throw new SQLException();
        }
        ArrayList<String> columnValues = new ArrayList<String>();
        columnValues = this.columnTypeAccess.get(columnName);
        
        for (Integer key : this.indexAccess.keySet()) {
            if (this.indexAccess.get(key).equals(columnValues)) {
                return key;
            }
        }
        
        return 0;
        
    }
    
    //////////////////////////////////////////////////////////
    ///////////////////////unused/////////////////////////////
    //////////////////////////////////////////////////////////
    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public void cancelRowUpdates() throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void clearWarnings() throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void deleteRow() throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public Array getArray(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Array getArray(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public InputStream getAsciiStream(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public InputStream getAsciiStream(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public BigDecimal getBigDecimal(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public BigDecimal getBigDecimal(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public BigDecimal getBigDecimal(int arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public BigDecimal getBigDecimal(String arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public InputStream getBinaryStream(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public InputStream getBinaryStream(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Blob getBlob(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Blob getBlob(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public boolean getBoolean(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public boolean getBoolean(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public byte getByte(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public byte getByte(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public byte[] getBytes(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public byte[] getBytes(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Reader getCharacterStream(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Reader getCharacterStream(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Clob getClob(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Clob getClob(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public int getConcurrency() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public String getCursorName() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Date getDate(int arg0, Calendar arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Date getDate(String arg0, Calendar arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public double getDouble(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public double getDouble(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public int getFetchDirection() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public int getFetchSize() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public int getHoldability() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public long getLong(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public long getLong(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public Reader getNCharacterStream(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Reader getNCharacterStream(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public NClob getNClob(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public NClob getNClob(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public String getNString(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public String getNString(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Object getObject(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Object getObject(int arg0, Map<String, Class<?>> arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public <T> T getObject(int arg0, Class<T> arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public <T> T getObject(String arg0, Class<T> arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Ref getRef(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Ref getRef(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public int getRow() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public RowId getRowId(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public RowId getRowId(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public SQLXML getSQLXML(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public SQLXML getSQLXML(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public short getShort(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public short getShort(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public Time getTime(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Time getTime(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Time getTime(int arg0, Calendar arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Time getTime(String arg0, Calendar arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Timestamp getTimestamp(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Timestamp getTimestamp(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Timestamp getTimestamp(int arg0, Calendar arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Timestamp getTimestamp(String arg0, Calendar arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public int getType() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public URL getURL(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public URL getURL(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public InputStream getUnicodeStream(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public InputStream getUnicodeStream(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public SQLWarning getWarnings() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public void insertRow() throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void moveToCurrentRow() throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void moveToInsertRow() throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void refreshRow() throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public boolean relative(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public boolean rowDeleted() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public boolean rowInserted() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public boolean rowUpdated() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public void setFetchDirection(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void setFetchSize(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateArray(int arg0, Array arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateArray(String arg0, Array arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateAsciiStream(int arg0, InputStream arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateAsciiStream(String arg0, InputStream arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateAsciiStream(String arg0, InputStream arg1, int arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateAsciiStream(String arg0, InputStream arg1, long arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateBigDecimal(String arg0, BigDecimal arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateBinaryStream(int arg0, InputStream arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateBinaryStream(String arg0, InputStream arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateBinaryStream(String arg0, InputStream arg1, int arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateBinaryStream(int arg0, InputStream arg1, long arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateBinaryStream(String arg0, InputStream arg1, long arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateBlob(int arg0, Blob arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateBlob(String arg0, Blob arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateBlob(int arg0, InputStream arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateBlob(String arg0, InputStream arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateBlob(int arg0, InputStream arg1, long arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateBlob(String arg0, InputStream arg1, long arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateBoolean(int arg0, boolean arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateBoolean(String arg0, boolean arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateByte(int arg0, byte arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateByte(String arg0, byte arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateBytes(int arg0, byte[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateBytes(String arg0, byte[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateCharacterStream(int arg0, Reader arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateCharacterStream(String arg0, Reader arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateCharacterStream(String arg0, Reader arg1, int arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateClob(int arg0, Clob arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateClob(String arg0, Clob arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateClob(int arg0, Reader arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateClob(String arg0, Reader arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateClob(int arg0, Reader arg1, long arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateClob(String arg0, Reader arg1, long arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateDate(int arg0, Date arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateDate(String arg0, Date arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateDouble(int arg0, double arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateDouble(String arg0, double arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateFloat(int arg0, float arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateFloat(String arg0, float arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateInt(int arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateInt(String arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateLong(int arg0, long arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateLong(String arg0, long arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateNCharacterStream(int arg0, Reader arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateNCharacterStream(String arg0, Reader arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateNCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateNClob(int arg0, NClob arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateNClob(String arg0, NClob arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateNClob(int arg0, Reader arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateNClob(String arg0, Reader arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateNClob(int arg0, Reader arg1, long arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateNClob(String arg0, Reader arg1, long arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateNString(int arg0, String arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateNString(String arg0, String arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateNull(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateNull(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateObject(int arg0, Object arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateObject(String arg0, Object arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateObject(int arg0, Object arg1, int arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateObject(String arg0, Object arg1, int arg2) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateRef(int arg0, Ref arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateRef(String arg0, Ref arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateRow() throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateRowId(int arg0, RowId arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateRowId(String arg0, RowId arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateSQLXML(int arg0, SQLXML arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateSQLXML(String arg0, SQLXML arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateShort(int arg0, short arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateShort(String arg0, short arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateString(int arg0, String arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateString(String arg0, String arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateTime(int arg0, Time arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateTime(String arg0, Time arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateTimestamp(int arg0, Timestamp arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void updateTimestamp(String arg0, Timestamp arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public boolean wasNull() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
    
    public static void main(String[] args) {
        LinkedHashMap<String, String> hey = new LinkedHashMap<String, String>();
        hey.put("lol", "hahaha");
        System.out.println(hey.get(""));
        
    }
    
}
