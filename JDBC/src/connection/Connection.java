package connection;

import java.io.File;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.Logger;

import dbms.DBMS;
import logger.CLILog;
import logger.Log4j;
import statement.Statement;

public class Connection implements java.sql.Connection {
    
    private Statement statement;
    private String url;
    private String path;
    private String protocol;
    private DBMS dbms;
    private File directory;
    static Logger logger;
    static Logger loggerP;
    
    public String getProtocol() {
        return this.protocol;
    }
    
    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }
    
    public File getDirectory() {
        return this.directory;
    }
    
    public void setDirectory(File directory) {
        this.directory = directory;
    }
    
    public String getUrl() {
        return this.url;
    }
    
    public void setUrl(String url) {
        this.url = url;
    }
    
    public String getPath() {
        return this.path;
    }
    
    public void setPath(String path) {
        this.path = path;
    }
    
    public Connection(String url, String path, String protocol, File directory) throws Exception {
        Connection.logger = Log4j.getInstance();
        Connection.loggerP = CLILog.getInstance();
        logger.info("Launching Connection.....");
        loggerP.info("Launching Connection...");
        this.url = url;
        this.path = path;
        this.protocol = protocol;
        this.directory = directory;
        dbms = new DBMS(directory, protocol);
    }
    
    @Override
    public Statement createStatement() throws SQLException {
        try {
            logger.info("Creating Statement...");
            logger.info("Creating Statement...");
            statement = new Statement(this, dbms);
        } catch (Exception e) {
            logger.error("Error while creating Statement!");
            throw new SQLException("Error while creating Statement!");
        }
        return statement;
    }
    
    @Override
    public void close() throws SQLException {
        loggerP.warn("Closing connection...");
        logger.warn("Closing connection...");
        try {
            statement.close();
            statement = null;
        } catch (Exception e) {
            logger.error("ERROR! while closing connection!");
        }
        
    }
    
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
    public void abort(Executor arg0) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void clearWarnings() throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void commit() throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Blob createBlob() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Clob createClob() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public NClob createNClob() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public SQLXML createSQLXML() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Statement createStatement(int arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Statement createStatement(int arg0, int arg1, int arg2) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public boolean getAutoCommit() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public String getCatalog() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Properties getClientInfo() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public String getClientInfo(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public int getHoldability() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public int getNetworkTimeout() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public String getSchema() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public int getTransactionIsolation() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public SQLWarning getWarnings() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public boolean isClosed() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public boolean isReadOnly() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public boolean isValid(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public String nativeSQL(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public CallableStatement prepareCall(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public CallableStatement prepareCall(String arg0, int arg1, int arg2) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public PreparedStatement prepareStatement(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public PreparedStatement prepareStatement(String arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public PreparedStatement prepareStatement(String arg0, int[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public PreparedStatement prepareStatement(String arg0, String[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public PreparedStatement prepareStatement(String arg0, int arg1, int arg2) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public PreparedStatement prepareStatement(String arg0, int arg1, int arg2, int arg3) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public void releaseSavepoint(Savepoint arg0) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void rollback() throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void rollback(Savepoint arg0) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void setAutoCommit(boolean arg0) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void setCatalog(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void setClientInfo(Properties arg0) throws SQLClientInfoException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void setClientInfo(String arg0, String arg1) throws SQLClientInfoException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void setHoldability(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void setNetworkTimeout(Executor arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void setReadOnly(boolean arg0) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public Savepoint setSavepoint() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Savepoint setSavepoint(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public void setSchema(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void setTransactionIsolation(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
}
