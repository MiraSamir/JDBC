package statement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.LinkedHashMap;
//import java.util.logging.Logger;

import org.apache.logging.log4j.Logger;

import adapter.Adapter;
import connection.Connection;
import dbms.DBMS;
import logger.Log4j;
import parser.Parser;
import resultSet.ResultSett;

public class Statement implements java.sql.Statement {
    
    private ArrayList<String> batch;
    private Boolean isClosed;
    private Parser parser;
    private Connection connection;
    
    private Adapter adapter;
    private ResultSet resultSet;
    
    static Logger logger;
    //= LogManager.getLogger(Statement.class);
    final static String stmtIsClosed = "Statement is closed!";
    final static String errorPasringQ = "ERROR! Error parsing SQL Query";
    final static String excQuery = "Parsing SQL Query.";
    final static String successExcQuery = "SQL Query parsed successfully.";
    
    public Statement(Connection connection, DBMS dbms) throws Exception {
        Statement.logger = Log4j.getInstance();
        this.connection = connection;
        this.batch = new ArrayList<String>();
        this.isClosed = false;
        this.parser = new Parser();
        this.adapter = new Adapter();
        this.parser.setDbms(dbms);
        this.parser.setAdapter(this.adapter);
    }
    
    @Override
    public void addBatch(String sql) throws SQLException {
        if (isClosed) {
            logger.error("FATAL ERROR !" + Statement.stmtIsClosed);
            throw new SQLException(Statement.stmtIsClosed);
        }
        batch.add(sql);
    }
    
    @Override
    public void clearBatch() throws SQLException {
        if (isClosed) {
            logger.error("FATAL ERROR !" + Statement.stmtIsClosed);
            throw new SQLException(Statement.stmtIsClosed);
        }
        batch.clear();
    }
    
    @Override
    public boolean execute(String sql) throws SQLException {
        if (isClosed) {
            logger.error("FATAL ERROR !" + Statement.stmtIsClosed);
            throw new SQLException(Statement.stmtIsClosed);
        }
        
        try {
            logger.info(Statement.excQuery);
            parser.setWordsIndex(0);
            parser.parse(sql);
            
        } catch (Exception e) {
            logger.error(Statement.errorPasringQ);
            throw new SQLException(Statement.errorPasringQ);
        }
        logger.info(Statement.successExcQuery);
        if (getUpdateCount() == -1) { // the result is a result set (not sure empty or not)
            
            // if empty 
            if (parser.getSelected().get(parser.getSelected().keySet().toArray()[0]).size() == 0) {
                logger.info("SELECT query executed.");
                return false;
            }
            adapter.setHashMaps(parser.getSelected());
            resultSet = new ResultSett(adapter, this);
            logger.info("UPDATE query executed.");
            return true;
        }
        
        return false;
    }
    
    @Override
    public int[] executeBatch() throws SQLException {
        
        if (isClosed) {
            logger.error("FATAL ERROR !" + Statement.stmtIsClosed);
            throw new SQLException(Statement.stmtIsClosed);
        }
        
        int[] rowsAffected = new int[batch.size()];
        int i = 0;
        
        try {
            for (i = 0; i < batch.size(); i++) {
                parser.setWordsIndex(0);
                parser.parse(batch.get(i));
                rowsAffected[i] = parser.getRowsAffected();
            }
            return rowsAffected;
        } catch (Exception e) {
            rowsAffected[i] = 0;
            i++;
            while (i < batch.size()) {
                rowsAffected[i] = executeAfterFail(i);
                i++;
            }
        }
        return rowsAffected;
    }
    
    private int executeAfterFail(int index) {
        try {
            parser.setWordsIndex(0);
            parser.parse(batch.get(index));
            return parser.getRowsAffected();
        } catch (Exception e) {
            return 0;
        }
    }
    
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        
        LinkedHashMap<String, ArrayList<String>> selectedMap;
        
        if (isClosed) {
            logger.error("FATAL ERROR !" + Statement.stmtIsClosed);
            throw new SQLException(Statement.stmtIsClosed);
        }
        
        try {
            logger.info(Statement.excQuery);
            parser.setWordsIndex(0);
            parser.parse(sql);
        } catch (Exception e) {
            throw new SQLException(Statement.errorPasringQ);
        }
        logger.info(Statement.successExcQuery);
        selectedMap = parser.getSelected();
        if (selectedMap == null) {
            logger.error("SQL query doesn't return resultSet");
            throw new SQLException("The query doesn't return resultset");
        } else {
            adapter.setHashMaps(selectedMap);
            resultSet = new ResultSett(adapter, this);
            return resultSet;
        }
    }
    
    @Override
    public int executeUpdate(String sql) throws SQLException {
        
        if (isClosed) {
            logger.error("FATAL ERROR !" + Statement.stmtIsClosed);
            throw new SQLException(Statement.stmtIsClosed);
        }
        try {
            
            parser.setWordsIndex(0);
            parser.parse(sql);
        } catch (Exception e) {
            logger.error(Statement.errorPasringQ);
            throw new SQLException(Statement.errorPasringQ);
        }
        
        if (parser.getSelected() == null) { //Not a resultset
            return (parser.getRowsAffected());
        } else {
            logger.info("Returning Result set while executing update!");
            throw new SQLException("Returned result set!");
        }
        
    }
    
    @Override
    public Connection getConnection() throws SQLException {
        logger.info("Getting connection...");
        if (isClosed) {
            logger.error("FATAL ERROR !" + Statement.stmtIsClosed);
            throw new SQLException(Statement.stmtIsClosed);
        }
        return connection;
        
    }
    
    @Override
    public int getUpdateCount() throws SQLException {
        logger.info("Getting update count.");
        if (isClosed) {
            logger.error("FATAL ERROR !" + Statement.stmtIsClosed);
            throw new SQLException(Statement.stmtIsClosed);
        }
        
        int rowsAffected;
        rowsAffected = parser.getRowsAffected();
        
        if (parser.getSelected() != null) {
            //          check if the selected col is empty hashmap
            if (parser.getSelected().get(parser.getSelected().keySet().toArray()[0]).size() != 0) {
                return -1;
            }
            return -1;
        }
        
        return rowsAffected;
    }
    
    @Override
    public void close() throws SQLException {
        try {
            logger.warn("Closing Statement.");
            isClosed = true;
            connection = null;
            batch.clear();
        } catch (Exception e) {
            logger.error("FATAL ERROR !" + Statement.stmtIsClosed);
            throw new SQLException(Statement.stmtIsClosed);
        }
    }
    
    @Override
    public ResultSet getResultSet() throws SQLException {
        if (isClosed) {
            logger.error("FATAL ERROR !" + Statement.stmtIsClosed);
            throw new SQLException(Statement.stmtIsClosed);
        }
        
        return this.resultSet;
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
    public void cancel() throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void clearWarnings() throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void closeOnCompletion() throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public boolean execute(String arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public boolean execute(String arg0, int[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public boolean execute(String arg0, String[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public int executeUpdate(String arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public int executeUpdate(String arg0, int[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public int executeUpdate(String arg0, String[] arg1) throws SQLException {
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
    public ResultSet getGeneratedKeys() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public int getMaxFieldSize() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public int getMaxRows() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public boolean getMoreResults() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public boolean getMoreResults(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public int getQueryTimeout() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public int getResultSetConcurrency() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public int getResultSetHoldability() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public int getResultSetType() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public SQLWarning getWarnings() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public boolean isClosed() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public boolean isPoolable() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public void setCursorName(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void setEscapeProcessing(boolean arg0) throws SQLException {
        // TODO Auto-generated method stub
        
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
    public void setMaxFieldSize(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void setMaxRows(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void setPoolable(boolean arg0) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void setQueryTimeout(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        
    }
    
}