package dbms;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import adapter.Adapter;

public interface IDBMS {
    
    /**
     * sets the used database in all function it is called in the use() fn in
     * the Parser class.
     * 
     * @param databaseName
     * @throws DatabaseException
     * @throws Exception
     */
    public int useDatabase(String databaseName) throws DatabaseException, Exception;
    
    /**
     * 
     * @param databaseName
     * @return
     * @throws Exception
     */
    
    public int createDatabase(String databaseName) throws Exception;
    
    /**
     * 
     * @param databaseName
     * @return
     * @throws Exception
     */
    
    public int dropDatabase(String databaseName) throws Exception;
    
    /**
     * Creates a new table.
     * 
     * @param tableName
     * @param columns
     *            the names of the columns
     * @param types
     *            the types of each column
     * @throws Exception
     */
    public int createTable(String tableName, ArrayList<String> columns, ArrayList<String> types) throws Exception;
    
    /**
     * 
     * @param tableName
     * @throws Exception
     */
    
    public int dropTable(String tableName) throws Exception;
    
    /**
     * inserts a row in a specified table.
     * 
     * @param tableName
     * @param columns
     *            names of the columns to insert in
     * @param values
     *            the values to be inserted
     * @throws Exception
     */
    public int insertIntoTable(String tableName, ArrayList<String> columns, ArrayList<String> values) throws Exception;
    
    /**
     * deletes from a table a row or the entire table but with keeping the
     * columns.
     * 
     * @param tableName
     * @param where
     *            array of size THREE which have the condition for deleting, if
     *            the user wants to delete all "* - doesn't specify a condition"
     *            is is null
     * @throws Exception
     */
    public int deleteFromTable(String tableName, String[] where) throws Exception;
    
    /**
     * update a row in the table.
     * 
     * @param tableName
     * @param columns
     *            names of the columns to be updated
     * @param values
     *            the new values to be put
     * @param where
     *            the condition, is sent null if the user doesn't state one
     * @throws Exception
     */
    public int updateTable(String tableName, ArrayList<String> columns, ArrayList<String> values, String[] where)
            throws Exception;
    
    /**
     * selects a specific column from a table
     * 
     * @param tableName
     * @param columns
     *            the columns needed to be selected, it is sent null if the user
     *            wants to select all the columns
     * @param where
     *            array of size THREE which have the condition for deleting, if
     *            the user wants to delete all "* - doesn't specify a condition"
     *            is is null
     * @throws Exception
     */
    public LinkedHashMap<String, ArrayList<String>> selectFromTable(String tableName, ArrayList<String> columns,
            String[] where, Adapter adapter) throws Exception;
    
    public LinkedHashMap<String, ArrayList<String>> selectDistinctFromTable(String tableName, ArrayList<String> columns,
            String[] where, Adapter adapter) throws DBMSException, Exception;
    
    /**
     * Adds extra (column(s)) to the selected table.
     * 
     * @param tableName
     * @param columns
     *            the names of the columns to be added
     * @param types
     *            the type corresponding to each column added
     * @throws DBMSException
     * @throws Exception
     */
    public int alterAdd(String tableName, ArrayList<String> columns, ArrayList<String> types)
            throws DBMSException, Exception;
    
    /**
     * Drops column(s) from the selected table.
     * 
     * @param tableName
     * @param columns
     *            the names of the columns need to be dropped
     * @throws DBMSException
     * @throws Exception
     */
    public int alterDrop(String tableName, ArrayList<String> columns) throws DBMSException, Exception;
}
