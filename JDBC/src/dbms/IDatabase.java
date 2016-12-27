package dbms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import javax.xml.stream.XMLStreamException;

import adapter.Adapter;
import dtd.DTDException;

public interface IDatabase {
    
    /**
     * create database table
     *
     * @param tableName
     * @param columnNames
     * @param columnValues
     * @throws Exception
     */
    public void createDBTable(String tableName, ArrayList<String> columnNames, ArrayList<String> columnValues)
            throws Exception;
    
    /**
     * insert into a database table
     *
     * @param tableName
     * @param columnNames
     * @param columnValues
     * @throws Exception
     */
    public int insertIntoTable(String tableName, ArrayList<String> columnNames, ArrayList<String> columnValues)
            throws Exception;
    
    /**
     * delete a database table
     *
     * @param tableName
     * @throws DatabaseException
     * @throws DTDException
     */
    public void dropTable(String tableName) throws DatabaseException, DTDException;
    
    /**
     * update a database table
     *
     * @param tableName
     * @param columnsNames
     * @param columnValues
     * @param where
     * @throws Exception
     */
    public int updateTable(String tableName, ArrayList<String> columnsNames, ArrayList<String> columnValues,
            String[] where) throws Exception;
    
    /**
     * deletes from a table a row or the entire table but with keeping the
     * columns.
     *
     * @param tableName
     * @param where
     *            array of size THREE which have the condition for deleting, if
     *            the user wants to delete all "* - doesn't specify a condition"
     *            is is null
     * @throws DatabaseException
     * @throws IOException
     * @throws XMLStreamException
     * @throws Exception
     */
    public int deleteFromDBTable(String tableName, String[] where)
            throws DatabaseException, XMLStreamException, IOException, Exception;
    
    /**
     * select columns from a database table
     *
     * @param tableName
     * @param columnNames
     * @param where
     * @return
     * @throws Exception
     */
    public LinkedHashMap<String, ArrayList<String>> selectFromTable(String tableName, ArrayList<String> columnNames,
            String[] where, Adapter adapter) throws Exception;
    
    /**
     * 
     * @param tableName
     * @param columnNames
     * @param where
     * @return
     * @throws Exception
     */
    
    public LinkedHashMap<String, ArrayList<String>> selectDistinctFromTable(String tableName,
            ArrayList<String> columnNames, String[] where, Adapter adapter) throws Exception;
    
    /**
     * 
     * @param tableName
     * @param columnNames
     * @throws Exception
     */
    
    public int alterDrop(String tableName, ArrayList<String> columnNames) throws Exception;
    
    /**
     * 
     * @param tableName
     * @param columnNames
     * @param types
     * @throws Exception
     */
    public int alterAdd(String tableName, ArrayList<String> columnNames, ArrayList<String> types) throws Exception;
    
}
