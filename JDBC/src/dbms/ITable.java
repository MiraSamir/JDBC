package dbms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import javax.xml.stream.XMLStreamException;

import adapter.Adapter;

public interface ITable {
    /**
     *
     * @param columnNames
     * @param columnValues
     * @throws Exception
     */
    public int insertRow(ArrayList<String> columnNames, ArrayList<String> columnValues) throws Exception;
    
    /**
     * @param tableName
     * @param where
     * @throws DatabaseException
     * @throws XMLStreamException
     * @throws IOException
     * @throws Exception
     */
    
    public int deleteRows(String tableName, String[] where)
            throws DatabaseException, XMLStreamException, IOException, Exception;
    
    /**
     *
     * @param columnsNames
     * @param columnValues
     * @param where
     * @throws Exception
     */
    public int updateRows(ArrayList<String> columnsNames, ArrayList<String> columnValues, String[] where)
            throws Exception;
    
    /**
     *
     * @param columnNames
     * @param where
     * @return
     * @throws Exception
     */
    public LinkedHashMap<String, ArrayList<String>> selectFromTable(ArrayList<String> columnNames, String[] where,
            Adapter adapter) throws Exception;
    
    /**
     * 
     * @param columnNames
     * @param where
     * @return
     * @throws Exception
     */
    public LinkedHashMap<String, ArrayList<String>> selectDistinctFromTable(ArrayList<String> columnNames,
            String[] where, Adapter adapter) throws Exception;
    
    /**
     * 
     * @param columnNames
     * @param columnTypes
     * @throws Exception
     */
    
    public int alterTableAdd(ArrayList<String> columnNames, ArrayList<String> columnTypes) throws Exception;
    
    /**
     * 
     * @param columnNames
     * @throws Exception
     */
    
    public int alterTableDrop(ArrayList<String> columnNames) throws Exception;
}
