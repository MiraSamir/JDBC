package dbms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import javax.xml.stream.XMLStreamException;

public interface ITable {
	/**
	 *
	 * @param columnNames
	 * @param columnValues
	 * @throws Exception
	 */
	public void insertRow(ArrayList<String> columnNames, ArrayList<String> columnValues) throws Exception;
	/**
	 *
	 * @param tableName
	 * @param where
	 * @throws DatabaseException
	 * @throws XMLStreamException
	 * @throws IOException
	 * @throws Exception 
	 */

	public void deleteRows(String tableName, String[] where) throws DatabaseException, XMLStreamException, IOException, Exception;

	/**
	 *
	 * @param columnsNames
	 * @param columnValues
	 * @param where
	 * @throws Exception
	 */
	public void updateRows(ArrayList<String> columnsNames, ArrayList<String> columnValues, String[] where)
			throws Exception;
	/**
	 *
	 * @param columnNames
	 * @param where
	 * @return
	 * @throws Exception
	 */
	public LinkedHashMap<String, ArrayList<String>> selectFromTable(ArrayList<String> columnNames, String[] where)
			throws Exception;
}
