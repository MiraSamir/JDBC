package dbms;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;

import dtd.DTD;
import dtd.DTDException;
import fileHandler.IFileReader;
import fileHandler.IFileWriter;
import fileHandler.XMLReader;
import fileHandler.XMLWriter;

public class Database implements IDatabase {
	
	private String databasePath;
	private IFileWriter fileWriter;
	private IFileReader fileReader;
	private DTD dtd;
	private String fileType;
	
	public Database(String databasePath, String fileType) throws DatabaseException {
		this.databasePath = databasePath;
		this.fileType = fileType;
		constructFileHandler();
	}
	
	@Override
	public void createDBTable(String tableName, ArrayList<String> columnNames, ArrayList<String> columnTypes)
			throws Exception {
		String tableNamePath = createTablePath(tableName);
		String tablePathDTD = this.databasePath + File.separator + tableName;
		if (containsTable(tableName) || (columnNames.size() != columnTypes.size())) {
			throw new DatabaseException("Table exists!");
		} else {
			isDuplicate(columnNames);
			fileWriter.initializeWriter(tableName, tableNamePath);
			fileWriter.createTableIdentifier(tableName, columnNames, columnTypes);
			fileWriter.endWriter(tableName);
			dtd = new DTD(tablePathDTD);
			dtd.writeDTD(tableName, columnNames, columnTypes);
		}
	}
	
	@Override
	public void insertIntoTable(String tableName, ArrayList<String> columnNames, ArrayList<String> columnValues)
			throws Exception {
		if (containsTable(tableName)) {
			String tablePath = createTablePath(tableName);
			Table table = new Table(tableName, tablePath, fileWriter, fileReader);
			table.insertRow(columnNames, columnValues);
		} else {
			throw new DatabaseException("ERROR! TABLE DOESNOT EXIST!");
		}
	}
	
	@Override
	public void dropTable(String tableName) throws DatabaseException, DTDException {
		String tableNamePath = createTablePath(tableName);
		String tablePathDTD = this.databasePath + File.separator + tableName;
		if (containsTable(tableName)) {
			File file = new File(tableNamePath);
			dtd = new DTD(tablePathDTD);
			dtd.deleteDTDFile();
			if (!file.delete()) {
				throw new DatabaseException("ERROR! CANNOT DELETE TABLE FILE!");
			}
		} else {
			throw new DatabaseException("ERROR! TABLE DOESNOT EXIST!");
		}
		
	}
	
	@Override
	public void deleteFromDBTable(String tableName, String[] where) throws Exception {
		String tablePath = createTablePath(tableName);
		
		if (containsTable(tableName)) {
			Table table = new Table(tableName, tablePath, fileWriter, fileReader);
			table.deleteRows(tableName, where);
		} else {
			throw new DatabaseException("The file doesn't exist!");
		}
	}
	
	@Override
	public void updateTable(String tableName, ArrayList<String> columnsNames, ArrayList<String> columnValues,
			String[] where) throws Exception {
		String tablePath = createTablePath(tableName);
		Table table = new Table(tableName, tablePath, fileWriter, fileReader);
		// Check that database has this table
		if (!containsTable(tableName)) {
			throw new DatabaseException("Cannot update table not in your database!");
		}
		table.updateRows(columnsNames, columnValues, where);
		
	}
	
	@Override
	public LinkedHashMap<String, ArrayList<String>> selectFromTable(String tableName, ArrayList<String> columnNames,
			String[] where) throws Exception {
		String tablePath = createTablePath(tableName);
		Table table = new Table(tableName, tablePath, fileWriter, fileReader);
		// Check that database has this table
		if (containsTable(tableName)) {
			return table.selectFromTable(columnNames, where);
		} else {
			throw new DatabaseException("The file doesn't exist!");
		}
		
	}
	
	@Override
	public LinkedHashMap<String, ArrayList<String>> selectDistinctFromTable(String tableName,
			ArrayList<String> columnNames, String[] where) throws Exception {
		String tablePath = createTablePath(tableName);
		Table table = new Table(tableName, tablePath, fileWriter, fileReader);
		if (containsTable(tableName)) {
			return table.selectDistinctFromTable(columnNames, where);
		} else {
			throw new DatabaseException("The file doesn't exist!");
		}
	}
	
	private boolean containsTable(String tableName) {
		String tableNamePath = createTablePath(tableName);
		Path path = Paths.get(tableNamePath);
		if (Files.exists(path)) {
			return true;
		}
		return false;
	}
	
	///// to be defined//////
	private void constructFileHandler() throws DatabaseException {
		switch (this.fileType) {
			case "xml":
				fileWriter = new XMLWriter();
				fileReader = new XMLReader();
				break;
			default:
				throw new DatabaseException("Unsupported File Format");
				
		}
	}
	
	private String createTablePath(String tableName) {
		String tableNamePath = this.databasePath + File.separator + tableName + "." + this.fileType;
		return tableNamePath;
	}
	
	private void isDuplicate(ArrayList<String> columnNames) throws DatabaseException {
		HashSet<String> set = new HashSet<>();
		for (int i = 0; i < columnNames.size(); i++) {
			if (!set.add(columnNames.get(i))) {
				throw new DatabaseException("Error !! Duplicate Column Names !! ");
			}
		}
	}
	
	@Override
	public void alterDrop(String tableName, ArrayList<String> columnNames) throws Exception {
		if (containsTable(tableName)) {
			String tablePath = createTablePath(tableName);
			Table table = new Table(tableName, tablePath, fileWriter, fileReader);
			table.alterTableDrop(columnNames);
		} else {
			throw new DatabaseException("ERROR! TABLE DOESNOT EXIST!");
		}
		
	}
	
	@Override
	public void alterAdd(String tableName, ArrayList<String> columnNames, ArrayList<String> types) throws Exception {
		if (containsTable(tableName)) {
			String tablePath = createTablePath(tableName);
			Table table = new Table(tableName, tablePath, fileWriter, fileReader);
			table.alterTableAdd(columnNames, types);
		} else {
			throw new DatabaseException("ERROR! TABLE DOESNOT EXIST!");
		}
		
	}
	
}