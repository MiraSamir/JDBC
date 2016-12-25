package dbms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import javax.xml.stream.XMLStreamException;

import dataTypes.TypeException;
import dataTypes.TypeInvoker;
import fileHandler.IFileReader;
import fileHandler.XMLReader;

public class TableColumn implements ITableColumn {
	
	private ArrayList<String> columnNames;
	private ArrayList<String> columnValues;
	private ArrayList<ArrayList<String>> columnIdentifiers;
	private String tablePath;
	private String columnName;
	private String columnType;
	private TypeInvoker typeInvoker;
	private IFileReader fileReader;
	
	public TableColumn(String tablePath, ArrayList<String> columnNames, ArrayList<String> columnValues) {
		this.tablePath = tablePath;
		this.columnNames = columnNames;
		this.columnValues = columnValues;
		this.typeInvoker = new TypeInvoker();
		
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	
	public TableColumn(String columnName, String columnType) {
		this.columnName = columnName;
		this.columnType = columnType;
	}
	
	public String getColumnType() {
		return columnType;
	}
	
	public void setColumnType(String columnType) {
		this.columnType = columnType;
	}
	
	@Override
	public ArrayList<ArrayList<String>> getColumnIdentifiers()
			throws XMLStreamException, IOException, DatabaseException {
		String extension = "";
		int i = tablePath.lastIndexOf('.');
		if (i >= 0) {
			extension = tablePath.substring(i + 1);
		}
		constructFileHandler(extension);
		fileReader.initializeReader(tablePath);
		fileReader.fastForward("TableIdentifier");
		columnIdentifiers = fileReader.readRow("TableIdentifier");
		fileReader.endReader();
		return columnIdentifiers;
	}
	
	@Override
	public boolean hasValidIdentifiers() throws XMLStreamException, IOException, TypeException, DatabaseException {
		getColumnIdentifiers();
		if (columnNames == null && (columnValues.size() != columnIdentifiers.get(0).size())) {
			return false;
		} else if (columnNames != null && (columnNames.size() != columnValues.size())) {
			return false;
		}
		if (columnNames != null) {
			if (!validateColumnNames()) {
				return false;
			}
			ArrayList<ArrayList<String>> arrangedArray = rearrangeColumn();
			columnNames = arrangedArray.get(0);
			columnValues = arrangedArray.get(1);
		} else {
			columnNames = columnIdentifiers.get(0);
		}
		if (!validateColumnValues()) {
			return false;
		}
		return true;
	}
	
	@Override
	public ArrayList<ArrayList<String>> rearrangeColumn() {
		ArrayList<String> tempNames = new ArrayList<String>();
		ArrayList<String> tempValues = new ArrayList<String>();
		ArrayList<ArrayList<String>> mergeArray = new ArrayList<ArrayList<String>>();
		for (int i = 0; i < columnIdentifiers.get(0).size(); i++) {
			if (columnNames.contains(columnIdentifiers.get(0).get(i))) {
				int index = columnNames.indexOf(columnIdentifiers.get(0).get(i));
				tempNames.add(columnNames.get(index));
				tempValues.add(columnValues.get(index));
			} else {
				tempNames.add(columnIdentifiers.get(0).get(i));
				tempValues.add(null);
			}
		}
		mergeArray.add(tempNames);
		mergeArray.add(tempValues);
		return mergeArray;
	}
	
	@Override
	public boolean isValidQuery(String[] where)
			throws XMLStreamException, IOException, TypeException, DatabaseException {
		getColumnIdentifiers();
		boolean whereFlag = true, namesFlag = true, valuesFlag = true;
		if (where != null) {
			whereFlag = validateWhereInput(where);
			//System.out.println(whereFlag);
		}
		if (columnNames != null) {
			namesFlag = validateColumnNames();
			//System.out.println(namesFlag);
		}
		if (columnValues != null) {
			valuesFlag = validateColumnValues();
			//System.out.println(valuesFlag);
		}
		return (whereFlag && namesFlag && valuesFlag);
	}
	
	public boolean validateColumnNames() throws XMLStreamException, IOException, DatabaseException {
		getColumnIdentifiers();
		for (int i = 0; i < columnNames.size(); i++) {
			if (!columnIdentifiers.get(0).contains(columnNames.get(i))) {
				return false;
			}
		}
		try {
			isDuplicate();
		} catch (DatabaseException e) {
			return false;
		}
		return true;
	}
	
	private boolean validateWhereInput(String[] where) throws TypeException {
		if ((!where[1].equals("=")) && (!where[1].equals(">")) && (!where[1].equals("<"))) {
			return false;
		}
		if (!columnIdentifiers.get(0).contains(where[0])) {
			return false;
		}
		int index = columnIdentifiers.get(0).indexOf(where[0]);
		String type = columnIdentifiers.get(1).get(index);
		return typeInvoker.invoke(type).isValid(where[2]);
	}
	
	private boolean validateColumnValues() throws TypeException {
		int valueIndex;
		for (int i = 0; i < columnNames.size(); i++) {
			if (columnIdentifiers.get(0).contains(columnNames.get(i))) {
				if (columnValues.get(i) == null) {
					continue;
				}
				valueIndex = columnIdentifiers.get(0).indexOf(columnNames.get(i));
				if (!typeInvoker.invoke(columnIdentifiers.get(1).get(valueIndex)).isValid(columnValues.get(i))) {
					System.out.println("sssssssss" + columnValues.get(i));
					return false;
				}
			}
		}
		return true;
	} // return true if column doesnot exist
	
	public boolean isColumnNameUnique() throws XMLStreamException, IOException, DatabaseException {
		getColumnIdentifiers();
		for (int i = 0; i < columnNames.size(); i++) {
			if (this.columnIdentifiers.get(0).contains(columnNames.get(i))) {
				return false;
			}
		}
		return true;
	}
	
	public String getColumnType(String columnName) throws XMLStreamException, IOException, DatabaseException {
		getColumnIdentifiers();
		int index = this.columnIdentifiers.get(0).indexOf(columnName);
		return this.columnIdentifiers.get(1).get(index);
	}
	
	public void isDuplicate() throws DatabaseException {
		HashSet<String> set = new HashSet<>();
		for (int i = 0; i < columnNames.size(); i++) {
			if (!set.add(columnNames.get(i))) {
				throw new DatabaseException("Error !! Duplicate Column Names !! ");
			}
		}
	}
	
	private void constructFileHandler(String type) throws DatabaseException {
		switch (type) {
			case "xml":
				fileReader = new XMLReader();
				break;
			default:
				throw new DatabaseException("Unsupported File Format");
				
		}
	}
	
}
