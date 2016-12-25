package dbms;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

import javax.xml.stream.XMLStreamException;

import dataTypes.TypeException;
import fileHandler.IFileReader;
import fileHandler.IFileWriter;

public class Table implements ITable {
	
	private String tableName;
	private String tablePath;
	private IFileReader fileReader;
	private IFileWriter fileWriter;
	private LinkedHashMap<String, ArrayList<String>> selectedColumns;
	private File tempFile, table;
	private WhereCondition testWhereCondition;
	
	public Table(String tableName, String tablePath, IFileWriter fileWriter, IFileReader fileReader) {
		this.tablePath = tablePath;
		this.tableName = tableName;
		this.fileReader = fileReader;
		this.fileWriter = fileWriter;
	}
	
	@Override
	public void insertRow(ArrayList<String> columnNames, ArrayList<String> columnValues) throws Exception {
		TableColumn tableColumn = new TableColumn(tablePath, columnNames, columnValues);
		if (tableColumn.hasValidIdentifiers()) {
			ArrayList<ArrayList<String>> arrangedArray = new ArrayList<ArrayList<String>>();
			arrangedArray = tableColumn.rearrangeColumn();
			performInsertion(arrangedArray);
		} else {
			throw new DatabaseException("Invalid Column Entry !!");
		}
		
	}
	
	private void performInsertion(ArrayList<ArrayList<String>> arrangedArray) throws Exception {
		TableColumn tableColumn = new TableColumn(tablePath, null, null);
		ArrayList<ArrayList<String>> row = initializeTempFile(tableColumn.getColumnIdentifiers());
		fileReader.fastForward("Row");
		while ((row = fileReader.readRow("Row")).size() != 0) {
			fileReader.fastForward("Row");
			fileWriter.writeRow(row.get(0), row.get(1));
		}
		fileWriter.writeRow(arrangedArray.get(0), arrangedArray.get(1));
		fileWriter.endWriter(this.tableName);
		fileReader.endReader();
		tempFile.delete();
	}
	
	@Override
	public void deleteRows(String tableName, String[] where) throws Exception {
		TableColumn tableColumn = new TableColumn(tablePath, null, null);
		if (tableColumn.isValidQuery(where)) {
			performDeletion(where);
		} else {
			throw new DatabaseException("Invalid Query");
		}
		
	}
	
	private void performDeletion(String[] where) throws Exception {
		TableColumn tableColumn = new TableColumn(tablePath, null, null);
		ArrayList<ArrayList<String>> row = initializeTempFile(tableColumn.getColumnIdentifiers());
		testWhereCondition = new WhereCondition(where, tableColumn.getColumnType(where[2]));
		fileReader.fastForward("Row");
		while ((row = fileReader.readRow("Row")).size() != 0) {
			fileReader.fastForward("Row");
			if ((!testWhereCondition.isTrueCondition(row))) {
				fileWriter.writeRow(row.get(0), row.get(1));
			}
		}
		fileWriter.endWriter(this.tableName);
		fileReader.endReader();
		tempFile.delete();
	}
	
	@Override
	public void updateRows(ArrayList<String> columnsNames, ArrayList<String> columnValues, String[] where)
			throws Exception {
		TableColumn tableColumn = new TableColumn(tablePath, columnsNames, columnValues);
		if (!tableColumn.isValidQuery(where)) { // check that the where is valid condition
			throw new DatabaseException("There is error in your  statement!");
		}
		performUpdate(columnsNames, columnValues, where);
	}
	
	private void performUpdate(ArrayList<String> columnsNames, ArrayList<String> columnValues, String[] where)
			throws Exception {
		TableColumn tableColumn = new TableColumn(tablePath, null, null);
		ArrayList<ArrayList<String>> row = initializeTempFile(tableColumn.getColumnIdentifiers());
		ArrayList<String> newColumnsNames;// the values which will be put ( updated)
		ArrayList<String> newColumnsValues;
		int valueIndex;
		testWhereCondition = new WhereCondition(where, tableColumn.getColumnType(where[2]));
		fileReader.fastForward("Row");
		while ((row = fileReader.readRow("Row")).size() != 0) {
			fileReader.fastForward("Row");
			newColumnsNames = row.get(0); // as default the same values
			newColumnsValues = row.get(1);
			if (testWhereCondition.isTrueCondition(row)) { // the row applies condition
				for (int i = 0; i < newColumnsNames.size(); i++) { // find which col will be changed 
					if (columnsNames.contains(newColumnsNames.get(i))) {
						valueIndex = columnsNames.indexOf(newColumnsNames.get(i)); // index of value  = same of column which will be  updated
						newColumnsValues.set(i, columnValues.get(valueIndex)); // change values to new values 
					}
				}
			}
			fileWriter.writeRow(newColumnsNames, newColumnsValues); // write the row updated or not
		}
		fileWriter.endWriter(this.tableName);
		fileReader.endReader();
		tempFile.delete();
	}
	
	@Override
	public LinkedHashMap<String, ArrayList<String>> selectFromTable(ArrayList<String> columnNames, String[] where)
			throws Exception {
		TableColumn tableColumn = new TableColumn(tablePath, columnNames, null);
		if (tableColumn.isValidQuery(where)) {
			selectedColumns = new LinkedHashMap<String, ArrayList<String>>();
			fileReader.initializeReader(tablePath);
			fileReader.fastForward("TableIdentifier");
			if (columnNames == null) {
				columnNames = fileReader.readRow("TableIdentifier").get(0);
			}
			for (int i = 0; i < columnNames.size(); i++) {
				ArrayList<String> columns = new ArrayList<String>();
				selectedColumns.put(columnNames.get(i), columns);
			}
			return performSelection(columnNames, where);
		} else {
			throw new DatabaseException("Invalid Query");
		}
	}
	
	private LinkedHashMap<String, ArrayList<String>> performSelection(ArrayList<String> columnNames, String[] where)
			throws XMLStreamException, IOException, TypeException, ParseException, DatabaseException {
		ArrayList<ArrayList<String>> row = new ArrayList<ArrayList<String>>();
		TableColumn tableColumn = new TableColumn(tablePath, null, null);
		if (where != null) {
			testWhereCondition = new WhereCondition(where, tableColumn.getColumnType(where[0]));
		}
		fileReader.fastForward("Row");
		while ((row = fileReader.readRow("Row")).size() != 0) {
			fileReader.fastForward("Row");
			if (where == null || (where != null && testWhereCondition.isTrueCondition(row))) {
				fillHashMapWithSelectedColumns(row);
			}
		}
		fileReader.endReader();
		return selectedColumns;
	}
	
	private void fillHashMapWithSelectedColumns(ArrayList<ArrayList<String>> row) {
		for (String key : selectedColumns.keySet()) {
			int index = row.get(0).indexOf(key);
			String value = row.get(1).get(index);
			if (value.equals("null") || value == null) {
				value = "-";
			} else {
				value = value.replaceAll("^\"|^\'|\"$|\'$", "");
			}
			selectedColumns.get(key).add(value);
		}
	}
	
	public LinkedHashMap<String, ArrayList<String>> selectDistinctFromTable(ArrayList<String> columnNames,
			String[] where) throws Exception {
		LinkedHashMap<String, ArrayList<String>> preDistinct = selectFromTable(columnNames, where);
		LinkedHashSet<ArrayList<String>> set = fillHashSet(preDistinct);
		return fillHashMapWithHashSet(set);
	}
	
	private LinkedHashSet<ArrayList<String>> fillHashSet(LinkedHashMap<String, ArrayList<String>> preDistinct) {
		LinkedHashSet<ArrayList<String>> set = new LinkedHashSet<ArrayList<String>>();
		ArrayList<String> columnNames = new ArrayList<String>(preDistinct.keySet());
		set.add(columnNames);
		for (int i = 0; i < preDistinct.get(preDistinct.keySet().toArray()[0]).size(); i++) {
			ArrayList<String> tempArray = new ArrayList<String>();
			for (String key : preDistinct.keySet()) {
				tempArray.add(preDistinct.get(key).get(i));
			}
			set.add(tempArray);
		}
		return set;
	}
	
	private LinkedHashMap<String, ArrayList<String>> fillHashMapWithHashSet(LinkedHashSet<ArrayList<String>> set) {
		
		LinkedHashMap<String, ArrayList<String>> afterDistinct = new LinkedHashMap<String, ArrayList<String>>();
		ArrayList<ArrayList<String>> convertedSet = new ArrayList<>(set);
		for (int i = 0; i < convertedSet.get(0).size(); i++) {
			ArrayList<String> column = new ArrayList<String>();
			for (int j = 1; j < convertedSet.size(); j++) {
				String value = convertedSet.get(j).get(i);
				column.add(value);
			}
			afterDistinct.put(convertedSet.get(0).get(i), column);
		}
		return afterDistinct;
	}
	
	public void alterTableAdd(ArrayList<String> columnNames, ArrayList<String> columnTypes) throws Exception {
		TableColumn tableColumn = new TableColumn(tablePath, columnNames, null);
		if (tableColumn.isColumnNameUnique()) {
			tableColumn.isDuplicate();
			ArrayList<ArrayList<String>> newTableIdentifiers = new ArrayList<ArrayList<String>>();
			newTableIdentifiers = tableColumn.getColumnIdentifiers();
			for (int i = 0; i < columnNames.size(); i++) {
				newTableIdentifiers.get(0).add(columnNames.get(i));
				newTableIdentifiers.get(1).add(columnTypes.get(i));
			}
			performAlterTableAdd(columnNames, columnTypes, newTableIdentifiers);
		} else {
			throw new DatabaseException("Column Names already exist!!");
		}
	}
	
	private void performAlterTableAdd(ArrayList<String> columnNames, ArrayList<String> columnTypes,
			ArrayList<ArrayList<String>> newTableIdentifiers) throws Exception {
		ArrayList<ArrayList<String>> row = initializeTempFile(newTableIdentifiers);
		fileReader.fastForward("Row");
		while ((row = fileReader.readRow("Row")).size() != 0) {
			fileReader.fastForward("Row");
			for (int i = 0; i < columnNames.size(); i++) {
				row.get(0).add(columnNames.get(i));
				row.get(1).add(null);
			}
			fileWriter.writeRow(row.get(0), row.get(1));
		}
		fileWriter.endWriter(this.tableName);
		fileReader.endReader();
		tempFile.delete();
	}
	
	public void alterTableDrop(ArrayList<String> columnNames) throws Exception {
		TableColumn tableColumn = new TableColumn(tablePath, columnNames, null);
		if (tableColumn.validateColumnNames()) {
			ArrayList<ArrayList<String>> newTableIdentifiers = new ArrayList<ArrayList<String>>();
			newTableIdentifiers = tableColumn.getColumnIdentifiers();
			for (int i = 0; i < columnNames.size(); i++) {
				int index = newTableIdentifiers.get(0).indexOf(columnNames.get(i));
				newTableIdentifiers.get(0).remove(index);
				newTableIdentifiers.get(1).remove(index);
			}
			performAlterTableDrop(columnNames, newTableIdentifiers);
		} else {
			throw new DatabaseException("Columns donot exist !!");
		}
	}
	
	private void performAlterTableDrop(ArrayList<String> columnNames, ArrayList<ArrayList<String>> newTableIdentifiers)
			throws Exception {
		ArrayList<ArrayList<String>> row = initializeTempFile(newTableIdentifiers);
		fileReader.fastForward("Row");
		while ((row = fileReader.readRow("Row")).size() != 0) {
			fileReader.fastForward("Row");
			for (int i = 0; i < columnNames.size(); i++) {
				int index = row.get(0).indexOf(columnNames.get(i));
				row.get(0).remove(index);
				row.get(1).remove(index);
			}
			fileWriter.writeRow(row.get(0), row.get(1));
		}
		fileWriter.endWriter(this.tableName);
		fileReader.endReader();
		tempFile.delete();
	}
	
	private ArrayList<ArrayList<String>> initializeTempFile(ArrayList<ArrayList<String>> identifiers) {
		try {
			tempFile = new File(tablePath + "temp");
			table = new File(tablePath);
			fileReader.copyFile(table, tempFile);
			fileWriter.initializeWriter(tableName, tablePath);
			fileReader.initializeReader(tablePath + "temp");
			fileReader.fastForward("TableIdentifier");
			fileWriter.createTableIdentifier(this.tableName, identifiers.get(0), identifiers.get(1));
			ArrayList<ArrayList<String>> row = new ArrayList<ArrayList<String>>();
			return row;
		} catch (Exception e) {
			System.out.println("Error gathering information about needed file!");
		}
		return null;
	}
	
}