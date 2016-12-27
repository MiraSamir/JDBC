package cmd;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import parser.Parser;

/**
 * The class that handles the operations in the CMD (works as a handler for the
 * CMDMain class).
 *
 * @author USER
 *
 */

public class CMD implements ICMD {
	
	private String operationName = null;
	String usedDatabase;
	String usedDatabasePath;
	private LinkedHashMap<String, ArrayList<String>> selected;
	
	Parser parser;
	
	public String getOperationName() {
		return operationName;
	}
	
	public LinkedHashMap<String, ArrayList<String>> getSelected() {
		return selected;
	}
	
	@Override
	public void drawTable(LinkedHashMap<String, ArrayList<String>> table) {
		if (table.isEmpty()) {
			System.out.println("This table has no columns!");
		}
		else {
			// string is the key (column name) , arraylist => values
			ArrayList<Integer> columnsWidth;
			int tableWidth;
			ArrayList<String> columnNames;
			ArrayList<String> row;
			int colLength;
			// calculate each column max width
			columnsWidth = calculateColumnsWidth(table);
			// find total width of the table (to write - ,|)
			tableWidth = totalTableWidth(columnsWidth);
			// draw column names
			drawHorizontal(tableWidth, '=');
			columnNames = new ArrayList<String>(table.keySet()); // get column names
																	// (keys)
			drawRaw(columnNames, columnsWidth, '|');
			drawHorizontal(tableWidth, '=');
			// draw each row
			colLength = (table.get(columnNames.get(0))).size();
			for (int i = 0; i < colLength; i++) {
				row = getRow(table, i);
				drawRaw(row, columnsWidth, '|');
				drawHorizontal(tableWidth, '-');
			}
			System.out.println("\n");	
		}
		
	}
	
	private ArrayList<Integer> calculateColumnsWidth(LinkedHashMap<String, ArrayList<String>> table) {
		// to fill array of columns widths
		ArrayList<Integer> columnsWidth = new ArrayList<Integer>();
		List<String> keys = new ArrayList<String>(table.keySet());
		String colName;
		for (int i = 0; i < keys.size(); i++) {
			// loop over table column by column to calculate max width of each
			// column
			// add max width to array of widths
			colName = (String) keys.get(i);
			columnsWidth.add(maxColumnWidth(table.get(keys.get(i)), colName.length()));
		}
		
		return columnsWidth;
		
	}
	
	private int maxColumnWidth(ArrayList<String> column, int columnNameSize) {
		// calculate column max width based on the size of its entries
		int maxWidth = columnNameSize;
		for (int i = 0; i < column.size(); i++) {
			if (column.get(i).length() > maxWidth) {
				maxWidth = column.get(i).length();
			}
		}
		
		return maxWidth;
		
	}
	
	private int totalTableWidth(ArrayList<Integer> columnsWidth) {
		// cal total width of the table
		// by adding each column width and 2 spaces and separator | (+3)
		int totalWidth = 0;
		for (int i = 0; i < columnsWidth.size(); i++) {
			totalWidth += columnsWidth.get(i);
			totalWidth += 3; // for space and |
		}
		
		return totalWidth;
		
	}
	
	private void drawHorizontal(int tableWidth, char symbol) {
		System.out.print("");
		for (int i = -1; i < tableWidth; i++) {
			System.out.print(symbol);
		}
	}
	
	private void drawRaw(ArrayList<String> rowValues, ArrayList<Integer> columnsWidth, char separator) {
		String adjustedWord;
		System.out.print("\n" + separator);
		for (int i = 0; i < rowValues.size(); i++) {
			adjustedWord = adjustWordSize(rowValues.get(i), columnsWidth.get(i));
			System.out.print(" " + adjustedWord + " " + separator);
		}
		System.out.print("\n");
	}
	
	private String adjustWordSize(String word, int width) {
		while (word.length() < width) {
			word += " ";
		}
		return word;
		
	}
	
	private ArrayList<String> getRow(LinkedHashMap<String, ArrayList<String>> table, int index) {
		ArrayList<String> keys = new ArrayList<String>(table.keySet());
		ArrayList<String> row = new ArrayList<String>();
		for (String key : keys) {
			row.add(table.get(key).get(index));
		}
		
		return row;
		
	}
	
	@Override
	public void printMessage() {
		if (operationName != null) {
			System.out.println("Operation " + operationName + " succeeded." + "\n");
		} else {
			System.out.println("Operation " + operationName + "failed.");
		}
		
	}
	
	@Override
	public void operation() throws Exception {
		switch (parser.getOperationNumber()) {
			case -1: // Enter is pressed more than once
				operationName = "ReEnter";
				break;
			case 0: // use
				operationName = "Use Database";
				usedDatabase = parser.getDatabaseName();
				// usedDatabasePath =
				break;
			case 1: // create database
				operationName = "Create Database";
				break;
			case 2: // drop database
				operationName = "Drop Database";
				break;
			case 3: // create table
				parser.setDatabaseName(usedDatabase);
				operationName = "Create Table";
				break;
			case 4: // drop table
				parser.setDatabaseName(usedDatabase);
				operationName = "Drop Table";
				break;
			case 5: // insert into table
				parser.setDatabaseName(usedDatabase);
				operationName = "Insert Into Table";
				break;
			case 6: // select from table
				parser.setDatabaseName(usedDatabase);
				operationName = "Select From Table";
				break;
			case 7: // delete from table
				parser.setDatabaseName(usedDatabase);
				operationName = "Delete From Table";
				break;
			case 8: // update table
				parser.setDatabaseName(usedDatabase);
				operationName = "Update table";
				break;
			case 9: //select distinct
				parser.setDatabaseName(usedDatabase);
				operationName = "Select Distinct";
				break;
			case 10: //alter add
				parser.setDatabaseName(usedDatabase);
				operationName = "Alter add";
				break;
			case 11: //alter drop
				parser.setDatabaseName(usedDatabase);
				operationName = "Alter drop";
				break;
			default:
				throw new CMDException("The query is invalid!");
		}
	}
	
	@Override
	public void getUserInput(String query) throws Exception {
		parser = new Parser();
		try {
			parser.setDatabaseName(usedDatabase);
			parser.parse(query);
			operation();
			selected = parser.getSelected();
		} catch (CMDException e) {
			e.printStackTrace();
			throw new CMDException(e.getErrorMessage());
		}
	}
	
}
