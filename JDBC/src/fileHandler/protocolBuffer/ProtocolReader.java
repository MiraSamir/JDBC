package fileHandler.protocolBuffer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import fileHandler.IFileReader;
import fileHandler.protocolBuffer.TableProto.Column;
import fileHandler.protocolBuffer.TableProto.Table;

public class ProtocolReader implements IFileReader {

	/**
	 * Table to be read from file.
	 */
	private Table table;

	/**
	 * The index of the value in a certain row
	 * it increases in fastforward method.
	 */
	private int index;

	/**
	 * List of columns in the read table.
	 */
	private List<Column> columnsList;

	/**
	 * Maximum number of values in the columns.
	 */
	private int maximumNumberOfValues;

	private boolean isUsingTemp;

	public int getIndex() {
		return index;
	}

	public ProtocolReader() {
		index = -1; //because fastforward is always called first
		maximumNumberOfValues = -1;
		isUsingTemp = false;
	}


	@Override
	public void initializeReader(String path) throws XMLStreamException,
			IOException {
		if (isUsingTemp) {
			path = path.substring(0, path.indexOf("temp"));
			isUsingTemp = false;
		}
		table = Table.parseFrom(new FileInputStream(path));
		columnsList = table.getColList();
	}

	@Override
	public void endReader() throws XMLStreamException, IOException {
		columnsList = null;
		index = 0;
	}

	@Override
	public void fastForward(String parentNode) throws XMLStreamException {
		switch (parentNode) {
		case "Row":
			index++;
			break;
		default:
			break;
		}
	}

	@Override
	public ArrayList<ArrayList<String>> readRow(String parentNode)
			throws XMLStreamException {

		ArrayList<ArrayList<String>> row = new ArrayList<ArrayList<String>>();

		if (parentNode.equals("TableIdentifier")) { //reading the table identifiers
			row = getTableIdentefiers();
		} else { //reading a row
			row = getRow();
		}
		return row;
	}

	@Override
	public void copyFile(File source, File dest) throws Exception {
		isUsingTemp = true;
        FileOutputStream fileOutputStream = new FileOutputStream(dest.getPath());
        Files.copy(source.toPath(), fileOutputStream);
        fileOutputStream.close();

	}

	/**
	 * Gets the maximum number of values in the columns.
	 * @param column
	 * @return maximun number of values
	 */
	private int getNumberOfValues(Column column) {
		if (column.getValuesCount() > maximumNumberOfValues) {
			maximumNumberOfValues = column.getValuesCount();
		}
		return maximumNumberOfValues;
	}

	/**
	 * Makes an array list of the table identifiers
	 * [[columnOneName, columnTowName], [columnOneType, columnTwoType]]
	 * @return table identifiers array list
	 */
	private ArrayList<ArrayList<String>> getTableIdentefiers() {
		ArrayList<ArrayList<String>> row = new ArrayList<ArrayList<String>>();
		Column column;
		ArrayList<String> columnNames = new ArrayList<String>();
		ArrayList<String> columnTypes = new ArrayList<String>();

		for (int i = 0; i < columnsList.size(); i++) {
			column = columnsList.get(i);
			columnNames.add(column.getColname());
			columnTypes.add(column.getColtype());
		}
		row.add(columnNames);
		row.add(columnTypes);
		return row;
	}

	/**
	 * Makes an array list of the desired row.
	 * [[columnOneName, columnTowName], [columnOneValue, columnTwoValue]]
	 * @return row array list
	 */
	private ArrayList<ArrayList<String>> getRow() {
		ArrayList<ArrayList<String>> row = new ArrayList<ArrayList<String>>();
		Column column;
		ArrayList<String> columnNames = new ArrayList<String>();
		ArrayList<String> columnValues = new ArrayList<String>();


//		column = columnsList.get(0);
		for (int i = 0; i < columnsList.size(); i++) {
			column = columnsList.get(i);
			columnNames.add(column.getColname());
			if (index < column.getValuesCount()) {
				if (column.getValues(index).equals("\0")) { //null
					columnValues.add(null);
				} else {
					columnValues.add(column.getValues(index));
				}
				maximumNumberOfValues = getNumberOfValues(column);
			}
		}

		if ((maximumNumberOfValues == -1) || ((maximumNumberOfValues != -1) && (index >= maximumNumberOfValues))) {
			return row;	//empty row
		}
		row.add(columnNames);
		row.add(columnValues);
		return row;
	}
}
