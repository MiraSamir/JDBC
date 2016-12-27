package fileHandler.protocolBuffer;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import javax.xml.stream.XMLStreamException;

import org.junit.Before;
import org.junit.Test;

public class writerMain {
	String tableName;
	String tablePath;
	String extension;
	Path path;
	FileOutputStream outputFile;
	static ProtocolWriter protocolWriter;
	static ArrayList<String> columnsName;
	ArrayList<String> columnsTypes;
	static ArrayList<String> columnsValues;
	static ArrayList<String> columnsValues2;
	static ArrayList<String> columnsValues3;
	
	public void setTest() {
		tableName = "tablemwwwwwwww";
		tablePath = "D:\\tables\\"+tableName+".ser";
		path = Paths.get(tablePath);
		extension = ".ser";
		columnsName = new ArrayList<>();
		columnsTypes = new ArrayList<>();
		columnsValues = new ArrayList<>();
		columnsValues2 = new ArrayList<>();
		columnsValues3 = new ArrayList<>();
		protocolWriter = new ProtocolWriter();
		
		columnsName.add("Name");
		columnsName.add("age");
		columnsName.add("Salary");
		columnsName.add("Date of birth");
		columnsTypes.add("varchar");
		columnsTypes.add("int");
		columnsTypes.add("float");
		columnsTypes.add("date");
		
		columnsValues.add("Aya");
		columnsValues.add("20");
		columnsValues.add("100000");
		columnsValues.add("13-4-1996");
		
		columnsValues2.add("rna");
		columnsValues2.add("17");
		columnsValues2.add("50000000");
		columnsValues2.add("24-3-1999");
		
		columnsValues3.add("jna");
		columnsValues3.add(null);
		columnsValues3.add("8200000");
		columnsValues3.add("4-5-2006");
	}

	public void testInitializeWriter() throws XMLStreamException, IOException {
//		test create file correct
		try {
			protocolWriter.initializeWriter(tableName, tablePath);
		} catch (Exception e) { 
			e.printStackTrace();
		}
//		test create file already created
		
		
	}


	public void testCreateTableIdentifier() {

		try {
			protocolWriter.createTableIdentifier(tableName, columnsName, columnsTypes);
//		    Table tableFromFile = Table.parseFrom(new FileInputStream(tablePath +"\\"+ tableName + extension));
//		    List list = tableFromFile.getColList();
		} catch (Exception e) {
			e.printStackTrace();
			
		}
		
	}

	public void testWriteRow() {
		try {
			protocolWriter.setIndex(3);
			protocolWriter.writeRow(columnsName, columnsValues3);
		} catch (Exception e) {
			
		}	
	}
	public static void main(String[] args) throws Exception {
		writerMain wm = new writerMain();
		wm.setTest();
		wm.testInitializeWriter();
		wm.testCreateTableIdentifier();
//		wm.testWriteRow();
		protocolWriter.setIndex(0);
		protocolWriter.writeRow(columnsName, columnsValues);
		protocolWriter.setIndex(1);
		protocolWriter.writeRow(columnsName, columnsValues2);
		protocolWriter.setIndex(2);
		protocolWriter.writeRow(columnsName, columnsValues3);
		protocolWriter.setIndex(3);
		protocolWriter.writeRow(columnsName, columnsValues);
	}
}
