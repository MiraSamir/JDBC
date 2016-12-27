package fileHandler.protocolBuffer;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.junit.Before;
import org.junit.Test;

public class testProtocolWriter {
	
	String tableName;
	String tablePath;
	String extension;
	FileOutputStream outputFile;
	ProtocolWriter protocolWriter;
	ArrayList<String> columnsName;
	ArrayList<String> columnsTypes;
	ArrayList<String> columnsValues;
	
	@Before
	public void setTest() {
		tableName = "table3";
		tablePath = "D:\\tables";
		extension = ".ser";
		columnsName = new ArrayList<>();
		columnsTypes = new ArrayList<>();
		columnsValues = new ArrayList<>();
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
	}



	@Test
	public void testInitializeWriter() throws XMLStreamException, IOException {
//		test create file correct
		try {
			protocolWriter.initializeWriter(tableName, tablePath);
			assertTrue(new File(tablePath +"\\"+ tableName + extension).exists());
		} catch (Exception e) {
			fail("expected no errors in creating new file");
		}
//		test create file already created
		
		try {
			protocolWriter.initializeWriter(tableName, tablePath);
		} catch (Exception e) {
			fail("expected no errors in overwriting new file");
		}
//		test create in invalid path
		
		try {
			protocolWriter.initializeWriter(tableName,"notApath");
		} catch (Exception e) {
			assertTrue(e instanceof protocolBufferException);
		}
		
		
		
	}


	@Test
	public void testCreateTableIdentifier() {

		try {
			protocolWriter.createTableIdentifier(tableName, columnsName, columnsTypes);
		} catch (Exception e) {
			fail("Expected no errors while adding table identifier");
			
		}
		
	}

	@Test
	public void testWriteRow() {
		try {
			protocolWriter.setIndex(0);
			protocolWriter.writeRow(columnsName, columnsValues);
		} catch (Exception e) {
			fail("Expected no errors while adding table values");
			
		}	
	}

}
