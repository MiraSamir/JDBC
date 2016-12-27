package junitTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.FileOutputStream;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import fileHandler.protocolBuffer.ProtocolReader;
import fileHandler.protocolBuffer.ProtocolWriter;
import fileHandler.protocolBuffer.TableProto.Table;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)

public class TestProtocolReader {
    
    String tableName;
    String tableName1;
    String tableName2;
    String tablePath;
    String tablePath1;
    String tablePath2;
    String extension;
    FileOutputStream outputFile;
    ProtocolWriter protocolWriter;
    ProtocolWriter protocolWriter1;
    ProtocolWriter protocolWriter2;
    ArrayList<String> columnsName;
    ArrayList<String> columnsTypes;
    ArrayList<String> columnsValues;
    ArrayList<String> columnsValues1;
    ArrayList<String> columnsValues2;
    ArrayList<String> columnsValues3;
    ArrayList<String> columnsValues4;
    
    ProtocolReader protocolReader;
    Table table;
    
    @Before
    public void setTable() {
        tableName = "table";
        tableName1 = "table1";
        tableName2 = "table2";
        tablePath = "C:\\Users\\USER\\Desktop\\tables\\" + tableName + ".ser";
        tablePath1 = "C:\\Users\\USER\\Desktop\\tables\\" + tableName1 + ".ser";
        tablePath2 = "C:\\Users\\USER\\Desktop\\tables\\" + tableName2 + ".ser";
        extension = ".ser";
        columnsName = new ArrayList<>();
        columnsTypes = new ArrayList<>();
        columnsValues = new ArrayList<>();
        columnsValues1 = new ArrayList<>();
        columnsValues2 = new ArrayList<>();
        columnsValues3 = new ArrayList<>();
        columnsValues4 = new ArrayList<>();
        protocolWriter = new fileHandler.protocolBuffer.ProtocolWriter();
        protocolWriter1 = new fileHandler.protocolBuffer.ProtocolWriter();
        protocolWriter2 = new ProtocolWriter();
        protocolReader = new ProtocolReader();
        
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
        
        columnsValues1.add("Salma");
        columnsValues1.add("21");
        columnsValues1.add("700000");
        columnsValues1.add("15-3-1995");
        
        columnsValues2.add("Soha");
        columnsValues2.add(null);
        columnsValues2.add("50000000");
        columnsValues2.add("5-12-1997");
        
        columnsValues3.add("Mohammed");
        columnsValues3.add("7");
        columnsValues3.add("800000");
        columnsValues3.add(null);
        
        columnsValues4.add("null");
        columnsValues4.add("22");
        columnsValues4.add("40");
        columnsValues4.add("16-12-2016");
        
        initialize();
        atestCreateTableIdentifier();
        bwriteTable();
        cwriteTable1();
        dwriteTable2();
    }
    
    public void initialize() {
        try {
            protocolWriter.initializeWriter(tableName, tablePath);
            protocolWriter1.initializeWriter(tableName, tablePath1);
            protocolWriter2.initializeWriter(tableName, tablePath2);
        } catch (Exception e) {
            
        }
    }
    
    public void atestCreateTableIdentifier() {
        
        try {
            protocolWriter.createTableIdentifier(tableName, columnsName, columnsTypes);
            protocolWriter1.createTableIdentifier(tableName, columnsName, columnsTypes);
            protocolWriter2.createTableIdentifier(tableName, columnsName, columnsTypes);
            
        } catch (Exception e) {
            fail("Expected no errors while adding table identifier");
            
        }
        
    }
    
    public void bwriteTable() {
        try {
            //writing with index zero
            protocolWriter.setIndex(0);
            protocolWriter.writeRow(columnsName, columnsValues);
            protocolWriter.setIndex(1);
            protocolWriter.writeRow(columnsName, columnsValues1);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Expected no errors while adding table values");
            
        }
    }
    
    public void cwriteTable1() {
        try {
            //writing with index zero
            protocolWriter1.setIndex(0);
            protocolWriter1.writeRow(columnsName, columnsValues2);
            protocolWriter1.setIndex(1);
            protocolWriter1.writeRow(columnsName, columnsValues3);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Expected no errors while adding table values");
            
        }
    }
    
    public void dwriteTable2() {
        try {
            //writing with index zero
            protocolWriter2.writeRow(columnsName, columnsValues4);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Expected no errors while adding table values");
            
        }
    }
    
    @Test
    public void etestReadIdentifiers() {
        try {
            ArrayList<ArrayList<String>> row;
            protocolReader.initializeReader(tablePath);
            protocolReader.fastForward("TableIdentifier");
            row = protocolReader.readRow("TableIdentifier");
            System.out.println(row);
            System.out.println("-----------------------------");
        } catch (Exception e) {
            fail("No Exception was expected!");
        }
        
    }
    
    @Test
    public void ftestReadTableWithFullValues() {
        try {
            ArrayList<ArrayList<String>> row;
            protocolReader.initializeReader(tablePath);
            protocolReader.fastForward("Row");
            row = protocolReader.readRow("Row");
            System.out.println(row);
            System.out.println("-----------------------------");
            
            protocolReader.fastForward("Row");
            row = protocolReader.readRow("Row");
            System.out.println(row);
            System.out.println("-----------------------------");
            
            protocolReader.fastForward("Row");
            row = protocolReader.readRow("Row");
            assertEquals(0, row.size());
            
        } catch (Exception e) {
            fail("No expected exception!");
        }
    }
    
    @Test
    public void gtestReadTableMissingSomeValues() {
        try {
            ArrayList<ArrayList<String>> row;
            protocolReader.initializeReader(tablePath1);
            protocolReader.fastForward("Row");
            row = protocolReader.readRow("Row");
            System.out.println(row);
            System.out.println("-----------------------------");
            
            protocolReader.fastForward("Row");
            row = protocolReader.readRow("Row");
            System.out.println(row);
            System.out.println("-----------------------------");
        } catch (Exception e) {
            
        }
    }
    
    @Test
    public void htestReadTableWithNullValueEntered() {
        try {
            ArrayList<ArrayList<String>> row;
            protocolReader.initializeReader(tablePath2);
            protocolReader.fastForward("Row");
            row = protocolReader.readRow("Row");
            System.out.println(row);
            System.out.println("-----------------------------");
        } catch (Exception e) {
            
        }
    }
    
}
