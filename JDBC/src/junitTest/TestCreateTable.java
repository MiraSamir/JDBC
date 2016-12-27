package junitTest;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import dbms.DBMS;
import dbms.DatabaseException;

public class TestCreateTable {
    
    @org.junit.Test
    public void testCreateTable() throws Exception {
        DBMS dbms = new DBMS();
        ArrayList<String> columnNames = new ArrayList<String>();
        ArrayList<String> columnTypes = new ArrayList<String>();
        
        try {
            dbms.createDatabase("databaseOne");
        } catch (Exception d) {
            dbms.dropDatabase("databaseOne");
            dbms.createDatabase("databaseOne");
        }
        dbms.useDatabase("databaseOne");
        
        columnNames.add("Names");
        columnNames.add("Age");
        
        columnTypes.add("varchar");
        columnTypes.add("int");
        columnTypes.add("int");
        
        // Test entering unequal arraylists
        
        try {
            dbms.createTable("tableOne", columnNames, columnTypes);
            fail("Expected Database Exception!");
        } catch (Exception e) {
            assertTrue(e instanceof DatabaseException);
        }
        
        columnNames.add("Height");
        
        // entering right values
        try {
            dbms.createTable("TableTwo", columnNames, columnTypes);
        } catch (Exception e) {
            fail("Expected Database Exception!");
        }
        
        // naming the table "null"
        try {
            dbms.createTable("null", columnNames, columnTypes);
        } catch (Exception e) {
            fail("Expected Database Exception!");
        }
        
        columnNames = new ArrayList<String>();
        
        //inserting a null columnNames array
        
        try {
            dbms.createTable("TableThree", columnNames, columnTypes);
        } catch (Exception e) {
            assertTrue(e instanceof DatabaseException);
        }
    }
}
