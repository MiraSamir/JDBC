package junitTest;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import dbms.DBMS;
import dbms.DatabaseException;

public class TestDeleteFromTable {
    
    @org.junit.Test
    public void testDeleteFromTable() throws Exception {
        DBMS dbms = new DBMS();
        ArrayList<String> columnNames = new ArrayList<String>();
        ArrayList<String> columnTypes = new ArrayList<String>();
        ArrayList<String> columnValues1 = new ArrayList<String>();
        ArrayList<String> columnValues2 = new ArrayList<String>();
        ArrayList<String> columnValues3 = new ArrayList<String>();
        String[] where = new String[3];
        
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
        
        try {
            dbms.createTable("TableFour", columnNames, columnTypes);
        } catch (Exception e) {
            dbms.dropTable("TableFour");
            dbms.createTable("TableFour", columnNames, columnTypes);
        }
        
        columnValues1.add("\"Student1\"");
        columnValues1.add("21");
        
        columnValues2.add("\"Student2\"");
        columnValues2.add("20");
        
        columnValues3.add("\"Student3\"");
        columnValues3.add("19");
        
        where[0] = "Names";
        where[1] = "=";
        where[2] = "\"Student1\"";
        
        dbms.insertIntoTable("TableFour", columnNames, columnValues1);
        dbms.insertIntoTable("TableFour", columnNames, columnValues2);
        dbms.insertIntoTable("TableFour", columnNames, columnValues3);
        
        try {
            dbms.deleteFromTable("TableFour", where);
        } catch (Exception e) {
            fail("Exception is not needed");
        }
        
        //delete from a table that doesn't exist
        try {
            dbms.deleteFromTable("NoTable", where);
        } catch (Exception e) {
            assertTrue(e instanceof DatabaseException);
        }
        
        where[0] = "Age";
        where[1] = "<";
        where[2] = "1";
        
        //less than all available integers
        try {
            dbms.deleteFromTable("TableFour", where);
        } catch (Exception e) {
            fail("No Exception needed!");
        }
    }
    
}
