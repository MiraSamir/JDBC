package junitTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import adapter.Adapter;
import dbms.DBMS;
import dbms.DatabaseException;

public class TestUpdateDatabase {
    @org.junit.Test
    public void updateDatabase() {
        DBMS dbms = new DBMS();
        ArrayList<String> columnNames = new ArrayList<String>();
        columnNames.add("Name");
        columnNames.add("age");
        columnNames.add("gender");
        
        ArrayList<String> columnTypes = new ArrayList<String>();
        columnTypes.add("varchar");
        columnTypes.add("int");
        columnTypes.add("varchar");
        
        ArrayList<String> rowValues1 = new ArrayList<String>();
        rowValues1.add("\"Bassent\"");
        rowValues1.add("21");
        rowValues1.add("\"female\"");
        
        ArrayList<String> rowValues2 = new ArrayList<String>();
        rowValues2.add("\"Mira\"");
        rowValues2.add("20");
        rowValues2.add("\"female\"");
        
        ArrayList<String> rowValues3 = new ArrayList<String>();
        rowValues3.add("\"Aya\"");
        rowValues3.add("20");
        rowValues3.add("\"female\"");
        
        ArrayList<String> rowValues4 = new ArrayList<String>();
        rowValues4.add("\"Salma\"");
        rowValues4.add("21");
        rowValues4.add("\"female\"");
        try {
            dbms.createDatabase("databaseForDropping");
            
        } catch (Exception e) {
            
            fail("ERROR! creating database!");
        }
        try {
            dbms.useDatabase("databaseForDropping");
            
            try {
                dbms.createTable("Table", columnNames, columnTypes);
            } catch (Exception e) {
                fail("ERROR! creating table!");
            }
            
            try {
                dbms.useDatabase("databaseForDropping");
                dbms.insertIntoTable("Table", columnNames, rowValues1);
                dbms.useDatabase("databaseForDropping");
                dbms.insertIntoTable("Table", columnNames, rowValues2);
                dbms.useDatabase("databaseForDropping");
                dbms.insertIntoTable("Table", columnNames, rowValues3);
                dbms.useDatabase("databaseForDropping");
                dbms.insertIntoTable("Table", columnNames, rowValues4);
            } catch (Exception e) {
                fail("Error! while inserting into Table");
            }
        } catch (DatabaseException e1) {
            fail("error using database!");
        } catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
        ArrayList<String> incorrectValue = new ArrayList<String>();
        incorrectValue.add("Medhat");
        incorrectValue.add("male");
        incorrectValue.add("011100002");
        
        ArrayList<String> column = new ArrayList<String>();
        column.add("Name");
        column.add("gender");
        column.add("telephone Number");
        
        String[] where = new String[3];
        where[0] = "age";
        where[1] = "<";
        where[2] = "21";
        
        try {
            dbms.useDatabase("databaseForDropping");
            dbms.updateTable("Table", column, incorrectValue, where);
            fail("ERROR! Accepting wrong column entries!");
        } catch (Exception e) {
            
        }
        column.clear();
        column.add("gender");
        incorrectValue.clear();
        incorrectValue.add("\"male\"");
        try {
            dbms.useDatabase("databaseForDropping");
            dbms.updateTable("lol", column, incorrectValue, where);
            fail("ERROR! Accepting wrong Table name entry!");
        } catch (Exception e) {
            
        }
        try {
            dbms.useDatabase("databaseForDropping");
            dbms.updateTable("Table", column, incorrectValue, where);
            
        } catch (Exception e) {
            fail("ERROR! While updating table");
            
        }
        LinkedHashMap<String, ArrayList<String>> linkedHashMap = new LinkedHashMap<String, ArrayList<String>>();
        try {
            dbms.useDatabase("databaseForDropping");
            Adapter adapter = new Adapter();
            linkedHashMap = dbms.selectFromTable("Table", columnNames, null, adapter);
        } catch (Exception e) {
            fail("Error! while selecting from table");
        }
        assertEquals("Updating table", "female", linkedHashMap.get("gender").get(0));
        assertEquals("Updating table", "male", linkedHashMap.get("gender").get(1));
        assertEquals("Updating table", "male", linkedHashMap.get("gender").get(2));
        assertEquals("Updating table", "female", linkedHashMap.get("gender").get(3));
        
        try {
            dbms.dropDatabase("databaseForDropping");
        } catch (Exception e) {
            fail("ERROR IN DROPPING DATABASE!");
        }
        
    }
    
}
