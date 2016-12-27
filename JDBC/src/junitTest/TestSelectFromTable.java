package junitTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import adapter.Adapter;
import dbms.DBMS;
import dbms.DatabaseException;

public class TestSelectFromTable {
    Adapter adapter;
    
    public TestSelectFromTable() {
        adapter = new Adapter();
    }
    
    @org.junit.Test
    public void testSelectFromTable() throws Exception {
        DBMS dbms = new DBMS();
        ArrayList<String> columnNames = new ArrayList<String>();
        ArrayList<String> columnTypes = new ArrayList<String>();
        ArrayList<String> selectedWrongColumnNames = new ArrayList<String>();
        ArrayList<String> selectedColumnNames = new ArrayList<String>();
        ArrayList<String> values = new ArrayList<String>();
        String[] where = new String[3];
        
        try {
            dbms.createDatabase("testSelect");
        } catch (Exception e) {
            dbms.dropDatabase("testSelect");
            dbms.createDatabase("testSelect");
        }
        
        columnNames.add("Password");
        columnNames.add("Id");
        columnNames.add("Gender");
        
        columnTypes.add("int");
        columnTypes.add("int");
        columnTypes.add("varchar");
        
        values.add("12");
        values.add("1233");
        values.add("\"female\"");
        selectedWrongColumnNames.add("name");
        
        dbms.useDatabase("testSelect");
        dbms.createTable("SelectTable", columnNames, columnTypes);
        
        // test select columnNames donot exist
        try {
            dbms.selectFromTable("SelectTable", selectedWrongColumnNames, null, adapter);
            fail("Expected db exception");
            
        } catch (Exception e) {
            assertTrue("Expected db exception", e instanceof DatabaseException);
        }
        
        // test select all
        LinkedHashMap<String, ArrayList<String>> map = new LinkedHashMap<String, ArrayList<String>>();
        ArrayList<String> value1 = new ArrayList<String>();
        value1.add("12");
        ArrayList<String> value2 = new ArrayList<String>();
        value2.add("1233");
        ArrayList<String> value3 = new ArrayList<String>();
        value3.add("female");
        map.put("Password", value1);
        map.put("Id", value2);
        map.put("Gender", value3);
        dbms.insertIntoTable("SelectTable", columnNames, values);
        assertEquals("Selection Error ", map, dbms.selectFromTable("SelectTable", null, null, adapter));
        
        // test select specific column with where condition
        LinkedHashMap<String, ArrayList<String>> map1 = new LinkedHashMap<String, ArrayList<String>>();
        ArrayList<String> value4 = new ArrayList<String>();
        value4.add("1233");
        map1.put("Id", value4);
        
        where[0] = "Password";
        where[1] = "=";
        where[2] = "12";
        selectedColumnNames.add("Id");
        assertEquals("Selection Error ", map1,
                dbms.selectFromTable("SelectTable", selectedColumnNames, where, adapter));
        
    }
}
