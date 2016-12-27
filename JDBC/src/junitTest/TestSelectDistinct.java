package junitTest;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import adapter.Adapter;
import dbms.DBMS;

public class TestSelectDistinct {
    Adapter adapter;
    
    public TestSelectDistinct() {
        adapter = new Adapter();
    }
    
    @org.junit.Test
    public void testSelectFromTable() throws Exception {
        DBMS dbms = new DBMS();
        ArrayList<String> columnNames = new ArrayList<String>();
        ArrayList<String> columnTypes = new ArrayList<String>();
        ArrayList<String> columnValues = new ArrayList<String>();
        
        try {
            dbms.createDatabase("testSelectDistinct");
        } catch (Exception e) {
            dbms.dropDatabase("testSelectDistinct");
            dbms.createDatabase("testSelectDistinct");
        }
        
        columnNames.add("Salary");
        columnNames.add("Id");
        columnNames.add("Gender");
        
        columnTypes.add("float");
        columnTypes.add("int");
        columnTypes.add("varchar");
        
        columnValues.add("1.22");
        columnValues.add("12554654");
        columnValues.add("\'FEMALE\'");
        
        dbms.useDatabase("testSelectDistinct");
        dbms.createTable("SelectTable", columnNames, columnTypes);
        dbms.insertIntoTable("SelectTable", columnNames, columnValues);
        dbms.insertIntoTable("SelectTable", columnNames, columnValues);
        dbms.insertIntoTable("SelectTable", columnNames, columnValues);
        
        assertEquals("Error Wrong Selected Values ", selectedHashmapAfterDistinct(),
                dbms.selectDistinctFromTable("SelectTable", null, null, adapter));
        
    }
    
    private LinkedHashMap<String, ArrayList<String>> selectedHashmapAfterDistinct() {
        LinkedHashMap<String, ArrayList<String>> map = new LinkedHashMap<>();
        ArrayList<String> arr1 = new ArrayList<>();
        arr1.add("1.22");
        ArrayList<String> arr2 = new ArrayList<>();
        arr2.add("12554654");
        ArrayList<String> arr3 = new ArrayList<>();
        arr3.add("FEMALE");
        
        map.put("Salary", arr1);
        map.put("Id", arr2);
        map.put("Gender", arr3);
        return map;
        
    }
}
