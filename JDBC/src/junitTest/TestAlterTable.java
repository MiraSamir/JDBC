package junitTest;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.junit.Test;

import adapter.Adapter;
import dbms.DBMS;

public class TestAlterTable {
    Adapter adapter;
    
    public TestAlterTable() {
        adapter = new Adapter();
    }
    
    @Test
    public void testAlterTable() throws Exception {
        ArrayList<String> columnNames = new ArrayList<String>();
        ArrayList<String> columnTypes = new ArrayList<String>();
        ArrayList<String> columnValues = new ArrayList<String>();
        ArrayList<String> newColumnsNames = new ArrayList<String>();
        ArrayList<String> newColumnsTypes = new ArrayList<String>();
        DBMS dbms = new DBMS();
        try {
            dbms.createDatabase("testAlter");
        } catch (Exception e) {
            dbms.dropDatabase("testAlter");
            dbms.createDatabase("testAlter");
        }
        dbms.useDatabase("testSelect");
        
        columnNames.add("Salary");
        columnNames.add("Id");
        columnNames.add("Gender");
        
        columnTypes.add("float");
        columnTypes.add("int");
        columnTypes.add("varchar");
        
        columnValues.add("1.22");
        columnValues.add("12554654");
        columnValues.add("\'FEMALE\'");
        
        newColumnsNames.add("BIRTH");
        newColumnsNames.add("PAssword");
        
        newColumnsTypes.add("date");
        newColumnsTypes.add("varchar");
        try {
            dbms.createTable("AlterTable", columnNames, columnTypes);
        } catch (Exception e) {
            dbms.dropTable("AlterTable");
            dbms.createTable("AlterTable", columnNames, columnTypes);
        }
        dbms.insertIntoTable("AlterTable", columnNames, columnValues);
        dbms.alterAdd("AlterTable", newColumnsNames, newColumnsTypes);
        
        assertEquals("Error Wrong Selected Values ", selectedHashmapAfterAlterAdd(),
                dbms.selectFromTable("AlterTable", null, null, adapter));
        
        dbms.alterDrop("AlterTable", columnNames);
        
        assertEquals("Error Wrong Selected Values ", selectedHashmapAfterAlterDrop(),
                dbms.selectFromTable("AlterTable", null, null, adapter));
        
    }
    
    private LinkedHashMap<String, ArrayList<String>> selectedHashmapAfterAlterAdd() {
        LinkedHashMap<String, ArrayList<String>> map = new LinkedHashMap<>();
        ArrayList<String> arr1 = new ArrayList<>();
        arr1.add("1.22");
        ArrayList<String> arr2 = new ArrayList<>();
        arr2.add("12554654");
        ArrayList<String> arr3 = new ArrayList<>();
        arr3.add("FEMALE");
        ArrayList<String> arr4 = new ArrayList<>();
        arr4.add("-");
        ArrayList<String> arr5 = new ArrayList<>();
        arr5.add("-");
        map.put("Salary", arr1);
        map.put("Id", arr2);
        map.put("Gender", arr3);
        map.put("BIRTH", arr4);
        map.put("PAssword", arr5);
        return map;
        
    }
    
    private LinkedHashMap<String, ArrayList<String>> selectedHashmapAfterAlterDrop() {
        LinkedHashMap<String, ArrayList<String>> map = new LinkedHashMap<>();
        
        ArrayList<String> arr4 = new ArrayList<>();
        arr4.add("-");
        ArrayList<String> arr5 = new ArrayList<>();
        arr5.add("-");
        map.put("BIRTH", arr4);
        map.put("PAssword", arr5);
        return map;
        
    }
}
