package junitTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.junit.Test;

import adapter.Adapter;
import resultSet.ResultSetMetaDataa;
import resultSet.ResultSett;

public class TestResultSet {
    
    private ResultSett resultSet;
    private Adapter adapter;
    
    public TestResultSet() {
        adapter = initiatingResultSet();
        
        resultSet = new ResultSett(adapter, null);
    }
    
    private LinkedHashMap<String, ArrayList<String>> constructingHashMap(ArrayList<String> columnNames,
            ArrayList<ArrayList<String>> columnValues) {
        LinkedHashMap<String, ArrayList<String>> returnedHash = new LinkedHashMap<String, ArrayList<String>>();
        for (int i = 0; i < columnNames.size(); i++) {
            returnedHash.put(columnNames.get(i), columnValues.get(i));
        }
        return returnedHash;
    }
    
    public Adapter initiatingResultSet() {
        ArrayList<String> columnNames = new ArrayList<String>();
        columnNames.add("Name");
        columnNames.add("Age");
        columnNames.add("Gender");
        columnNames.add("Birth Date");
        columnNames.add("Height");
        ArrayList<String> columnTypes = new ArrayList<String>();
        columnTypes.add("varchar");
        columnTypes.add("int");
        columnTypes.add("varchar");
        columnTypes.add("date");
        columnTypes.add("float");
        ArrayList<String> column1 = new ArrayList<String>();
        column1.add("\'Bassent Mostafa\'");
        column1.add("\'Mira Samir\'");
        column1.add("\'Aya Sameh\'");
        column1.add("\'Salma Hesham\'");
        
        ArrayList<String> column2 = new ArrayList<String>();
        
        column2.add("21");
        column2.add("20");
        column2.add("20");
        column2.add("21");
        
        ArrayList<String> column3 = new ArrayList<String>();
        
        column3.add("\'Female\'");
        column3.add("\'Female\'");
        column3.add("\'Female\'");
        column3.add("\'Female\'");
        
        ArrayList<String> column4 = new ArrayList<String>();
        
        column4.add("\'26-08-1995\'");
        column4.add("\'24-02-1996\'");
        column4.add("\'13-04-1996\'");
        column4.add("\'15-03-1995\'");
        
        ArrayList<String> column5 = new ArrayList<String>();
        column5.add("2.0");
        column5.add("1.60");
        column5.add("1.0");
        column5.add("1.65");
        
        ArrayList<ArrayList<String>> columnValues = new ArrayList<ArrayList<String>>();
        columnValues.add(column1);
        columnValues.add(column2);
        columnValues.add(column3);
        columnValues.add(column4);
        columnValues.add(column5);
        Adapter adapter = new Adapter();
        adapter.setTableName("Our Team");
        adapter.setTableIdentifierTypes(columnTypes);
        adapter.setHashMaps(this.constructingHashMap(columnNames, columnValues));
        return adapter;
        
    }
    
    @Test
    public void testResultSet() {
        
        try {
            resultSet.absolute(3);
            
        } catch (Exception e) {
            fail("Invalid Exception was thrown");
        }
        try {
            String s = resultSet.getString(1);
            assertEquals("Invalid return from resultSet", "Aya Sameh", s);
            int i = resultSet.getInt(2);
            assertEquals("Invalid return from resultSet", 20, i);
            resultSet.previous();
            float f = resultSet.getFloat(5);
            assertEquals("Invalid return from resultSet", "1.6", f + "");
        } catch (Exception e) {
            fail("Invalid Exception was thrown");
        }
        try {
            resultSet.afterLast();
            boolean result = resultSet.isAfterLast();
            assertTrue("Invalid return from resultSet", result);
            
        } catch (Exception e) {
            fail("Invalid Exception was thrown");
        }
        
    }
    
    @Test
    public void testResultSetMetaData() {
        try {
            ResultSetMetaDataa rsm = resultSet.getMetaData();
            String s = rsm.getTableName(2);
            assertEquals("Invalid return from resultSetMetaData", "Our Team", s);
            int type = rsm.getColumnType(1);
            assertEquals("Invalid return from resultSetMetaData", Types.VARCHAR, type);
        } catch (Exception e) {
            fail("Invalid Exception was thrown");
        }
        
    }
}
