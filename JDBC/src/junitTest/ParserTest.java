package junitTest;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

public class ParserTest {
    
    private String query;
    private Class<?> parserClass;
    private Object test;
    private Class<?> argClasses;
    private Method method;
    private Field field;
    
    private ArrayList<String> returnedQuery;
    private ArrayList<String> returnedColumnNames;
    private ArrayList<String> returnedColumnValues;
    private ArrayList<String> returnedColumnTypes;
    private ArrayList<String> returnedWhere;
    private ArrayList<String> expectedQuery;
    private ArrayList<String> expectedWhere;
    private ArrayList<String> expectedColumnTypes;
    private ArrayList<String> expectedColumnNames;
    private ArrayList<String> expectedColumnValues;
    private String expectedTableName;
    private String returnedTableName;
    
    @Before
    public void setTest() throws Exception {

        expectedQuery = new ArrayList<String>();
        returnedQuery = new ArrayList<String>();
        expectedColumnNames = new ArrayList<String>();
        returnedColumnNames = new ArrayList<String>();
        expectedColumnValues = new ArrayList<String>();
        returnedColumnValues = new ArrayList<String>();
        expectedColumnTypes = new ArrayList<String>();
        returnedColumnTypes = new ArrayList<String>();
        expectedWhere = new ArrayList<String>();
        returnedWhere = new ArrayList<String>();
        parserClass = Class.forName("parser.Parser");
        test = parserClass.newInstance();
        argClasses = String.class;
        
    }
    
    @SuppressWarnings("unchecked")
    public void testWordsList() throws Exception {
        method = parserClass.getDeclaredMethod("split", argClasses);
        method.setAccessible(true);
        method.invoke(test, query);
        field = parserClass.getDeclaredField("words");
        field.setAccessible(true);
        returnedQuery = (ArrayList<String>) field.get(test);
        assertEquals(expectedQuery, returnedQuery);
    }
    
    public void tableName() throws Exception {
        method = parserClass.getDeclaredMethod("parse", argClasses);
        method.setAccessible(true);
        method.invoke(test, query);
        field = parserClass.getDeclaredField("tableName");
        field.setAccessible(true);
        returnedTableName = (String) field.get(test);
        assertEquals(expectedTableName, returnedTableName);
    }
    
    @SuppressWarnings("unchecked")
    public void ColumnNames() throws Exception {
        method = parserClass.getDeclaredMethod("parse", argClasses);
        method.setAccessible(true);
        method.invoke(test, query);
        field = parserClass.getDeclaredField("columns");
        field.setAccessible(true);
        returnedColumnNames = (ArrayList<String>) field.get(test);
        assertEquals(expectedColumnNames, returnedColumnNames);
    }
    
    @SuppressWarnings("unchecked")
    public void ColumnTypes() throws Exception {
        method = parserClass.getDeclaredMethod("parse", argClasses);
        method.setAccessible(true);
        method.invoke(test, query);
        field = parserClass.getDeclaredField("types");
        field.setAccessible(true);
        returnedColumnTypes = (ArrayList<String>) field.get(test);
        assertEquals(expectedColumnTypes, returnedColumnTypes);
    }
    
    @SuppressWarnings("unchecked")
    public void ColumnValues() throws Exception {
        method = parserClass.getDeclaredMethod("parse", argClasses);
        method.setAccessible(true);
        method.invoke(test, query);
        field = parserClass.getDeclaredField("values");
        field.setAccessible(true);
        returnedColumnValues = (ArrayList<String>) field.get(test);
        assertEquals(expectedColumnValues, returnedColumnValues);
    }
    
    @SuppressWarnings("unchecked")
    public void where() throws Exception {
        method = parserClass.getDeclaredMethod("parse", argClasses);
        method.setAccessible(true);
        method.invoke(test, query);
        field = parserClass.getDeclaredField("where");
        field.setAccessible(true);
        returnedWhere = (ArrayList<String>) field.get(test);
        assertEquals(expectedWhere, returnedWhere);
    }
    
    @Test
    public void testSplitCorretQuery() throws Exception {
        query = "Create table t1;";
        expectedQuery.add("Create");
        expectedQuery.add("table");
        expectedQuery.add("t1");
        expectedQuery.add(";");
        testWordsList();
        
        query = "use database d;";
        expectedQuery = new ArrayList<String>();
        expectedQuery.add("use");
        expectedQuery.add("database");
        expectedQuery.add("d");
        expectedQuery.add(";");
        testWordsList();
        
        query = "insert into t1 values (name , 25 , 2.3 , 22-3-6);";
        expectedQuery = new ArrayList<String>();
        expectedQuery.add("insert");
        expectedQuery.add("into");
        expectedQuery.add("t1");
        expectedQuery.add("values");
        expectedQuery.add("(");
        expectedQuery.add("name");
        expectedQuery.add(",");
        expectedQuery.add("25");
        expectedQuery.add(",");
        expectedQuery.add("2.3");
        expectedQuery.add(",");
        expectedQuery.add("22-3-6");
        expectedQuery.add(")");
        expectedQuery.add(";");
        testWordsList();
        
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void TestAlterAdd() throws Exception {
        query = "Alter table t1 ADD ( name Varchar , age int , birth date );";
        expectedColumnNames.add("name");
        expectedColumnNames.add("age");
        expectedColumnNames.add("birth");
        expectedColumnTypes.add("Varchar");
        expectedColumnTypes.add("int");
        expectedColumnTypes.add("date");
        try {
            ColumnNames();
            test = parserClass.newInstance();
            ColumnTypes();
        } catch (Exception e) {
            if (e instanceof dbms.DBMSException || e instanceof dbms.DatabaseException) {
                returnedColumnTypes = (ArrayList<String>) field.get(test);
                assertEquals(expectedColumnTypes, returnedColumnTypes);
                
            }
        }
        
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testInsert() throws Exception {
        query = "insert into t1 (c1 , c2 ) values (v1 , v2 );";
        expectedColumnNames.add("c1");
        expectedColumnNames.add("c2");
        expectedColumnValues.add("v1");
        expectedColumnValues.add("v2");
        try {
            ColumnNames();
            test = parserClass.newInstance();
            ColumnValues();
        } catch (Exception e) {
            if (e instanceof dbms.DBMSException || e instanceof dbms.DatabaseException) {
                returnedColumnValues = (ArrayList<String>) field.get(test);
                assertEquals(expectedColumnValues, returnedColumnValues);
                returnedColumnNames = (ArrayList<String>) field.get(test);
                assertEquals(expectedColumnNames, returnedColumnNames);
                
            }
        }
        
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testInsert2() throws Exception {
        
        query = "insert into t values (name , 25 , 2.3 , 22-3-6);";
        expectedColumnValues.add("name");
        expectedColumnValues.add("25");
        expectedColumnValues.add("2.3");
        expectedColumnValues.add("22-3-6");
        try {
            ColumnValues();
        } catch (Exception e) {
            if (e instanceof dbms.DBMSException || e instanceof dbms.DatabaseException) {
                returnedColumnValues = (ArrayList<String>) field.get(test);
                assertEquals(expectedColumnValues, returnedColumnValues);
                
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testSelect() throws Exception {
        query = "Select distinct from t1 (name , age , size) where age > 5 ; ";
        expectedColumnNames.add("name");
        expectedColumnNames.add("age");
        expectedColumnNames.add("size");
        expectedWhere.add("age");
        expectedWhere.add(">");
        expectedWhere.add("5");
        
        try {
            ColumnNames();
            test = parserClass.newInstance();
            where();
        } catch (Exception e) {
            if (e instanceof dbms.DBMSException || e instanceof dbms.DatabaseException) {
                returnedColumnNames = (ArrayList<String>) field.get(test);
                assertEquals(expectedColumnNames, returnedColumnNames);
                returnedWhere = (ArrayList<String>) field.get(test);
                assertEquals(expectedWhere, returnedWhere);
                
            }
        }
        
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testSelectNoWhere() throws Exception {
        query = "Select distinct from t1 name , age ; ";
        expectedColumnNames.add("name");
        expectedColumnNames.add("age");
        
        try {
            ColumnNames();
            test = parserClass.newInstance();
            where();
        } catch (Exception e) {
            if (e instanceof dbms.DBMSException || e instanceof dbms.DatabaseException) {
                returnedColumnNames = (ArrayList<String>) field.get(test);
                assertEquals(expectedColumnNames, returnedColumnNames);
                returnedWhere = (ArrayList<String>) field.get(test);
                assertEquals(expectedWhere, returnedWhere);
                
            }
        }
        
    }
}
