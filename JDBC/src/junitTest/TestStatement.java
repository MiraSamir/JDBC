package junitTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import connection.Connection;

public class TestStatement {
    
    File databaseDirectory;
    
    private Statement statement;
    private Connection connection;
    private eg.edu.alexu.csd.oop.jdbc.Driver driver;
    private Properties info;
    
    private String protocol = "xmldb";
    private String temp;
    private String databaseName;
    
    @Before
    public void setVariables() throws Exception {
        temp = System.getProperty("java.io.tmpdir");
        driver = new eg.edu.alexu.csd.oop.jdbc.Driver();
        databaseName = "DatabaseOne";
        info = new Properties();
        databaseDirectory = new File(temp + "/jdbc/" + Math.round((((float) Math.random()) * 100000)));
        info.put("path", databaseDirectory.getAbsoluteFile());
        connection = driver.connect("jdbc:" + protocol + "://localhost", info);
        
        statement = connection.createStatement();
        statement.execute("CREATE DATABASE " + databaseName);
        statement.execute("USE " + databaseName);
        statement.close();
        
    }
    
    @Test
    public void testCreateTables() {
        try {
            statement.Statement statement1 = connection.createStatement();
            statement1.execute("create TAblE tableOne" + "(Name varchar, Age int, Birthdate date)");
            statement1.execute("CREATE TABLE tableTwo" + "(FatherName varchar, FatherAge int, FatherBirthdate date)");
            statement1.execute("CREATE TABLE tableThree" + "(MotherName varchar, MotherAge int, MotherBirthdate date)");
            statement1.close();
        } catch (Exception e) {
            fail("No exception!");
        }
    }
    
    @Test
    public void testCreateTablesWithSameName() {
        try {
            statement.Statement statement1 = connection.createStatement();
            
            statement1.execute("create TAblE tableOne" + "(Name varchar, Age int, Birthdate date)");
            statement1.close();
            
            statement1 = connection.createStatement();
            statement1.execute("CREATE TABLE tableTwo" + "(FatherName varchar, FatherAge int, FatherBirthdate date)");
            statement1.close();
            
            statement1 = connection.createStatement();
            statement1.execute("CREATE TABLE tableOne" + "(MotherName varchar, MotherAge int, MotherBirthdate date)");
            statement1.close();
        } catch (SQLException s) {
            
        } catch (Exception e) {
            fail("Wrong type of exception!");
        }
    }
    
    @Test
    public void testInsertIntoTableDifferentLetterCase() {
        try {
            statement.Statement statement3 = connection.createStatement();
            statement3.execute("CREATE TABLE tableOne" + "(Name varchar, Age int, Weight float)");
            
            int result1 = statement3.executeUpdate("InSeRT inTO tAbLeoNe vALuEs ('name1', 1, 58.3)");
            assertEquals(1, result1);
        } catch (Exception e) {
            fail();
        }
    }
    
    @Test
    public void testInsertIntoTableWrongColumnNames() {
        try {
            statement.Statement statement3 = connection.createStatement();
            statement3.execute("CREATE TABLE tableOne" + "(Name varchar, Age int, Weight float)");
            
            statement3.executeUpdate("INSERT INTO tableOne (notName, age, weight)" + "VALUES ('name1', 1, 58.3)");
            
        } catch (SQLException s) {
            
        } catch (Exception e) {
            fail();
        }
    }
    
    @Test
    public void testUpdateTable() {
        try {
            statement.Statement statement3 = connection.createStatement();
            statement3.execute("CREATE TABLE tableOne" + "(Name varchar, Age int, Weight float)");
            
            statement3.execute("INSERT INTO tableOne (Name, age, weight)" + "VALUES ('name1', 1, 58.3)");
            statement3.execute("INSERT INTO tableOne (Name, age, weight)" + "VALUES ('name2', 2, 68.3)");
            statement3.execute("INSERT INTO tableOne (Name, age, weight)" + "VALUES ('name3', 3, 78.3)");
            
            statement3.execute("UPDATE tableOne SET name='name4', " + "age=4, weIght=58.3");
            
        } catch (Exception e) {
            fail();
        }
    }
    
    @Test
    public void testDeleteFromTable() {
        try {
            statement.Statement statement4 = connection.createStatement();
            statement4.execute("CREATE TABLE tableOne" + "(Name varchar, Age int, Weight float)");
            
            statement4.execute("INSERT INTO tableOne (Name, age, weight)" + "VALUES ('name1', 1, 58.3)");
            statement4.execute("INSERT INTO tableOne (Name, age, weight)" + "VALUES ('name2', 2, 68.3)");
            statement4.execute("INSERT INTO tableOne (Name, age, weight)" + "VALUES ('name3', 3, 78.3)");
            
            int result3 = statement4.executeUpdate("DELETE From tableone");
            assertEquals(3, result3);
        } catch (Exception e) {
            fail();
        }
    }
    
    @Test
    public void testOnClosedStatement() {
        try {
            statement.Statement statement5 = connection.createStatement();
            statement5.execute("CREATE TABLE tableOne" + "(Name varchar, Age int, Weight float)");
            statement5.close();
            statement5.execute("INSERT INTO tableOne (Name, age, weight)" + "VALUES ('name1', 1, 58.3)");
        } catch (SQLException s) {
            
        } catch (Exception e) {
            fail("Wrong type of exception!");
        }
    }
    
    @Test
    public void testBatch() {
        try {
            statement.Statement statement6 = connection.createStatement();
            statement6.addBatch("CREATE TABLE tableOne" + "(Name varchar, Age int, Weight float)");
            statement6.addBatch("INSERT INTO tableOne (Name, age, weight)" + "VALUES ('name1', 1, 58.3)"); //0
            statement6.addBatch("INSERT INTO tableOne (Name, age, weight)" + "VALUES ('name2', 2, 68.3)"); //1
            statement6.addBatch("INSERT INTO tableOne (Name, age, weight)" + "VALUES ('name3', 3, 78.3)"); //2
            statement6.addBatch("UPDATE tableOne SET name='name4', " + "age=4, weIght=58.3"); //3
            statement6.addBatch("DELETE From tableone"); //4
            int[] results = statement6.executeBatch();
            assertEquals(0, results[0]);
            assertEquals(3, results[4]);
        } catch (SQLException s) {
            
        } catch (Exception e) {
            fail("Wrong type of exception!");
        }
    }
    
}
