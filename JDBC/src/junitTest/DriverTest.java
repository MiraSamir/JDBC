package junitTest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import eg.edu.alexu.csd.oop.jdbc.Driver;

public class DriverTest {
    
    Driver driver;
    Properties info;
    File dbDir;
    String tmp;
    Connection connection;
    
    @Before
    public void setTest() {
        driver = new Driver();
        info = new Properties();
        tmp = System.getProperty("java.io.tmpdir");
        dbDir = new File(tmp + "/jdbc/" + Math.round((((float) Math.random()) * 100000)));
        info.put("path", dbDir.getAbsoluteFile());
    }
    
    @Test
    public void testAcceptsURL() throws SQLException {
        String url1 = "jdbc:xmldb://localhost";
        String url2 = "jdbc:jsondb://localhost";
        String url3 = "jdbc:jsondb:/localhost"; //wrong
        String url4 = "jdbc:jsondb:AA//localhost"; //wrong
        String url5 = "jdbc:jsondb://localhost...."; //wrong
        String url6 = "jdbcjsondb://localhost...."; //wrong
        String url7 = "jdbc:jsondb://locAalhost...."; //wrong
        String url8 = "j:jsondb://localhost...."; //wrong
        String url9 = "jdbc:notSupported://localhost"; // wrong url
        assertTrue(driver.acceptsURL(url1));
        assertTrue(driver.acceptsURL(url2));
        assertFalse(driver.acceptsURL(url3));
        assertFalse(driver.acceptsURL(url4));
        assertFalse(driver.acceptsURL(url5));
        assertFalse(driver.acceptsURL(url6));
        assertFalse(driver.acceptsURL(url7));
        assertFalse(driver.acceptsURL(url8));
        assertFalse(driver.acceptsURL(url9));
    }
    
    @Test
    public void testConnect() throws SQLException {
        
        connection = driver.connect("jdbc:" + "xmldb" + "://localhost", info);
        
    }
    
    @Test
    public void testGetPropertyInfo() {
    }
    
}
