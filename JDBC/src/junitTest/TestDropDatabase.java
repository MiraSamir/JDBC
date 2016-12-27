package junitTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;

import dbms.DBMS;

public class TestDropDatabase {
    
    @org.junit.Test
    public void dropDatabase() {
        DBMS dbms = new DBMS();
        try {
            dbms.createDatabase("databaseForDropping");
            
        } catch (Exception e) {
            
            fail("ERROR! creating database!");
        }
        String databaseMSPath = dbms.getDatabasesPath();
        String databasePath = databaseMSPath + File.separator + "databaseForDropping";
        
        File databaseFile = new File(databasePath);
        
        try {
            dbms.dropDatabase("databaseForDropping");
        } catch (Exception e) {
            fail("Error dropping database!");
        }
        assertEquals("Dropping Database", false, databaseFile.exists());
        
    }
    
}
