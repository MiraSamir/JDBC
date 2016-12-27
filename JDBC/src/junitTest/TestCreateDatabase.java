package junitTest;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import dbms.DBMS;

public class TestCreateDatabase {
    
    @org.junit.Test
    public void createDatabases() throws Exception {
        DBMS dbms = new DBMS();
        try {
            dbms.createDatabase("database1");
            dbms.createDatabase("database2");
            dbms.createDatabase("database3");
        } catch (Exception e) {
            dbms.dropDatabase("database1");
            dbms.createDatabase("database1");
            dbms.dropDatabase("database2");
            dbms.createDatabase("database2");
            dbms.dropDatabase("database3");
            dbms.createDatabase("database3");
        }
        String databasesPath = dbms.getDatabasesPath();
        Path path1 = Paths.get(databasesPath + File.separator + "database1");
        Path path2 = Paths.get(databasesPath + File.separator + "database2");
        Path path3 = Paths.get(databasesPath + File.separator + "database3");
        assertEquals("database creation failed", true, Files.exists(path1));
        assertEquals("database creation failed", true, Files.exists(path2));
        assertEquals("database creation failed", true, Files.exists(path3));
        
    }
    
}
