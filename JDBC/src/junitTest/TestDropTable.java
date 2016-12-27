package junitTest;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import dbms.DBMS;
import dbms.DBMSException;
import dbms.DatabaseException;

public class TestDropTable {
    
    @org.junit.Test
    public void testDropTable() throws Exception {
        DBMS dbms = new DBMS();
        ArrayList<String> columnNames = new ArrayList<String>();
        ArrayList<String> columnTypes = new ArrayList<String>();
        columnNames.add("Name");
        columnNames.add("age");
        columnNames.add("gender");
        columnTypes.add("varchar");
        columnTypes.add("int");
        columnTypes.add("varchar");
        
        try {
            dbms.createDatabase("testDropTable");
        } catch (Exception e) {
            dbms.dropDatabase("testDropTable");
            dbms.createDatabase("testDropTable");
        }
        
        // test 01 not using USE
        try {
            dbms.createTable("dropTable01", columnNames, columnTypes);
            fail("expected DBMS Exception");
        } catch (Exception e) {
            assertTrue("Expected DBMS Exception", e instanceof DBMSException);
        }
        
        dbms.useDatabase("testDropTable");
        dbms.createTable("dropTable01", columnNames, columnTypes);
        dbms.dropTable("dropTable01");
        
        // test drop table already deleted
        try {
            dbms.dropTable("dropTable01");
            fail("expected Database Exception");
        } catch (Exception e) {
            assertTrue("Expected Database Exception", e instanceof DatabaseException);
        }
        
        // test drop a table doesnot exist
        
        try {
            dbms.dropTable("dropTableDoestnotExist");
            fail("expected Database Exception");
        } catch (Exception e) {
            assertTrue("Expected Database  Exception", e instanceof DatabaseException);
        }
    }
    
}
