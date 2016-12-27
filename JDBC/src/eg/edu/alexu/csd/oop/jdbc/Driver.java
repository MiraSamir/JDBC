package eg.edu.alexu.csd.oop.jdbc;

import java.io.File;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Logger;

import connection.Connection;
import logger.Log4j;

public class Driver implements java.sql.Driver {
    
    private String protocol;
    static org.apache.logging.log4j.Logger logger;
    
    static {
        // register the driver:
        try {
            DriverManager.registerDriver(new Driver());
            Driver.logger = Log4j.getInstance();
        } catch (SQLException e1) {
        }
    }
    
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        
        //      supported format => jdbc:protocol_name://localhost
        boolean jdbc = false, protocol = false, localhost = false;
        //      scan jbdc:
        jdbc = containsJdbc(url);
        //      scan protocolname :
        protocol = isValidProtocol(url);
        //      scan localHost
        localhost = isLocalhost(url);
        logger.info("Checking URL:" + (jdbc && protocol && localhost));
        return jdbc && protocol && localhost;
    }
    
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        logger.info("Launching connection");
        //      define connection
        Connection connection;
        //  DriverPropertyInfo[] information;
        String path = null;
        //  String protocolString = null;
        File directory;
        //      check url
        if (!acceptsURL(url)) {
            logger.error("ERROR! Invalid URL.");
            throw new SQLException("the url used is not correct");
        }
        //      check that the path exist or create the database
        path = info.get("path").toString();
        if (path == null) {
            logger.error("ERROR ! Incomplete info file " + "Database path is not included.");
            throw new SQLException("The info file doesnot contain the database path!");
        }
        directory = new File(path);
        
        //      add properties to the connection (url , path)
        try {
            connection = new Connection(url, path, protocol, directory);
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
        //
        return connection;
    }
    
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        ArrayList<String> propertiesToGet = new ArrayList<String>();
        propertiesToGet.add("username");
        propertiesToGet.add("password");
        propertiesToGet.add("path");
        DriverPropertyInfo[] dpiArray = new DriverPropertyInfo[propertiesToGet.size()];
        for (int i = 0; i < propertiesToGet.size(); i++) {
            dpiArray[i] = new DriverPropertyInfo(propertiesToGet.get(i), info.getProperty(propertiesToGet.get(i)));
        }
        return dpiArray;
    }
    
    private boolean containsJdbc(String url) {
        //      supported format => jdbc:protocol_name://localhost
        
        String matchingWord;
        try {
            matchingWord = url.substring(0, 5);
        } catch (Exception e) {
            return false; // url shorter than jbdc:
        }
        if (!matchingWord.equals("jdbc:")) {
            return false;
        }
        
        return true;
    }
    
    private boolean isValidProtocol(String url) {
        String protocolName = "";
        int i = 5;
        Character c = url.charAt(i);
        
        try {
            while (!c.equals(':')) {
                protocolName += c;
                i++;
                c = url.charAt(i);
            }
            protocolName += c;
            if (protocolName.equals("xmldb:") || protocolName.equals("pbdb:") || protocolName.equals("jsondb:")
                    || protocolName.equals("altdb:")) {
                protocol = protocolName.substring(0, protocolName.length() - 1);
                return true;
            }
            
        } catch (Exception e) {
            return false; // url not valid:
        }
        return false;
    }
    
    private boolean isLocalhost(String url) {
        //      supported format => jdbc:protocol_name://localhost
        //                                             ^^^^^^^^^^^
        try {
            if (!url.endsWith("//localhost")) {
                return false;
            }
            return url.matches("\\w+:\\w+://localhost");
        } catch (Exception e) {
            return false;
        }
    }
    
    @Override
    public int getMajorVersion() {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public int getMinorVersion() {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public boolean jdbcCompliant() {
        // TODO Auto-generated method stub
        return false;
    }
    
}
