package dbms;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import adapter.Adapter;

public class DBMS implements IDBMS {
    private Database database;
    private String databasesPath;
    private File databasesFile;
    private String type;
    private FileHandlerType fileHandlerType;
    private String usedDatabaseName;
    
    public void setUsedDatabaseName(String usedDatabaseName) {
        this.usedDatabaseName = usedDatabaseName;
    }
    
    public String getUsedDatabaseName() {
        return usedDatabaseName;
    }
    
    public DBMS(File file, String type) throws DatabaseException { //constructor 1
        databasesPath = file.getAbsolutePath();
        String constructorFolder = databasesPath.substring(0, databasesPath.lastIndexOf(File.separator));
        Path directoryPath = Paths.get(constructorFolder);
        if (!Files.exists(directoryPath)) {
            File directoryFile = new File(constructorFolder);
            if (directoryFile.mkdir()) {
                
            } else {
                throw new DatabaseException("ERROR!! Cannot create directory");
            }
        }
        type = type.replace("db", "");
//        type = type.replace("alt", "json");
           type = type.replace("alt", "ser");
        this.type = type;
        Path path = Paths.get(databasesPath);
        this.fileHandlerType = new FileHandlerType();
        if (!Files.exists(path)) {
            databasesFile = new File(databasesPath);
            if (databasesFile.mkdir()) {
                
            } else {
                throw new DatabaseException("ERROR!! Cannot create directory");
            }
        }
    }
    
    public DBMS() { //constructor2
        databasesPath = System.getProperty("user.home") + File.separator + "Databases";
        this.type = "xml";
        Path path = Paths.get(databasesPath);
        this.fileHandlerType = new FileHandlerType();
        if (!Files.exists(path)) {
            databasesFile = new File(databasesPath);
            try {
                databasesFile.mkdir();
            } catch (Exception e) {
                System.out.print("ERROR!! Cannot create directory");
            }
        }
    }
    
    public String getDatabasesPath() {
        return databasesPath;
    }
    
    @Override
    public int createDatabase(String databaseName) throws Exception {
        String databaseNameLowerCase = databaseName.toLowerCase();
        String databasePath = databasesPath + File.separator + databaseNameLowerCase;
        if (!containsDatabase(databasePath)) {
            File databaseFolder = new File(databasePath);
            if (databaseFolder.mkdir()) {
                fileHandlerType.createFile(this.type, databasePath + File.separator + "type.txt");
            } else {
                throw new DBMSException("Cannot create database!");
            }
        } else {
            throw new DBMSException("Database already exists!");
        }
        return 0;
    }
    
    @Override
    public int dropDatabase(String databaseName) throws Exception {
        String databaseNameLowerCase = databaseName.toLowerCase();
        String databasePath = databasesPath + File.separator + databaseNameLowerCase;
        if (!containsDatabase(databasePath)) {
            throw new DBMSException("Database folder doesn't exist!");
        } else {
            File databaseFolder = new File(databasePath);
            deleteDir(databaseFolder);
            if (databaseFolder.exists()) {
                throw new DBMSException("Cannot delete database!");
            } else {
                System.out.println("Database " + databaseName + " is successfully deleted ! ");
            }
        }
        return 0;
    }
    
    @Override
    public int useDatabase(String databaseName) throws Exception {
        databaseName = databaseName.toLowerCase();
        usedDatabaseName = databaseName;
        String databasePathh = databasesPath + File.separator + databaseName;
        Path path = Paths.get(databasePathh);
        String fileBackendWriter = databasePathh + File.separator + "type.txt";
        if (Files.exists(path) && checkFileType(fileBackendWriter)) {
            database = new Database(databasePathh, fileBackendWriter);
        } else {
            throw new DatabaseException("You must CREATE then USE a Database!");
        }
        return 0;
    }
    
    @Override
    public int createTable(String tableName, ArrayList<String> columnNames, ArrayList<String> columnTypes)
            throws Exception {
        if (database == null) {
            throw new DBMSException("You should choose a database first!");
        } else {
            tableName = tableName.toLowerCase();
            database.createDBTable(tableName, columnNames, columnTypes);
        }
        return 0;
    }
    
    @Override
    public int dropTable(String tableName) throws Exception {
        if (database == null) {
            throw new DBMSException("You should choose a database first!");
        } else {
            tableName = tableName.toLowerCase();
            database.dropTable(tableName);
        }
        return 0;
    }
    
    @Override
    public int insertIntoTable(String tableName, ArrayList<String> columnNames, ArrayList<String> columnValues)
            throws Exception {
        if (database == null) {
            throw new DBMSException("You should choose a database first!");
        } else {
            tableName = tableName.toLowerCase();
            return database.insertIntoTable(tableName, columnNames, columnValues);
        }
        
    }
    
    @Override
    public int deleteFromTable(String tableName, String[] where) throws Exception {
        if (database == null) {
            throw new DBMSException("You should choose a database first!");
        } else {
            tableName = tableName.toLowerCase();
            return database.deleteFromDBTable(tableName, where);
        }
        
    }
    
    @Override
    public int updateTable(String tableName, ArrayList<String> columns, ArrayList<String> values, String[] where)
            throws Exception {
        if (database == null) {
            throw new DBMSException("You should choose a database first!");
        } else {
            tableName = tableName.toLowerCase();
            return database.updateTable(tableName, columns, values, where);
        }
        
    }
    
    @Override
    public LinkedHashMap<String, ArrayList<String>> selectFromTable(String tableName, ArrayList<String> columnNames,
            String[] where, Adapter adapter) throws Exception {
        if (database == null) {
            throw new DBMSException("You should choose a database first!");
        } else {
            tableName = tableName.toLowerCase();
            return database.selectFromTable(tableName, columnNames, where, adapter);
        }
        
    }
    
    @Override
    public LinkedHashMap<String, ArrayList<String>> selectDistinctFromTable(String tableName,
            ArrayList<String> columnNames, String[] where, Adapter adapter) throws Exception {
        if (database == null) {
            throw new DBMSException("You should choose a database first!");
        } else {
            tableName = tableName.toLowerCase();
            return database.selectDistinctFromTable(tableName, columnNames, where, adapter);
        }
        
    }
    
    @Override
    public int alterAdd(String tableName, ArrayList<String> columnNames, ArrayList<String> types) throws Exception {
        if (database == null) {
            throw new DBMSException("You should choose a database first!");
        } else {
            tableName = tableName.toLowerCase();
            return database.alterAdd(tableName, columnNames, types);
        }
        
    }
    
    @Override
    public int alterDrop(String tableName, ArrayList<String> columnNames) throws Exception {
        
        if (database == null) {
            throw new DBMSException("You should choose a database first!");
        } else {
            tableName = tableName.toLowerCase();
            return database.alterDrop(tableName, columnNames);
        }
    }
    
    private boolean containsDatabase(String databasePath) {
        Path path = Paths.get(databasePath);
        if (Files.exists(path)) {
            return true;
        }
        return false;
    }
    
    private void deleteDir(File file) {
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                deleteDir(f);
            }
        }
        file.delete();
    }
    
    private boolean checkFileType(String path) throws Exception {
        String originalType = fileHandlerType.readFile(path);
        if (originalType.equals(this.type)) {
            return true;
        }
        throw new DBMSException("ERROR !! Database already exists with diffrent backend !!");
        
    }
    
}
