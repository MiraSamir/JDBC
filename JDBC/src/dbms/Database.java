package dbms;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;

import adapter.Adapter;
import dtd.DTD;
import dtd.DTDException;
import fileHandler.BackendFactory;
import fileHandler.IFileReader;
import fileHandler.IFileWriter;

public class Database implements IDatabase {
    
    private String databasePath;
    private IFileWriter fileWriter;
    private IFileReader fileReader;
    private String fileTypePath;
    private DTD dtd;
    private String fileType;
    
    public Database(String databasePath, String fileTypePath) throws Exception {
        this.databasePath = databasePath;
        this.fileTypePath = fileTypePath;
        this.fileType = getBackendType();
        BackendFactory backendFactory = new BackendFactory(this.fileType);
        this.fileWriter = backendFactory.createWriter();
        this.fileReader = backendFactory.createReader();
        this.fileWriter.DefineReader(this.fileReader);
    }
    
    @Override
    public void createDBTable(String tableName, ArrayList<String> columnNames, ArrayList<String> columnTypes)
            throws Exception {
        String tableNamePath = createTablePath(tableName);
        String tablePathDTD = this.databasePath + File.separator + tableName;
        if (containsTable(tableName) || (columnNames.size() != columnTypes.size())) {
            throw new DatabaseException("Table exists!");
        } else {
            isDuplicate(columnNames);
            fileWriter.initializeWriter(tableName, tableNamePath);
            fileWriter.createTableIdentifier(tableName, columnNames, columnTypes);
            fileWriter.endWriter(tableName);
            dtd = new DTD(tablePathDTD);
            dtd.writeDTD(tableName, columnNames, columnTypes);
        }
    }
    
    @Override
    public int insertIntoTable(String tableName, ArrayList<String> columnNames, ArrayList<String> columnValues)
            throws Exception {
        if (containsTable(tableName)) {
            String tablePath = createTablePath(tableName);
            Table table = new Table(tableName, tablePath, fileWriter, fileReader, this.fileTypePath);
            return table.insertRow(columnNames, columnValues);
        } else {
            throw new DatabaseException("ERROR! TABLE DOESNOT EXIST!");
        }
    }
    
    @Override
    public void dropTable(String tableName) throws DatabaseException, DTDException {
        String tableNamePath = createTablePath(tableName);
        String tablePathDTD = this.databasePath + File.separator + tableName;
        if (containsTable(tableName)) {
            File file = new File(tableNamePath);
            dtd = new DTD(tablePathDTD);
            dtd.deleteDTDFile();
            if (!file.delete()) {
                throw new DatabaseException("ERROR! CANNOT DELETE TABLE FILE!");
            }
        } else {
            throw new DatabaseException("ERROR! TABLE DOESNOT EXIST!");
        }
        
    }
    
    @Override
    public int deleteFromDBTable(String tableName, String[] where) throws Exception {
        String tablePath = createTablePath(tableName);
        
        if (containsTable(tableName)) {
            Table table = new Table(tableName, tablePath, fileWriter, fileReader, this.fileTypePath);
            return table.deleteRows(tableName, where);
        } else {
            throw new DatabaseException("The file doesn't exist!");
        }
    }
    
    @Override
    public int updateTable(String tableName, ArrayList<String> columnsNames, ArrayList<String> columnValues,
            String[] where) throws Exception {
        String tablePath = createTablePath(tableName);
        if (containsTable(tableName)) {
            Table table = new Table(tableName, tablePath, fileWriter, fileReader, this.fileTypePath);
            return table.updateRows(columnsNames, columnValues, where);
        }
        
        throw new DatabaseException("Cannot update table not in your database!");
    }
    
    @Override
    public LinkedHashMap<String, ArrayList<String>> selectFromTable(String tableName, ArrayList<String> columnNames,
            String[] where, Adapter adapter) throws Exception {
        String tablePath = createTablePath(tableName);
        Table table = new Table(tableName, tablePath, fileWriter, fileReader, this.fileTypePath);
        if (containsTable(tableName)) {
            return table.selectFromTable(columnNames, where, adapter);
        } else {
            throw new DatabaseException("The file doesn't exist!");
        }
        
    }
    
    @Override
    public LinkedHashMap<String, ArrayList<String>> selectDistinctFromTable(String tableName,
            ArrayList<String> columnNames, String[] where, Adapter adapter) throws Exception {
        String tablePath = createTablePath(tableName);
        Table table = new Table(tableName, tablePath, fileWriter, fileReader, this.fileTypePath);
        if (containsTable(tableName)) {
            return table.selectDistinctFromTable(columnNames, where, adapter);
        } else {
            throw new DatabaseException("The file doesn't exist!");
        }
    }
    
    @Override
    public int alterDrop(String tableName, ArrayList<String> columnNames) throws Exception {
        if (containsTable(tableName)) {
            String tablePath = createTablePath(tableName);
            Table table = new Table(tableName, tablePath, fileWriter, fileReader, this.fileTypePath);
            return table.alterTableDrop(columnNames);
        } else {
            throw new DatabaseException("ERROR! TABLE DOESNOT EXIST!");
        }
        
    }
    
    @Override
    public int alterAdd(String tableName, ArrayList<String> columnNames, ArrayList<String> types) throws Exception {
        if (containsTable(tableName)) {
            String tablePath = createTablePath(tableName);
            Table table = new Table(tableName, tablePath, fileWriter, fileReader, this.fileTypePath);
            return table.alterTableAdd(columnNames, types);
        } else {
            throw new DatabaseException("ERROR! TABLE DOESNOT EXIST!");
        }
        
    }
    
    private boolean containsTable(String tableName) {
        String tableNamePath = createTablePath(tableName);
        Path path = Paths.get(tableNamePath);
        if (Files.exists(path)) {
            return true;
        }
        return false;
    }
    
    private String createTablePath(String tableName) {
        String tableNamePath = this.databasePath + File.separator + tableName + "." + this.fileType;
        return tableNamePath;
    }
    
    private void isDuplicate(ArrayList<String> columnNames) throws DatabaseException {
        HashSet<String> set = new HashSet<>();
        for (int i = 0; i < columnNames.size(); i++) {
            if (!set.add(columnNames.get(i))) {
                throw new DatabaseException("Error !! Duplicate Column Names !! ");
            }
        }
    }
    
    private String getBackendType() {
        FileHandlerType fileHandlerType = new FileHandlerType();
        return fileHandlerType.readFile(this.fileTypePath);
    }
    
}