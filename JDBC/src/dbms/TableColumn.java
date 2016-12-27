package dbms;

import java.util.ArrayList;
import java.util.HashSet;

import dataTypes.TypeFactory;
import fileHandler.BackendFactory;
import fileHandler.IFileReader;

public class TableColumn implements ITableColumn {
    
    private ArrayList<String> columnNames;
    private ArrayList<String> columnValues;
    private ArrayList<ArrayList<String>> columnIdentifiers;
    private String tablePath;
    private String columnName;
    private String columnType;
    private TypeFactory typeInvoker;
    private String fileTypePath;
    private CaseInsensitive caseInsensitive;
    
    public TableColumn(String tablePath, ArrayList<String> columnNames, ArrayList<String> columnValues,
            String fileTypePath) {
        this.tablePath = tablePath;
        this.columnNames = columnNames;
        this.columnValues = columnValues;
        this.fileTypePath = fileTypePath;
        this.typeInvoker = new TypeFactory();
        this.caseInsensitive = new CaseInsensitive();
    }
    
    public String getColumnName() {
        return columnName;
    }
    
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
    
    public TableColumn(String columnName, String columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
    }
    
    public String getColumnType() {
        return columnType;
    }
    
    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }
    
    @Override
    public ArrayList<ArrayList<String>> getColumnIdentifiers() throws Exception {
        IFileReader fileReader = constructFileReader(this.fileTypePath);
        fileReader.initializeReader(tablePath);
        fileReader.fastForward("TableIdentifier");
        columnIdentifiers = fileReader.readRow("TableIdentifier");
        fileReader.endReader();
        return columnIdentifiers;
    }
    
    @Override
    public boolean hasValidIdentifiers() throws Exception {
        getColumnIdentifiers();
        if (columnNames == null && (columnValues.size() != columnIdentifiers.get(0).size())) {
            return false;
        } else if (columnNames != null && (columnNames.size() != columnValues.size())) {
            return false;
        }
        if (columnNames != null) {
            if (!validateColumnNames()) {
                return false;
            }
            ArrayList<ArrayList<String>> arrangedArray = rearrangeColumn();
            columnNames = arrangedArray.get(0);
            columnValues = arrangedArray.get(1);
        } else {
            columnNames = columnIdentifiers.get(0);
        }
        if (!validateColumnValues()) {
            return false;
        }
        return true;
    }
    
    @Override
    public ArrayList<ArrayList<String>> rearrangeColumn() {
        ArrayList<String> tempNames = new ArrayList<String>();
        ArrayList<String> tempValues = new ArrayList<String>();
        ArrayList<ArrayList<String>> mergeArray = new ArrayList<ArrayList<String>>();
        for (int i = 0; i < columnIdentifiers.get(0).size(); i++) {
            if (caseInsensitive.containsCaseInsensitive(columnNames, columnIdentifiers.get(0).get(i))) {
                int index = this.caseInsensitive.indexOfCaseInsensitive(columnNames, columnIdentifiers.get(0).get(i));
                tempNames.add(columnNames.get(index));
                tempValues.add(columnValues.get(index));
            } else {
                tempNames.add(columnIdentifiers.get(0).get(i));
                tempValues.add(null);
            }
        }
        mergeArray.add(tempNames);
        mergeArray.add(tempValues);
        return mergeArray;
    }
    
    @Override
    public boolean isValidQuery(String[] where) throws Exception {
        getColumnIdentifiers();
        boolean whereFlag = true, namesFlag = true, valuesFlag = true;
        if (where != null) {
            whereFlag = validateWhereInput(where);
        }
        if (columnNames != null) {
            namesFlag = validateColumnNames();
        }
        if (columnValues != null) {
            valuesFlag = validateColumnValues();
        }
        return (whereFlag && namesFlag && valuesFlag);
    }
    
    public boolean validateColumnNames() throws Exception {
        getColumnIdentifiers();
        for (int i = 0; i < columnNames.size(); i++) {
            if (!caseInsensitive.containsCaseInsensitive(columnIdentifiers.get(0), columnNames.get(i))) {
                return false;
            }
        }
        try {
            isDuplicate();
        } catch (DatabaseException e) {
            return false;
        }
        return true;
    }
    
    private boolean validateWhereInput(String[] where) throws Exception {
        if ((!where[1].equals("=")) && (!where[1].equals(">")) && (!where[1].equals("<"))) {
            return false;
        }
        if (!caseInsensitive.containsCaseInsensitive(columnIdentifiers.get(0), where[0])) {
            return false;
        }
        int index = this.caseInsensitive.indexOfCaseInsensitive(columnIdentifiers.get(0), where[0]);
        String type = columnIdentifiers.get(1).get(index);
        return typeInvoker.invoke(type).isValid(where[2]);
    }
    
    private boolean validateColumnValues() throws Exception {
        int valueIndex;
        for (int i = 0; i < columnNames.size(); i++) {
            if (caseInsensitive.containsCaseInsensitive(this.columnIdentifiers.get(0), this.columnNames.get(i))) {
                if (columnValues.get(i) == null) {
                    continue;
                }
                valueIndex = this.caseInsensitive.indexOfCaseInsensitive(this.columnIdentifiers.get(0),
                        this.columnNames.get(i));
                if (!typeInvoker.invoke(this.columnIdentifiers.get(1).get(valueIndex))
                        .isValid(this.columnValues.get(i))) {
                    return false;
                }
            }
        }
        return true;
    }
    
    public boolean isColumnNameUnique() throws Exception {
        getColumnIdentifiers();
        for (int i = 0; i < columnNames.size(); i++) {
            if (caseInsensitive.containsCaseInsensitive(this.columnIdentifiers.get(0), columnNames.get(i))) {
                return false;
            }
        }
        return true;
    }
    
    public String getColumnType(String columnName) throws Exception {
        getColumnIdentifiers();
        int index = caseInsensitive.indexOfCaseInsensitive(this.columnIdentifiers.get(0), columnName);
        return this.columnIdentifiers.get(1).get(index);
    }
    
    public ArrayList<String> getColumnTypes(ArrayList<String> columnNames) throws Exception {
        getColumnIdentifiers();
        ArrayList<String> columnTypes = new ArrayList<String>();
        for (int i = 0; i < columnNames.size(); i++) {
            int index = this.caseInsensitive.indexOfCaseInsensitive(this.columnIdentifiers.get(0), columnNames.get(i));
            columnTypes.add(this.columnIdentifiers.get(1).get(index));
        }
        return columnTypes;
    }
    
    public void isDuplicate() throws DatabaseException {
        HashSet<String> set = new HashSet<>();
        for (int i = 0; i < columnNames.size(); i++) {
            if (!set.add(columnNames.get(i))) {
                throw new DatabaseException("Error !! Duplicate Column Names !! ");
            }
        }
    }
    
    private IFileReader constructFileReader(String fileTypePath) throws Exception {
        FileHandlerType fileHandlerType = new FileHandlerType();
        String backEndtype = fileHandlerType.readFile(fileTypePath);
        
        BackendFactory backendFactory = new BackendFactory(backEndtype);
        return backendFactory.createReader();
        
    }
    
}
