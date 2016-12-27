package dbms;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

import adapter.Adapter;
import fileHandler.IFileReader;
import fileHandler.IFileWriter;

public class Table implements ITable {
    
    private String tableName;
    private String tablePath;
    private IFileReader fileReader;
    private IFileWriter fileWriter;
    private LinkedHashMap<String, ArrayList<String>> selectedColumns;
    private File tempFile, table;
    private WhereCondition testWhereCondition;
    private String fileTypePath;
    private CaseInsensitive caseInsensitive;
    
    public Table(String tableName, String tablePath, IFileWriter fileWriter, IFileReader fileReader,
            String fileTypePath) {
        this.tablePath = tablePath;
        this.tableName = tableName;
        this.fileReader = fileReader;
        this.fileWriter = fileWriter;
        this.fileTypePath = fileTypePath;
        this.caseInsensitive = new CaseInsensitive();
    }
    
    @Override
    public int insertRow(ArrayList<String> columnNames, ArrayList<String> columnValues) throws Exception {
        TableColumn tableColumn = new TableColumn(this.tablePath, columnNames, columnValues, this.fileTypePath);
        if (tableColumn.hasValidIdentifiers()) {
            ArrayList<ArrayList<String>> arrangedArray = new ArrayList<ArrayList<String>>();
            arrangedArray = tableColumn.rearrangeColumn();
            performInsertion(arrangedArray);
            
            return 1;
        } else {
            throw new DatabaseException("Invalid Column Entry !!");
        }
        
    }
    
    private void performInsertion(ArrayList<ArrayList<String>> arrangedArray) throws Exception {
        TableColumn tableColumn = new TableColumn(tablePath, null, null, this.fileTypePath);
        ArrayList<ArrayList<String>> row = initializeTempFile(tableColumn.getColumnIdentifiers());
        this.fileReader.fastForward("Row");
        
        while ((row = this.fileReader.readRow("Row")).size() != 0) {
            this.fileWriter.writeRow(row.get(0), row.get(1));
            this.fileReader.fastForward("Row");
        }
        fileWriter.writeRow(arrangedArray.get(0), arrangedArray.get(1));
        fileWriter.endWriter(this.tableName);
        fileReader.endReader();
        tempFile.delete();
    }
    
    @Override
    public int deleteRows(String tableName, String[] where) throws Exception {
        TableColumn tableColumn = new TableColumn(tablePath, null, null, this.fileTypePath);
        if (tableColumn.isValidQuery(where)) {
            return performDeletion(where);
        } else {
            throw new DatabaseException("Invalid Query");
        }
        
    }
    
    private int performDeletion(String[] where) throws Exception {
        TableColumn tableColumn = new TableColumn(tablePath, null, null, this.fileTypePath);
        if (where != null)
            testWhereCondition = new WhereCondition(where, tableColumn.getColumnType(where[2]));
        ArrayList<ArrayList<String>> row = initializeTempFile(tableColumn.getColumnIdentifiers());
        int counter = 0;
        if (where == null) { //delete all the table entries
            counter = deleteAllRecords(row, counter);
        } else {
            
            fileReader.fastForward("Row");
            while ((row = fileReader.readRow("Row")).size() != 0) {
                fileReader.fastForward("Row");
                if ((!testWhereCondition.isTrueCondition(row))) {
                    fileWriter.writeRow(row.get(0), row.get(1));
                } else {
                    counter++;
                }
            }
        }
        
        fileWriter.endWriter(this.tableName);
        fileReader.endReader();
        tempFile.delete();
        return counter;
    }
    
    private int deleteAllRecords(ArrayList<ArrayList<String>> row, int counter) throws Exception {
        fileReader.fastForward("Row");
        while ((row = fileReader.readRow("Row")).size() != 0) {
            fileReader.fastForward("Row");
            counter++;
        }
        return counter;
    }
    
    @Override
    public int updateRows(ArrayList<String> columnsNames, ArrayList<String> columnValues, String[] where)
            throws Exception {
        TableColumn tableColumn = new TableColumn(tablePath, columnsNames, columnValues, this.fileTypePath);
        if (!tableColumn.isValidQuery(where)) { // check that the where is valid condition
            throw new DatabaseException("There is error in your  statement!");
        }
        return performUpdate(columnsNames, columnValues, where);
    }
    
    private int performUpdate(ArrayList<String> columnsNames, ArrayList<String> columnValues, String[] where)
            throws Exception {
        int counter = 0;
        boolean trueCondition = false; // is set to true when where is null
        boolean apply = false; // is set to true when where is null
        TableColumn tableColumn = new TableColumn(tablePath, null, null, this.fileTypePath);
        if (where == null) {
            trueCondition = true;
        } else {
            testWhereCondition = new WhereCondition(where, tableColumn.getColumnType(where[2]));
        }
        ArrayList<ArrayList<String>> row = initializeTempFile(tableColumn.getColumnIdentifiers());
        ArrayList<String> newColumnsNames;// the values which will be put ( updated)
        ArrayList<String> newColumnsValues;
        int valueIndex;
        //        check if there is where condition or not
        
        fileReader.fastForward("Row");
        while ((row = fileReader.readRow("Row")).size() != 0) {
            apply = false;
            fileReader.fastForward("Row");
            newColumnsNames = row.get(0); // as default the same values
            newColumnsValues = row.get(1);
            if (!trueCondition) {
                //                System.out.println(row + " " + testWhereCondition.isTrueCondition(row));
                if (testWhereCondition.isTrueCondition(row)) {
                    apply = true;
                }
            } else {
                apply = true;
            }
            if (apply) {
                counter++;
                for (int i = 0; i < newColumnsNames.size(); i++) { // find which col will be changed
                    if (this.caseInsensitive.containsCaseInsensitive(columnsNames, newColumnsNames.get(i))) {
                        // valueIndex = columnsNames.indexOf(newColumnsNames.get(i)); // index of value  = same of column which will be  updated
                        valueIndex = this.caseInsensitive.indexOfCaseInsensitive(columnsNames, newColumnsNames.get(i));
                        newColumnsValues.set(i, columnValues.get(valueIndex)); // change values to new values
                    }
                }
            }
            fileWriter.writeRow(newColumnsNames, newColumnsValues); // write the row updated or not
        }
        fileWriter.endWriter(this.tableName);
        fileReader.endReader();
        tempFile.delete();
        return counter;
    }
    
    @Override
    public LinkedHashMap<String, ArrayList<String>> selectFromTable(ArrayList<String> columnNames, String[] where,
            Adapter adapter) throws Exception {
        TableColumn tableColumn = new TableColumn(tablePath, columnNames, null, this.fileTypePath);
        if (tableColumn.isValidQuery(where)) {
            selectedColumns = new LinkedHashMap<String, ArrayList<String>>();
            fileReader.initializeReader(tablePath);
            fileReader.fastForward("TableIdentifier");
            if (columnNames == null) {
                columnNames = fileReader.readRow("TableIdentifier").get(0);
            }
            for (int i = 0; i < columnNames.size(); i++) {
                ArrayList<String> columns = new ArrayList<String>();
                selectedColumns.put(columnNames.get(i), columns);
            }
            LinkedHashMap<String, ArrayList<String>> selectedColumns = performSelection(columnNames, where);
            fillAdapter(adapter, selectedColumns);
            return selectedColumns;
        } else {
            throw new DatabaseException("Invalid Query");
        }
    }
    
    private LinkedHashMap<String, ArrayList<String>> performSelection(ArrayList<String> columnNames, String[] where)
            throws Exception {
        ArrayList<ArrayList<String>> row = new ArrayList<ArrayList<String>>();
        TableColumn tableColumn = new TableColumn(tablePath, null, null, this.fileTypePath);
        if (where != null) {
            testWhereCondition = new WhereCondition(where, tableColumn.getColumnType(where[0]));
        }
        fileReader.fastForward("Row");
        while ((row = fileReader.readRow("Row")).size() != 0) {
            if (where == null || (where != null && testWhereCondition.isTrueCondition(row))) {
                fillHashMapWithSelectedColumns(row);
            }
            fileReader.fastForward("Row");
            
        }
        fileReader.endReader();
        return selectedColumns;
    }
    
    private void fillHashMapWithSelectedColumns(ArrayList<ArrayList<String>> row) {
        for (String key : selectedColumns.keySet()) {
            //   int index = row.get(0).indexOf(key);
            int index = this.caseInsensitive.indexOfCaseInsensitive(row.get(0), key);
            String value = row.get(1).get(index);
            if (value == null || value.equals("null")) {
                value = "-";
            } else {
                value = value.replaceAll("^\"|^\'|\"$|\'$", "");
            }
            selectedColumns.get(key).add(value);
        }
    }
    
    public LinkedHashMap<String, ArrayList<String>> selectDistinctFromTable(ArrayList<String> columnNames,
            String[] where, Adapter adapter) throws Exception {
        LinkedHashMap<String, ArrayList<String>> preDistinct = selectFromTable(columnNames, where, adapter);
        LinkedHashSet<ArrayList<String>> set = fillHashSet(preDistinct);
        LinkedHashMap<String, ArrayList<String>> selectedColumns = fillHashMapWithHashSet(set);
        fillAdapter(adapter, selectedColumns);
        return selectedColumns;
        
    }
    
    private LinkedHashSet<ArrayList<String>> fillHashSet(LinkedHashMap<String, ArrayList<String>> preDistinct) {
        LinkedHashSet<ArrayList<String>> set = new LinkedHashSet<ArrayList<String>>();
        ArrayList<String> columnNames = new ArrayList<String>(preDistinct.keySet());
        set.add(columnNames);
        for (int i = 0; i < preDistinct.get(preDistinct.keySet().toArray()[0]).size(); i++) {
            ArrayList<String> tempArray = new ArrayList<String>();
            for (String key : preDistinct.keySet()) {
                tempArray.add(preDistinct.get(key).get(i));
            }
            set.add(tempArray);
        }
        return set;
    }
    
    private LinkedHashMap<String, ArrayList<String>> fillHashMapWithHashSet(LinkedHashSet<ArrayList<String>> set) {
        
        LinkedHashMap<String, ArrayList<String>> afterDistinct = new LinkedHashMap<String, ArrayList<String>>();
        ArrayList<ArrayList<String>> convertedSet = new ArrayList<>(set);
        for (int i = 0; i < convertedSet.get(0).size(); i++) {
            ArrayList<String> column = new ArrayList<String>();
            for (int j = 1; j < convertedSet.size(); j++) {
                String value = convertedSet.get(j).get(i);
                column.add(value);
            }
            afterDistinct.put(convertedSet.get(0).get(i), column);
        }
        return afterDistinct;
    }
    
    public int alterTableAdd(ArrayList<String> columnNames, ArrayList<String> columnTypes) throws Exception {
        TableColumn tableColumn = new TableColumn(tablePath, columnNames, null, this.fileTypePath);
        if (tableColumn.isColumnNameUnique()) {
            tableColumn.isDuplicate();
            ArrayList<ArrayList<String>> newTableIdentifiers = new ArrayList<ArrayList<String>>();
            newTableIdentifiers = tableColumn.getColumnIdentifiers();
            for (int i = 0; i < columnNames.size(); i++) {
                newTableIdentifiers.get(0).add(columnNames.get(i));
                newTableIdentifiers.get(1).add(columnTypes.get(i));
            }
            return performAlterTableAdd(columnNames, columnTypes, newTableIdentifiers);
        } else {
            throw new DatabaseException("Column Names already exist!!");
        }
    }
    
    private int performAlterTableAdd(ArrayList<String> columnNames, ArrayList<String> columnTypes,
            ArrayList<ArrayList<String>> newTableIdentifiers) throws Exception {
        int counter = 0;
        ArrayList<ArrayList<String>> row = initializeTempFile(newTableIdentifiers);
        fileReader.fastForward("Row");
        while ((row = fileReader.readRow("Row")).size() != 0) {
            fileReader.fastForward("Row");
            for (int i = 0; i < columnNames.size(); i++) {
                row.get(0).add(columnNames.get(i));
                row.get(1).add(null);
            }
            counter++;
            fileWriter.writeRow(row.get(0), row.get(1));
        }
        fileWriter.endWriter(this.tableName);
        fileReader.endReader();
        tempFile.delete();
        return counter;
    }
    
    public int alterTableDrop(ArrayList<String> columnNames) throws Exception {
        TableColumn tableColumn = new TableColumn(tablePath, columnNames, null, this.fileTypePath);
        if (tableColumn.validateColumnNames()) {
            ArrayList<ArrayList<String>> newTableIdentifiers = new ArrayList<ArrayList<String>>();
            newTableIdentifiers = tableColumn.getColumnIdentifiers();
            for (int i = 0; i < columnNames.size(); i++) {
                int index = this.caseInsensitive.indexOfCaseInsensitive(newTableIdentifiers.get(0), columnNames.get(i));
                newTableIdentifiers.get(0).remove(index);
                newTableIdentifiers.get(1).remove(index);
            }
            return performAlterTableDrop(columnNames, newTableIdentifiers);
        } else {
            throw new DatabaseException("Columns donot exist !!");
        }
    }
    
    private int performAlterTableDrop(ArrayList<String> columnNames, ArrayList<ArrayList<String>> newTableIdentifiers)
            throws Exception {
        int counter = 0;
        boolean flag = false;
        ArrayList<ArrayList<String>> row = initializeTempFile(newTableIdentifiers);
        fileReader.fastForward("Row");
        while ((row = fileReader.readRow("Row")).size() != 0) {
            fileReader.fastForward("Row");
            for (int i = 0; i < columnNames.size(); i++) {
                int index = caseInsensitive.indexOfCaseInsensitive(row.get(0), (columnNames.get(i)));
                row.get(0).remove(index);
                row.get(1).remove(index);
            }
            if (row.size() == 0) {
                flag = true;
                break;
            }
            fileWriter.writeRow(row.get(0), row.get(1));
            counter++;
        }
        fileWriter.endWriter(this.tableName);
        fileReader.endReader();
        tempFile.delete();
        if (flag) {
            table.delete();
        }
        return counter;
    }
    
    private ArrayList<ArrayList<String>> initializeTempFile(ArrayList<ArrayList<String>> identifiers)
            throws DatabaseException {
        try {
            tempFile = new File(tablePath + "temp");
            table = new File(tablePath);
            fileReader.copyFile(table, tempFile);
            fileWriter.initializeWriter(tableName, tablePath);
            fileReader.initializeReader(tablePath + "temp");
            fileReader.fastForward("TableIdentifier");
            fileReader.readRow("TableIdentifier");
            fileWriter.createTableIdentifier(this.tableName, identifiers.get(0), identifiers.get(1));
            ArrayList<ArrayList<String>> row = new ArrayList<ArrayList<String>>();
            return row;
        } catch (Exception e) {
            throw new DatabaseException("Error gathering information about needed file!");
        }
        
    }
    
    private void fillAdapter(Adapter adapter, LinkedHashMap<String, ArrayList<String>> selectedColumns)
            throws Exception {
        adapter.setTableName(this.tableName);
        adapter.setHashMaps(selectedColumns);
        ArrayList<String> columnNames = new ArrayList<String>();
        TableColumn tableColumn = new TableColumn(tablePath, null, null, this.fileTypePath);
        for (String key : selectedColumns.keySet()) {
            columnNames.add(key);
        }
        adapter.setTableIdentifierTypes(tableColumn.getColumnTypes(columnNames));
    }
    
}