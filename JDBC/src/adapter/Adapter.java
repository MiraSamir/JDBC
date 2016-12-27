package adapter;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import dbms.ResultData;

public class Adapter extends ResultData implements IResultSetAdapter {
    private LinkedHashMap<Integer, ArrayList<String>> indexAccess;
    
    public Adapter() {
        super();
        indexAccess = new LinkedHashMap<Integer, ArrayList<String>>();
    }
    
    public LinkedHashMap<Integer, ArrayList<String>> getIndexAccess() {
        return indexAccess;
    }
    
    @Override
    public void setHashMaps(LinkedHashMap<String, ArrayList<String>> selectedColumns) {
        
        this.columnTypeAccess = selectedColumns;
        int i = 1;
        for (String key : selectedColumns.keySet()) {
            this.indexAccess.put(i, selectedColumns.get(key));
            i++;
        }
    }
    
    @Override
    public ArrayList<String> getColumnNames() {
        ArrayList<String> columnNames = new ArrayList<String>();
        for (String key : this.columnTypeAccess.keySet()) {
            columnNames.add(key);
        }
        return columnNames;
    }
    
}
