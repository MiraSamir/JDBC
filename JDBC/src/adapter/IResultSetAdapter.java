package adapter;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public interface IResultSetAdapter {
    
    public void setHashMaps(LinkedHashMap<String, ArrayList<String>> selectedColumns);
    
    public ArrayList<String> getColumnNames();
    
}
