package dbms;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public class CaseInsensitive {
    
    public boolean containsCaseInsensitive(ArrayList<String> array, String value) {
        for (String s : array) {
            if (value.equalsIgnoreCase(s))
                return true;
        }
        return false;
    }
    
    public boolean containsCaseInsensitive(LinkedHashMap<String, ArrayList<String>> hashmap, String value) {
        for (String key : hashmap.keySet()) {
            if (value.equalsIgnoreCase(key))
                return true;
        }
        return false;
    }
    
    public int indexOfCaseInsensitive(ArrayList<String> array, String value) {
        for (int i = 0; i < array.size(); i++) {
            if (array.get(i).equalsIgnoreCase(value.toLowerCase())) {
                return i;
            }
        }
        return 0;
    }
    
}
