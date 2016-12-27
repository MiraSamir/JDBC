package dbms;

import java.util.ArrayList;

import dataTypes.TypeFactory;

public class WhereCondition {
    
    private String[] where;
    private String columnType;
    private TypeFactory invoker;
    private CaseInsensitive caseInsensitive;
    
    public WhereCondition(String[] where, String columnType) {
        this.where = where;
        this.columnType = columnType;
        this.invoker = new TypeFactory();
        this.caseInsensitive = new CaseInsensitive();
        
    }
    
    public boolean isTrueCondition(ArrayList<ArrayList<String>> row) throws Exception {
        
        String value = row.get(1).get(caseInsensitive.indexOfCaseInsensitive(row.get(0), where[0]));
        if (value == "null") {
            return false;
        }
        int comparisonResult = invoker.invoke(columnType).compare(value, where[2]);
        if (where[1].equals("=")) {
            return comparisonResult == 0;
        } else if (where[1].equals(">")) {
            return comparisonResult > 0;
        } else {
            return (comparisonResult < 0);
        }
    }
    
}