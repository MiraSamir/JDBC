package dbms;

import java.text.ParseException;
import java.util.ArrayList;

import dataTypes.TypeException;
import dataTypes.TypeInvoker;

public class WhereCondition {
	
	private String[] where;
	private String columnType;
	private TypeInvoker invoker;
	
	public WhereCondition(String[] where, String columnType) {
		this.where = where;
		this.columnType = columnType;
		this.invoker = new TypeInvoker();
		
	}
	
	public boolean isTrueCondition(ArrayList<ArrayList<String>> row) throws TypeException, ParseException {
		String value = row.get(1).get(row.get(0).indexOf(where[0]));
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
