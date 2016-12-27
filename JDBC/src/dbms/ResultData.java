package dbms;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public class ResultData {
	protected String tableName;
	protected LinkedHashMap<String, ArrayList<String>> columnTypeAccess;

	protected ArrayList<String> tableIdentifierTypes;

	public ResultData() {
		tableName = "";
		columnTypeAccess = new LinkedHashMap<String, ArrayList<String>>();
		tableIdentifierTypes = new ArrayList<String>();
	}

	public String getTableName() {
		return this.tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public ArrayList<String> getTableIdentifierTypes() {
		return tableIdentifierTypes;
	}

	public void setTableIdentifierTypes(ArrayList<String> tableIdentifierTypes) {
		this.tableIdentifierTypes = tableIdentifierTypes;
	}

	public LinkedHashMap<String, ArrayList<String>> getColumnTypeAccess() {
		return columnTypeAccess;
	}


}
