package dbms;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;

public class DBMS implements IDBMS {
	private Database database;
	private String databasesPath;
	private File databasesFile;
	
	public DBMS() { // <<<=======================================
		databasesPath = System.getProperty("user.home") + File.separator + "Databases";
		Path path = Paths.get(databasesPath);
		if (!Files.exists(path)) {
			databasesFile = new File(databasesPath);
			if (databasesFile.mkdir()) {
				System.out.println("Launching DB System....");
			} else {
				System.out.println("FATAL ERROR! UNABLE TO LAUNCH SYSTEM !");
			}
		}
		
	}
	
	public String getDatabasesPath() {
		return databasesPath;
	}
	
	@Override
	public void createDatabase(String databaseName) throws Exception {
		
		String databasePath = databasesPath + File.separator + databaseName;
		if (!containsDatabase(databasePath)) {
			File databaseFolder = new File(databasePath);
			if (databaseFolder.mkdir()) {
				System.out.println("Database " + databaseName + " is successfully created ");
			} else {
				throw new DBMSException("Cannot create database!");
			}
		} else {
			throw new DBMSException("Database already exists!");
		}
		
	}
	
	@Override
	public void dropDatabase(String databaseName) throws Exception {
		String databasePath = databasesPath + File.separator + databaseName;
		
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
		
	}
	
	@Override
	public void useDatabase(String databaseName) throws DatabaseException {
		String databasePathh = databasesPath + File.separator + databaseName;
		Path path = Paths.get(databasePathh);
		if (Files.exists(path)) { // 3la 7sb l type hygely ezay
			database = new Database(databasePathh, "xml"); // <<<<<<<==========================================
		} else {
			throw new DatabaseException("You must CREATE then USE a Database!");
		}
		
	}
	
	@Override
	public void createTable(String tableName, ArrayList<String> columnNames, ArrayList<String> columnTypes)
			throws Exception {
		if (database == null) {
			throw new DBMSException("You should choose a database first!");
		} else {
			database.createDBTable(tableName, columnNames, columnTypes);
		}
		
	}
	
	@Override
	public void dropTable(String tableName) throws Exception {
		if (database == null) {
			throw new DBMSException("You should choose a database first!");
		} else {
			database.dropTable(tableName);
		}
		
	}
	
	@Override
	public void insertIntoTable(String tableName, ArrayList<String> columnNames, ArrayList<String> columnValues)
			throws Exception {
		if (database == null) {
			throw new DBMSException("You should choose a database first!");
		} else {
			database.insertIntoTable(tableName, columnNames, columnValues);
		}
		
	}
	
	@Override
	public void deleteFromTable(String tableName, String[] where) throws Exception {
		if (database == null) {
			throw new DBMSException("You should choose a database first!");
		} else {
			database.deleteFromDBTable(tableName, where);
		}
		
	}
	
	@Override
	public void updateTable(String tableName, ArrayList<String> columns, ArrayList<String> values, String[] where)
			throws Exception {
		if (database == null) {
			throw new DBMSException("You should choose a database first!");
		} else {
			database.updateTable(tableName, columns, values, where);
		}
		
	}
	
	@Override
	public LinkedHashMap<String, ArrayList<String>> selectFromTable(String tableName, ArrayList<String> columnNames,
			String[] where) throws Exception {
		if (database == null) {
			throw new DBMSException("You should choose a database first!");
		} else {
			return database.selectFromTable(tableName, columnNames, where);
		}
		
	}
	
	@Override
	public LinkedHashMap<String, ArrayList<String>> selectDistinctFromTable(String tableName,
			ArrayList<String> columnNames, String[] where) throws Exception {
		if (database == null) {
			throw new DBMSException("You should choose a database first!");
		} else {
			return database.selectDistinctFromTable(tableName, columnNames, where);
		}
		
	}
	
	@Override
	public void alterAdd(String tableName, ArrayList<String> columnNames, ArrayList<String> types) throws Exception {
		if (database == null) {
			throw new DBMSException("You should choose a database first!");
		} else {
			database.alterAdd(tableName, columnNames, types);
		}
		
	}
	
	@Override
	public void alterDrop(String tableName, ArrayList<String> columnNames) throws Exception {
		
		if (database == null) {
			throw new DBMSException("You should choose a database first!");
		} else {
			database.alterDrop(tableName, columnNames);
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
	
}
