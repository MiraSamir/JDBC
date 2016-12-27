package fileHandler.json;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.nio.file.Files;
import java.util.ArrayList;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import fileHandler.IFileReader;
import fileHandler.IFileWriter;

public class JSONWriter implements IFileWriter {
    //private String tablePath;
    private FileWriter file;
    private JSONObject table;
    private JSONArray tableRecords; //<===main array
    
    @Override
    public void initializeWriter(String tableName, String tablePath) throws Exception {
        //	this.tablePath = tablePath;
        this.file = new FileWriter(tablePath);
        table = new JSONObject();
        tableRecords = new JSONArray();
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void endWriter(String tableName) throws Exception {
        //////
        
        this.table.put(tableName, this.tableRecords);
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        
        this.file.write(gson.toJson(this.table));
        file.flush();
        file.close();
        
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void createTableIdentifier(String tableName, ArrayList<String> columnNames, ArrayList<String> columnTypes)
            throws Exception {
        JSONObject tableIdentifiersRow = new JSONObject();
        JSONArray tableIdentifierTypes = new JSONArray();
        
        for (int i = 0; i < columnNames.size(); i++) {
            JSONObject obj = new JSONObject();
            obj.put(columnNames.get(i), columnTypes.get(i));
            tableIdentifierTypes.add(obj);
        }
        tableIdentifiersRow.put("TableIdentifier", tableIdentifierTypes);
        tableRecords.add(tableIdentifiersRow);
        
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void writeRow(ArrayList<String> columnNames, ArrayList<String> columnValues) throws Exception {
        JSONObject tableIdentifiersRow = new JSONObject();
        JSONArray tableIdentifierTypes = new JSONArray();
        
        for (int i = 0; i < columnNames.size(); i++) {
            JSONObject obj = new JSONObject();
            if (columnValues.get(i) == null) {
                obj.put(columnNames.get(i), "null");
            } else {
                obj.put(columnNames.get(i), columnValues.get(i));
            }
            tableIdentifierTypes.add(obj);
        }
        tableIdentifiersRow.put("Row", tableIdentifierTypes);
        tableRecords.add(tableIdentifiersRow);
        
    }
    
    @Override
    public void copyFile(File source, File dest) throws Exception {
        FileOutputStream fileOutputStream = new FileOutputStream(dest.getPath());
        Files.copy(source.toPath(), fileOutputStream);
        fileOutputStream.close();
        
    }



	@Override
	public void DefineReader(IFileReader fileReader) {
		// TODO Auto-generated method stub
		
	}


    
}
