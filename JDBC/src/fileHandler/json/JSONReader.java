package fileHandler.json;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.nio.file.Files;
import java.util.ArrayList;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import fileHandler.IFileReader;

public class JSONReader implements IFileReader {
    private File file;
    private FileReader fileReader;
    private JSONParser jsonParser;
    private JSONObject table;
    private JSONArray tableRecords;
    private String tableName;
    private int pointer;
    
    @Override
    public void initializeReader(String path) throws Exception {
        
        String subStringPath = path.substring(path.lastIndexOf(File.separator) + 1);
        this.tableName = subStringPath.substring(0, subStringPath.lastIndexOf('.'));
        this.file = new File(path);
        this.fileReader = new FileReader(this.file);
        this.jsonParser = new JSONParser();
        this.table = (JSONObject) this.jsonParser.parse(this.fileReader);
        this.tableRecords = (JSONArray) this.table.get(this.tableName);
        this.pointer = 0;
    }
    
    @Override
    public void endReader() throws Exception {
        this.fileReader.close();
    }
    
    @Override
    public void fastForward(String parentNode) throws Exception {
        if (!parentNode.equals("TableIdentifier")) {
            this.pointer++;
        }
    }
    
    @Override
    public ArrayList<ArrayList<String>> readRow(String parentNode) throws Exception {
        ArrayList<ArrayList<String>> row = new ArrayList<ArrayList<String>>();
        ArrayList<String> columnNames = new ArrayList<String>();
        ArrayList<String> columnValues = new ArrayList<String>();
        if (this.pointer < this.tableRecords.size()) {
            JSONObject rowContent = new JSONObject();
            rowContent = (JSONObject) this.tableRecords.get(this.pointer);
            JSONArray rowValues = new JSONArray();
            rowValues = (JSONArray) rowContent.get(parentNode);
            int len = rowValues.size();
            for (int i = 0; i < len; i++) {
                
                String rValues = rowValues.get(i).toString().replaceAll("[{}\"]", "");
                String[] array = rValues.split(":");
                columnNames.add(array[0]);
                columnValues.add(array[1]);
            }
            row = mergeColumnArrays(columnNames, columnValues);
            //    this.pointer = this.pointer + 1;
        }
        return row;
    }
    
    @Override
    public void copyFile(File source, File dest) throws Exception {
        FileOutputStream fileOutputStream = new FileOutputStream(dest.getPath());
        Files.copy(source.toPath(), fileOutputStream);
        fileOutputStream.close();
    }
    
    private ArrayList<ArrayList<String>> mergeColumnArrays(ArrayList<String> columnNames,
            ArrayList<String> columnValues) {
        ArrayList<ArrayList<String>> row = new ArrayList<ArrayList<String>>();
        row.add(columnNames);
        row.add(columnValues);
        return row;
    }

	@Override
	public int getIndex() {
		// TODO Auto-generated method stub
		return 0;
	}
    
}
