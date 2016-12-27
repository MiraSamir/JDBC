package dbms;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class FileHandlerType implements IFileHandlerType {
    
    public FileHandlerType() {
        
    }
    
    @Override
    public void createFile(String type, String path) {
        
        type = type.replace("db", "");
//        type = type.replace("alt", "json");
                type = type.replace("alt", "ser");
        
        Properties prop = new Properties();
        OutputStream output = null;
        
        try {
            
            output = new FileOutputStream(path);
            // set the properties value
            prop.setProperty("type", type);
            
            // save properties to project root folder
            prop.store(output, null);
            
        } catch (IOException io) {
            io.printStackTrace();
        } finally {
            if (output != null) {
                try {
                    output.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            
        }
    }
    
    @Override
    public String readFile(String path) {
        Properties prop = new Properties();
        InputStream input = null;
        String type = "";
        
        try {
            
            input = new FileInputStream(path);
            // load a properties file
            prop.load(input);
            // get the property value and print it out
            type = prop.getProperty("type");
            
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return type;
    }
    
}