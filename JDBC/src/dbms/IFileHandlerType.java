package dbms;

public interface IFileHandlerType {
    
    public void createFile(String type, String path);
    
    public String readFile(String path);
    
}
