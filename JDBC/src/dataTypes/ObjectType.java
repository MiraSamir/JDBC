package dataTypes;

import java.text.ParseException;

public class ObjectType extends Type {
    
    @Override
    public boolean isValid(String value) throws Exception {
        throw new Exception();
    }
    
    @Override
    public int compare(String firstValue, String secondValue) throws Exception {
        throw new Exception();
    }
    
    @Override
    public Object castType(String value) throws TypeException, ParseException {
        throw new TypeException("");
    }
    
    @Override
    public Object castType(String value, String type) throws TypeException, ParseException {
        value = value.replaceAll("^\'|^\"|\'$|\"$", "");
        TypeFactory invoker = new TypeFactory();
        return invoker.invoke(type).castType(value);
        
    }
    
}
