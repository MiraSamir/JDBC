package dataTypes;

import java.text.ParseException;

public abstract class Type {
    
    public abstract boolean isValid(String value) throws Exception;
    
    public abstract int compare(String firstValue, String secondValue) throws ParseException, Exception;
    
    public abstract Object castType(String value) throws TypeException, ParseException;
    
    public abstract Object castType(String value, String type) throws ParseException, TypeException;
    
    public boolean hasQuotes(String s) {
        return (s.startsWith("\"") || s.startsWith("\'"));
    }
}
