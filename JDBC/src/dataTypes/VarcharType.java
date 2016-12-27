package dataTypes;

public class VarcharType extends ObjectType {
    
    @Override
    public boolean isValid(String value) {
        return (hasQuotes(value) && !isWhiteSpace(value));
    }
    
    private boolean isWhiteSpace(String s) {
        String f = s.replaceAll("\\s", "");
        return (f.length() == 2);
    }
    
    @Override
    public int compare(String firstValue, String secondValue) {
        firstValue = castType(firstValue).toString();
        secondValue = castType(secondValue).toString();
        return firstValue.compareTo(secondValue);
    }
    
    @Override
    public Object castType(String value) {
        return value.replaceAll("^\'|^\"|\'$|\"$", "");
    }
    
}
