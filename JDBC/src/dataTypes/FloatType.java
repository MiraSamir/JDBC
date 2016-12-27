package dataTypes;

import java.util.regex.Pattern;

public class FloatType extends ObjectType {
    
    @Override
    public boolean isValid(String value) {
        return (!hasQuotes(value) && isFloat(value));
    }
    
    private static boolean isFloat(String s) {
        String decimalPattern = "^[-+]?[0-9]+[.][0-9]+$";
        if (Pattern.matches(decimalPattern, s)) {
            if (!(s.length() > 9)) {
                return true;
            }
            return false;
        }
        return false;
    }
    
    @Override
    public int compare(String firstValue, String secondValue) {
        Float firstFloat = (Float) castType(firstValue);
        Float secondFloat = (Float) castType(secondValue);
        return firstFloat.compareTo(secondFloat);
    }
    
    @Override
    public Object castType(String value) {
        return Float.parseFloat(value);
    }
    
}
