package dataTypes;

import java.text.ParseException;

public abstract class Type {
	public abstract boolean isValid(String value);
	
	public abstract int compare(String firstValue, String secondValue) throws ParseException;
	
	public abstract Object castType(String value) throws ParseException;
	
	public boolean hasQuotes(String s) {
		return (s.startsWith("\"") || s.startsWith("\'"));
		
	}
}
