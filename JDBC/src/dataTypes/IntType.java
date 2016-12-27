package dataTypes;

public class IntType extends ObjectType {
	
	@Override
	public boolean isValid(String value) {
		return (!hasQuotes(value) && isInteger(value));
	}
	
	private boolean isInteger(String s) {
		try {
			Integer.parseInt(s);
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	@Override
	public int compare(String firstValue, String secondValue) {
		Integer firstInteger = (Integer) castType(firstValue);
		Integer secondInteger = (Integer) castType(secondValue);
		return firstInteger.compareTo(secondInteger);
	}
	
	@Override
	public Object castType(String value) {
		return Integer.parseInt(value);
	}
	
}
