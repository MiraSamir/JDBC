package dataTypes;

public class TypeInvoker {
	
	public TypeInvoker() {
	} //<========
	
	public Type invoke(String cellType) throws TypeException {
		switch (cellType) {
			case "varchar":
				return new VarcharType();
			case "int":
				return new IntType();
			case "float":
				return new FloatType();
			case "date":
				return new DateType();
			default:
				throw new TypeException("Unsupported Data Type");
		}
	}
	
}
