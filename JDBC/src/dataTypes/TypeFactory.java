package dataTypes;

public class TypeFactory {
    
    public Type invoke(String cellType) throws TypeException {
        switch (cellType.toLowerCase()) {
            case "varchar":
                return new VarcharType();
            case "int":
                return new IntType();
            case "float":
                return new FloatType();
            case "date":
                return new DateType();
            case "object":
                return new ObjectType();
            default:
                throw new TypeException("Unsupported Data Type");
        }
    }
    
}
