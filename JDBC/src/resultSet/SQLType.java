package resultSet;

import java.sql.SQLException;
import java.sql.Types;

public class SQLType {
    private String type;
    
    public SQLType(String type) {
        this.type = type;
    }
    
    public int getSQLType() throws SQLException {
        switch (this.type.toLowerCase()) {
            case "int":
                return Types.INTEGER;
            case "varchar":
                return Types.VARCHAR;
            case "date":
                return Types.DATE;
            case "float":
                return Types.FLOAT;
            default:
                throw new SQLException();
        }
    }
    
}
