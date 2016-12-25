package dtd;

import java.util.ArrayList;


public interface IDTD {

	/**
	 * used to write in dtd file
	 * @param tableName
	 * @param columns
	 * @param types
	 * @throws DTDException
	 */
	public void writeDTD (String tableName, ArrayList<String> columns, ArrayList<String> types) throws DTDException;


	/**
	 * to delete dtd file when dropping table
	 * @throws DTDException
	 */
	public  void deleteDTDFile () throws DTDException;


}
