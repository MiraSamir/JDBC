package cmd;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public interface ICMD {

	void getUserInput(String query) throws Exception;

	/**
	 * draws the table
	 */
	public void drawTable(LinkedHashMap<String, ArrayList<String>> table);
	void printMessage();

	/**
	 * gets the number of the operation from the parser
	 * @throws Exception
	 */
	void operation() throws Exception;
}
