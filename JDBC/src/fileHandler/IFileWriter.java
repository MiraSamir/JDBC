package fileHandler;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public interface IFileWriter {
    /**
     * XML writer. Initializes StAX writing variables. Writes .XML file default
     * head
     *
     * @param using
     *            XMLHandler initializer parameter => tablePath
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public void initializeWriter(String tableName, String tablePath) throws Exception;

    /**
     * XML end writer. Ends StAX writer variables writes end node of the file.
     *
     * @param using
     *            XMLHandler initializer parameter => tablePath
     * @throws IOException
     */
    public void endWriter(String tableName) throws Exception;

    /**
     * XML create table identifier. Writes Indentifiers of table (row 0)
     *
     *
     * ====> Must initialize XMLHandler and XMLWriter first
     *
     * @param tableName
     *            the table name
     * @param Array
     *            list of column names
     * @param Array
     *            list of column types
     * @throws Exception
     *             the exception
     */
    public void createTableIdentifier(String tableName, ArrayList<String> columnNames, ArrayList<String> columnTypes)
            throws Exception;

    /**
     * XML write row.
     *
     * @param Array
     *            list of column names
     * @param Array
     *            list of column values ==> writes null in front of each null
     *            node
     * @throws Exception
     *             the exception
     */
    public void writeRow(ArrayList<String> columnNames, ArrayList<String> columnValues) throws Exception;

    /**
     * Copy file. Copy file content to another.
     *
     * @param source
     *            the source file
     * @param dest
     *            the destination file
     */
    public void copyFile(File source, File dest) throws Exception;
    
    /**
     * to remove all the columns in the table
     * @param tablePath 
     * @param tableName 
     */

	public void DefineReader(IFileReader fileReader);

}
