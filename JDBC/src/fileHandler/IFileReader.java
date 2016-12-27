package fileHandler;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import javax.xml.stream.XMLStreamException;

public interface IFileReader {
    /**
     * XML reader. Initialized StAX reading variables
     *
     * @param path
     *            of the file to be read
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public void initializeReader(String path) throws Exception;

    /**
     * XML end reader. Ends StAX reading variables.
     *
     * @param uses
     *            XMLReader param
     * @throws XMLStreamException
     *             the XML stream exception
     * @throws IOException
     */
    public void endReader() throws Exception;

    /**
     * XML fast forward. Runs the StAX reading pointer to the desired node.
     *
     * @param parentNode
     *            => the node to start reading from (not included)
     * @throws XMLStreamException
     *             the XML stream exception
     */
    public void fastForward(String parentNode) throws Exception;

    /**
     * XML read row. Reads row and returns [[Name, age][varchar, int]]
     *
     * @param parent
     *            node to start reading from ===> Must use XMLFastForward to
     *            reach first occurrance of node only
     * @return the array list
     * @throws XMLStreamException
     *             the XML stream exception
     */
    public ArrayList<ArrayList<String>> readRow(String parentNode) throws Exception;

    /**
     * Copy file. Copy file content to another.
     *
     * @param source
     *            the source file
     * @param dest
     *            the destination file
     */
    public void copyFile(File source, File dest) throws Exception;

	public int getIndex();

}
