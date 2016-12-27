package fileHandler.xml;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

import fileHandler.IFileReader;
import fileHandler.IFileWriter;

public class XMLWriter implements IFileWriter {
    
    private XMLEventWriter writer;
    private XMLOutputFactory factory;
    private XMLEvent end, tab;
    private XMLEventFactory eventFactory;
    private OutputStream outputStream;
    
    public XMLWriter() {
        
        this.eventFactory = XMLEventFactory.newInstance();
        this.end = this.eventFactory.createDTD("\r");
        this.tab = this.eventFactory.createDTD(" ");
    }
    
    @Override
    public void initializeWriter(String tableName, String tablePath) throws XMLStreamException, IOException {
        this.factory = XMLOutputFactory.newInstance();
        this.factory.setProperty("escapeCharacters", false);
        Path path = Paths.get(tablePath);
        this.outputStream = Files.newOutputStream(path);
        this.writer = factory.createXMLEventWriter(outputStream, "UTF-8");
        this.writer.add(eventFactory.createStartDocument());
        this.writer.add(end);
        this.writer.add(eventFactory.createCharacters("<!DOCTYPE " + tableName + " SYSTEM \"" + tableName + ".dtd\">"));
        this.writer.add(end);
        this.writer.add(eventFactory.createStartElement("", "", tableName));
        this.writer.add(end);
    }
    
    @Override
    public void endWriter(String tableName) throws Exception {
        this.writer.add(end);
        this.writer.add(eventFactory.createEndElement("", "", tableName));
        this.writer.add(eventFactory.createEndDocument());
        this.writer.flush();
        this.writer.close();
        this.outputStream.flush();
        this.outputStream.close();
        
    }
    
    @Override
    public void createTableIdentifier(String tableName, ArrayList<String> columnNames, ArrayList<String> columnTypes)
            throws Exception {
        this.writer.add(eventFactory.createStartElement("", "", "TableIdentifier"));
        this.writer.add(end);
        
        for (int i = 0; i < columnNames.size(); i++) {
            writer.add(tab);
            writer.add(eventFactory.createStartElement("", "", columnNames.get(i)));
            writer.add(eventFactory.createAttribute("type", columnTypes.get(i)));
            writer.add(eventFactory.createEndElement("", "", columnNames.get(i)));
            writer.add(end);
        }
        writer.add(eventFactory.createEndElement("", "", "TableIdentifier"));
        
    }
    
    @Override
    public void writeRow(ArrayList<String> columnNames, ArrayList<String> columnValues) throws Exception {
        writer.add(end);
        writer.add(eventFactory.createStartElement("", "", "Row"));
        writer.add(end);
        for (int i = 0; i < columnNames.size(); i++) {
            writer.add(tab);
            writer.add(eventFactory.createStartElement("", "", columnNames.get(i)));
            if (columnValues.get(i) == (null)) { ///////////////////////
                writer.add(eventFactory.createCharacters("null"));
                
            } else {
                writer.add(eventFactory.createCharacters(columnValues.get(i)));
            }
            writer.add(eventFactory.createEndElement("", "", columnNames.get(i)));
            writer.add(end);
        }
        writer.add(eventFactory.createEndElement("", "", "Row"));
    }
    
    @Override
    public void copyFile(File source, File dest) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(dest.getPath());
        Files.copy(source.toPath(), fileOutputStream);
        fileOutputStream.close();
        
    }

	

	@Override
	public void DefineReader(IFileReader fileReader) {
		// TODO Auto-generated method stub
		
	}


}
