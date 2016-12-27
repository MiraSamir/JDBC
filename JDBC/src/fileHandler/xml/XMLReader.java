package fileHandler.xml;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import fileHandler.IFileReader;

public class XMLReader implements IFileReader {
    
    private XMLInputFactory inFactory;
    private XMLEventReader reader;
    private XMLEvent event;
    private InputStream inputStream;
    
    //	public XMLReader(String tablePath) {
    //
    //	}
    
    @Override
    public void initializeReader(String path) throws XMLStreamException, IOException {
        this.inFactory = XMLInputFactory.newInstance();
        Path pathh = Paths.get(path);
        this.inputStream = Files.newInputStream(pathh);
        this.reader = inFactory.createXMLEventReader(this.inputStream);
        
    }
    
    @Override
    public void endReader() throws XMLStreamException, IOException {
        this.reader.close();
        this.inputStream.close();
        
    }
    
    @Override
    public void fastForward(String parentNode) throws XMLStreamException {
        Boolean flag = false;
        while (reader.hasNext() && !flag) {
            event = reader.nextEvent();
            switch (event.getEventType()) {
                case XMLStreamConstants.START_ELEMENT:
                    StartElement startElement = event.asStartElement();
                    String qName = startElement.getName().getLocalPart();
                    if (qName.equalsIgnoreCase(parentNode)) {
                        flag = true;
                    }
                    break;
            }
        }
        
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ArrayList<ArrayList<String>> readRow(String parentNode) throws XMLStreamException {
        boolean isEnded = false;
        ArrayList<ArrayList<String>> C = new ArrayList<ArrayList<String>>();
        ArrayList<String> A = new ArrayList<String>();
        ArrayList<String> B = new ArrayList<String>();
        while (reader.hasNext() && !isEnded) {
            event = reader.nextEvent();
            switch (event.getEventType()) {
                case XMLStreamConstants.START_ELEMENT:
                    StartElement startElement = event.asStartElement();
                    A.add(startElement.getName().toString());
                    Iterator<Attribute> attributes = startElement.getAttributes();
                    if (attributes.hasNext()) {
                        String attributeValue = attributes.next().getValue();
                        B.add(attributeValue);
                    }
                    break;
                case XMLStreamConstants.END_ELEMENT:
                    EndElement endElement = event.asEndElement();
                    if (endElement.getName().getLocalPart().equalsIgnoreCase(parentNode)) {
                        isEnded = true;
                        C.add(A);
                        C.add(B);
                    }
                    break;
                case XMLStreamConstants.CHARACTERS:
                    Characters characters = event.asCharacters();
                    if (!(characters.isWhiteSpace())) {
                        B.add(characters.toString());
                    }
                    break;
            }
        }
        return C;
    }
    
    @Override
    public void copyFile(File source, File dest) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(dest.getPath());
        Files.copy(source.toPath(), fileOutputStream);
        fileOutputStream.close();
        
    }

	@Override
	public int getIndex() {
		// TODO Auto-generated method stub
		return 0;
	}
    
}
