package jdbc.driver;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class Driver implements java.sql.Driver {
	
	@Override
	public boolean acceptsURL(String url) throws SQLException {
		//		supported format => jdbc:protocol_name://localhost 
		boolean jdbc = false, protocol = false, localhost = false;
		//		scan jbdc:
		jdbc = containsJdbc(url);
		//		scan protocolname :
		protocol = isValidProtocol(url);
		//		scan localHost
		localhost = isLocalhost(url);
		return jdbc && protocol && localhost;
	}
	
	@Override
	public Connection connect(String arg0, Properties arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		
		DriverPropertyInfo dpi = new DriverPropertyInfo(url, info.getProperty(url));
		return null;
	}
	
	private boolean containsJdbc(String url) {
		//		supported format => jdbc:protocol_name://localhost 
		//							^^^^^
		String matchingWord;
		try {
			matchingWord = url.substring(0, 5);
		} catch (Exception e) {
			return false; // url shorter than jbdc:
		}
		if (!matchingWord.equals("jdbc:")) {
			return false;
		}
		
		return true;
	}
	
	private boolean isValidProtocol(String url) {
		//		supported format => jdbc:protocol_name://localhost 
		//								 ^^^^^^^^^^^^^^ 
		//								 5             	
		String protocolName = "";
		int i = 5;
		Character c = url.charAt(i);
		
		try {
			while (!c.equals(':')) {
				protocolName += c;
				i++;
				c = url.charAt(i);
			}
			protocolName += c;
			if (protocolName.equals("xmldb:") || protocolName.equals("jsondb:")) {
				return true;
			}
			
		} catch (Exception e) {
			return false; // url not valid:
		}
		return false;
	}
	
	private boolean isLocalhost(String url) {
		//		supported format => jdbc:protocol_name://localhost 
		//											   ^^^^^^^^^^^
		try {
			if (!url.endsWith("//localhost")) {
				return false;
			}
			return url.matches("\\w+:\\w+://localhost");
		} catch (Exception e) {
			return false;
		}
	}
	
	@Override
	public int getMajorVersion() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public int getMinorVersion() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public boolean jdbcCompliant() {
		// TODO Auto-generated method stub
		return false;
	}
	
}
