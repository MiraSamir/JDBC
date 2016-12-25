package cmd;

import java.util.Scanner;

import dbms.DBMSException;
import dbms.DatabaseException;
import parser.ParserException;

/**
 * The main method of the CMD is found here and it calls the methods from the
 * CMD class.
 * 
 * @author USER
 *
 */
public class CMDMain {
	
	static Scanner input = new Scanner(System.in);
	static CMD cmd;
	
	public static void main(String[] args) throws Exception {
		cmd = new CMD();
		while (true) {
			printStart();
			sequence();
		}
		
	}
	
	static void printStart() {
		System.out.println("Enter your Query!");
		System.out.print(">> ");
	}
	
	static void sequence() throws Exception {
		String query = getUserInput();
		try {
			cmd.getUserInput(query);
			if (cmd.getOperationName().equals("ReEnter")) {
			} else if (cmd.getOperationName().equals("Select From Table")) {
				cmd.drawTable(cmd.getSelected());
			} else if (cmd.getOperationName().equals("Select Distinct")) {
				cmd.drawTable(cmd.getSelected());
			} else {
				cmd.printMessage();
			}
		} catch (CMDException c) {
			System.out.println(c.getErrorMessage() + "\n");
			c.printStackTrace();
		} catch (ParserException p) {
			System.out.println(p.getErrorMessage() + "\n");
			p.printStackTrace();
		} catch (DBMSException d) {
			System.out.println(d.getErrorMessage() + "\n");
			d.printStackTrace();
		} catch (DatabaseException db) {
			System.out.println(db.getErrorMessage() + "\n");
			db.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error! try again" + "\n");
		}
	}
	
	static String getUserInput() {
		String query = input.nextLine();
		return query;
	}
	
}
