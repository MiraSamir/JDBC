package cli;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;
import java.util.Scanner;

import cmd.CMD;
import connection.Connection;
import eg.edu.alexu.csd.oop.jdbc.Driver;
import resultSet.ResultSett;
import statement.Statement;

public class CLI {
    private static String username;
    private static String password;
    private static String path;
    private static String url;
    private static Connection connection;
    private static Statement statement;
    private static Properties info;
    static Scanner input;
    static String protocol;
    static CMD cmd;
    
    public static void main(String[] args) {
        try {
            getUserInfo();
            startConnection();
            while (true) {
                printStart();
                executeQuery();
            }
            
        } catch (Exception e) {
            System.out.println("FATAL ERROR OCCURED! \n PROGRAM WILL SHUT DOWN");
        }
        
    }
    
    private static void getUserInfo() {
        readConfig();
        System.out.println("Enter the username");
        if (!validate(username, getUserInput())) {
            System.out.println("Error in your username! try again");
            getUserInfo();
        }
        System.out.println("Enter the password");
        if (!validate(password, getUserInput())) {
            System.out.println("Error in your password! try again");
            getUserInfo();
        }
        
        System.out.println("logged in successfully ");
        
    }
    
    private static void startConnection() throws Exception {
        try {
            url = "jdbc:" + protocol + "://localhost";
            setInfoFile();
            Driver driver;
            driver = new Driver();
            connection = driver.connect(url, info);
            statement = connection.createStatement();
        } catch (Exception e) {
            System.out.println("Error in establishing connection check your information");
            throw new RuntimeException();
        }
        
    }
    
    private static void readConfig() {
        File configFile = new File("jdbcConfig.properties");
        try {
            FileReader reader = new FileReader(configFile);
            Properties prop = new Properties();
            prop.load(reader);
            username = prop.getProperty("username");
            password = prop.getProperty("password");
            path = prop.getProperty("path");
            protocol = path = prop.getProperty("protocol");
        } catch (Exception e) {
            System.out.println("error in configuration file try again");
            throw new RuntimeException("FATAL ERROR");
        }
    }
    
    private static void setInfoFile() {
        info = new Properties();
        info.put("path", path);
        info.put("username", username);
        info.put("password", password);
        
    }
    
    static String getUserInput() {
        input = new Scanner(System.in);
        String userInput = input.nextLine();
        return userInput;
    }
    
    static boolean validate(String valid, String toValidate) {
        if (valid.equals(toValidate)) {
            return true;
        }
        
        return false;
        
    }
    
    static void printStart() {
        System.out.println("Enter your Query!");
        System.out.print(">> ");
    }
    
    static void executeQuery() {
        String query;
        boolean isSelect;
        ResultSett result;
        cmd = new CMD();
        try {
            query = getUserInput();
            isSelect = statement.execute(query);
            if (isSelect) {
                result = (ResultSett) statement.getResultSet();
                cmd.drawTable(result.getColumnTypeAccess());
            } else {
                System.out.println("Query executed successfully");
            }
            
        } catch (Exception e) {
            
            System.out.println("Error in executing query");
        }
        
    }
    
}
