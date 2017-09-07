package com.view;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.JSONObject;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by sayed on 05.09.17.
 */

public class Services extends HttpServlet {

    private static Logger logger = Logger.getLogger(Services.class.getName());
    private Admin admin;
    private Connection connection;
    private HbaseParser parser;

    @Override
    public void init(ServletConfig config) throws ServletException {

        System.out.println("Initialising log4j");
        String log4jLocation = config.getInitParameter("log4j-properties-location");

        ServletContext sc = config.getServletContext();

        if (log4jLocation == null) {
            System.out.println("No log4j properites...");
            BasicConfigurator.configure();
        } else {
            String webAppPath = sc.getRealPath("/");
            String log4jProp = webAppPath + log4jLocation;
            File output = new File(log4jProp);

            if (output.exists()) {
                System.out.println("Initialising log4j with: " + log4jProp);
                PropertyConfigurator.configure(log4jProp);
            } else {
                System.out.println("Find not found (" + log4jProp + ").");
                BasicConfigurator.configure();
            }
        }

        super.init(config);

        parser = new HbaseParser(); // get HbaseParser object
        connection = parser.makeConnection(); // make connection
        admin = parser.getAdmin(connection); // get the admin
        logger.info("Connection successfully done!");
    }

    /*@Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        *//*response.setContentType("text/html");
        PrintWriter out = response.getWriter();

        // Generate HTML content to check
        out.println("<html><body>");
        out.println("<h2>Hello Hbase</h2>");
        out.println("<hr>");
        out.println("Time on the Server is: " + new java.util.Date());
        out.println("</br>Hbase Parser Object : " + parser);
        out.println("</br>Connection Object : " + connection);
        out.println("</br>Admin Object : " + admin);
        out.println("</body></html>");*//*

        // Declarations of Variables
        String query = new String("list"); // listing table

        BufferedReader reader = request.getReader();
        StringBuilder builder = new StringBuilder();
        PrintWriter writer = response.getWriter();
        ArrayList<String[]> rows = new ArrayList<String[]>();
        String[] row = null;
        String line;


        // Main Processing Starts here
        List<String> parse = StatementParser.removeWhiteSpaceAndQuotes(query);
        // List the table
        if (parse.get(0).equalsIgnoreCase("list")) {
            List<String> listTable = parser.listTable(admin); // Create Table
            int iter = 0;
            row = new String[listTable.size()];
            for (String st : listTable) {
                row[iter] = new String(st);
                iter += 1;
            }
        }

        writer.println("<html><body>");
        writer.println("<h2>Listing all the Tables</h2>");
        writer.println("<hr>");
        for(String rw : row) {
            writer.println(rw);
            writer.println("</br>");
        }
        writer.println("</body></html>");
    }*/

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {

        // Declarations of Variables
        // String query = new String("list"); // listing table

        String query = new String();
        BufferedReader reader = request.getReader();
        StringBuilder builder = new StringBuilder();
        PrintWriter writer = response.getWriter();
        String[] row = null;
        String line;


        // Main Processing Starts here
        while ( (line = reader.readLine()) != null ) {
            builder.append(line);
        }
        query = builder.toString();
        JSONObject json = new JSONObject();
        json.put("query", query);

        // Remove the semicolon (;) if present in the end of the line
        if (query.length() > 0 && query.charAt(query.length() - 1) == ';') {
            query = query.substring(0, query.length() - 1);
        }

        List<String> parse = StatementParser.removeWhiteSpaceAndQuotes(query);
        System.out.println(parse.get(0));
        json.put("columns", parse.get(0));

        // Creating the table
        if (parse.get(0).equalsIgnoreCase("create")) {
            parse.remove(0);
            TableName tableName = TableName.valueOf(parse.get(0));
            parse.remove(0);

            List<String> familyNames = new ArrayList<String>(parse);
            row = new String[1]; // For String type return length is 1
            row[0] = parser.createTable(admin, tableName, familyNames);
        }

        // Describe the table
        else if (parse.get(0).equalsIgnoreCase("describe")) {
            parse.remove(0);

            TableName tableName = TableName.valueOf(parse.get(0));
            parse.remove(0);

            List<String> listTable = parser.describeTable(admin, tableName);
            int iter = 0;
            row = new String[listTable.size()];
            for (String table : listTable) {
                row[iter] = new String(table);
                iter += 1;
            }
        }

        // List the table
        else if (parse.get(0).equalsIgnoreCase("list")) {
            List<String> listTable = parser.listTable(admin);
            int iter = 0;
            row = new String[listTable.size()];
            for (String st : listTable) {
                row[iter] = new String(st);
                iter += 1;
            }
        }

        // Check if the table Exists
        else if (parse.get(0).equalsIgnoreCase("exists")) {
            parse.remove(0);
            TableName tableName = TableName.valueOf(parse.get(0));
            row = new String[1]; // For String type return length is 1
            row[0] = parser.isTableExists(admin, tableName);
        }

        // Check if the table disabled
        else if (parse.get(0).equalsIgnoreCase("is_disabled")) {
            parse.remove(0);
            TableName tableName = TableName.valueOf(parse.get(0));
            row = new String[1]; // For String type return length is 1
            row[0] = String.valueOf(parser.isTableDisabled(admin, tableName));
        }

        // Check if the table is enable
        else if (parse.get(0).equalsIgnoreCase("is_enabled")) {
            parse.remove(0);
            TableName tableName = TableName.valueOf(parse.get(0));
            row = new String[1]; // For String type return length is 1
            row[0] = String.valueOf(parser.isTableEnabled(admin, tableName));
        }

        // Disable the table
        else if (parse.get(0).equalsIgnoreCase("disable")) {
            parse.remove(0);
            TableName tableName = TableName.valueOf(parse.get(0));
            row = new String[1]; // For String type return length is 1
            row[0] = parser.disableTable(admin, tableName);
        }

        // Check if the table disabled
        else if (parse.get(0).equalsIgnoreCase("enable")) {
            parse.remove(0);
            TableName tableName = TableName.valueOf(parse.get(0));
            row = new String[1]; // For String type return length is 1
            row[0] = parser.enableTable(admin, tableName);
        }

        // Check if the table Exists
        else if (parse.get(0).equalsIgnoreCase("drop")) {
            parse.remove(0);
            TableName tableName = TableName.valueOf(parse.get(0));
            row = new String[1]; // For String type return length is 1
            row[0] = parser.dropTable(admin, tableName);
        }

        // Put/Update the table
        else if (parse.get(0).equalsIgnoreCase("put")) {
            parse.remove(0);

            Table table = null;
            try {
                table = connection.getTable(TableName.valueOf(parse.get(0))); // get Table
                parse.remove(0);
            } catch (IOException e) {
                e.printStackTrace();
            }
            String rowName = parse.get(0).toString(); // get Row
            parse.remove(0);

            List<String> familyNamesValue = new ArrayList<String>(parse);
            row = new String[1]; // For String type return length is 1
            row[0] = parser.putData(table, rowName, familyNamesValue); // Put in the Table
        }

        // Get the table
        else if (parse.get(0).equalsIgnoreCase("get")) {
            parse.remove(0);

            Table table = null;
            try {
                table = connection.getTable(TableName.valueOf(parse.get(0))); // get Table
                parse.remove(0);
            } catch (IOException e) {
                e.printStackTrace();
            }
            String rowName = parse.get(0).toString(); // get Row
            parse.remove(0);

            List<String> familyNamesValue = new ArrayList<String>(parse);

            List<String> listSt = new ArrayList<String>();
            if(familyNamesValue.size() <= 0)
                listSt = parser.getData(table, rowName); // get the Table
            else
                listSt = parser.getData(table, rowName, familyNamesValue); // get the Table

            int iter = 0;
            row = new String[listSt.size()];
            for (String st : listSt) {
                row[iter] = new String(st);
                iter += 1;
            }
        }

        // Delete the row/column
        else if (parse.get(0).equalsIgnoreCase("delete")) {
            parse.remove(0);

            Table table = null;
            try {
                table = connection.getTable(TableName.valueOf(parse.get(0))); // get Table
                parse.remove(0);
            } catch (IOException e) {
                e.printStackTrace();
            }
            String rowName = parse.get(0).toString(); // get Row
            parse.remove(0);

            String familyNameColumnName = parse.get(0).toString();
            parse.remove(0);

            row = new String[1]; // For String type return length is 1
            row[0] = parser.deleteData(table, rowName, familyNameColumnName); // get full Table
        }

        // Scan the table
        else if (parse.get(0).equalsIgnoreCase("scan")) {
            parse.remove(0);

            Table table = null;
            try {
                table = connection.getTable(TableName.valueOf(parse.get(0))); // get Table
                parse.remove(0);
            } catch (IOException e) {
                row = new String[1]; // For String type return length is 1
                row[0] = e.toString();
                e.printStackTrace();
            }

            List<String> listSt = parser.scanData(table); // get full Table

            int iter = 0;
            row = new String[listSt.size()];
            for (String st : listSt) {
                row[iter] = new String(st);
                iter += 1;
            }
        } else {
            // Handle the Proper Error !
            row = new String[1];
            row[0] = "Command Not Found.";
        }

        json.put("result", row);
        writer.println(json.toString());
        System.out.println(json);
    }
}