package com.view;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Hello world!
 *
 */
public class App {

    public static void showUsage() {

        System.out.println(
                "01. create 'tab1', 'fam1', 'fam2' // creating table\n" +
                "02. list // listing table\n" +
                "03. put 'tab1', 'row2', 'fam1:city', 'Chittagong' // putting data in the table\n" +
                "04. scan 'tab1' // scan full table\n" +
                "05. get 'tab1', 'row2' // get full row\n" +
                "06. get 'tab1', 'row1', 'fam1:id' // get row with familyname:columnvalue\n" +
                "07. get 'tab1', 'row2', 'fam1:id', 'fam1:name' // get row with familyname:columnvalue\n" +
                "08. is_disabled 'tab1' // check if table disabled\n" +
                "09. is_enabled 'tab1' // check if table enabled\n" +
                "10. disable 'tab1' // Disable table\n" +
                "11. enable 'tab1' // Re-enable table\n" +
                "12. exists, 'tab1' // Check if the table exists\n" +
                "13. drop 'test04' // drop a table\n" +
                "14. delete 'tab1', 'row2', 'fam1:city'\");");
    }


    public static void main( String[] args ) throws IOException {

        HbaseParser parser = new HbaseParser();

        Connection connection = parser.makeConnection(); // make connection
        Admin admin = parser.getAdmin(connection); // get the admin

        // String stmt = new String("create 'tab1', 'fam1', 'fam2'"); // creating table
        // String stmt = new String("list"); // listing table
        // String stmt = new String("put 'tab1', 'row2', 'fam1:city', 'Chittagong'"); // putting data in the table
        // String stmt = new String("scan 'tab1'"); // scan full table
        // String stmt = new String("get 'tab1', 'row2'"); // get full row
        // String stmt = new String("get 'tab1', 'row1', 'fam1:id'"); // get row with familyname:columnvalue
        // String stmt = new String("get 'tab1', 'row2', 'fam1:id', 'fam1:name'"); // get row with familyname:columnvalue
        // String stmt = new String("is_disabled 'tab1'"); // check if table disabled
        // String stmt = new String("is_enabled 'tab1'"); // check if table enabled
        // String stmt = new String("disable 'tab1'"); // Disable table
        // String stmt = new String("enable 'tab1'"); // Re-enable table
        // String stmt = new String("exists, 'tab1"); // Check if the table exists
        // String stmt = new String("drop 'test04'");
        // String stmt = new String("delete 'tab1', 'row2', 'fam1:city'");

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String stmt = new String("describe 'tablk1'");
        // String stmt;
        // String stmt = new String("create 'tablk1', 'fam1', 'fam2'");
        System.out.println("Implemented Commands: ");
        showUsage();
        System.out.println("");

        // while ((stmt = br.readLine()) != null)
        {
            String outputStatement = new String();

            List<String> parse = StatementParser.removeWhiteSpaceAndQuotes(stmt);

            // Creating the table
            if (parse.get(0).equalsIgnoreCase("create")) {
                parse.remove(0);
                TableName tableName = TableName.valueOf(parse.get(0));
                parse.remove(0);

                List<String> familyNames = new ArrayList<>(parse);
                outputStatement = parser.createTable(admin, tableName, familyNames); // Create Table
                System.out.println("sst: " + outputStatement);
            }

            // Describe the table
            else if (parse.get(0).equalsIgnoreCase("describe")) {
                parse.remove(0);

                TableName tableName = TableName.valueOf(parse.get(0));
                parse.remove(0);

                List<String> listTable = parser.describeTable(admin, tableName); // Create Table
                for (String table : listTable) {
                    outputStatement = new String(table);
                    System.out.println("sst: " + outputStatement);
                }
            }

            // Check if the table disabled
            else if (parse.get(0).equalsIgnoreCase("is_disabled")) {
                parse.remove(0);
                TableName tableName = TableName.valueOf(parse.get(0));
                outputStatement = String.valueOf(parser.isTableDisabled(admin, tableName));
                System.out.println("sst: " + outputStatement);
            }

            // Check if the table is enable
            else if (parse.get(0).equalsIgnoreCase("is_enabled")) {
                parse.remove(0);
                TableName tableName = TableName.valueOf(parse.get(0));
                outputStatement = String.valueOf(parser.isTableEnabled(admin, tableName));
                System.out.println("sst: " + outputStatement);
            }

            // Disable the table
            else if (parse.get(0).equalsIgnoreCase("disable")) {
                parse.remove(0);
                TableName tableName = TableName.valueOf(parse.get(0));
                outputStatement = parser.disableTable(admin, tableName);
                System.out.println("sst: " + outputStatement);
            }

            // Check if the table disabled
            else if (parse.get(0).equalsIgnoreCase("enable")) {
                parse.remove(0);
                TableName tableName = TableName.valueOf(parse.get(0));
                outputStatement = parser.enableTable(admin, tableName);
                System.out.println("sst: " + outputStatement);
            }

            // Check if the table Exists
            else if (parse.get(0).equalsIgnoreCase("exists")) {
                parse.remove(0);
                TableName tableName = TableName.valueOf(parse.get(0));
                outputStatement = parser.isTableExists(admin, tableName);
                System.out.println("sst: " + outputStatement);

            }

            // Check if the table Exists
            else if (parse.get(0).equalsIgnoreCase("drop")) {
                parse.remove(0);
                TableName tableName = TableName.valueOf(parse.get(0));
                outputStatement = parser.dropTable(admin, tableName);
                System.out.println("sst: " + outputStatement);
            }

            // List the table
            else if (parse.get(0).equalsIgnoreCase("list")) {
                List<String> listTable = parser.listTable(admin); // Create Table
                for (String table : listTable) {
                    outputStatement = new String(table);
                    System.out.println("sst: " + outputStatement);
                }
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

                List<String> familyNamesValue = new ArrayList<>(parse);
                outputStatement = parser.putData(table, rowName, familyNamesValue); // Put in the Table
                System.out.println("sst: " + outputStatement);
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

                List<String> familyNamesValue = new ArrayList<>(parse);

                List<String> listSt = new ArrayList<String>();
                if(familyNamesValue.size() <= 0)
                    listSt = parser.getData(table, rowName); // get the Table
                else
                    listSt = parser.getData(table, rowName, familyNamesValue); // get the Table

                for (String st : listSt) {
                    outputStatement = new String(st);
                    System.out.println("sst: " + outputStatement);
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

                outputStatement = parser.deleteData(table, rowName, familyNameColumnName); // get full Table
                System.out.println("sst: " + outputStatement);
            }

            // Scan the table
            else if (parse.get(0).equalsIgnoreCase("scan")) {
                parse.remove(0);

                Table table = null;
                try {
                    table = connection.getTable(TableName.valueOf(parse.get(0))); // get Table
                    parse.remove(0);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                List<String> listSt = new ArrayList<String>();
                listSt = parser.scanData(table); // get full Table

                for (String st : listSt) {
                    outputStatement = new String(st);
                    System.out.println("sst: " + outputStatement);
                }

            }


            System.out.println("");
        }
    }
}
