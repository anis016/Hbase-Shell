package com.view;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sayed on 30.08.17.
 */
public class HbaseParser {

    private static Logger logger = Logger.getLogger(HbaseParser.class.getName());

    public Connection makeConnection() {

        Connection connection = null;

        // Hbase Configurationsmave
        Configuration hbaseConfiguration = HBaseConfiguration.create();
        hbaseConfiguration.setInt("timeout", 120000);
        hbaseConfiguration.set("hbase.zookeeper.property.clientPort", CONSTANTS.zKclientPort);
        hbaseConfiguration.set("hbase.zookeeper.quorum", CONSTANTS.zKquorum);
        hbaseConfiguration.set("zookeeper.znode.parent", CONSTANTS.zKNodeParent);

        try {
            connection = ConnectionFactory.createConnection(hbaseConfiguration);
            logger.info("Hbase Running!");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Connection with Hbase Error!");
            System.exit(1);
        }

        return connection;
    }

    public Admin getAdmin(Connection connection) {

        Admin hbaseAdmin = null;
        try {
            hbaseAdmin = connection.getAdmin();
            logger.debug("hbaseAdmin creation success");
        } catch (IOException e) {
            logger.error("Could not setup HBaseAdmin as no master is running, Start HBase?...");
        }

        return hbaseAdmin;
    }

    public String createTable(Admin hbaseAdmin, TableName tableName, List<String> family_name) {
        String st = new String();

        try {
            // Check if Table exists. If not, then create Table
            if (hbaseAdmin.tableExists(tableName)) {
                st = String.format("Table already exists.");
                // System.out.println("Table already exists.");
            } else {
                // System.out.println("Creating Table");

                // Creating the Table
                HTableDescriptor hTable = new HTableDescriptor(tableName);
                for(String family : family_name) {
                    hTable.addFamily(new HColumnDescriptor(family.toString()));
                }

                hbaseAdmin.createTable(hTable);
                st = String.format("Table " + tableName + " Created !");
                // System.out.println("Table " + tableName + " Created !");
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return st;
    }

    public boolean isTableDisabled(Admin hbaseAdmin, TableName tableName) {
        Boolean checkTable = false;
        try {
            checkTable = hbaseAdmin.isTableDisabled(tableName);
            System.out.println(checkTable);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return checkTable;
    }

    public boolean isTableEnabled(Admin hbaseAdmin, TableName tableName) {
        Boolean checkTable = false;
        try {
            checkTable = hbaseAdmin.isTableEnabled(tableName);
            System.out.println(checkTable);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return checkTable;
    }

    public String isTableExists(Admin hbaseAdmin, TableName tableName) {
        String st = new String();
        try {
            if (hbaseAdmin.tableExists(tableName)) {
                st = String.format("Table " + tableName + " exist");
                // System.out.println("Table " + tableName + " exist");
            } else {
                st = String.format("Table " + tableName + " does not exist");
                // System.out.println("Table " + tableName + " does not exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return st;
    }

    public String dropTable(Admin hbaseAdmin, TableName tableName) {
        String st = new String();

        try {
            hbaseAdmin.disableTable(tableName);
            hbaseAdmin.deleteTable(tableName);
            st = String.format("1 Table successfully dropped");
            // System.out.println("1 Table successfully dropped");
        } catch (IOException e) {
            e.printStackTrace();
        }

        return st;
    }

    public String disableTable(Admin hbaseAdmin, TableName tableName) {
        String st = new String();
        try {
            Boolean bool = isTableDisabled(hbaseAdmin, tableName);
            if(bool == false) {
                hbaseAdmin.disableTable(tableName);
                st = String.format("Table Disabled");
                // System.out.println("Table Disabled");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return st;
    }

    public String enableTable(Admin hbaseAdmin, TableName tableName) {
        String st = new String();
        try {
            Boolean bool = isTableDisabled(hbaseAdmin, tableName);
            if(bool == true) {
                hbaseAdmin.enableTable(tableName);
                st = String.format("Table Enabled");
                // System.out.println("Table Enabled");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return st;
    }

    public List<String> describeTable(Admin hbaseAdmin, TableName tableName) {

        List<String> list = new ArrayList<String>();
        try {
            HTableDescriptor[] tableDescriptor = hbaseAdmin.listTables();

            for (int i=0; i < tableDescriptor.length;i++ ){
                String name = tableDescriptor[i].getNameAsString();
                if (tableName.toString().equalsIgnoreCase(name)) {
                    Collection<HColumnDescriptor> collection = new ArrayList<HColumnDescriptor>();
                    collection = tableDescriptor[i].getFamilies();
                    Iterator<HColumnDescriptor> iterator = collection.iterator();
                    while(iterator.hasNext()) {
                        list.add(iterator.next().toString());
                    }
                }
            }
        } catch (IOException e){
            e.printStackTrace();
        }

        return list;
    }

    public List<String> listTable (Admin hbaseAdmin) {

        List<String> list = new ArrayList<String>();
        try {
            HTableDescriptor[] tableDescriptor = hbaseAdmin.listTables();
            // printing all the table names.
            for (int i=0; i < tableDescriptor.length;i++ ){
                // System.out.println(tableDescriptor[i].getNameAsString());
                list.add(tableDescriptor[i].getNameAsString().toString());
            }
        } catch (IOException e){
            e.printStackTrace();
        }

        return list;
    }

    public String  putData(Table table, String rowName, List<String> familyNameColName) {
        String st = new String();

        String[] splitFamilyNameColName = familyNameColName.get(0).split(":");
        String familyName = splitFamilyNameColName[0];
        String colName = splitFamilyNameColName[1];

        familyNameColName.remove(0);

        // Add some data to the table
        try {
            Put put = new Put(Bytes.toBytes(rowName));  // which row ?
            put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(colName),Bytes.toBytes(familyNameColName.get(0)));
            table.put(put);
            st = String.format("1 record added");
            // System.out.println("1 record added");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return st;
    }

    private String cellIterate(Cell cell) {
        String st = new String();
        String value = new String(CellUtil.cloneValue(cell));
        if(value != null) {
            String row = new String(CellUtil.cloneRow(cell));
            String family = new String(CellUtil.cloneFamily(cell));
            String column = new String(CellUtil.cloneQualifier(cell));
            long timeStamp = cell.getTimestamp();
            st = String.format("%-20s column=%s:%s, timestamp=%s, value=%s\n", row, family, column, timeStamp, value);
            // System.out.printf("%-20s column=%s:%s, timestamp=%s, value=%s\n", row, family, column, timeStamp, value);
        }
        return st;
    }

    public List<String> getData(Table table, String rowName) {
        List<String> st = new ArrayList<String>();

        try {
            Get get = new Get(Bytes.toBytes(rowName)); // which row ?
            Result res = table.get(get);
            for (Cell cell : res.listCells()) {
                st.add(cellIterate(cell));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return st;
        // filter example: http://www.baeldung.com/hbase
    }

    public List<String> getData(Table table, String rowName, List<String> familyNamesValueList) {
        List<String> st = new ArrayList<String>();

        try {
            Get get = new Get(Bytes.toBytes(rowName));
            Result res = table.get(get);
            // System.out.println(res);
            int counter = 0;
            for (String familyNamesValue : familyNamesValueList) {
                String[] familyNameSplit = familyNamesValue.split(":");
                String familyName  = familyNameSplit[0];
                String columnValue = familyNameSplit[1];

                String value =  Bytes.toString(res.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnValue)));
                long timeStamp = res.rawCells()[counter].getTimestamp();
                if (value != null) {
                    String stringCell = new String();
                    stringCell = String.format("%-20s column=%s:%s, timestamp=%s, value=%s\n", rowName, familyName, columnValue, timeStamp, value);
                    st.add(stringCell);
                    // System.out.printf("%-20s column=%s:%s, timestamp=%s, value=%s\n", rowName, familyName, columnValue, timeStamp, value);
                }
                counter ++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return st;
        // filter example: http://www.baeldung.com/hbase
    }

    public List<String> scanData(Table table) {
        List<String> st = new ArrayList<String>();

        Scan scan = new Scan();
        try {
            ResultScanner scanner = table.getScanner(scan);
            for (Result res : scanner) {
                for (Cell cell : res.listCells()) {
                    st.add(cellIterate(cell));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return st;
        // filter example: http://www.baeldung.com/hbase
    }

    public String deleteData(Table table, String rowName, String familyNamesValue) {
        String st = new String();

        String[] familyNameSplit = familyNamesValue.split(":");
        String familyName  = familyNameSplit[0];
        String columnValue = familyNameSplit[1];

        try {
            Delete delete = new Delete(Bytes.toBytes(rowName)); // which row ?
            delete.addColumns(Bytes.toBytes(familyName), Bytes.toBytes(columnValue));
            table.delete(delete);
            st = String.format("1 record deleted");
            // System.out.println("1 record deleted");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return st;
    }
}
