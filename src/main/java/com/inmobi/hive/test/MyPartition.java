package com.inmobi.hive.test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by panos on 21/8/2016.
 */

/* Used to model a Hive Partition. Partitions and Tables are handled
   the same by Exareme while the same is not true for Hive. This class
   helps fix this difference by adding the extra columns a Hive Partition
   is missing (because they are the partition keys) and also creates
   a set of Queries that Exareme will run in order to properly handle
   the Hive Partition
*/

public class MyPartition {
    String belongingDatabase = null;
    String belongingTable = null;
    String partitionName = null;
    URI location = null;
    Path partitionPath = null;
    List<FieldSchema> allFields = new LinkedList<>();
    LinkedHashMap<String, String> keyValuePairs = new LinkedHashMap<>();
    List<FieldSchema> partitionKeys = new LinkedList<>();
    List<String> partitionValues;
    List<String> bucketColsList = new LinkedList<>();
    int bucketCount = 0;

    //Handle Exareme Tables differently from Hive Tables
    boolean isRootInput = false;
    String rootHiveTableDefinition = "";
    String rootHiveLocationPath = "";
    List<String> secondaryNeededQueries = new LinkedList<>(); //Extra queries needed to properly transform Hive Partition to Exareme Partition

    public MyPartition(){
        belongingDatabase = null;
        belongingTable = null;
        location = null;
        partitionName = null;
        bucketColsList = new LinkedList<>();
        allFields = new LinkedList<>();
        partitionKeys = new LinkedList<>();
        bucketCount = 0;
        partitionPath = null;
        keyValuePairs = new LinkedHashMap<>();
    }

    public MyPartition(String dbName, String tbName, String pName){
        belongingDatabase = dbName;
        belongingTable = tbName;
        partitionName = pName;
    }

    public List<String> getSecondaryNeededQueries() { return secondaryNeededQueries; }

    public void createRootHiveTableDefinition(){
        String columnsString = "";
        String columnsString2 = "";

        rootHiveTableDefinition = "";
        rootHiveTableDefinition = "select ";
        int i = 0;

        List<FieldSchema> fieldsAndPartitionColumns = new LinkedList<>();

        for(FieldSchema f : allFields){
            fieldsAndPartitionColumns.add(f);
        }

        //for(FieldSchema f : partitionKeys){
        //    fieldsAndPartitionColumns.add(f);
        //}

        for(FieldSchema f : fieldsAndPartitionColumns){
            String colName = f.getName();
            String colType = f.getType();
            String exaremeType = "";

            int colNumber = i+1;

            String arithmeticName = "c"+colNumber;

            if(colType.contains("int")){
                exaremeType = "INT";
            }
            else if(colType.contains("string")){
                exaremeType = "TEXT";
            }
            else if(colType.contains("decimal")){
                exaremeType = "DECIMAL(10,5)";
            }
            else if(colType.contains("char")){
                exaremeType = "TEXT";
            }
            else if(colType.contains("float")){
                exaremeType = "FLOAT";
            }
            else if(colType.contains("varchar")){
                exaremeType = "TEXT";
            }
            else if(colType.contains("date")){
                exaremeType = "DATE";
            }
            else if(colType.contains("double")){
                exaremeType = "REAL";
            }
            else if(colType.contains("double precision")){
                exaremeType = "REAL";
            }
            else if(colType.contains("bigint")){
                exaremeType = "BIGINT";
            }
            else if(colType.contains("smallint")){
                exaremeType = "SMALLINT";
            }
            else if(colType.contains("tinyint")){
                exaremeType = "TINYINT";
            }
            else{
                System.out.println("createRootHiveTableDefinition: Unknown Hive Type: "+colType);
                System.exit(0);
            }

            if(i == allFields.size() - 1){
                columnsString = columnsString.concat(" "+"cast("+arithmeticName+" as "+exaremeType+") as "+colName+" ");
            }
            else{
                columnsString = columnsString.concat(" "+"cast("+arithmeticName+" as "+exaremeType+") as "+colName+",");
            }

            System.out.println("createRootHiveTableDefinition: Current columnsString: "+columnsString);

            i++;
        }

        rootHiveTableDefinition = rootHiveTableDefinition.concat(columnsString);

        System.out.println("RootHiveTableDefinition is: "+ rootHiveTableDefinition);

        System.out.println("Partition Columns come last for partition... Partition Columns: " + partitionKeys.toString());

        System.out.println("Creating extra required Queries for this Hive Partition...");

        int j=0;
        for(FieldSchema partitionCol : partitionKeys){
            String alterQuery = "";
            String updateQuery = "";
            String partitionValue = partitionValues.get(j);

            String colName = partitionCol.getName();
            String colType = partitionCol.getType();
            String exaremeType = "";

            if(colType.contains("int")){
                exaremeType = "INT";
            }
            else if(colType.contains("string")){
                exaremeType = "TEXT";
            }
            else if(colType.contains("decimal")){
                exaremeType = "NUM";
            }
            else if(colType.contains("char")){
                exaremeType = "TEXT";
            }
            else if(colType.contains("float")){
                exaremeType = "NUM";
            }
            else{
                System.out.println("createRootHiveTableDefinition: Unknown Hive Type: (Partition Case) - "+colType);
                System.exit(0);
            }

            alterQuery = " add column " + colName + " " + exaremeType + " ";
            if(exaremeType.equals("TEXT"))
                updateQuery = " set " + colName + " = '"+partitionValue+"' ";
            else
                updateQuery = " set " + colName + " = "+partitionValue+" ";

            secondaryNeededQueries.add(alterQuery);
            secondaryNeededQueries.add(updateQuery);

            j++;
        }


    }

    public void setRootHiveLocationPath(String path) {
        rootHiveLocationPath = path; //Set the location path leading to the associated file in the HDFS
        isRootInput = true;
        createRootHiveTableDefinition();

        System.out.println("RootHiveTableLocation is: "+ rootHiveLocationPath);
    }

    public String getRootHiveLocationPath() { return rootHiveLocationPath; }

    public String getRootHiveTableDefinition() { return rootHiveTableDefinition; }

    public boolean getIsRootInput() { return isRootInput; }

    public LinkedHashMap<String, String> getKeyValuePairs() { return keyValuePairs; }

    public void setKeyValuePairs(LinkedHashMap<String, String> map ) { keyValuePairs = map; }

    public String getBelongingDataBaseName() { return belongingDatabase; }

    public void setBelongingDatabaseName(String dbName) { belongingDatabase = dbName; }

    public String getBelogingTableName() { return belongingTable; }

    public void setBelongingTableName(String tbName) { belongingTable = tbName; }

    public String getPartitionName() { return partitionName; }

    public void setPartitionName(String pName) { partitionName = pName; }

    public URI getURIdetails() { return location; }

    public void setURIdetails(URI d) { location = d; }

    public String getURIAuthority() { return location.getAuthority(); }

    public String getURIHostName() { return location.getHost(); }

    public String getURILocationPath() { return location.getPath(); }

    public String getURIQuery() { return location.getQuery(); }

    public String getURIScheme() { return location.getScheme(); }

    public String getURIFullLocationPath() { return location.getSchemeSpecificPart(); }

    public int getURIPort() { return location.getPort(); }

    public String getPartitionHDFSPath() { return partitionPath.toString(); }

    public void setPartitionHDFSPath(Path path) { partitionPath = path; }

    public List<FieldSchema> getAllFields() { return allFields; }

    public void setAllFields(List<FieldSchema> c) { allFields = c; }

    public List<FieldSchema> getAllPartitionKeys() { return partitionKeys; }

    public void setAllPartitionKeys(List<FieldSchema> c) { partitionKeys = c; }

    public List<String> getAllValues() { return partitionValues; }

    public void setAllPartitionValues(List<String> v) { partitionValues = v; }


    public void setBucketColsList(List<String> bList) { bucketColsList = bList; }

    public List<String> getBucketColsList() { return bucketColsList; }

    public void addBucketCol(String bucketCol) {

        if(bucketColsList.size() > 0){
            for(String s : bucketColsList){
                if(bucketCol.equals(s)) return;
            }
        }
        bucketColsList.add(bucketCol);
        bucketCount++;

    }

    public int getBucketCount() { return bucketCount; }

    public void setBucketCount(int b) { bucketCount = b; }

}
