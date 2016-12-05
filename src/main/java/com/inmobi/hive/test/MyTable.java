package com.inmobi.hive.test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

import java.net.URI;
import java.util.*;

/**
 * Created by panos on 21/8/2016.
 */
public class MyTable { //Represents a Hive or Exareme Table
    String belongingDatabase; //Belonging database for example: tpcds_db.db
    String tableName;
    URI location; //Additional (Metastore) Location details
    Path dataLocation; //Full HDFS LocationPath
    List<FieldSchema> allCols;
    ArrayList<StructField> allFields;
    boolean hasPartitions;
    boolean isAFile; //Input/Output Entity might be file in Hive
    List<FieldSchema> partitionKeys;
    List<MyPartition> allPartitions; //List of associated Partitions
    LinkedHashMap<List<FieldSchema>, LinkedHashMap<List<String> , MyPartition>> mapOfKeyValuePartitions;

    List<MyTable> ancestorTables = new LinkedList<>();

    String sqliteDefinition = "";

    //Handle Exareme Tables differently from Hive Tables
    boolean isRootInput = false;
    String rootHiveTableDefinition = "";
    String rootHiveLocationPath = "";

    public MyTable(){
        belongingDatabase = null;
        tableName = null;
        location = null;
        dataLocation = null;
        allCols = new LinkedList<>();
        allFields = new ArrayList<>();
        partitionKeys = new LinkedList<>();
        allPartitions = new LinkedList<>();
        mapOfKeyValuePartitions = new LinkedHashMap<>();
        hasPartitions=false;
        isAFile=false;
        isRootInput = false;
        rootHiveTableDefinition = "";
        rootHiveLocationPath = "";
    }

    public MyTable(String dbName, String tbName){
        belongingDatabase = dbName;
        tableName = tbName;
        isAFile = false;

        isRootInput = false;
        rootHiveTableDefinition = "";
        rootHiveLocationPath = "";

        allCols = new LinkedList<>();
        allFields = new ArrayList<>();
        partitionKeys = new LinkedList<>();
        allPartitions = new LinkedList<>();
        mapOfKeyValuePartitions = new LinkedHashMap<>();
        hasPartitions=false;
    }

    public String getSqliteDefinition() { return sqliteDefinition;}

    public void addAncestorTable(MyTable table){

        if(ancestorTables.size() > 0){
            for(MyTable m : ancestorTables){
                if(m.getTableName().equals(table.getTableName())){
                    if(m.getBelongingDataBaseName().equals(table.getBelongingDataBaseName())){
                        return;
                    }
                }
            }
        }
        else{
            ancestorTables.add(table);
        }

    }

    public List<MyTable> getAncestorTables() { return ancestorTables; }

    public void createRootHiveTableDefinition(){
        String columnsString = "";

        rootHiveTableDefinition = "";
        rootHiveTableDefinition = "select ";
        sqliteDefinition = "create table "+tableName.toLowerCase()+" (";
        int i = 0;

        for(FieldSchema f : allCols){
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

            if(i == allCols.size()  - 1){
                columnsString = columnsString.concat(" "+"cast("+arithmeticName+" as "+exaremeType+") as "+colName+" ");
                sqliteDefinition = sqliteDefinition.concat(colName+" "+exaremeType+" )");
            }
            else{
                columnsString = columnsString.concat(" "+"cast("+arithmeticName+" as "+exaremeType+") as "+colName+",");
                sqliteDefinition = sqliteDefinition.concat(colName+" "+exaremeType+" , ");
            }

            System.out.println("createRootHiveTableDefinition: Current columnsString: "+columnsString);

            i++;
        }

        rootHiveTableDefinition = rootHiveTableDefinition.concat(columnsString);

        System.out.println("RootHiveTableDefinition is: "+ rootHiveTableDefinition);
    }

    public void setRootHiveLocationPath(String path) {
        rootHiveLocationPath = path; //Set the location path leading to the associated file in the HDFS
        isRootInput = true;
        createRootHiveTableDefinition();

        System.out.println("RootHiveTableLocation is: "+ rootHiveLocationPath);
    }

    public String getRootHiveLocationPath() { return rootHiveLocationPath; }

    public String getRootHiveTableDefinition() { return rootHiveTableDefinition; }

    public MyTable(String fileName, boolean isAFile){
        if(isAFile == true){
            tableName = fileName;
            isAFile = true;

            isRootInput = false;
            rootHiveTableDefinition = "";
            rootHiveLocationPath = "";

            allCols = new LinkedList<>();
            allFields = new ArrayList<>();
            partitionKeys = new LinkedList<>();
            allPartitions = new LinkedList<>();
            mapOfKeyValuePartitions = new LinkedHashMap<>();
            hasPartitions=false;
        }
        else{
            System.out.println("Constructor for File Entity invoked with false parameter!");
            System.exit(0);
        }
    }

    public boolean getIsRootInput() { return isRootInput; }

    public void setIsAFile(boolean f) { isAFile = f; }

    public boolean getIsAFile() { return isAFile; }

    public boolean getHasPartitions() { return hasPartitions; }

    public void setHasPartitions(boolean b) { hasPartitions=b; }

    public String getBelongingDataBaseName() { return belongingDatabase; }

    public void setBelongingDatabaseName(String dbName) { belongingDatabase = dbName; }

    public String getTableName() { return tableName; }

    public void setTableName(String tbName) { tableName = tbName; }

    public URI getURIdetails() { return location; }

    public void setURIdetails(URI d) { location = d; }

    public String getURIAuthority() { return location.getAuthority(); }

    public String getURIHostName() { return location.getHost(); }

    public String getURILocationPath() { return location.getPath(); }

    public String getURIQuery() { return location.getQuery(); }

    public String getURIScheme() { return location.getScheme(); }

    public String getURIFullLocationPath() { return location.getSchemeSpecificPart(); }

    public int getURIPort() { return location.getPort(); }

    public String getTableHDFSPath() { return dataLocation.toString(); }

    public void setTableHDFSPath(Path path) { dataLocation = path; }

    public List<FieldSchema> getAllCols() { return allCols; }

    public void setAllCols(List<FieldSchema> c) { allCols = c; }

    public ArrayList<StructField> getAllFields() { return allFields; }

    public void setAllFields(ArrayList<StructField> c) { allFields = c; }

    public List<FieldSchema> getAllPartitionKeys() { return partitionKeys; }

    public void setAllPartitionKeys(List<FieldSchema> c) { partitionKeys = c; }

    public List<MyPartition> getAllPartitions() { return allPartitions; }

    public void setAllPartitions(List<MyPartition> parts) { allPartitions = parts; }

    public boolean compareFieldSchemaCombos(List<FieldSchema> a, List<FieldSchema> b){

        if(a.size() == b.size()){
            for(int i = 0; i < a.size(); i++){
                if(a.get(i).getName().equals(b.get(i).getName())){
                    if(a.get(i).getType().equals(b.get(i).getType())){
                        continue;
                    }
                    else{
                        return false;
                    }
                }
                else{
                    return false;
                }
            }
            return true;
        }
        return false;
     }

    public boolean compareFieldValueCombos(List<String> a, List<String> b){

        if(a.size() == b.size()){
            for(int i = 0; i < a.size(); i++){
                if(a.get(i).equals(b.get(i))){
                    continue;
                }
                else{
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public void addPartition(MyPartition partition){
        for(MyPartition p : allPartitions){
            if(p.getPartitionName().equals(partition.getPartitionName())) return;
        }
        allPartitions.add(partition);
        List<FieldSchema> fieldSchemaCombination = partition.getAllPartitionKeys();
        if(mapOfKeyValuePartitions.size() > 0){
            boolean fieldComboExists = false;
            for(Map.Entry<List<FieldSchema>, LinkedHashMap<List<String> , MyPartition>> entry : mapOfKeyValuePartitions.entrySet()) {
                if (compareFieldSchemaCombos(entry.getKey(), fieldSchemaCombination)) {
                    fieldComboExists = true;
                    LinkedHashMap<List<String> , MyPartition > insideMap = entry.getValue();
                    boolean ValueComboExists = false;
                    for(Map.Entry<List<String>, MyPartition> entry2 : insideMap.entrySet()){
                        List<String> valueList = entry2.getKey();
                        if(compareFieldValueCombos(valueList, partition.getAllValues())){
                            ValueComboExists = true;
                            break;
                        }
                    }
                    if(ValueComboExists == false){
                        insideMap.put(partition.getAllValues(), partition);
                        System.out.println("Pair: [ValueCombo: "+partition.getAllValues().toString()+" - PartitionName: "+partition.getPartitionName()+" ]");
                        System.out.println("Added for FieldCombo: "+fieldSchemaCombination.toString());
                        mapOfKeyValuePartitions.put(fieldSchemaCombination, insideMap);
                    }
                    break;
                }
            }
            if(fieldComboExists == false){
                List<String> valueCombination = partition.getAllValues();
                LinkedHashMap<List<String>, MyPartition> newValueMap = new LinkedHashMap<>();
                newValueMap.put(valueCombination, partition);
                System.out.println("Pair: [ValueCombo: "+valueCombination.toString()+" - PartitionName: "+partition.getPartitionName()+" ]");
                System.out.println("Added for FieldCombo: "+fieldSchemaCombination.toString());
                mapOfKeyValuePartitions.put(fieldSchemaCombination, newValueMap);
            }
        }
        else{
            List<String> valueCombination = partition.getAllValues();
            LinkedHashMap<List<String>, MyPartition> newValueMap = new LinkedHashMap<>();
            newValueMap.put(valueCombination, partition);
            System.out.println("Pair: [ValueCombo: "+valueCombination.toString()+" - PartitionName: "+partition.getPartitionName()+" ]");
            System.out.println("Added for FieldCombo: "+fieldSchemaCombination.toString());
            mapOfKeyValuePartitions.put(fieldSchemaCombination, newValueMap);
        }
    }

    public LinkedHashMap<List<FieldSchema>, LinkedHashMap<List<String> , MyPartition>> getPartitionKeysValuesMap() { return mapOfKeyValuePartitions; }

    public void setPartitionKeysValuesMap(LinkedHashMap<List<FieldSchema>, LinkedHashMap<List<String> , MyPartition>> map) { mapOfKeyValuePartitions = map; }

}
