package com.inmobi.hive.test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by panos on 21/8/2016.
 */
public class MyPartition {
    String belongingDatabase;
    String belongingTable;
    String partitionName;
    URI location;
    Path partitionPath;
    List<FieldSchema> allFields;
    LinkedHashMap<String, String> keyValuePairs;
    List<FieldSchema> partitionKeys;
    List<String> partitionValues;
    List<String> bucketColsList;
    int bucketCount;

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
