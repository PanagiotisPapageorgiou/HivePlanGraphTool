package com.inmobi.hive.test;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hive.service.cli.HiveSQLException;

import java.io.*;
import java.util.*;

/*
 * This class is the primary interface to create new test cases using the 
 * minicluster and mini hive server.  The basic sequence a test case would 
 * need to follow is:
 * 
 *   HiveTestSuite testSuite = testSuite = new HiveTestSuite();
 *   testSuite.createTestCluster();
 *   testSuite.executeScript(<some_script>);
 *   ...
 *   testSuite.shutdownTestCluster();
 *   
 * In addition, a test case can directly reference HDFS by retrieving the 
 * FileSystem object:
 * 
 *   FileSystem fs = testSuite.getFS();
 *   fs.copyFromLocalFile(inputData, rawHdfsData);
 * 
 */
public class HiveTestSuite {
    
    private HiveTestCluster cluster;
    int numberOfDataNodes;
    int numberOfNodeManagers;

    public HiveTestSuite(int numData, int numManagers){
        numberOfDataNodes = numData;
        numberOfNodeManagers = numManagers;
    }

    public void createTestCluster() {
        this.createTestCluster(false, 0 , 0, "");
    }

    public void createTestCluster(boolean allowDynamicPartitioning, int maxParts, int maxPartsPerNode, String exaremeIP) {
        cluster = new HiveTestCluster(numberOfDataNodes, numberOfNodeManagers, exaremeIP);
        try {
            cluster.start(allowDynamicPartitioning, maxParts, maxPartsPerNode);
        } catch (Exception e) {
            throw new RuntimeException("Unable to start test cluster", e);
        }
    }
    
    public void shutdownTestCluster() {
        if (cluster == null) {
            return;
        }
        try {
            cluster.stop();
        } catch (Exception e) {
            throw new RuntimeException("Unable to stop test cluster", e);
        }
    }
    
    public List<String> executeScript(String scriptFile) {
        return executeScript(scriptFile, null, null, null, null, null, null);
    }
    
    public List<String> executeScript(String scriptFile, Map<String, String> params) {
        return executeScript(scriptFile, params, null, null, null, null, null);
    }
    
    public List<String> executeScript(String scriptFile, Map<String, String> params, List<String> excludes, PrintWriter compileLogFile, PrintWriter resultsLogFile, String exaremePlanPath, String flag) {
        HiveScript hiveScript = new HiveScript(scriptFile, params, excludes);
        if (cluster == null) {
            throw new IllegalStateException("No active cluster to run script with");
        }

        //if(scriptFile.equals("src/test/resources/scripts/tpcds18Case1.sql")){
            //System.out.println("This is tpcds18 query script!");
        //}
        List<String> results = null;
        try {
            results = cluster.executeStatements(hiveScript.getStatements(), compileLogFile, resultsLogFile, exaremePlanPath, flag);
        } catch (HiveSQLException e) {
            throw new RuntimeException("Unable to execute script", e);
        }
        return results;
    }
    
    public FileSystem getFS() {
        if (cluster == null) {
            return null;
        }
        return this.cluster.getFS();
    }

}
