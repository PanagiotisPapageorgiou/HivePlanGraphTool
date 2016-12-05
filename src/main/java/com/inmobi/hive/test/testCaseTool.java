//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.inmobi.hive.test;

import com.inmobi.hive.test.HiveTestSuite;

import java.io.*;
import java.util.List;
import java.util.Map;

/* This class provides an easy way to test our program
*  similarly to JUNIT:
*
*  - the setUp method will run a script
*  that will do any setup operations required before
*  submitting queries to Hive for Exareme translation
*  such as setting up tables/partitions etc. All setup
*  operations are to placed in a setup.sql script
*
*  -the runQueryScript method allows us when the "EXAREME"
*  flag is set to handle each HiveQL statement seperately
*  and choose if we want to convert its execution plan
*  into an Exareme Plan. If the "EXAREME" flag has been not
*  set then any HiveQL command will be run automatically.
*  Similarly to above, all HiveQL queries are to placed in a script.
*
*  -the tearDown method as its name says simply runs any DROP
*  command to destroy an table before the cluster stops.
*/

public class testCaseTool {
    private HiveTestSuite testSuite;
    private String setUpScriptPath;
    private String tearDownScriptPath;
    private String queryScriptPath;
    private String flag;
    private String timesFilePath;
    private String rootDirForHiveExaremeSession;
    private PrintWriter outputWriter;
    PrintWriter compileLogFile;
    PrintWriter resultsLogFile;
    int exaremeNodes;

    String madisPath = null;

    public testCaseTool(String s1, String s2, String s3, String f, String hiveExaremeRootSessionPath, String madisP, String timesForQueries) {
        this.setUpScriptPath = s1;
        this.tearDownScriptPath = s2;
        this.queryScriptPath = s3;
        this.flag = f;
        this.compileLogFile = null;
        this.resultsLogFile = null;
        this.rootDirForHiveExaremeSession = hiveExaremeRootSessionPath;
        this.madisPath = madisP;
        timesFilePath = timesForQueries;

        try {
            outputWriter = new PrintWriter(new BufferedWriter(new FileWriter(timesFilePath, true)));
        } catch (IOException e) {
            System.out.println("Exception creating new PrintWriter..."+e.getMessage());
            System.exit(0);
        }

    }

    public testCaseTool(String s1, String s2, String s3, String f, int size) {
        this.setUpScriptPath = s1;
        this.tearDownScriptPath = s2;
        this.queryScriptPath = s3;
        this.flag = f;
        this.compileLogFile = null;
        this.resultsLogFile = null;

        File timesFile = new File(timesFilePath);

        if(size == 1){

        }
        else if(size == 10){

        }
        else if(size == 50){

        }
        else{

        }

    }

    public void setUp(int numberOfDatanodes, int numberOfTaskTrackers, boolean allowDynamicPartitioning, int maxParts, int maxPartsPerNode, String exaremeMiniClusterIP, String exaremeMode, int exaNodes, int numOfReducers, String baseDir, String warehouseDir) throws Exception {

        if(exaremeMode.equals("EXAREME")){
            this.testSuite = new HiveTestSuite(1, 1);
            this.testSuite.createTestCluster(allowDynamicPartitioning, maxParts, maxPartsPerNode, exaremeMiniClusterIP, "EXAREME", exaNodes, numOfReducers, baseDir, warehouseDir);
            List results = this.testSuite.executeScript(this.setUpScriptPath, (Map)null);

            File rootFile = new File(rootDirForHiveExaremeSession);
            if(rootFile.exists() == false) {
                System.out.println("Creating session directory: "+rootFile.getPath());
                rootFile.mkdirs();
            }

        }
        else{
            this.testSuite = new HiveTestSuite(numberOfDatanodes, numberOfTaskTrackers);
            this.testSuite.createTestCluster(allowDynamicPartitioning, maxParts, maxPartsPerNode, exaremeMiniClusterIP, "HIVE", 0, numOfReducers, baseDir, warehouseDir);
            List results = this.testSuite.executeScript(this.setUpScriptPath, (Map)null);

            File rootFile = new File(rootDirForHiveExaremeSession);
            if(rootFile.exists() == false) {
                System.out.println("Creating session directory: "+rootFile.getPath());
                rootFile.mkdirs();
            }
        }

    }

    public void setUp(int numberOfDatanodes, int numberOfTaskTrackers, boolean allowDynamicPartitioning, int maxParts, int maxPartsPerNode, String exaremeMiniClusterIP, String exaremeMode, int exaNodes, int numOfReducers) throws Exception {

        if(exaremeMode.equals("EXAREME")){
            this.testSuite = new HiveTestSuite(1, 1);
            this.testSuite.createTestCluster(allowDynamicPartitioning, maxParts, maxPartsPerNode, exaremeMiniClusterIP, "EXAREME", exaNodes, numOfReducers);
            List results = this.testSuite.executeScript(this.setUpScriptPath, (Map)null);

            File rootFile = new File(rootDirForHiveExaremeSession);
            if(rootFile.exists() == false) {
                System.out.println("Creating session directory: "+rootFile.getPath());
                rootFile.mkdirs();
            }

        }
        else{
            this.testSuite = new HiveTestSuite(numberOfDatanodes, numberOfTaskTrackers);
            this.testSuite.createTestCluster(allowDynamicPartitioning, maxParts, maxPartsPerNode, exaremeMiniClusterIP, "HIVE", 0, numOfReducers);
            List results = this.testSuite.executeScript(this.setUpScriptPath, (Map)null);

            File rootFile = new File(rootDirForHiveExaremeSession);
            if(rootFile.exists() == false) {
                System.out.println("Creating session directory: "+rootFile.getPath());
                rootFile.mkdirs();
            }
        }

    }

    public void tearDown() throws Exception {
        List results = this.testSuite.executeScript(this.tearDownScriptPath, (Map)null);
        this.testSuite.shutdownTestCluster();
    }

    public void runQueryScript(String compileLogPath, String resultsLogPath, String exaremePlanPath, String caseLabel) throws Throwable {
        File f = new File(compileLogPath);
        if(f.exists() && !f.isDirectory()) {
            f.delete();
        }

        PrintWriter compileLogFile;
        try {
            compileLogFile = new PrintWriter(f);
        } catch (FileNotFoundException var9) {
            throw new RuntimeException("Failed to open FileOutputStream for outputQuery.txt", var9);
        }

        File f2 = new File(resultsLogPath);
        if(f2.exists() && !f2.isDirectory()) {
            f2.delete();
        }

        PrintWriter resultsLogFile;
        try {
            resultsLogFile = new PrintWriter(f2);
        } catch (FileNotFoundException var8) {
            throw new RuntimeException("Failed to open FileOutputStream for outputQuery.txt", var8);
        }

        this.testSuite.executeScript(this.queryScriptPath, (Map)null, (List)null, compileLogFile, resultsLogFile, exaremePlanPath, this.flag, this.madisPath, outputWriter, caseLabel);
        compileLogFile.close();
        resultsLogFile.close();
    }

    public void runOtherScript(String compileLogPath, String resultsLogPath, String exaremePlanPath, String caseLabel, String otherScript) throws Throwable {
        File f = new File(compileLogPath);
        if(f.exists() && !f.isDirectory()) {
            f.delete();
        }

        PrintWriter compileLogFile;
        try {
            compileLogFile = new PrintWriter(f);
        } catch (FileNotFoundException var9) {
            throw new RuntimeException("Failed to open FileOutputStream for outputQuery.txt", var9);
        }

        File f2 = new File(resultsLogPath);
        if(f2.exists() && !f2.isDirectory()) {
            f2.delete();
        }

        PrintWriter resultsLogFile;
        try {
            resultsLogFile = new PrintWriter(f2);
        } catch (FileNotFoundException var8) {
            throw new RuntimeException("Failed to open FileOutputStream for outputQuery.txt", var8);
        }

        this.testSuite.executeScript(otherScript, (Map)null, (List)null, compileLogFile, resultsLogFile, exaremePlanPath, this.flag, this.madisPath, outputWriter, caseLabel);
        compileLogFile.close();
        resultsLogFile.close();
    }

    public void closeFiles() {
        if(this.compileLogFile != null) {
            this.compileLogFile.close();
        }

        if(this.resultsLogFile != null) {
            this.resultsLogFile.close();
        }

        if(this.outputWriter != null) {
            this.outputWriter.close();
        }


    }
}
