//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.inmobi.hive.test;

import com.inmobi.hive.test.HiveTestSuite;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

public class testCaseTool {
    private HiveTestSuite testSuite;
    private String setUpScriptPath;
    private String tearDownScriptPath;
    private String queryScriptPath;
    private String flag;
    PrintWriter compileLogFile;
    PrintWriter resultsLogFile;

    public testCaseTool(String s1, String s2, String s3, String f) {
        this.setUpScriptPath = s1;
        this.tearDownScriptPath = s2;
        this.queryScriptPath = s3;
        this.flag = f;
        this.compileLogFile = null;
        this.resultsLogFile = null;
    }

    public void setUp(int numberOfDatanodes, int numberOfTaskTrackers, boolean allowDynamicPartitioning, int maxParts, int maxPartsPerNode) throws Exception {
        this.testSuite = new HiveTestSuite(numberOfDatanodes, numberOfTaskTrackers);
        this.testSuite.createTestCluster(allowDynamicPartitioning, maxParts, maxPartsPerNode);
        List results = this.testSuite.executeScript(this.setUpScriptPath, (Map)null);
    }

    public void tearDown() throws Exception {
        List results = this.testSuite.executeScript(this.tearDownScriptPath, (Map)null);
        this.testSuite.shutdownTestCluster();
    }

    public void runQueryScript(String compileLogPath, String resultsLogPath, String exaremePlanPath) throws Throwable {
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

        this.testSuite.executeScript(this.queryScriptPath, (Map)null, (List)null, compileLogFile, resultsLogFile, exaremePlanPath, this.flag);
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


    }
}
