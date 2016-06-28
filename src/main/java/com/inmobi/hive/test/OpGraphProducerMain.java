package com.inmobi.hive.test;

/**
 * Created by panos on 11/5/2016.
 */
public class OpGraphProducerMain {

    public static void main(String[] args){

        String setupScript = "src/main/resources/scripts/tpcds18setup.sql";
        String tearDownScript = "src/main/resources/scripts/tpcds18teardown.sql";
        String queryScript = "src/main/resources/scripts/tpcds18Case1.sql";
        String exaremeOpGraphFile = "src/main/resources/files/exaremeGraphsLog.txt";
        String executionResultsFile = "src/main/resources/files/resultsLog.txt";

        int numberOfDatanodes = 6;
        int numberOfTaskTrackers = 4;

        testCaseTool testTool = new testCaseTool(setupScript, tearDownScript, queryScript, "EXAREME");

        try{
            System.out.println("Setup Cluster with "+numberOfDatanodes+" and "+numberOfTaskTrackers+" and load tables...");
            testTool.setUp(numberOfDatanodes, numberOfTaskTrackers, true, 1000, 100);
        }
        catch(Exception ex){
            System.out.println("Setup Failed!");
        }

        try{
            System.out.println("Run Query...");
            testTool.runQueryScript(exaremeOpGraphFile, executionResultsFile);
        }
        catch(Throwable ex){
            System.out.println("Query execution failed! Exception: "+ex.toString());
            testTool.closeFiles();
        }

        try{
            System.out.println("Tearing down tables...");
            testTool.tearDown();
        }
        catch(Exception ex){
            System.out.println("Teardown Failed!");
        }

    }

}

