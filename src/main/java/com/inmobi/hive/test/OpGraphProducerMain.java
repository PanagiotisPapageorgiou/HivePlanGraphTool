package com.inmobi.hive.test;
/**
 * Created by panos on 11/5/2016.
 */
public class OpGraphProducerMain {

    public static void main(String[] args){

        /*-----Setup Settings------*/
        String setupScript = "src/main/resources/scripts/tpcds18setup.sql";
        String tearDownScript = "src/main/resources/scripts/tpcds18teardown.sql";
        String queryScript = "src/main/resources/scripts/tpcds18Case1.sql";
        String exaremeOpGraphFile = "src/main/resources/files/exaremeGraphsLog.txt";
        String executionResultsFile = "src/main/resources/files/resultsLog.txt";
        String exaremePlanFile = "src/main/resources/files/exaremePlan.json";

        int numberOfNodes = 1; //Number of Nodes in Cluster (numberOfDataNodes==NumberOfNodeManagers)
        int maxDynamicPartitions = 1000; //Total NumberOfPartitions in Cluster
        int maxDynamicPartitionsPerNode = 100; //Total NumberOfPartitions in Node
        boolean dynamicPartitionsEnabled = true; //Toggle dynamic partitioning

        testCaseTool testTool = new testCaseTool(setupScript, tearDownScript, queryScript, "EXAREME");

        try{
            System.out.println("Setup Cluster with DataNodes: "+numberOfNodes+" and NodeManagers: "+numberOfNodes+" and load tables...");
            if(dynamicPartitionsEnabled == true)
                System.out.println("MaxDynamicPartitions: "+maxDynamicPartitions + " and MaxDynamicPartitionsPerNode: " +maxDynamicPartitionsPerNode);
            testTool.setUp(numberOfNodes, numberOfNodes, dynamicPartitionsEnabled, maxDynamicPartitions, maxDynamicPartitionsPerNode);
        }
        catch(Exception ex){
            System.out.println("Setup Failed!");
        }

        try{
            System.out.println("Run Query...");
            testTool.runQueryScript(exaremeOpGraphFile, executionResultsFile, exaremePlanFile);
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

