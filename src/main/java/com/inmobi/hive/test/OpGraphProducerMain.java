package com.inmobi.hive.test;
/**
 * Created by panos on 11/5/2016.
 */
public class OpGraphProducerMain {

    public static void main(String[] args){ //TODO Allow loading of multiple .dat files into .db

        //ExperimentSuite experiment1 = new ExperimentSuite();
        //experiment1.setUpHiveExperiment(1, "src/main/resources/files/1node/1/results", "src/main/resources/script/1node/setup.sql", "src/main/resources/scripts/tpcds18teardown.sql", "src/main/resources/scripts/hiveExperiment1.sql", true, 1000, 100);

        //try {
            //System.out.println("Go to sleep...");
            //Thread.sleep(1000 * 60 * 30);
            //System.out.println("WOKE UP!");
        //} catch (InterruptedException ex) {
        //    System.out.println("System sleep failed!");
        //    System.exit(0);
        //}

        //ExperimentSuite experiment1 = new ExperimentSuite("4NODE", 4, "HIVE", "src/main/resources/executionTimes4NODE.txt", null, "50GB", false);

        //ExperimentSuite experiment2 = new ExperimentSuite("8NODE", 8, "HIVE", "src/main/resources/executionTimes8NODE.txt", null, "50GB", false);

        //ExperimentSuite experiment2 = new ExperimentSuite("2NODE", 2, "HIVE", "src/main/resources/executionTimes2NODE.txt", null, "50GB", true);

        //ExperimentSuite experiment3 = new ExperimentSuite("4NODE", 4, "HIVE", "src/main/resources/executionTimes4NODE.txt", null, "100GB", false);

        //quiExperimentSuite experiment4 = new ExperimentSuite("8NODE", 8, "HIVE", "src/main/resources/executionTimes8NODE.txt", null, "100GB", false);

        //ExperimentSuite experiment2 = new ExperimentSuite("2NODE", 2, "HIVE", "src/main/resources/executionTimes2NODE.txt");

        //ExperimentSuite experiment3 = new ExperimentSuite("4NODE", 4, "HIVE", "src/main/resources/executionTimes4NODE.txt");

        //ExperimentSuite experiment4 = new ExperimentSuite("8NODE", 8, "HIVE", "src/main/resources/executionTimes8NODE.txt");

        //System.exit(0);

        /*-----Setup Settings------*/
        //String setupScript = "src/main/resources/scripts/tpcds18setup.sql";
        String setupScript = "src/main/resources/scripts/setup1GB.sql";
        String tearDownScript = "src/main/resources/scripts/tpcds18teardown.sql";
        //String queryScript = "src/main/resources/scripts/experimentsAll.sql";
        String queryScript = "src/main/resources/scripts/tpcds18Case1.sql";
        String exaremeOpGraphFile = "src/main/resources/files/exaremeGraphsLog.txt";
        String executionResultsFile = "src/main/resources/files/resultsLog.txt";
        String exaremePlanFile = "src/main/resources/files/exaremePlan.json";
        String exaremeMiniClusterIP = "192.168.1.3";
        String timesForQueries = "src/main/resources/executionTimes.txt";

        //Madis settings - WARNING Madis must be installed
        String madisPath = "/opt/madis/src/mterm.py";

        //Hive Cluster settings
        int numberOfNodes = 1; //Number of Nodes in Cluster (numberOfDataNodes==NumberOfNodeManagers)
        int maxDynamicPartitions = 1000; //Total NumberOfPartitions in Cluster
        int maxDynamicPartitionsPerNode = 100; //Total NumberOfPartitions in Node
        boolean dynamicPartitionsEnabled = true; //Toggle dynamic partitioning
        int numOfReducers = 8;

        //Exareme Settings
        int exaremeNodes = 1;

        String rootDirForHiveExaremeSession = "/tmp/adpHive/"; //Don't touch this

        //ExaremeExperimentSuite exaExperiment = new ExaremeExperimentSuite(exaremeMiniClusterIP);

        //System.exit(0);

        testCaseTool testTool = new testCaseTool(setupScript, tearDownScript, queryScript, "EXAREME", rootDirForHiveExaremeSession, madisPath, timesForQueries);

        //Just a normal run
        try{
            System.out.println("Setup Cluster with DataNodes: "+numberOfNodes+" and NodeManagers: "+numberOfNodes+" and load tables...");
            if(dynamicPartitionsEnabled == true)
                System.out.println("MaxDynamicPartitions: "+maxDynamicPartitions + " and MaxDynamicPartitionsPerNode: " +maxDynamicPartitionsPerNode);
            testTool.setUp(numberOfNodes, numberOfNodes, dynamicPartitionsEnabled, maxDynamicPartitions, maxDynamicPartitionsPerNode, exaremeMiniClusterIP, "EXAREME", exaremeNodes, numOfReducers);
        }
        catch(Exception ex){
            System.out.println("Setup Failed!");
        }

        try{
            System.out.println("Run Query...");
            testTool.runQueryScript(exaremeOpGraphFile, executionResultsFile, exaremePlanFile, "1GB");
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

