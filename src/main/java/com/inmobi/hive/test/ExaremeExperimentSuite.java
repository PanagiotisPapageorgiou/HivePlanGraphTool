package com.inmobi.hive.test;

/**
 * Created by panos on 23/10/2016.
 */
public class ExaremeExperimentSuite {

    public void runSubExperiment(String titleTag, int exaremeNodes, String setupScript, String tearDownScript, String queryScript, String madisPath, String timesForQueries, int numberOfNodes, boolean dynamicPartitionsEnabled, int maxDynamicPartitions, int maxDynamicPartitionsPerNode, String exaremeMiniClusterIP, int numOfReducers, String exaremeOpGraphFile, String executionResultsFile){

        System.out.println("Experiment - "+titleTag);
        String rootDirForHiveExaremeSession = "/home/panos/exareme1GB/"; //Don't touch this
        String exaremePlanFile = "src/main/resources/files/"+titleTag+"+1GB.json";

        testCaseTool testTool = new testCaseTool(setupScript, tearDownScript, queryScript, "EXAREME", rootDirForHiveExaremeSession, madisPath, timesForQueries);

        try{
            System.out.println("Setup Cluster with DataNodes: "+numberOfNodes+" and NodeManagers: "+numberOfNodes+" and load tables...");
            if(dynamicPartitionsEnabled == true)
                System.out.println("MaxDynamicPartitions: "+maxDynamicPartitions + " and MaxDynamicPartitionsPerNode: " +maxDynamicPartitionsPerNode);
            testTool.setUp(numberOfNodes, numberOfNodes, dynamicPartitionsEnabled, maxDynamicPartitions, maxDynamicPartitionsPerNode, exaremeMiniClusterIP, "EXAREME", exaremeNodes, numOfReducers, "/home/panos", "exareme1GB");
        }
        catch(Exception ex){
            System.out.println("Setup Failed!");
        }

        try{
            System.out.println("Run Query...");
            testTool.runQueryScript(exaremeOpGraphFile, executionResultsFile, exaremePlanFile, titleTag);
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

        System.out.println("Experiment - "+titleTag);
        rootDirForHiveExaremeSession = "/home/panos/exareme10GB/"; //Don't touch this
        exaremePlanFile = "src/main/resources/files/"+titleTag+"+10GB.json";

        testTool = new testCaseTool(setupScript, tearDownScript, queryScript, "EXAREME", rootDirForHiveExaremeSession, madisPath, timesForQueries);

        try{
            System.out.println("Setup Cluster with DataNodes: "+numberOfNodes+" and NodeManagers: "+numberOfNodes+" and load tables...");
            if(dynamicPartitionsEnabled == true)
                System.out.println("MaxDynamicPartitions: "+maxDynamicPartitions + " and MaxDynamicPartitionsPerNode: " +maxDynamicPartitionsPerNode);
            testTool.setUp(numberOfNodes, numberOfNodes, dynamicPartitionsEnabled, maxDynamicPartitions, maxDynamicPartitionsPerNode, exaremeMiniClusterIP, "EXAREME", exaremeNodes, numOfReducers, "/home/panos", "exareme10GB");
        }
        catch(Exception ex){
            System.out.println("Setup Failed!");
        }

        try{
            System.out.println("Run Query...");
            testTool.runQueryScript(exaremeOpGraphFile, executionResultsFile, exaremePlanFile, titleTag);
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

        System.out.println("Experiment - "+titleTag);
        rootDirForHiveExaremeSession = "/home/panos/exareme50GB/"; //Don't touch this
        exaremePlanFile = "src/main/resources/files/"+titleTag+"+50GB.json";

        testTool = new testCaseTool(setupScript, tearDownScript, queryScript, "EXAREME", rootDirForHiveExaremeSession, madisPath, timesForQueries);

        try{
            System.out.println("Setup Cluster with DataNodes: "+numberOfNodes+" and NodeManagers: "+numberOfNodes+" and load tables...");
            if(dynamicPartitionsEnabled == true)
                System.out.println("MaxDynamicPartitions: "+maxDynamicPartitions + " and MaxDynamicPartitionsPerNode: " +maxDynamicPartitionsPerNode);
            testTool.setUp(numberOfNodes, numberOfNodes, dynamicPartitionsEnabled, maxDynamicPartitions, maxDynamicPartitionsPerNode, exaremeMiniClusterIP, "EXAREME", exaremeNodes, numOfReducers, "/home/panos", "exareme50GB");
        }
        catch(Exception ex){
            System.out.println("Setup Failed!");
        }

        try{
            System.out.println("Run Query...");
            testTool.runQueryScript(exaremeOpGraphFile, executionResultsFile, exaremePlanFile, titleTag);
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

        System.out.println("Experiment - "+titleTag);
        rootDirForHiveExaremeSession = "/home/panos/exareme100GB/"; //Don't touch this
        exaremePlanFile = "src/main/resources/files/"+titleTag+"+100GB.json";

        testTool = new testCaseTool(setupScript, tearDownScript, queryScript, "EXAREME", rootDirForHiveExaremeSession, madisPath, timesForQueries);

        try{
            System.out.println("Setup Cluster with DataNodes: "+numberOfNodes+" and NodeManagers: "+numberOfNodes+" and load tables...");
            if(dynamicPartitionsEnabled == true)
                System.out.println("MaxDynamicPartitions: "+maxDynamicPartitions + " and MaxDynamicPartitionsPerNode: " +maxDynamicPartitionsPerNode);
            testTool.setUp(numberOfNodes, numberOfNodes, dynamicPartitionsEnabled, maxDynamicPartitions, maxDynamicPartitionsPerNode, exaremeMiniClusterIP, "EXAREME", exaremeNodes, numOfReducers, "/home/panos", "exareme100GB");
        }
        catch(Exception ex){
            System.out.println("Setup Failed!");
        }

        try{
            System.out.println("Run Query...");
            testTool.runQueryScript(exaremeOpGraphFile, executionResultsFile, exaremePlanFile, titleTag);
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

    public ExaremeExperimentSuite(String exaremeMiniClusterIP){

        /*-----Setup Settings------*/
        //String setupScript = "src/main/resources/scripts/tpcds18setup.sql";
        String setupScript = "src/main/resources/scripts/setup1GB.sql";
        String tearDownScript = "src/main/resources/scripts/tpcds18teardown.sql";
        String queryScript = "src/main/resources/scripts/experimentsAll.sql";

        String exaremeOpGraphFile = "src/main/resources/files/exaremeGraphsLog.txt";
        String executionResultsFile = "src/main/resources/files/resultsLog.txt";

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

        /*-------------------------------------Experiments--------------------------*/

        //runSubExperiment("1NODEQ1", 1, setupScript, tearDownScript, queryScript, madisPath, timesForQueries, 1, dynamicPartitionsEnabled, maxDynamicPartitions, maxDynamicPartitionsPerNode, exaremeMiniClusterIP, numOfReducers, exaremeOpGraphFile, executionResultsFile);

        //runSubExperiment("2NODEQ1", 2, setupScript, tearDownScript, queryScript, madisPath, timesForQueries, 1, dynamicPartitionsEnabled, maxDynamicPartitions, maxDynamicPartitionsPerNode, exaremeMiniClusterIP, numOfReducers, exaremeOpGraphFile, executionResultsFile);

        //runSubExperiment("3NODEQ1", 4, setupScript, tearDownScript, queryScript, madisPath, timesForQueries, 1, dynamicPartitionsEnabled, maxDynamicPartitions, maxDynamicPartitionsPerNode, exaremeMiniClusterIP, numOfReducers, exaremeOpGraphFile, executionResultsFile);

        runSubExperiment("4NODEQ1", 8, setupScript, tearDownScript, queryScript, madisPath, timesForQueries, 1, dynamicPartitionsEnabled, maxDynamicPartitions, maxDynamicPartitionsPerNode, exaremeMiniClusterIP, numOfReducers, exaremeOpGraphFile, executionResultsFile);
    }

}
