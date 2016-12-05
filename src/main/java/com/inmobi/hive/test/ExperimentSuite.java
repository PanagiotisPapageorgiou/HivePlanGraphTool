package com.inmobi.hive.test;

/**
 * Created by panos on 18/10/2016.
 */
public class ExperimentSuite { //TargetSize = Specific dataset size you wish to test against
                               //LimitSize = Specific size limit that will be the last size to run before return
                               //If both are null then all possible tests will be run for this node setup

    public ExperimentSuite(String caseLabel, int numOfNodes, String systemFlag, String outputFile, String limitSize, String targetSize, boolean allowLoops) {

        caseLabel = caseLabel + "-" + Integer.toString(numOfNodes) + "-" + systemFlag;

        if(limitSize != null){
            if(targetSize != null){
                System.out.println("CAN'T HAVE BOTH NOT NULL TARGETSIZE AND LIMITSIZE - PICK ONE OR NONE!");
                System.exit(0);
            }
        }

        /*-----Setup Settings------*/
        String exaremeOpGraphFile = "src/main/resources/files/exaremeGraphsLog.txt";
        String executionResultsFile = "src/main/resources/files/resultsLog.txt";
        String exaremePlanFile = "src/main/resources/files/exaremePlan.json";
        String exaremeMiniClusterIP = "192.168.1.3";
        String timesForQueries = outputFile;
        String setupScript = "src/main/resources/scripts/setup1GB.sql";
        String tearDownScript = "src/main/resources/scripts/tpcds18teardown.sql";
        String queryScript = "src/main/resources/scripts/experimentsAll.sql";

        //Madis settings - WARNING Madis must be installed
        String madisPath = "/opt/madis/src/mterm.py";

        //Hive Cluster settings
        int numberOfNodes = numOfNodes; //Number of Nodes in Cluster (numberOfDataNodes==NumberOfNodeManagers)
        int maxDynamicPartitions = 1000; //Total NumberOfPartitions in Cluster
        int maxDynamicPartitionsPerNode = 100; //Total NumberOfPartitions in Node
        boolean dynamicPartitionsEnabled = true; //Toggle dynamic partitioning
        int numOfReducers = 8;

        //Exareme Settings
        int exaremeNodes;
        if (systemFlag.equals("HIVE"))
            exaremeNodes = 1;
        else
            exaremeNodes = numberOfNodes;

        String rootDirForHiveExaremeSession = "/tmp/adpHive/"; //Don't touch this

        setupScript = "src/main/resources/scripts/setup1GB.sql";

        testCaseTool testTool;

        if ((targetSize == null) || ((targetSize != null) && (targetSize.equals("1GB")))) {

            testTool = new testCaseTool(setupScript, tearDownScript, queryScript, systemFlag, rootDirForHiveExaremeSession, madisPath, timesForQueries);

            try {
                System.out.println("Setup Cluster with DataNodes: " + numberOfNodes + " and NodeManagers: " + numberOfNodes + " and load tables...");
                if (dynamicPartitionsEnabled == true)
                    System.out.println("MaxDynamicPartitions: " + maxDynamicPartitions + " and MaxDynamicPartitionsPerNode: " + maxDynamicPartitionsPerNode);
                testTool.setUp(numberOfNodes, numberOfNodes, dynamicPartitionsEnabled, maxDynamicPartitions, maxDynamicPartitionsPerNode, exaremeMiniClusterIP, systemFlag, exaremeNodes, numOfReducers);
            } catch (Exception ex) {
                System.out.println("Setup Failed! Exception: "+ex.getMessage());
                System.exit(0);
            }


            //1GB
            System.out.println("------------------1GB EXPERIMENTS START - NODES: " + numberOfNodes + "----------------");
            if(allowLoops){
                for(int i = 0; i < 3; i++){
                    try {
                        System.out.println("Run Query...");
                        testTool.runQueryScript(exaremeOpGraphFile, executionResultsFile, exaremePlanFile, caseLabel + "1GB");
                    } catch (Throwable ex) {
                        System.out.println("Query execution failed! Exception: " + ex.toString());
                        testTool.closeFiles();
                    }
                }
            }
            else{
                try {
                    System.out.println("Run Query...");
                    testTool.runQueryScript(exaremeOpGraphFile, executionResultsFile, exaremePlanFile, caseLabel);
                } catch (Throwable ex) {
                    System.out.println("Query execution failed! Exception: " + ex.toString());
                    testTool.closeFiles();
                }
            }


            try {
                System.out.println("Tearing down tables...");
                testTool.tearDown();
            } catch (Exception ex) {
                System.out.println("Teardown Failed!");
            }

            if (targetSize == null) {
                if(limitSize != null) {
                    if (limitSize.equals("1GB")) return;
                }
            }
            else{
                return;
            }

        }

        setupScript = "src/main/resources/scripts/setup10GB.sql";

        if ((targetSize == null) || ((targetSize != null) && (targetSize.equals("10GB")))) {

            setupScript = "src/main/resources/scripts/setup10GB.sql";
            tearDownScript = "src/main/resources/scripts/tpcds18teardown.sql";
            queryScript = "src/main/resources/scripts/experimentsAll.sql";

            testCaseTool testTool2 = new testCaseTool(setupScript, tearDownScript, queryScript, systemFlag, rootDirForHiveExaremeSession, madisPath, timesForQueries);

            try {
                System.out.println("Setup Cluster with DataNodes: " + numberOfNodes + " and NodeManagers: " + numberOfNodes + " and load tables...");
                if (dynamicPartitionsEnabled == true)
                    System.out.println("MaxDynamicPartitions: " + maxDynamicPartitions + " and MaxDynamicPartitionsPerNode: " + maxDynamicPartitionsPerNode);
                testTool2.setUp(numberOfNodes, numberOfNodes, dynamicPartitionsEnabled, maxDynamicPartitions, maxDynamicPartitionsPerNode, exaremeMiniClusterIP, systemFlag, exaremeNodes, numOfReducers);
            } catch (Exception ex) {
                System.out.println("Setup Failed! Exception: "+ex.getMessage());
                System.exit(0);
            }

            //10GB
            System.out.println("------------------10GB EXPERIMENTS START - NODES: " + numberOfNodes + "----------------");
            if(allowLoops){
                for(int i = 0; i < 3; i++){
                    try {
                        System.out.println("Run Query...");
                        testTool2.runQueryScript(exaremeOpGraphFile, executionResultsFile, exaremePlanFile, caseLabel + "10GB");
                    } catch (Throwable ex) {
                        System.out.println("Query execution failed! Exception: " + ex.toString());
                        testTool2.closeFiles();
                    }
                }
            }
            else{
                try {
                    System.out.println("Run Query...");
                    testTool2.runQueryScript(exaremeOpGraphFile, executionResultsFile, exaremePlanFile, caseLabel);
                } catch (Throwable ex) {
                    System.out.println("Query execution failed! Exception: " + ex.toString());
                    testTool2.closeFiles();
                }
            }


            try {
                System.out.println("Tearing down tables...");
                testTool2.tearDown();
            } catch (Exception ex) {
                System.out.println("Teardown Failed!");
            }

            if (targetSize == null) {
                if(limitSize != null) {
                    if (limitSize.equals("10GB")) return;
                }
            }
            else{
                return;
            }

        }

        if(numOfNodes > 1) { //50GB not counted for 1 NODE

            setupScript = "src/main/resources/scripts/setup50GB.sql";

            if ((targetSize == null) || ((targetSize != null) && (targetSize.equals("50GB")))) {

                tearDownScript = "src/main/resources/scripts/tpcds18teardown.sql";
                queryScript = "src/main/resources/scripts/experimentsAll.sql";

                numOfReducers = 24;

                testCaseTool testTool3 = new testCaseTool(setupScript, tearDownScript, queryScript, systemFlag, rootDirForHiveExaremeSession, madisPath, timesForQueries);

                try {
                    System.out.println("Setup Cluster with DataNodes: " + numberOfNodes + " and NodeManagers: " + numberOfNodes + " and load tables...");
                    if (dynamicPartitionsEnabled == true)
                        System.out.println("MaxDynamicPartitions: " + maxDynamicPartitions + " and MaxDynamicPartitionsPerNode: " + maxDynamicPartitionsPerNode);
                    testTool3.setUp(numberOfNodes, numberOfNodes, dynamicPartitionsEnabled, maxDynamicPartitions, maxDynamicPartitionsPerNode, exaremeMiniClusterIP, systemFlag, exaremeNodes, numOfReducers);
                } catch (Exception ex) {
                    System.out.println("Setup Failed! Exception: "+ex.getMessage());
                    System.exit(0);
                }

                //50GB
                try {
                    System.out.println("Run Query...");
                    testTool3.runQueryScript(exaremeOpGraphFile, executionResultsFile, exaremePlanFile, caseLabel);
                } catch (Throwable ex) {
                    System.out.println("Query execution failed! Exception: " + ex.toString());
                    testTool3.closeFiles();
                }


                try {
                    System.out.println("Tearing down tables...");
                    testTool3.tearDown();
                } catch (Exception ex) {
                    System.out.println("Teardown Failed!");
                }

                if (targetSize == null) {
                    if (limitSize != null) {
                        if (limitSize.equals("50GB")) return;
                    }
                } else {
                    return;
                }

            }

            if(numOfNodes > 2) { //More than 2 Nodes only for 100GB

                setupScript = "src/main/resources/scripts/setup100GB.sql";
                tearDownScript = "src/main/resources/scripts/tpcds18teardown.sql";
                queryScript = "src/main/resources/scripts/experimentsAll.sql";

                testCaseTool testTool4 = new testCaseTool(setupScript, tearDownScript, queryScript, systemFlag, rootDirForHiveExaremeSession, madisPath, timesForQueries);

                numOfReducers = 36;

                try {
                    System.out.println("Setup Cluster with DataNodes: " + numberOfNodes + " and NodeManagers: " + numberOfNodes + " and load tables...");
                    if (dynamicPartitionsEnabled == true)
                        System.out.println("MaxDynamicPartitions: " + maxDynamicPartitions + " and MaxDynamicPartitionsPerNode: " + maxDynamicPartitionsPerNode);
                    testTool4.setUp(numberOfNodes, numberOfNodes, dynamicPartitionsEnabled, maxDynamicPartitions, maxDynamicPartitionsPerNode, exaremeMiniClusterIP, systemFlag, exaremeNodes, numOfReducers);
                } catch (Exception ex) {
                    System.out.println("Setup Failed! Exception: "+ex.getMessage());
                    System.exit(0);
                }

                //100GB
                System.out.println("------------------100GB EXPERIMENTS START - NODES: " + numberOfNodes + "----------------");
                try {
                    System.out.println("Run Query...");
                    testTool4.runQueryScript(exaremeOpGraphFile, executionResultsFile, exaremePlanFile, caseLabel + "100GB");
                } catch (Throwable ex) {
                    System.out.println("Query execution failed! Exception: " + ex.toString());
                    testTool4.closeFiles();
                }


                try {
                    System.out.println("Tearing down tables...");
                    testTool4.tearDown();
                } catch (Exception ex) {
                    System.out.println("Teardown Failed!");
                }

            }

        }

    }


}
