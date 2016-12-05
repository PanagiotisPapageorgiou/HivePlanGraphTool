package madgik.exareme.master.engine;

import madgik.exareme.master.app.cluster.ExaremeCluster;
import madgik.exareme.master.app.cluster.ExaremeClusterFactory;
import madgik.exareme.utils.file.FileUtil;
import madgik.exareme.worker.art.executionEngine.ExecutionEngineLocator;
import madgik.exareme.worker.art.executionEngine.ExecutionEngineProxy;
import madgik.exareme.worker.art.executionEngine.session.ExecutionEngineSession;
import madgik.exareme.worker.art.executionEngine.session.ExecutionEngineSessionPlan;
import madgik.exareme.worker.art.executionPlan.ExecutionPlan;
import madgik.exareme.worker.art.executionPlan.ExecutionPlanParser;
import org.apache.log4j.Logger;

import java.io.*;
import java.rmi.registry.LocateRegistry;
import java.util.Scanner;

/**
 * Created by panos on 5/10/2016.
 */

/* Test class used to test Exareme Plans that are produced from Hive
   with plan resources fetched from HDFS
 */

public class HiveToExaremePlanTest {
    private static final Logger log = Logger.getLogger(HiveToExaremePlanTest.class);

    public static void subExperiment(String planFile, ExecutionPlanParser planParser, PrintWriter outputWriter){

        try {

            //Run Query
            String art = FileUtil.readFile(new File(planFile));
            //StartTime
            long startTime = System.currentTimeMillis();

            log.info("Reading File...");
            log.info("Art plan :" + art);

            //log.info("PHASE 1");

            long midTime = System.currentTimeMillis() - startTime;
            long midPoint = System.currentTimeMillis();

            ExecutionPlan executionPlan = planParser.parse(art);

            //MidPoint - Don't count the print time
            log.info("Parsed :" + executionPlan.toString());

            //log.info("PHASE 2");

            ExecutionEngineProxy engineProxy = ExecutionEngineLocator.getExecutionEngineProxy();
            ExecutionEngineSession engineSession = engineProxy.createSession();
            final ExecutionEngineSessionPlan sessionPlan = engineSession.startSession();
            sessionPlan.submitPlan(executionPlan);
            log.info("Submitted.");
            while (sessionPlan.getPlanSessionStatusManagerProxy().hasFinished() == false
                    && sessionPlan.getPlanSessionStatusManagerProxy().hasError() == false) {
                Thread.sleep(100);
                //log.info("NOT FINISHED YET...");
            }

            //EndPoint - Count execution time
            long endPoint = System.currentTimeMillis();
            log.info("Execution Plan Duration: " + (midTime + (endPoint - midPoint)));
            long totalDuration = midTime + (endPoint - midPoint);

            outputWriter.println("Time: "+totalDuration);
            outputWriter.flush();
            log.info("Exited");
            if (sessionPlan.getPlanSessionStatusManagerProxy().hasError() == true) {
                log.error(sessionPlan.getPlanSessionStatusManagerProxy().getErrorList().get(0));
            }


        } catch (Exception e) {
            log.error(e);
            e.printStackTrace();
        }

    }


    public static void runExperiment(int numberOfNodes, int port, int dataTport, String planFileQ1, String planFileQ2, String planFileQ3, String planFileQ4, int size){

        String timesForQueries = "";

        Scanner intScanner = new Scanner(System.in);

        log.info("--------------Starting Exareme Mini Cluster-------------");
        log.info("Port: "+port);
        log.info("DataTransfer Starting Port: "+dataTport);
        log.info("Number of Nodes: "+numberOfNodes+"\n");

        ExaremeCluster miniCluster = ExaremeClusterFactory.createMiniCluster(1098, 8088, numberOfNodes);
        //do {
            try {
                miniCluster.start();
                //break;
            } catch (java.rmi.RemoteException ex) {
                System.out.println("Excepetion while Starting MiniCluster: " + ex.getMessage());
                //try {
                //    LocateRegistry.getRegistry();
                //    miniCluster.start();
                //    break;
                //}
                //catch(java.rmi.RemoteException ex2){
                    //System.out.println("FAILED TO LOCATE REGISTRY! "+ex2.getMessage());
                //}
                System.exit(0);
            }
        //}while(true);


        log.info("Mini cluster started.");
        ExecutionPlanParser planParser = new ExecutionPlanParser();

        if(numberOfNodes == 1) {
            if(size <= 10){
                PrintWriter outputWriter = null;
                try {
                    outputWriter = new PrintWriter(new BufferedWriter(new FileWriter("/home/panos/allHiveExaremeTimes/1NODE"+size+"GB.txt", true)));
                } catch (IOException e) {
                    System.out.println("Exception creating new PrintWriter..."+e.getMessage());
                    System.exit(0);
                }

                outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q1");
                outputWriter.flush();
                for(int i = 0; i < 3; i++){ //Loop 3 times for better results

                    subExperiment(planFileQ1, planParser, outputWriter);

                }

                outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q2");
                outputWriter.flush();
                for(int i = 0; i < 3; i++){ //Loop 3 times for better results

                    subExperiment(planFileQ2, planParser, outputWriter);

                }

                outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q3");
                outputWriter.flush();
                for(int i = 0; i < 3; i++){ //Loop 3 times for better results

                    subExperiment(planFileQ3, planParser, outputWriter);

                }

                outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q4");
                outputWriter.flush();
                for(int i = 0; i < 3; i++){ //Loop 3 times for better results

                    subExperiment(planFileQ4, planParser, outputWriter);

                }


            }

        }
        else if(numberOfNodes == 2){

            PrintWriter outputWriter = null;
            try {
                outputWriter = new PrintWriter(new BufferedWriter(new FileWriter("/home/panos/allHiveExaremeTimes/2NODE"+size+"GB.txt", true)));
            } catch (IOException e) {
                System.out.println("Exception creating new PrintWriter..."+e.getMessage());
                System.exit(0);
            }

            if(size <= 50){
                /*if(size < 50){
                    outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q1");
                    outputWriter.flush();
                    for(int i = 0; i < 3; i++){ //Loop 3 times for better results

                        subExperiment(planFileQ1, planParser, outputWriter);

                    }

                    outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q2");
                    outputWriter.flush();
                    for(int i = 0; i < 3; i++){ //Loop 3 times for better results

                        subExperiment(planFileQ2, planParser, outputWriter);

                    }

                    outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q3");
                    outputWriter.flush();
                    for(int i = 0; i < 3; i++){ //Loop 3 times for better results

                        subExperiment(planFileQ3, planParser, outputWriter);

                    }

                    outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q4");
                    outputWriter.flush();
                    for(int i = 0; i < 3; i++){ //Loop 3 times for better results

                        subExperiment(planFileQ4, planParser, outputWriter);

                    }
                }
                else{*/

                    //outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q1");
                    //outputWriter.flush();

                    //subExperiment(planFileQ1, planParser, outputWriter);

                    //outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q2");
                    //outputWriter.flush();

                    //subExperiment(planFileQ2, planParser, outputWriter);

                    //outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q3");
                    //outputWriter.flush();

                    //subExperiment(planFileQ3, planParser, outputWriter);

                    outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q4");
                    outputWriter.flush();

                    //subExperiment(planFileQ4, planParser, outputWriter);

                try {

                    //Run Query
                    String art = FileUtil.readFile(new File(planFileQ4));
                    //StartTime
                    long startTime = System.currentTimeMillis();

                    log.info("Reading File...");
                    log.info("Art plan :" + art);

                    //log.info("PHASE 1");

                    long midTime = System.currentTimeMillis() - startTime;
                    long midPoint = System.currentTimeMillis();

                    ExecutionPlan executionPlan = planParser.parse(art);

                    //MidPoint - Don't count the print time
                    log.info("Parsed :" + executionPlan.toString());

                    //log.info("PHASE 2");

                    ExecutionEngineProxy engineProxy = ExecutionEngineLocator.getExecutionEngineProxy();
                    ExecutionEngineSession engineSession = engineProxy.createSession();
                    final ExecutionEngineSessionPlan sessionPlan = engineSession.startSession();
                    sessionPlan.submitPlan(executionPlan);
                    log.info("Submitted.");
                    while (sessionPlan.getPlanSessionStatusManagerProxy().hasFinished() == false
                            && sessionPlan.getPlanSessionStatusManagerProxy().hasError() == false) {
                        Thread.sleep(100);
                        //log.info("NOT FINISHED YET...");
                    }

                    //EndPoint - Count execution time
                    long endPoint = System.currentTimeMillis();
                    log.info("Execution Plan Duration: " + (midTime + (endPoint - midPoint)));
                    long totalDuration = midTime + (endPoint - midPoint);

                    outputWriter.println("Time: "+totalDuration);
                    outputWriter.flush();
                    log.info("Exited");
                    if (sessionPlan.getPlanSessionStatusManagerProxy().hasError() == true) {
                        log.error(sessionPlan.getPlanSessionStatusManagerProxy().getErrorList().get(0));
                    }


                } catch (Exception e) {
                    log.error(e);
                    e.printStackTrace();
                }


                //}
            }
        }
        else if(numberOfNodes == 4){
            PrintWriter outputWriter = null;
            try {
                outputWriter = new PrintWriter(new BufferedWriter(new FileWriter("/home/panos/allHiveExaremeTimes/4NODE"+size+"GB.txt", true)));
            } catch (IOException e) {
                System.out.println("Exception creating new PrintWriter..."+e.getMessage());
                System.exit(0);
            }

            if(size < 50){
                outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q1");
                outputWriter.flush();
                for(int i = 0; i < 3; i++){ //Loop 3 times for better results

                    subExperiment(planFileQ1, planParser, outputWriter);

                }

                outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q2");
                outputWriter.flush();
                for(int i = 0; i < 2; i++){ //Loop 3 times for better results

                    subExperiment(planFileQ2, planParser, outputWriter);

                }

                outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q3");
                outputWriter.flush();
                for(int i = 0; i < 2; i++){ //Loop 3 times for better results

                    subExperiment(planFileQ3, planParser, outputWriter);

                }

                outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q4");
                outputWriter.flush();
                for(int i = 0; i < 2; i++){ //Loop 3 times for better results

                    subExperiment(planFileQ4, planParser, outputWriter);

                }
            }
            else if(size == 50){
                outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q1");
                outputWriter.flush();
                //for(int i = 0; i < 2; i++){ //Loop 3 times for better results

                    subExperiment(planFileQ1, planParser, outputWriter);

                //}

                outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q2");
                outputWriter.flush();
                //for(int i = 0; i < 2; i++){ //Loop 3 times for better results

                    subExperiment(planFileQ2, planParser, outputWriter);

                //}

                //outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q3");
                //outputWriter.flush();
                //for(int i = 0; i < 2; i++){ //Loop 3 times for better results

                    //subExperiment(planFileQ3, planParser, outputWriter);

                //}

                outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q4");
                outputWriter.flush();
                //for(int i = 0; i < 2; i++){ //Loop 3 times for better results

                    subExperiment(planFileQ4, planParser, outputWriter);

                //}
            }
            else{

                //outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q1");
                //outputWriter.flush();

                //subExperiment(planFileQ1, planParser, outputWriter);

                //outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q2");
                //outputWriter.flush();

                //subExperiment(planFileQ2, planParser, outputWriter);

                //outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q3");
                //outputWriter.flush();

                //subExperiment(planFileQ3, planParser, outputWriter);

                outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q4");
                outputWriter.flush();

                subExperiment(planFileQ4, planParser, outputWriter);

            }

        }
        else{
            PrintWriter outputWriter = null;
            try {
                outputWriter = new PrintWriter(new BufferedWriter(new FileWriter("/home/panos/allHiveExaremeTimes/8NODE"+size+"GB.txt", true)));
            } catch (IOException e) {
                System.out.println("Exception creating new PrintWriter..."+e.getMessage());
                System.exit(0);
            }

            if(size < 50){
                outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q1");
                outputWriter.flush();
                for(int i = 0; i < 2; i++){ //Loop 3 times for better results

                    subExperiment(planFileQ1, planParser, outputWriter);

                }

                outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q2");
                outputWriter.flush();
                for(int i = 0; i < 2; i++){ //Loop 3 times for better results

                    subExperiment(planFileQ2, planParser, outputWriter);

                }

                outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q3");
                outputWriter.flush();
                for(int i = 0; i < 3; i++){ //Loop 3 times for better results

                    subExperiment(planFileQ3, planParser, outputWriter);

                }

                outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q4");
                outputWriter.flush();
                for(int i = 0; i < 2; i++){ //Loop 3 times for better results

                    subExperiment(planFileQ4, planParser, outputWriter);

                }
            }
            else{

                outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q1");
                outputWriter.flush();

                subExperiment(planFileQ1, planParser, outputWriter);

                outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q2");
                outputWriter.flush();

                subExperiment(planFileQ2, planParser, outputWriter);

                outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q3");
                outputWriter.flush();

                subExperiment(planFileQ3, planParser, outputWriter);

                outputWriter.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+size+" - Query: Q4");
                outputWriter.flush();

                subExperiment(planFileQ4, planParser, outputWriter);

            }
        }


        try{
            miniCluster.stop(true);
        } catch(java.rmi.RemoteException remoteEx){
            System.out.println("Interrupted Exception: "+remoteEx.getMessage());
            System.exit(0);
        }
        catch(Exception ex){
            System.out.println("Exception: Failure in stopping miniCluster...");
        }

        log.info("Mini Cluster stopping...");
        try{
            Thread.sleep(10 * 1000);
        } catch(java.lang.InterruptedException interEx){
            System.out.println("Interrupted Exception: "+interEx.getMessage());
            System.exit(0);
        }

        log.info("Mini Cluster stopped.");

    }

    public static void main(String[] args) throws Exception{ //TODO: Switch clusterStorage delete if exists to OFF

        String planFileQ1 = "";
        String planFileQ2 = "";
        String planFileQ3 = "";
        String planFileQ4 = "";

        //Run Experiments

        //1GB
        //planFileQ1 = "/home/panos/allExaremePlans/1NODE1GBQ1.json";
        //planFileQ2 = "/home/panos/allExaremePlans/1NODE1GBQ2.json";
        //planFileQ3 = "/home/panos/allExaremePlans/1NODE1GBQ3.json";
        //planFileQ4 = "/home/panos/allExaremePlans/1NODE1GBQ4.json";
        //runExperiment(1, 1098, 8088, planFileQ1, planFileQ2, planFileQ3, planFileQ4, 1);

        //planFileQ1 = "/home/panos/allExaremePlans/2NODE1GBQ1.json";
        //planFileQ2 = "/home/panos/allExaremePlans/2NODE1GBQ2.json";
        //planFileQ3 = "/home/panos/allExaremePlans/2NODE1GBQ3.json";
        //planFileQ4 = "/home/panos/allExaremePlans/2NODE1GBQ4.json";
        //runExperiment(2, 1099, 8089, planFileQ1, planFileQ2, planFileQ3, planFileQ4, 1);
        //runExperiment(2, 1098, 8088, planFileQ1, planFileQ2, planFileQ3, planFileQ4, 1);

        /*planFileQ1 = "/home/panos/allExaremePlans/4NODE1GBQ1.json";
        planFileQ2 = "/home/panos/allExaremePlans/4NODE1GBQ2.json";
        planFileQ3 = "/home/panos/allExaremePlans/4NODE1GBQ3.json";
        planFileQ4 = "/home/panos/allExaremePlans/4NODE1GBQ4.json";
        runExperiment(4, 1098, 8088, planFileQ1, planFileQ2, planFileQ3, planFileQ4, 1);*/

        //planFileQ1 = "/home/panos/allExaremePlans/8NODE1GBQ1.json";
        //planFileQ2 = "/home/panos/allExaremePlans/8NODE1GBQ2.json";
        //planFileQ3 = "/home/panos/allExaremePlans/8NODE1GBQ3.json";
        //planFileQ4 = "/home/panos/allExaremePlans/8NODE1GBQ4.json";
        //runExperiment(8, 1098, 8088, planFileQ1, planFileQ2, planFileQ3, planFileQ4, 1);

        //10GB
        //planFileQ1 = "/home/panos/allExaremePlans/1NODE10GBQ1.json";
        //planFileQ2 = "/home/panos/allExaremePlans/1NODE10GBQ2.json";
       // planFileQ3 = "/home/panos/allExaremePlans/1NODE10GBQ3.json";
       // planFileQ4 = "/home/panos/allExaremePlans/1NODE10GBQ4.json";
       //  runExperiment(1, 1098, 8088, planFileQ1, planFileQ2, planFileQ3, planFileQ4, 10);

       // planFileQ1 = "/home/panos/allExaremePlans/2NODE10GBQ1.json";
        //planFileQ2 = "/home/panos/allExaremePlans/2NODE10GBQ2.json";
       // planFileQ3 = "/home/panos/allExaremePlans/2NODE10GBQ3.json";
       // planFileQ4 = "/home/panos/allExaremePlans/2NODE10GBQ4.json";
       // runExperiment(2, 1098, 8088, planFileQ1, planFileQ2, planFileQ3, planFileQ4, 10);

        //planFileQ1 = "/home/panos/allExaremePlans/4NODE10GBQ1.json";
        //planFileQ2 = "/home/panos/allExaremePlans/4NODE10GBQ2.json";
        //planFileQ3 = "/home/panos/allExaremePlans/4NODE10GBQ3.json";
        //planFileQ4 = "/home/panos/allExaremePlans/4NODE10GBQ4.json";
        //runExperiment(4, 1098, 8088, planFileQ1, planFileQ2, planFileQ3, planFileQ4, 10);

        //planFileQ1 = "/home/panos/allExaremePlans/8NODE10GBQ1.json";
        //planFileQ2 = "/home/panos/allExaremePlans/8NODE10GBQ2.json";
        //planFileQ3 = "/home/panos/allExaremePlans/8NODE10GBQ3.json";
        //planFileQ4 = "/home/panos/allExaremePlans/8NODE10GBQ4.json";
        //runExperiment(8, 1098, 8088, planFileQ1, planFileQ2, planFileQ3, planFileQ4, 10);

        //50GB
        //planFileQ1 = "/home/panos/allExaremePlans/1NODE10GBQ1";
        ///planFileQ2 = "/home/panos/allExaremePlans/1NODE10GBQ2";
        //planFileQ3 = "/home/panos/allExaremePlans/1NODE10GBQ3";
        //planFileQ4 = "/home/panos/allExaremePlans/1NODE10GBQ4";
        //runExperiment(1, 1098, 8088, planFileQ1, planFileQ2, planFileQ3, planFileQ4, 10);

        //planFileQ1 = "/home/panos/allExaremePlans/2NODE50GBQ1.json";
        //planFileQ2 = "/home/panos/allExaremePlans/2NODE50GBQ2.json";
        //planFileQ3 = "/home/panos/allExaremePlans/2NODE50GBQ3.json";
       // planFileQ4 = "/home/panos/allExaremePlans/2NODE50GBQ4.json";
      // runExperiment(2, 1098, 8088, planFileQ1, planFileQ2, planFileQ3, planFileQ4, 50);

       // planFileQ1 = "/home/panos/allExaremePlans/4NODE50GBQ1.json";
       // planFileQ2 = "/home/panos/allExaremePlans/4NODE50GBQ2.json";
       // planFileQ3 = "/home/panos/allExaremePlans/4NODE50GBQ3.json";
      // planFileQ4 = "/home/panos/allExaremePlans/4NODE50GBQ4.json";
      // runExperiment(4, 1098, 8088, planFileQ1, planFileQ2, planFileQ3, planFileQ4, 50);

       // planFileQ1 = "/home/panos/allExaremePlans/8NODE50GBQ1.json";
       // planFileQ2 = "/home/panos/allExaremePlans/8NODE50GBQ2.json";
       // planFileQ3 = "/home/panos/allExaremePlans/8NODE50GBQ3.json";
       // planFileQ4 = "/home/panos/allExaremePlans/8NODE50GBQ4.json";
       // runExperiment(8, 1098, 8088, planFileQ1, planFileQ2, planFileQ3, planFileQ4, 50);

        //100GB
        //planFileQ1 = "/home/panos/allExaremePlans/1NODE10GBQ1";
        //planFileQ2 = "/home/panos/allExaremePlans/1NODE10GBQ2";
        //planFileQ3 = "/home/panos/allExaremePlans/1NODE10GBQ3";
        //planFileQ4 = "/home/panos/allExaremePlans/1NODE10GBQ4";
        //runExperiment(1, 1098, 8088, planFileQ1, planFileQ2, planFileQ3, planFileQ4, 10);

        //planFileQ1 = "/home/panos/allExaremePlans/2NODE100GBQ1";
        //planFileQ2 = "/home/panos/allExaremePlans/2NODE50GBQ2";
        //planFileQ3 = "/home/panos/allExaremePlans/2NODE50GBQ3";
        //planFileQ4 = "/home/panos/allExaremePlans/2NODE50GBQ4";
        //runExperiment(2, 1098, 8088, planFileQ1, planFileQ2, planFileQ3, planFileQ4, 100);

        //planFileQ1 = "/home/panos/allExaremePlans/4NODE100GBQ1.json";
        //planFileQ2 = "/home/panos/allExaremePlans/4NODE100GBQ2.json";
        //planFileQ3 = "/home/panos/allExaremePlans/4NODE100GBQ3.json";
        //planFileQ4 = "/home/panos/allExaremePlans/4NODE100GBQ4.json";
        //runExperiment(4, 1098, 8088, planFileQ1, planFileQ2, planFileQ3, planFileQ4, 100);

       // planFileQ1 = "/home/panos/allExaremePlans/8NODE100GBQ1.json";
       // planFileQ2 = "/home/panos/allExaremePlans/8NODE100GBQ2.json";
       // planFileQ3 = "/home/panos/allExaremePlans/8NODE100GBQ3.json";
       // planFileQ4 = "/home/panos/allExaremePlans/8NODE100GBQ4.json";
       // runExperiment(8, 1098, 8088, planFileQ1, planFileQ2, planFileQ3, planFileQ4, 100);

        //Start threads for exareme testing
        //Thread t = new Thread(new ExperimentThread(50, 2, null));

       // t.start();

       // System.out.println("Thread isAlive: " + t.isAlive());

       // System.out.println("Waiting for thread to finish...");

       // try {
       //     System.out.println("Waiting for threads to finish.");
      //      t.join();
      //  } catch (InterruptedException e) {
      //      System.out.println("Main thread Interrupted");
       // }

      //  try{
      //      System.out.println("Sleeping for 10 seconds...");
       //     Thread.sleep(10 * 1000);
       // } catch (InterruptedException e) {
      //      System.out.println("Main thread Interrupted while sleeiping");
       //     System.exit(0);
      //  }

      //  System.out.println("Thread isAlive: " + t.isAlive());

      //  System.exit(0);

      //  Thread t2 = new Thread(new ExperimentThread(50,4, null));

      //  t2.start();

      //  System.out.println("Thread isAlive: " + t2.isAlive());

     //   System.out.println("Waiting for thread to finish...");

      //  try {
      ///      System.out.println("Waiting for threads to finish.");
     //       t2.join();
     //   } catch (InterruptedException e) {
     //       System.out.println("Main thread Interrupted");
     //   }

      //  System.out.println("Thread isAlive: " + t2.isAlive());

      //  System.exit(0);

        /*---Setup Settings----*/
        String art = FileUtil.readFile(new File(BuggyPlanEngineClientTest.class.getClassLoader()
                .getResource("madgik/exareme/master/engine/planFromApacheHive.json").getFile()));
        int port = 1098;
        int dataTport = 8088;
        int numberOfNodes = 4;
        Scanner intScanner = new Scanner(System.in);

        log.info("--------------Starting Exareme Mini Cluster-------------");
        log.info("Port: "+port);
        log.info("DataTransfer Starting Port: "+dataTport);
        log.info("Number of Nodes: "+numberOfNodes+"\n");

        ExaremeCluster miniCluster = ExaremeClusterFactory.createMiniCluster(1098, 8088, numberOfNodes);
        miniCluster.start();
        log.info("Mini cluster started.");
        ExecutionPlanParser planParser = new ExecutionPlanParser();

        do {
            try {

                //StartTime
                long startTime = System.currentTimeMillis();

                log.info("Reading File...");
                log.info("Art plan :" + art);

                //log.info("PHASE 1");

                long midTime = System.currentTimeMillis() - startTime;
                long midPoint = System.currentTimeMillis();

                ExecutionPlan executionPlan = planParser.parse(art);

                //MidPoint - Don't count the print time
                log.info("Parsed :" + executionPlan.toString());

                //log.info("PHASE 2");

                ExecutionEngineProxy engineProxy = ExecutionEngineLocator.getExecutionEngineProxy();
                ExecutionEngineSession engineSession = engineProxy.createSession();
                final ExecutionEngineSessionPlan sessionPlan = engineSession.startSession();
                sessionPlan.submitPlan(executionPlan);
                log.info("Submitted.");
                while (sessionPlan.getPlanSessionStatusManagerProxy().hasFinished() == false
                        && sessionPlan.getPlanSessionStatusManagerProxy().hasError() == false) {
                    Thread.sleep(100);
                    //log.info("NOT FINISHED YET...");
                }

                //EndPoint - Count execution time
                long endPoint = System.currentTimeMillis();
                log.info("Execution Plan Duration: " + (midTime + (endPoint - midPoint)));

                log.info("Exited");
                if (sessionPlan.getPlanSessionStatusManagerProxy().hasError() == true) {
                    log.error(sessionPlan.getPlanSessionStatusManagerProxy().getErrorList().get(0));
                }

            } catch (Exception e) {
                log.error(e);
                e.printStackTrace();
            }

            int choice = -1;
            log.info("Run another script?");
            do {
                log.info("1. Yes");
                log.info("2. No");
                choice = intScanner.nextInt();
            } while((choice != 1) && (choice != 2));

            if(choice == 2) break;

        } while(true);

        log.info("Mini Cluster stopping...");
        Thread.sleep(10 * 1000);
        miniCluster.stop(true);
        log.info("Mini Cluster stopped.");

        System.exit(0);
    }

}
