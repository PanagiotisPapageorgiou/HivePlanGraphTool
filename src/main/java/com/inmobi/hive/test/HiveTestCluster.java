package com.inmobi.hive.test;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.physical.StageIDsRearranger;
import org.apache.hadoop.hive.ql.parse.ColumnAccessInfo;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.plan.api.*;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.yarn.state.Graph;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.cli.HiveSQLException;

import org.apache.hadoop.hive.ql.*;
/*
 * This is class is used to model a minicluster and mini hive server to be 
 * used for testing.  
 */

public class HiveTestCluster {
    
    private FileSystem fs;
    private MiniHS2 miniHS2 = null;
    private Map<String, String> confOverlay;
    private HiveConf hiveConf;
    private int numberOfTaskTrackers;
    private int numberOfDataNodes;

    public HiveTestCluster(int numData, int numTasks){
        numberOfTaskTrackers = numTasks;
        numberOfDataNodes = numData;
    }

    public void start() throws Exception {
        this.start(false, 0 , 0);
    }

    public void start(boolean dynamicPartitioning, int maxParts, int maxPartPerNode) throws Exception {
        Configuration conf = new Configuration();
        hiveConf = new HiveConf(conf, 
                org.apache.hadoop.hive.ql.exec.CopyTask.class);
        miniHS2 = new MiniHS2(hiveConf, true, numberOfDataNodes, numberOfTaskTrackers);
        confOverlay = new HashMap<String, String>();
        confOverlay.put(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
        if(dynamicPartitioning == true) {
            confOverlay.put(ConfVars.DYNAMICPARTITIONING.varname, "true");
            confOverlay.put(ConfVars.DYNAMICPARTITIONINGMODE.varname, "nonstrict");
            String maxPartitions = Integer.toString(maxParts);
            String maxPartitionsNode = Integer.toString(maxPartPerNode);
            confOverlay.put(ConfVars.DYNAMICPARTITIONMAXPARTS.varname, maxPartitions);
            confOverlay.put(ConfVars.DYNAMICPARTITIONMAXPARTSPERNODE.varname, maxPartitionsNode);
        }
        confOverlay.put(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
        miniHS2.start(confOverlay);
        fs = miniHS2.getDfs().getFileSystem();
        SessionState ss = new SessionState(hiveConf);

        //printConfiguration();

        SessionState.start(ss);
    }
    
    public FileSystem getFS() {
        return this.fs;
    }
    
    public void stop() throws Exception {
        LocalFileSystem localFileSystem = FileSystem.getLocal(miniHS2.getHiveConf());
        miniHS2.stop();
        FileFilter filter = new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                if (pathname.isDirectory() && 
                        pathname.getName().startsWith("MiniMRCluster_")) {
                    return true;
                }
                return false;
            }
        };
        File targetDir = new File("target");
        File[] files = targetDir.listFiles(filter);
        for (File file : files) {
            Path clusterRoot = new Path(file.getAbsolutePath());
            localFileSystem.delete(clusterRoot, true);
        }
    }

    /*public void printConfiguration(){

        System.out.println("Now printing current Configuration for Testing purposes...");
        if(hiveConf != null){
            hiveConf.
        }
    }*/

    public List<String> executeStatements(List<String> statements, PrintWriter compileLogFile, PrintWriter resultsLogFile, String flag) throws HiveSQLException {
        List<String> results = new LinkedList<String>();

        long i = 1;

        for (String statement : statements) {
            results.addAll(processStatement(statement, compileLogFile, resultsLogFile, flag, i));
            i++;
        }

        return results;
    }

    public Stage locateStageID(String stageID, List<Stage> stagesList){

        if(stagesList != null){

            for(Stage s : stagesList){
                if(s.getStageId().equals(stageID)){
                    return s;
                }
            }
        }

        return null;
    }

    public org.apache.hadoop.hive.ql.plan.api.Operator locateOperatorID(String operatorID, List<org.apache.hadoop.hive.ql.plan.api.Operator> operatorList){

        if(operatorList != null){

            for(org.apache.hadoop.hive.ql.plan.api.Operator o : operatorList){
                if(o.getOperatorId().equals(operatorID)){
                    return o;
                }
            }
        }

        return null;
    }

    public void diveFromOperatorRoot(org.apache.hadoop.hive.ql.exec.Operator rootOperator, List<org.apache.hadoop.hive.ql.exec.Operator> discoveredOperators){

        if(discoveredOperators.contains(rootOperator)){
            System.out.println("Operator: "+rootOperator.getOperatorId()+" has already been discovered before!");
            return;
        }

        discoveredOperators.add(rootOperator);
        System.out.println("Operator: "+rootOperator.getOperatorId()+" was successfully discovered!");

        List<org.apache.hadoop.hive.ql.exec.Operator> children = rootOperator.getChildOperators();

        if(children != null) {
            for (org.apache.hadoop.hive.ql.exec.Operator op : children) {
                System.out.println("Operator: "+rootOperator.getOperatorId()+" has child: "+op.getOperatorId());
                diveFromOperatorRoot(op, discoveredOperators);
            }
        }
        else{
            System.out.println("Operator: "+rootOperator.getOperatorId()+" has no children!");
            return;
        }
    }

    public void diveInStageFromRootExec(Task stage, List<org.apache.hadoop.hive.ql.exec.Operator> previousFinalOperators, ExaremeGraph exaremeGraph, List<Task> visitedStages){

        List<org.apache.hadoop.hive.ql.exec.Operator> leaves = new LinkedList<>();
        boolean alreadyVisited = false;

        System.out.println("diveInStageFromRootExec: Currently exploring Stage: "+stage.getId());
        if(previousFinalOperators == null)
            System.out.println("diveInStageFromRootExec: PreviousFinals were: NULL");
        else{
            System.out.println("diveInStageFromRootExec: PreviousFinals were: "+previousFinalOperators.toString());
        }

        for(Task tempS : visitedStages){
            if(tempS.getId().equals(stage.getId())){
                alreadyVisited = true;
            }
        }

        Collection<org.apache.hadoop.hive.ql.exec.Operator<? extends OperatorDesc>> topOps = new LinkedList<>(); //Get Operator Graph roots
        /*if(stage instanceof FetchTask){
            stage.getFetchOperator();
        }*/
        for(Object o : stage.getTopOperators()){
            topOps.add((org.apache.hadoop.hive.ql.exec.Operator<? extends OperatorDesc>) o);
        }

        if(topOps.size() > 0){ //
            List<org.apache.hadoop.hive.ql.exec.Operator> stageOperators = new LinkedList<>();
            for(org.apache.hadoop.hive.ql.exec.Operator root : topOps) {
                diveFromOperatorRoot(root, stageOperators);
            }

            if(alreadyVisited == false) {
                System.out.println("diveInStageFromRooExect: Adding all Nodes from this graph...");
                for (org.apache.hadoop.hive.ql.exec.Operator tempOp : stageOperators) { //Add all operators from this graph
                    OperatorNode myNode = new OperatorNode(tempOp, stage);
                    exaremeGraph.addNode(myNode);
                }
            }
            else{
                System.out.println("diveInStageFromRootExec: Nodes of this Stage are already added...");
            }

            if(alreadyVisited == false) {
                System.out.println("diveInStageFromRootExec: Adding all Edges from this graph...");
                for(org.apache.hadoop.hive.ql.exec.Operator someOperator : stageOperators){
                    List<org.apache.hadoop.hive.ql.exec.Operator> children = someOperator.getChildOperators();
                    if(children != null){
                        if(children.size() > 0){
                            for(org.apache.hadoop.hive.ql.exec.Operator child : children){
                                DirectedEdge myEdge = new DirectedEdge(someOperator.getOperatorId(), child.getOperatorId(), "DON'T KNOW");
                                exaremeGraph.addDirectedEdge(myEdge);
                            }
                        }
                    }
                }
            }
            else{
                System.out.println("diveInStageFromRootExec: Edges of this Stage are already added...");
            }

            //if(previousFinalOperators != null) { //Even if we've been in this stage before, the previousFinalOperators matter
                //Now add edges from previous final operators to every root operator in this stage
                //System.out.println("diveInStageFromRootExec: Adding Edges from Previous Final Operators to current Roots...");
                //for (Operator op: previousFinalOperators) {
                    //for (Operator root : topOps) {
                        //DirectedEdge myEdge = new DirectedEdge(op.getOperatorId(), root.getOperatorId(), "CONJUCTIVE");
                        /*exaremeGraph.addDirectedEdge(myEdge);
                    }
                }
            }
            else{
                System.out.println("diveInStageFromRootExec: No previousFinalOperators exists!");
            }*/

            if(alreadyVisited == false) {
                //Build leaves (final Nodes to connect with next roots)
                System.out.println("diveInStageFromRootExec: Creating new leaves...");
                for (org.apache.hadoop.hive.ql.exec.Operator tempOp : stageOperators) {
                    if(tempOp.getChildOperators() != null){
                        List<Operator <? extends OperatorDesc>> children = ((List<Operator <? extends OperatorDesc>>) tempOp.getChildOperators());
                        if(children.size() == 0){
                            leaves.add(tempOp);
                        }
                    }
                    else{
                        leaves.add(tempOp);
                    }
                }
            }
            else{
                System.out.println("diveInStageFromRoot: Leaves of this Stage have already been created..");
            }

            if(alreadyVisited == true) {
                return;
            }
        }
        else{
            System.out.println("diveInStageFromRootExec: No Operators in this Stage!");
        }


        if(alreadyVisited == false)
            visitedStages.add(stage);

        List<org.apache.hadoop.hive.ql.exec.Operator> nextLeaves;

        if(leaves.size() == 0){
            nextLeaves = previousFinalOperators;
        }
        else{
            nextLeaves = leaves;
            if(leaves.size() == 0) nextLeaves = null;
        }

        System.out.println("diveInStageFromRootExec: Checking if Stage has children...");
        List<Task> stageChildren = stage.getDependentTasks();
        if(stageChildren != null){
            if(stageChildren.size() > 0){
                for(Task childStage : stageChildren){
                    if(nextLeaves != null)
                        System.out.println("diveInStageFromRootExec: FROM: "+stage.getId()+" Moving to child: "+childStage.getId()+" with finalOps: "+nextLeaves.toString());
                    else
                        System.out.println("diveInStageFromRootExec: FROM: "+stage.getId()+" Moving to child: "+childStage.getId()+" with finalOps: NULL");
                    diveInStageFromRootExec(childStage, nextLeaves, exaremeGraph, visitedStages);
                }
            }
            else{
                System.out.println("diveInStageFromRootExec: Stage has no children! Returning!");
                return;
            }
        }
        else{
            System.out.println("diveInStageFromRootExec: Stage has no children! Returning!");
            return;
        }

        return;

    }

    public void discoverOpsFromRoot(Operator<? extends Serializable> op, List<Operator <? extends Serializable>> allOperators){

        boolean contains = false;

        if(allOperators.contains(op)) contains = true;

        /*for(Operator<? extends Serializable> o1 : allOperators){
            if(o1.getOperatorId().equals(op.getOperatorId())){
                contains = true;
                break;
            }
        }*/

        if(contains == true){
            return;
        }

        allOperators.add(op);

        if(op.getChildOperators() != null){
            List<Operator<? extends OperatorDesc>> children = op.getChildOperators();
            if(children.size() > 0){
                for(Operator<? extends OperatorDesc> o1 : children){
                    discoverOpsFromRoot(o1, allOperators);
                }
            }
        }

        return;

    }

    public List<Operator <? extends Serializable>> discoverAllStageOperators(Task<? extends Serializable> task){

        if(task.getTopOperators() == null){
            return null;
        }

        List<Operator <? extends Serializable>> topOps = ((List<Operator <? extends Serializable>>) task.getTopOperators());

        if(topOps.size() == 0) return null;

        List<Operator <? extends Serializable>> allOperators = new LinkedList<>();

        for(Operator<? extends Serializable> op : topOps){
            if(op != null){
                discoverOpsFromRoot(op, allOperators);
            }
        }

        if(allOperators.size() == 0 ) return null;

        return allOperators;

    }

    public List<Operator <? extends Serializable>> getStageOperatorLeaves(Task<? extends java.io.Serializable> task){

        if(task.getTopOperators() == null){
            return null;
        }

        List<Operator <? extends Serializable>> topOps = ((List<Operator <? extends Serializable>>) task.getTopOperators());

        if(topOps.size() == 0) return null;

        List<Operator <? extends Serializable>> leaves = new LinkedList<>();

        List<Operator <? extends Serializable>> allOperators = discoverAllStageOperators(task);

        if(allOperators == null){
            return null;
        }

        for(Operator<? extends Serializable> o1 : allOperators){
            if(o1.getChildOperators() != null) {
                List<Operator <? extends OperatorDesc>> children = o1.getChildOperators();
                if(children.size() == 0)
                    leaves.add(o1);
            }
            else{
                leaves.add(o1);
            }
        }

        if(leaves.size() == 0) return null;

        return leaves;

    }

    public void simplifyStages(List<org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable>> stages, List<org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable>> newRoots){

        if(stages != null){
            if(stages.size() > 0){
                System.out.println("Shrinking Conditional Tasks...");
                List<Task<? extends Serializable>> bannedStages = new LinkedList<>();
                for(org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable> t : stages){ //Make every conditional Task have only 1 child
                    if(t != null){
                        System.out.println("STAGE: "+t.getId()+" has children...");
                        if(t.getChildTasks() != null){
                            for(org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable> c : t.getChildTasks()){
                                System.out.println("\tchild: "+c.getId());
                            }
                        }
                        if(t.toString().contains("CONDITIONAL") == true){ //CONDITIONAL TASK
                            System.out.println("Shrinking Conditional Task: "+t.getId()+"...");
                            org.apache.hadoop.hive.ql.exec.Task<ConditionalWork> conditionalWorkTask = (org.apache.hadoop.hive.ql.exec.Task<ConditionalWork>) t;
                            ConditionalTask cd = (ConditionalTask) conditionalWorkTask;

                            List<org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable>> children = cd.getListTasks();
                            if(children != null){
                                if(children.size() > 0){ //Find the child which contains both all operator trees
                                    org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable> wantedChild;
                                    int numberTopOps = 50000;
                                    wantedChild = null;
                                    for(org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable> child : children){
                                        if(child != null) {
                                            if(child.getTopOperators() != null) {
                                                if (numberTopOps > child.getTopOperators().size()) {
                                                    numberTopOps = child.getTopOperators().size();
                                                    wantedChild = child;
                                                }
                                            }
                                            bannedStages.add(child);
                                        }
                                    }
                                    if(wantedChild == null){
                                        System.out.println("Error! Wanted Child is null!");
                                        System.exit(9);
                                    }
                                    bannedStages.remove(wantedChild);

                                    List<org.apache.hadoop.hive.ql.exec.Task<? extends Serializable>> realChildrenList = new LinkedList<>();
                                    realChildrenList.add(wantedChild);
                                    cd.setListTasks(realChildrenList);

                                    System.out.println("Successfully shrinked Conditional Task: "+t.getId()+"!");
                                    System.out.println("WantedChild: "+wantedChild.getId());
                                    for(org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable> d : cd.getListTasks()){
                                        System.out.println("CHILD: "+d.getId());
                                    }

                                }
                            }
                        }
                    }
                }

                //Now continue by completely omitting CONDITIONAL stages and linking properly all stages
                System.out.println("Linking Parents and Children of Conditional Tasks together...");
                for(org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable> t : stages){
                    //System.out.println("Lalalala");
                    if(t != null) {
                        //System.out.println("Accessing task..."+t.getId());
                        if (t.toString().contains("CONDITIONAL") == true) { //TODO: DEN DOULEUEI SETCHILDTASKS
                            org.apache.hadoop.hive.ql.exec.Task<ConditionalWork> conditionalWorkTask = (org.apache.hadoop.hive.ql.exec.Task<ConditionalWork>) t;
                            ConditionalTask ct = (ConditionalTask) conditionalWorkTask;
                            List<org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable>> parents = ct.getParentTasks();
                            System.out.println("Working in Stage: "+ct.getId());
                            if (parents != null) {
                                //System.out.println("Non null Parent List found!");
                                if (parents.size() > 0) { //Link every Parent of this Conditional Task with Every Child of Conditional
                                    //System.out.println("More than 0 parent...");
                                    List<org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable>> children = ct.getListTasks();
                                   // System.out.println("before loop PARENTS: "+parents.toString());
                                    boolean setBefore = false;
                                    //List<org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable>> copyOfParents = new LinkedList<>();
                                    //for(org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable> parent : parents){
                                     //   copyOfParents.add(parent);
                                    //}
                                    for (org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable> parent : parents) {
                                        if (parent != null) {
                                            //System.out.println("Parent is not null...");
                                            if (children != null) {
                                                //System.out.println("Children is not null...");
                                                if(children.size() == 1){
                                                    //System.out.println("One child!");
                                                    if(children.get(0) == null){
                                                        //System.out.println("CHILD IS NULL!!!!");
                                                        System.exit(0);
                                                    }
                                                    //System.out.println("Removing dependent task from parent...");
                                                    //System.out.println("TESTING parents: "+parents.toString());
                                                    //System.out.println("TESTING parents: "+parents.toString());
                                                    //System.out.println("Removed!");
                                                    //System.out.println("Adding dependent task from parent...");
                                                    //System.out.println("TESTING parents: "+parents.toString());
                                                    parent.addDependentTask(children.get(0));
                                                    if ((parent.getChildTasks() != null) && (parent.getChildTasks().contains(t))) {
                                                        parent.getChildTasks().remove(t);
                                                    }
                                                    //System.out.println("TESTING parents: "+parents.toString());
                                                    //System.out.println("Added!");
                                                    if(setBefore == false){
                                                        setBefore = true;
                                                        //System.out.println("Setting new parents for child...");
                                                        //System.out.println("TESTING parents: "+parents.toString());
                                                        children.get(0).setParentTasks(parents);
                                                        //System.out.println("TESTING parents: "+parents.toString());
                                                        //System.out.println("Success!");
                                                    }

                                                    //System.out.println("Child: "+children.get(0).getId());
                                                    if(children.get(0).getParentTasks() == null){
                                                        System.out.println("CHILD HAS NO PARENTS!!!!!!!!! AHDIA!!");
                                                        System.exit(0);
                                                    }
                                                    //System.out.println("PARENTS: "+parents.toString());
                                                    //System.out.println("SHOULD BE THE SAME: "+children.get(0).getParentTasks().toString());

                                                    //for(Task<? extends Serializable> par : children.get(0).getParentTasks()){
                                                        //System.out.println("\tHasParent: "+par.getId());
                                                    //}
                                                    /*List<Task<? extends Serializable>> newParents = new LinkedList<>();
                                                    List<Task<? extends Serializable>> oldParents = children.get(0).getParentTasks();
                                                    if(oldParents != null) {
                                                        for (Task<? extends Serializable> la : oldParents) {
                                                            if(la != null){
                                                                newParents.add(la);
                                                            }
                                                        }
                                                    }
                                                    newParents.add(parent);
                                                    System.out.println("Setting new parents for child...");
                                                    children.get(0).setParentTasks(newParents);
                                                    System.out.println("Success!");*/
                                                }
                                                else{
                                                    System.out.println("CONDITIONAL TASK Has more than one child!");
                                                    System.exit(0);
                                                }
                                            }
                                            else{
                                                System.out.println("CONDITIONAL TASK Has no Children! NULL Children");
                                                System.exit(0);
                                            }
                                        } else {
                                            System.out.println("CONDITIONAL TASK Parent is NULL!");
                                            System.exit(0);
                                        }
                                    }
                                } else {
                                    System.out.println("Parent List is empty!");
                                    parents = null;
                                }
                            }
                            if(parents == null) {
                                System.out.println("Parent List is null!");
                                List<org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable>> children = ct.getListTasks();

                                if (children != null) {
                                    System.out.println("Children List found not null!");
                                    if (children.size() == 1) {
                                        System.out.println("Children has one children...");
                                        org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable> child = children.get(0);
                                        if (child != null) {
                                            System.out.println("Child is not null...");
                                            child.setParentTasks(null); //This will the new Root Stage
                                            child.setRootTask(true);
                                        } else {
                                            System.out.println("CONDITIONAL TASK Has NULL Child 2!");
                                            System.exit(0);
                                        }
                                    }
                                    else if(children.size() == 0){
                                        System.out.println("CONDITIONAL TASK HAS NO CHILDREN AT ALL!!");
                                        System.exit(0);
                                    }
                                    else{
                                        System.out.println("CONDITIONAL TASK HAS MORE THAN 1Children! Task: "+t.getId());
                                        for(org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable> child : children) {
                                            System.out.println("CHILD: " + child.getId());
                                        }
                                        System.exit(0);
                                    }
                                } else {
                                    System.out.println("CONDITIONAL TASK Has null Children!");
                                    System.exit(0);
                                }
                            }
                        }
                    }
                }

                System.out.println("Discovering new Roots...");
                for(org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable> t : stages){
                    if(t != null){
                        if(!bannedStages.contains(t)) {
                            if (t.toString().contains("CONDITIONAL") == false) {
                                if (t.getParentTasks() == null) {
                                    System.out.println("New Root Task discovered: " + t.getId());
                                    newRoots.add(t);
                                }
                            }
                        }
                    }
                    else{
                        System.out.println("Task is null!");
                        System.exit(0);
                    }
                }

            }
        }

    }

    public void createExaremeOutputFromExec(List<org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable>> roots, PrintWriter outputFile, QueryPlan queryPlan, List<org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable>> stagesList){

        ExaremeGraph exaremeGraphSimpler = new ExaremeGraph("Hive Native(Simplified)");

        List<Task> visitedStagesSimpler = new LinkedList<>();

        List<org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable>> newRoots = new LinkedList<>();

        outputFile.println("\t=======================Accessing QueryPlan Information===========================");
        outputFile.flush();
        HashMap<String, String> idTableNameMap = queryPlan.getIdToTableNameMap();
        if(idTableNameMap != null) {
            outputFile.println("\t\tPrinting IdToTableName HashMap...");
            outputFile.flush();
            for (HashMap.Entry<String, String> entry : idTableNameMap.entrySet()) {
                outputFile.println("\t\t\t"+entry.getKey() + " : " + entry.getValue());
                outputFile.flush();
            }
        }
        //outputFile.println("\nTo String: "+queryPlan.toString());
        outputFile.println("\t\tOperationName: "+queryPlan.getOperationName());
        outputFile.flush();
        ColumnAccessInfo columnAccessInfo = queryPlan.getColumnAccessInfo();
        if(columnAccessInfo != null) {
            outputFile.println("\t\tColumnAccessInfo to String: " + columnAccessInfo);
            outputFile.flush();
            Map<String, List<String>> columnAccessMap = columnAccessInfo.getTableToColumnAccessMap();
            if(columnAccessMap != null) {
                outputFile.println("\t\tPrinting columnAccessInfo Map...");
                outputFile.flush();
                for (Map.Entry<String, List<String>> entry : columnAccessMap.entrySet()) {
                    outputFile.println("\t\t\tEntry: " + entry.getKey());
                    outputFile.flush();
                    List<String> list = entry.getValue();
                    if(list != null)
                        for (String s : list) {
                            outputFile.println("\t\t\t\tValue: " + s);
                            outputFile.flush();
                        }
                }
            }
        }
        Map<String, Map<String, Long>> mapCounters = queryPlan.getCounters();
        if(mapCounters != null) {
            outputFile.println("\t\tPrinting map of Counters...");
            outputFile.flush();
            for (Map.Entry<String, Map<String, Long>> entry : mapCounters.entrySet()) {
                outputFile.println("\t\t\tAccessing map in entry: " + entry.getKey());
                Map<String, Long> counters = entry.getValue();
                if(counters != null)
                    for (Map.Entry<String, Long> entry1 : counters.entrySet()) {
                        outputFile.println("\t\t\t\t" + entry1.getKey() + " : " + entry1.getValue());
                        outputFile.flush();
                    }
            }
        }

        outputFile.println("\t\tAccessing InputSet...");
        outputFile.flush();
        HashSet<ReadEntity> inputSet = queryPlan.getInputs();
        if(inputSet != null) {
            for (ReadEntity readEntity : inputSet) {
                outputFile.println("\t\t\tEntity in InputSet (to String): " + readEntity.toString());
                outputFile.flush();
                List<String> accessColumns = readEntity.getAccessedColumns();
                if (accessColumns != null) {
                    outputFile.println("\t\t\t\tPrinting Accessed Columns of Entity...");
                    outputFile.flush();
                    for (String s : accessColumns) {
                        outputFile.println(s);
                        outputFile.flush();
                    }
                }
                Set<ReadEntity> parentEntities = readEntity.getParents();
                if(parentEntities != null){
                    for(ReadEntity p : parentEntities){
                        if(p != null){
                            outputFile.println("\t\t\t\tParent: "+p.toString());
                            outputFile.flush();
                        }
                    }
                }

                Path pathD = readEntity.getD();
                if(pathD != null){
                    outputFile.println("\t\t\t\tgetD: "+pathD.toString());
                    outputFile.flush();
                }

                try {
                    URI location = readEntity.getLocation();
                    if(location != null) {
                        outputFile.println("\t\t\t\tURI details: ");
                        outputFile.flush();
                        outputFile.println("\t\t\t\t\tAuthority: " + location.getAuthority());
                        outputFile.flush();
                        outputFile.println("\t\t\t\t\tFragment: " + location.getFragment());
                        outputFile.flush();
                        outputFile.println("\t\t\t\t\tHost: " + location.getHost());
                        outputFile.flush();
                        outputFile.println("\t\t\t\t\tPath: " + location.getPath());
                        outputFile.flush();
                        outputFile.println("\t\t\t\t\tQuery: " + location.getQuery());
                        outputFile.flush();
                        outputFile.println("\t\t\t\t\tRaw Authority: " + location.getRawAuthority());
                        outputFile.flush();
                        outputFile.println("\t\t\t\t\tRaw Fragment: " + location.getRawFragment());
                        outputFile.flush();
                        outputFile.println("\t\t\t\t\tRawPath: " + location.getRawPath());
                        outputFile.flush();
                        outputFile.println("\t\t\t\t\tRawQuery: " + location.getRawQuery());
                        outputFile.flush();
                        outputFile.println("\t\t\t\t\tRawSchemeSpecificPart: " + location.getRawSchemeSpecificPart());
                        outputFile.flush();
                        outputFile.println("\t\t\t\t\tScheme: " + location.getScheme());
                        outputFile.flush();
                        outputFile.println("\t\t\t\t\tSchemeSpecificPart: " + location.getSchemeSpecificPart());
                        outputFile.flush();
                        outputFile.println("\t\t\t\t\tUserInfo: " + location.getUserInfo());
                        outputFile.flush();
                        outputFile.println("\t\t\t\t\tPort: " + location.getPort());
                        outputFile.flush();
                    }
                }
                catch(java.lang.Exception ex){
                    outputFile.println("\t\t\t\tURI: Failed to get URI!");
                    outputFile.flush();
                }

                Partition partition = readEntity.getP();
                if(partition != null){
                    outputFile.println("\t\t\t\tPartition details: ");
                    outputFile.flush();
                    List<String> bucketColsList = partition.getBucketCols();
                    if(bucketColsList != null){
                        outputFile.println("\t\t\t\t\tbucketCols: "+bucketColsList.toString());
                        outputFile.flush();
                    }
                    outputFile.println("\t\t\t\t\tbucketCount: "+partition.getBucketCount());
                    outputFile.flush();
                    List<FieldSchema> colsFieldSchemas = partition.getCols();
                    if(colsFieldSchemas != null){
                        outputFile.println("\t\t\t\t\tCols FieldSchemas: ");
                        outputFile.flush();
                        for(FieldSchema f : colsFieldSchemas){
                            if(f != null){
                                outputFile.println("\t\t\t\t\t\tCol FieldSchema(toString): "+f.toString());
                                outputFile.flush();
                            }
                        }
                    }
                    outputFile.println("\t\t\t\t\tCompleteName: "+partition.getCompleteName());
                    outputFile.flush();
                    Path dataLocation = partition.getDataLocation();
                    if(dataLocation != null){
                        outputFile.println("\t\t\t\t\tData Location: "+dataLocation.toString());
                        outputFile.flush();
                    }
                    outputFile.println("\t\t\t\t\tLocation: "+partition.getLocation());
                    outputFile.flush();
                    Path partitionPath = partition.getPartitionPath();
                    if(partitionPath != null){
                        outputFile.println("\t\t\t\t\tPartition Path: "+partitionPath.toString());
                        outputFile.flush();
                    }
                }

                Table table = readEntity.getTable();
                if(table != null){
                    outputFile.println("\t\t\t\tTable details: ");
                    outputFile.flush();
                    outputFile.println("\t\t\t\t\tCompleteName: "+table.getCompleteName());
                    outputFile.flush();
                    Path dL = table.getDataLocation();
                    if(dL != null){
                        outputFile.println("\t\t\t\t\tDataLocation: "+dL.toString());
                        outputFile.flush();
                    }
                    List<FieldSchema> allCols = table.getAllCols();
                    if(allCols != null){
                        outputFile.println("\t\t\t\t\tAllCols: ");
                        outputFile.flush();
                        for(FieldSchema col : allCols){
                            if(col != null) {
                                outputFile.println("\t\t\t\t\t\tCol: " +col.toString());
                                outputFile.flush();
                            }
                        }
                    }

                    ArrayList<StructField> allFields = table.getFields();
                    if(allFields != null){
                        outputFile.println("\t\t\t\t\tAllFields: ");
                        outputFile.flush();
                        for(StructField field : allFields){
                            if(field != null){
                                outputFile.println("\t\t\t\t\t\tFieldName: " +field.getFieldName());
                                outputFile.flush();
                            }
                        }
                    }
                    outputFile.println("\t\t\t\t\tOwner: "+table.getOwner());
                    outputFile.flush();
                    List<FieldSchema> partitionKeys = table.getPartitionKeys();
                    if(partitionKeys != null){
                        outputFile.println("\t\t\t\t\tPartition Keys: ");
                        outputFile.flush();
                        for(FieldSchema col : partitionKeys){
                            if(col != null) {
                                outputFile.println("\t\t\t\t\t\tPartitionKey: " +col.toString());
                                outputFile.flush();
                            }
                        }
                    }
                }

                outputFile.println("\t\t\tIsDirect: " + readEntity.isDirect());
                outputFile.flush();
            }
        }

        outputFile.println("\t\tAccessing OutputSet...");
        outputFile.flush();
        HashSet<WriteEntity> outputSet = queryPlan.getOutputs();
        if(outputSet != null) {
            for (WriteEntity writeEntity : outputSet) {
                outputFile.println("\t\t\tEntity in OutputSet (to String): " + writeEntity.toString());
                outputFile.flush();
                WriteEntity.WriteType writeType = writeEntity.getWriteType();
                if(writeType != null) {
                    outputFile.println("\t\t\tWriteType: " + writeType.toString());
                    outputFile.flush();
                }
                Path path = writeEntity.getD();
                if(path != null) {
                    outputFile.println("\t\t\tPath: " + path.toString());
                    outputFile.flush();
                }
                outputFile.println("\t\t\tName: "+writeEntity.getName());
                outputFile.flush();
                Table table = writeEntity.getTable();
                if(table != null) {
                    outputFile.println("\t\t\tTable Name: " + table.getCompleteName());
                    outputFile.flush();
                }
            }
        }

        if(stagesList.size() > 0) { //Normal Stages/Tasks except from FetchTask exist
            System.out.println("\nSimplifying DAG of Stages...\n");

            simplifyStages(stagesList, newRoots);

            if (newRoots.size() == 0) {
                System.out.println("Something went wrong with newRoots...");
                System.exit(1);
            }

            List<org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable>> newStagesList = new LinkedList<>();

            System.out.println("\nGathering all Stages...\n");

            discoverStages(newRoots, newStagesList);

            /*if(queryPlan.getFetchTask() != null){
                System.out.println("Accessing FetchTask for QueryPlan...");
                FetchTask fetchTask = queryPlan.getFetchTask();
                if(fetchTask != null){
                    outputFile.println("FetchStageID: "+fetchTask.getId());
                    StageType stageType = fetchTask.getType();
                    if(stageType != null){
                        outputFile.println("StageType: "+stageType.toString());
                    }
                    outputFile.println("MaxTableRows: "+fetchTask.getMaxRows());
                    outputFile.println("TotalTableRows: "+fetchTask.getTotalRows());
                    TableDesc tableDesc = fetchTask.getTblDesc();
                    if(tableDesc != null){
                        outputFile.println("TableDesc: "+tableDesc.toString());
                    }
                    if(fetchTask.getChildTasks() != null){
                        outputFile.println("Children: "+fetchTask.getChildTasks().toString());
                    }
                    if(fetchTask.getParentTasks() != null){
                        outputFile.println("Parents: "+fetchTask.getParentTasks().toString());
                    }
                    if(fetchTask.getTopOperators() != null){
                        outputFile.println("Top Operators: "+fetchTask.getTopOperators().toString());
                    }
                    if(fetchTask.getFetchOperator() != null){
                        FetchOperator fetch = fetchTask.getFetchOperator();
                        Operator<? extends Serializable> insideOp = fetch.getInsideOperator();
                        if(insideOp != null){
                            outputFile.println("FetchOperatorID: "+insideOp.getOperatorId());
                            OperatorNode opNode = new OperatorNode(insideOp, fetchTask);
                            exaremeGraphSimpler.addNode(opNode);
                        }
                        if(fetch.getOutputObjectInspector() != null){
                            ObjectInspector outputInspector = fetch.getOutputObjectInspector();
                            if(outputInspector != null){
                                outputFile.println("OutputInspector: "+outputInspector.toString());
                            }
                        }
                    }
                    FetchWork fetchWork = fetchTask.getWork();
                    if(fetchWork != null){
                        outputFile.println("FetchWork: "+fetchWork.toString());
                    }
                }
                exaremeGraphSimpler.setFetchTask(fetchTask);
            }*/

            exaremeGraphSimpler.setPlanStages(newStagesList);

            System.out.println("\nGathering all Operators and given connections...\n");

            for (org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable> root : newRoots) {
                System.out.println("createExaremeOutputFromExec: Diving from Root Stage: " + root.getId());
                diveInStageFromRootExec(root, null, exaremeGraphSimpler, visitedStagesSimpler);
            }
            /*if(queryPlan.getFetchTask() != null){
                diveFromFetchTask(fetchTask, exaremeGraphSimp)
            }*/

            System.out.println("\nLinking MapJoins...\n");

            exaremeGraphSimpler.linkMapJoins();

            System.out.println("\nDiscovering current Roots...\n");

            exaremeGraphSimpler.discoverRoots();

            System.out.println("\nDiscovering current Leaves...\n");

            exaremeGraphSimpler.discoverCurrentLeaves();

            System.out.println("\nLinking appropriate Roots and Leaves based on same RowSchema...\n");

            exaremeGraphSimpler.linkRootsAndLeaves();

            System.out.println("\nDiscovering true Roots...\n");

            exaremeGraphSimpler.discoverRoots(); //Again

            exaremeGraphSimpler.discoverCurrentLeaves();

            //outputFile.println("============================================ QUERY ===============================================\n");
            //outputFile.println("\nQuery: ["+queryPlan.getQueryString()+"]\n");
            //outputFile.flush();

            FetchTask fetchTask = queryPlan.getFetchTask();
            if(fetchTask != null){

                System.out.println("\nFetchTask Exists and needs to be added to Graph...\n");

                FetchWork fetchWork = fetchTask.getWork();

                Operator<? extends Serializable> operatorSource = fetchWork.getSource();
                if(operatorSource != null){
                    exaremeGraphSimpler.addOperatorAndDiscoverChildren(operatorSource, fetchTask, null);
                    OperatorNode wanted = exaremeGraphSimpler.getOperatorNodeByName(operatorSource.getOperatorId());
                    if(wanted == null){
                        System.out.println("Something went wrong when trying to retrieve fetch Source operator from Graph...");
                        System.exit(0);
                    }
                    exaremeGraphSimpler.linkLeavesToOperatorNode(wanted);
                }
                Operator<? extends Serializable> listSinkOp = fetchWork.getSink();
                if(listSinkOp != null){
                    exaremeGraphSimpler.addOperatorAndDiscoverChildren(listSinkOp, fetchTask, null);
                    OperatorNode wanted = exaremeGraphSimpler.getOperatorNodeByName(listSinkOp.getOperatorId());
                    exaremeGraphSimpler.linkLeavesToOperatorNode(wanted);
                }

                newStagesList.add(fetchTask);
                exaremeGraphSimpler.setPlanStages(newStagesList);

            }

            exaremeGraphSimpler.printGraph(outputFile);

            exaremeGraphSimpler.printStagesList(outputFile);

            exaremeGraphSimpler.printOperatorList(outputFile);
        }
        else{
            FetchTask fetchTask = queryPlan.getFetchTask();
            if(fetchTask != null){

                System.out.println("\nOnly a FetchTask exists...\n");

                FetchWork fetchWork = fetchTask.getWork();

                Operator<? extends Serializable> operatorSource = fetchWork.getSource();
                if(operatorSource != null){
                    exaremeGraphSimpler.addOperatorAndDiscoverChildren(operatorSource, fetchTask, null);
                    OperatorNode wanted = exaremeGraphSimpler.getOperatorNodeByName(operatorSource.getOperatorId());
                    if(wanted == null){
                        System.out.println("Something went wrong when trying to retrieve fetch Source operator from Graph...");
                        System.exit(0);
                    }
                    exaremeGraphSimpler.discoverRoots();
                    exaremeGraphSimpler.discoverCurrentLeaves();
                }
                Operator<? extends Serializable> listSinkOp = fetchWork.getSink();
                if(listSinkOp != null){
                    exaremeGraphSimpler.addOperatorAndDiscoverChildren(listSinkOp, fetchTask, null);
                    OperatorNode wanted = exaremeGraphSimpler.getOperatorNodeByName(listSinkOp.getOperatorId());
                    if(wanted == null){
                        System.out.println("Something went wrong when trying to retrieve ListSink operator from Graph...");
                        System.exit(0);
                    }
                    if(operatorSource != null) {
                        exaremeGraphSimpler.linkLeavesToOperatorNode(wanted);
                    }
                    else{
                        exaremeGraphSimpler.discoverRoots();
                        exaremeGraphSimpler.discoverCurrentLeaves();
                    }
                }

                List<Task <?extends Serializable>> newStagesList = new LinkedList<>();
                newStagesList.add(fetchTask);
                exaremeGraphSimpler.setPlanStages(newStagesList);

            }

            exaremeGraphSimpler.printGraph(outputFile);

            exaremeGraphSimpler.printStagesList(outputFile);

            exaremeGraphSimpler.printOperatorList(outputFile);

        }

    }

    public void discoverStages(List<org.apache.hadoop.hive.ql.exec.Task <?extends java.io.Serializable> > rootTasks, List<org.apache.hadoop.hive.ql.exec.Task <?extends java.io.Serializable> > stagesList){

        if(rootTasks != null){
            for(Task t : rootTasks){
                discoverStagesFromRoot(t, stagesList);
            }
        }
    }

    public void discoverStagesFromRoot(Task rootTask, List<org.apache.hadoop.hive.ql.exec.Task <?extends java.io.Serializable> > stagesList){

        if(stagesList.contains(rootTask)){
            return;
        }

        stagesList.add(rootTask);

        System.out.println("Discovered new Stage: "+rootTask.getId());

        if(rootTask.getDependentTasks() != null){
            List<org.apache.hadoop.hive.ql.exec.Task <?extends java.io.Serializable> > dependents = rootTask.getDependentTasks();
            for(Task t1 : dependents){
                discoverStagesFromRoot(t1, stagesList);
            }
        }

        if(rootTask.getDependentTasks() != null){
            List<org.apache.hadoop.hive.ql.exec.Task <?extends java.io.Serializable> > children = rootTask.getDependentTasks();
            for(Task t2 : children){
                discoverStagesFromRoot(t2, stagesList);
            }
        }

        return;
    }

    private List<String> processStatement(String statement, PrintWriter compileLogFile, PrintWriter resultsLogFile, String flag, long i) {
        List<String> results = new LinkedList<String>();
        String[] tokens = statement.trim().split("\\s+");
        CommandProcessor proc = null;

        try {
            // Hive does special handling for the commands: 
            //   SET,RESET,DFS,CRYPTO,ADD,LIST,RELOAD,DELETE,COMPILE
            proc = CommandProcessorFactory.getForHiveCommand(tokens, hiveConf);
        } catch (SQLException e) {
          throw new RuntimeException("SQL error when creating command processor", e);
        }
        if (proc == null) {
            // this is for all other commands
            proc = new Driver(hiveConf);
        }
        try {

            if((flag == null) || ((flag != null) && (!flag.equals("EXAREME")))){ //Normal Hive Statement
                System.out.println("\n\nExecuting Query Normally!\nStatement:["+statement+"]\n\n");
                proc.run(statement);
                if (proc instanceof org.apache.hadoop.hive.ql.Driver) {
                    ((Driver) proc).setMaxRows(1000000); /* Set the number of rows returned by getResults */
                    ((Driver) proc).getResults(results);
                    if(resultsLogFile != null){
                        String number = Long.toString(i);
                        resultsLogFile.println("=====================================QUERY: "+number+"========================================\n");
                        resultsLogFile.flush();
                        resultsLogFile.println("\tQueryString: ["+statement+"]\n");
                        resultsLogFile.flush();
                        resultsLogFile.println("\tResults: ");
                        resultsLogFile.flush();
                        for(String s : results){
                            resultsLogFile.println("\t\t"+s);
                            resultsLogFile.flush();
                        }

                        resultsLogFile.println("\n\n");
                        resultsLogFile.flush();
                    }
                }
            }
            else{
                int choice = -1;
                if(compileLogFile == null) throw new RuntimeException("CompileLogFile is NULL!");

                do{
                    System.out.println("\n\nWhat would you like to do for Query:["+statement+"]\n\n");
                    System.out.println("Choices...");
                    Scanner intScanner = new Scanner(System.in);
                    choice = -1;
                    do{
                       System.out.println("1. Compile & Extract OperatorGraph");
                       System.out.println("2. Compile & Run Query");
                       System.out.println("3. Exit");
                       choice = intScanner.nextInt();
                    } while((choice != 1) && (choice != 2) && (choice != 3));

                    if(choice == 1){

                        System.out.println("\nCompiling and Extracting OperatorGraph!\n");
                        if(proc instanceof org.apache.hadoop.hive.ql.Driver){
                            ((Driver) proc).compile(statement, true);
                            org.apache.hadoop.hive.ql.QueryPlan queryPlan = ((Driver) proc).getPlan();
                            List<String> resultsCompile = new LinkedList<String>();

                            String number = Long.toString(i);
                            compileLogFile.println("=====================================QUERY: "+number+"========================================\n");
                            compileLogFile.flush();
                            compileLogFile.println("\tQueryString: ["+statement+"]\n");
                            compileLogFile.flush();

                            compileLogFile.println("\tCurrent Driver Details:");
                            compileLogFile.flush();
                            Schema schema = ((Driver) proc).getSchema();
                            if(schema != null){
                                //compileLogFile.println("Accessing Schema...");
                                compileLogFile.println("\t\tSchema(toString): "+schema.toString());
                                compileLogFile.flush();
                            }
                            ClusterStatus clusterStatus = ((Driver) proc).getClusterStatus();
                            if(clusterStatus != null){
                                //compileLogFile.println("Accessing ClusterStatus...");
                                compileLogFile.println("\t\tClusterStatus(toString): "+clusterStatus.toString());
                                compileLogFile.flush();
                                List<String> activeTrackers = (List<String>) clusterStatus.getActiveTrackerNames();
                                if(activeTrackers != null){
                                    compileLogFile.println("\t\tActiveTrackers: ");
                                    compileLogFile.flush();
                                    for(String s : activeTrackers){
                                        compileLogFile.println("\t\t\tActiveTracker: "+s);
                                        compileLogFile.flush();
                                    }
                                }
                                List<String> blackListTrackerNames = (List<String>) clusterStatus.getBlacklistedTrackerNames();
                                if(blackListTrackerNames != null){
                                    compileLogFile.println("\t\tBlackListedTrackers: ");
                                    compileLogFile.flush();
                                    for(String s : blackListTrackerNames){
                                        compileLogFile.println("\t\t\tBlackListedTracker: "+s);
                                        compileLogFile.flush();
                                    }
                                }
                            }

                            HashMap<String, String> idTableNameMap = queryPlan.getIdToTableNameMap();
                            if(idTableNameMap != null) {
                                compileLogFile.println("\t\tIdToTableName HashMap: ");
                                for (HashMap.Entry<String, String> entry : idTableNameMap.entrySet()) {
                                    if(entry != null){
                                        compileLogFile.println("\t\t\t"+entry.getKey() + " : " + entry.getValue());
                                        compileLogFile.flush();
                                    }
                                }
                            }
                            //compileLogFile.println("\nTo String: "+queryPlan.toString());
                            compileLogFile.println("\t\tOperationName: "+queryPlan.getOperationName());
                            compileLogFile.flush();
                            ColumnAccessInfo columnAccessInfo = queryPlan.getColumnAccessInfo();
                            if(columnAccessInfo != null) {
                                compileLogFile.println("\t\tColumnAccessInfo to String: " + columnAccessInfo);
                                compileLogFile.flush();
                                Map<String, List<String>> columnAccessMap = columnAccessInfo.getTableToColumnAccessMap();
                                if(columnAccessMap != null) {
                                    compileLogFile.println("\t\tPrinting columnAccessInfo Map...");
                                    compileLogFile.flush();
                                    for (Map.Entry<String, List<String>> entry : columnAccessMap.entrySet()) {
                                        if(entry != null) {
                                            compileLogFile.print("\t\t\tEntry: " + entry.getKey());
                                            compileLogFile.flush();
                                            List<String> list = entry.getValue();
                                            if (list != null){
                                                for (String s : list) {
                                                    if (s != null) {
                                                        compileLogFile.println("Value: " + s);
                                                        compileLogFile.flush();
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                                /*Map<String, Map<String, Long>> mapCounters = queryPlan.getCounters();
                                if(mapCounters != null) {
                                    outputFile.println("\nPrinting map of Counters...");
                                    for (Map.Entry<String, Map<String, Long>> entry : mapCounters.entrySet()) {
                                        outputFile.println("Accessing map in entry: " + entry.getKey());
                                        Map<String, Long> counters = entry.getValue();
                                        if(counters != null)
                                            for (Map.Entry<String, Long> entry1 : counters.entrySet()) {
                                                outputFile.println("\t" + entry1.getKey() + " : " + entry1.getValue());
                                            }
                                    }
                                }*/

                            compileLogFile.println("\t\tAccessing InputSet...");
                            compileLogFile.flush();
                            HashSet<ReadEntity> inputSet = queryPlan.getInputs();
                            if(inputSet != null) {
                                for (ReadEntity readEntity : inputSet) {
                                    compileLogFile.println("\t\t\tEntity in InputSet (to String): " + readEntity.toString());
                                    compileLogFile.flush();
                                    List<String> accessColumns = readEntity.getAccessedColumns();
                                    if (accessColumns != null) {
                                        compileLogFile.println("\t\t\tPrinting Accessed Columns of Entity...");
                                        compileLogFile.flush();
                                        for (String s : accessColumns) {
                                            if(s != null) {
                                                compileLogFile.println("\t\t\t\t" + s);
                                                compileLogFile.flush();
                                            }
                                        }
                                    }
                                    compileLogFile.println("\t\t\tEach entity seems to have parents...need to check this...");
                                    compileLogFile.flush();
                                    compileLogFile.println("\t\t\tIsDirect: " + readEntity.isDirect());
                                    compileLogFile.flush();
                                }
                            }

                            compileLogFile.println("\t\tAccessing OutputSet...");
                            HashSet<WriteEntity> outputSet = queryPlan.getOutputs();
                            if(outputSet != null) {
                                for (WriteEntity writeEntity : outputSet) {
                                    compileLogFile.println("\t\t\tEntity in OutputSet (to String): " + writeEntity.toString());
                                    compileLogFile.flush();
                                    WriteEntity.WriteType writeType = writeEntity.getWriteType();
                                    if (writeType != null) {
                                        compileLogFile.println("\t\t\tWriteType: " + writeType.toString());
                                        compileLogFile.flush();
                                    }
                                    Path path = writeEntity.getD();
                                    if (path != null) {
                                        compileLogFile.println("\t\t\tPath: " + path.toString());
                                        compileLogFile.flush();
                                    }
                                    if(writeEntity.getName() != null) {
                                        compileLogFile.println("\t\t\tName: " + writeEntity.getName());
                                        compileLogFile.flush();
                                    }
                                    Table table = writeEntity.getTable();
                                    if (table != null) {
                                        compileLogFile.println("\t\t\tTable Name: " + table.getCompleteName());
                                        compileLogFile.flush();
                                    }

                                }
                            }

                            List<org.apache.hadoop.hive.ql.exec.Task <?extends java.io.Serializable> > rootTasks;
                            List<org.apache.hadoop.hive.ql.exec.Task <?extends java.io.Serializable> > trueStagesList;

                            if((queryPlan.getRootTasks() != null) && (queryPlan.getRootTasks().size() > 0)){
                                rootTasks = queryPlan.getRootTasks();
                                trueStagesList = new LinkedList<>();

                                discoverStages(rootTasks, trueStagesList);

                                System.out.println("\nPrinting all discovered Stages\n");
                                for(Task t1 : trueStagesList){
                                    System.out.println(t1.toString());
                                }

                                System.out.println("\nTotal number of items: "+trueStagesList.size());

                                createExaremeOutputFromExec(rootTasks, compileLogFile, queryPlan, trueStagesList);

                            }
                            else if(queryPlan.getFetchTask() != null) {
                                System.out.println("\nFetchTask exists and needs to be added...");
                                FetchTask fetchTask = queryPlan.getFetchTask();
                                rootTasks = new LinkedList<>();
                                trueStagesList = new LinkedList<>();
                                createExaremeOutputFromExec(rootTasks, compileLogFile, queryPlan, trueStagesList);
                            }

                            System.out.println("\n\n\n");

                            String statement2 = "explain ".concat(statement);
                            proc.run(statement2);

                            ((Driver) proc).setMaxRows(1000000); /* Set the number of rows returned by getResults */
                            ((Driver) proc).getResults(resultsCompile);

                            compileLogFile.println("\n");
                            compileLogFile.flush();
                            compileLogFile.println("\tExplain(Human Readable) Output: \n");
                            compileLogFile.flush();

                            for(String str : resultsCompile){
                                compileLogFile.println("\t\t"+str);
                                compileLogFile.flush();
                            }

                            resultsCompile.clear();

                            compileLogFile.println("\n\n");
                            compileLogFile.flush();

                        }
                    }
                    else if(choice == 2){
                        System.out.println("\n\nExecuting Query Normally!\nStatement:["+statement+"]\n\n");
                        proc.run(statement);
                        if (proc instanceof org.apache.hadoop.hive.ql.Driver) {
                            ((Driver) proc).setMaxRows(1000000); /* Set the number of rows returned by getResults */
                            ((Driver) proc).getResults(results);
                        }

                        if(resultsLogFile != null){
                            String number = Long.toString(i);
                            resultsLogFile.println("=====================================QUERY: "+number+"========================================\n");
                            resultsLogFile.flush();
                            resultsLogFile.println("\tQueryString: ["+statement+"]\n");
                            resultsLogFile.flush();
                            resultsLogFile.println("\tResults: ");
                            resultsLogFile.flush();
                            for(String s : results){
                                resultsLogFile.println("\t\t"+s);
                                resultsLogFile.flush();
                            }

                            resultsLogFile.println("\n\n");
                            resultsLogFile.flush();
                        }

                    }
                    else{
                        throw new RuntimeException("User Stopped Script Execution!");
                    }

                    choice = -1;
                    System.out.println("\nProceed to next Query?");
                    do {
                        System.out.println("1. Yes");
                        System.out.println("2. ReRun the same Query");
                        System.out.println("3. Exit");
                        choice = intScanner.nextInt();
                    } while((choice != 1) && (choice != 2) && (choice != 3));

                    if(choice == 1) break;
                    else if(choice == 3) throw new RuntimeException("User Stopped Script Execution!");

                } while(true);
            }

        } catch (Exception ex) {
            throw new RuntimeException("Hive SQL exception", ex);
        }

        return results;
    }

}
