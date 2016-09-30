package com.inmobi.hive.test;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import madgik.exareme.common.app.engine.AdpDBOperatorType;
import madgik.exareme.common.app.engine.AdpDBSelectOperator;
import madgik.exareme.common.schema.Select;
import madgik.exareme.common.schema.TableView;
import madgik.exareme.common.schema.expression.Comments;
import madgik.exareme.common.schema.expression.DataPattern;
import madgik.exareme.common.schema.expression.SQLSelect;
import madgik.exareme.utils.encoding.Base64Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.exec.mr.MapredLocalTask;
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
    PrintWriter clusterInfo = null;
    String currentDatabasePath;

    public HiveTestCluster(int numData, int numTasks){
        currentDatabasePath = "";
        numberOfTaskTrackers = numTasks;
        numberOfDataNodes = numData;
        File f = new File("src/main/resources/files/clusterInfo.txt");
        if(f.exists() && !f.isDirectory()) {
            f.delete();
        }

        try {
            clusterInfo = new PrintWriter(f);
        } catch (FileNotFoundException var9) {
            throw new RuntimeException("Failed to open FileOutputStream for clusterInfo.txt", var9);
        }

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
            clusterInfo.println("dynamicPartitioning=true");
            clusterInfo.flush();
            clusterInfo.println("maxPartitions(total)="+maxParts);
            clusterInfo.flush();
            clusterInfo.println("maxPartitions(perNode)="+maxPartPerNode);
            clusterInfo.flush();
            confOverlay.put(ConfVars.DYNAMICPARTITIONINGMODE.varname, "nonstrict");
            String maxPartitions = Integer.toString(maxParts);
            String maxPartitionsNode = Integer.toString(maxPartPerNode);
            confOverlay.put(ConfVars.DYNAMICPARTITIONMAXPARTS.varname, maxPartitions);
            confOverlay.put(ConfVars.DYNAMICPARTITIONMAXPARTSPERNODE.varname, maxPartitionsNode);
        }
        else{
            clusterInfo.println("dynamicPartitioning=false");
            clusterInfo.flush();
        }
        confOverlay.put(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);

        miniHS2.start(confOverlay);
        fs = miniHS2.getDfs().getFileSystem();
        SessionState ss = new SessionState(hiveConf);

        SessionState.start(ss);

        clusterInfo.println("NameNode Port: "+fs.getUri().getPort());
        clusterInfo.flush();

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

        if(clusterInfo != null) {
            clusterInfo.close();
        }
    }

    public List<String> executeStatements(List<String> statements, PrintWriter compileLogFile, PrintWriter resultsLogFile, String exaremePlanPath, String flag) throws HiveSQLException {
        List<String> results = new LinkedList<String>();

        long i = 1;

        for (String statement : statements) {
            results.addAll(processStatement(statement, compileLogFile, resultsLogFile, exaremePlanPath, flag, i));
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

        if(stage instanceof MapRedTask){
            MapRedTask mapRedStage = (MapRedTask) stage;
            if(mapRedStage != null){
                MapredWork mapRedWork = mapRedStage.getWork();
                if(mapRedWork != null){
                    System.out.println("MapReduce Stage...");
                    MapWork mapWork = mapRedWork.getMapWork();
                    Set<Operator<?>> allRootOperators;
                    List<org.apache.hadoop.hive.ql.exec.Operator> stageOperators = new LinkedList<>();
                    if(mapWork != null){
                        System.out.println("Grabbing Map Part of MapRedWork...");
                        allRootOperators = mapWork.getAllRootOperators();
                        if(allRootOperators != null){
                            for(Operator<?> o : allRootOperators){
                                topOps.add((org.apache.hadoop.hive.ql.exec.Operator<? extends OperatorDesc>) o);
                            }

                            if(topOps.size() > 0){
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
                                System.out.println("diveInStageFromRootExec: No Operators in MAP part of this Stage!");
                                System.exit(1);
                            }
                        }
                    }
                    ReduceWork reduceWork = mapRedWork.getReduceWork();
                    if(reduceWork != null){
                        System.out.println("Grabbing Reduce Part of MapRedWork...");
                        Set<Operator<?>> reduceRoots = reduceWork.getAllRootOperators();
                        if(reduceRoots != null){
                            if(reduceRoots.size() > 0){
                                for(org.apache.hadoop.hive.ql.exec.Operator root : reduceRoots) {
                                    diveFromOperatorRoot(root, stageOperators);
                                    root.setParentOperators(leaves);
                                    for(Operator<?> l : leaves){
                                        if(l != null){
                                            List<Operator<?>> paidia = new LinkedList<>();
                                            if(l.getChildOperators() == null){
                                                paidia.add(root);
                                            }
                                            else{
                                                for(Operator<?> p : l.getChildOperators()){
                                                    if(p != null){
                                                        paidia.add(p);
                                                    }
                                                }
                                                paidia.add(root);
                                            }
                                            l.setChildOperators(paidia);

                                            DirectedEdge myEdge = new DirectedEdge(l.getOperatorId(), root.getOperatorId(),  "DON'T KNOW");
                                            exaremeGraph.addDirectedEdge(myEdge);

                                        }
                                    }
                                }

                                for (org.apache.hadoop.hive.ql.exec.Operator tempOp : stageOperators) { //Add all operators from this graph
                                    OperatorNode myNode = new OperatorNode(tempOp, stage);
                                    exaremeGraph.addNode(myNode);
                                }

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

                                leaves = new LinkedList<>();

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
                        }
                    }
                }
            }
        }
        /*else if(stage instanceof MoveTask){
            MoveTask moveTask = (MoveTask) stage;
            if(moveTask != null){
                MoveWork moveWork = moveTask.getWork();
                if(moveWork != null){
                    LoadFileDesc loadFileDesc = moveWork.getLoadFileWork();
                    if(loadFileDesc != null){
                        loadFileDesc.
                    }
                }
            }
        }*/
        else {
        /*if(stage instanceof FetchTask){
            stage.getFetchOperator();
        }*/
            System.out.println("Non MapReduceStage...");

            for (Object o : stage.getTopOperators()) {
                topOps.add((org.apache.hadoop.hive.ql.exec.Operator<? extends OperatorDesc>) o);
            }

            if (topOps.size() > 0) { //
                List<org.apache.hadoop.hive.ql.exec.Operator> stageOperators = new LinkedList<>();
                for (org.apache.hadoop.hive.ql.exec.Operator root : topOps) {
                    diveFromOperatorRoot(root, stageOperators);
                }

                if (alreadyVisited == false) {
                    System.out.println("diveInStageFromRooExect: Adding all Nodes from this graph...");
                    for (org.apache.hadoop.hive.ql.exec.Operator tempOp : stageOperators) { //Add all operators from this graph
                        OperatorNode myNode = new OperatorNode(tempOp, stage);
                        exaremeGraph.addNode(myNode);
                    }
                } else {
                    System.out.println("diveInStageFromRootExec: Nodes of this Stage are already added...");
                }

                if (alreadyVisited == false) {
                    System.out.println("diveInStageFromRootExec: Adding all Edges from this graph...");
                    for (org.apache.hadoop.hive.ql.exec.Operator someOperator : stageOperators) {
                        List<org.apache.hadoop.hive.ql.exec.Operator> children = someOperator.getChildOperators();
                        if (children != null) {
                            if (children.size() > 0) {
                                for (org.apache.hadoop.hive.ql.exec.Operator child : children) {
                                    DirectedEdge myEdge = new DirectedEdge(someOperator.getOperatorId(), child.getOperatorId(), "DON'T KNOW");
                                    exaremeGraph.addDirectedEdge(myEdge);
                                }
                            }
                        }
                    }
                } else {
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

                if (alreadyVisited == false) {
                    //Build leaves (final Nodes to connect with next roots)
                    System.out.println("diveInStageFromRootExec: Creating new leaves...");
                    for (org.apache.hadoop.hive.ql.exec.Operator tempOp : stageOperators) {
                        if (tempOp.getChildOperators() != null) {
                            List<Operator<? extends OperatorDesc>> children = ((List<Operator<? extends OperatorDesc>>) tempOp.getChildOperators());
                            if (children.size() == 0) {
                                leaves.add(tempOp);
                            }
                        } else {
                            leaves.add(tempOp);
                        }
                    }
                } else {
                    System.out.println("diveInStageFromRoot: Leaves of this Stage have already been created..");
                }

                if (alreadyVisited == true) {
                    return;
                }
            } else {
                System.out.println("diveInStageFromRootExec: No Operators in this Stage!");
            }
        }

        if(previousFinalOperators != null){
            if(previousFinalOperators.size() > 0){
                if(topOps != null){
                    if(topOps.size() > 0){
                        for(Operator<?> op : topOps){
                            if(op != null){
                                if(op.getSchema() != null){
                                    if(op.getSchema().toString().contains("col")){
                                        for(Operator<?> leaf : previousFinalOperators){
                                            if(leaf != null){
                                                if(leaf.getSchema() != null){
                                                    if(leaf.getSchema().toString().contains("col")){
                                                        if(op.getSchema().toString().equals(leaf.getSchema().toString())){
                                                            List<Operator<?>> children = new LinkedList<>();
                                                            List<Operator<?>> parents = new LinkedList<>();
                                                            if(op.getParentOperators() != null){
                                                                for(Operator<?> e : op.getParentOperators()){
                                                                    parents.add(e);
                                                                }
                                                            }
                                                            if(parents.contains(leaf) == false){
                                                                parents.add(leaf);
                                                                op.setParentOperators(parents);
                                                            }
                                                            if(leaf.getChildOperators() != null){
                                                                for(Operator<?> e : op.getChildOperators()){
                                                                    children.add(e);
                                                                }
                                                            }
                                                            if(children.contains(op) == false){
                                                                children.add(op);
                                                                leaf.setChildOperators(children);
                                                            }

                                                            DirectedEdge e = new DirectedEdge(leaf.getOperatorId(), op.getOperatorId(), "LEAF TO ROOT");
                                                            exaremeGraph.addDirectedEdge(e);
                                                            System.out.println("Added Edge from Leaf: " + leaf.getOperatorId() + " to Root: " + op.getOperatorId());
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
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

    public String getDataBaseOfEntity(String entityString){
        String dbName = "";

        if(entityString.contains("@")){
            String[] entityParts = entityString.split("@");
            dbName = entityParts[0];
        }
        else{
            System.out.println("Invalid entity string! Does not contain @!");
            System.exit(1);
        }

        return dbName;

    }

    public boolean isPartition(String entityString){

        if(entityString.contains("@")){
            String[] entityParts = entityString.split("@");
            if(entityParts.length == 2){ //IsTable
                return false;
            }
            else {
                if (entityParts.length == 1) {
                    System.out.println("Invalid entity string! Does not contain anything after DBNAME!");
                    System.exit(1);
                }
                else{
                    return true;
                }
            }
        }
        else{
            System.out.println("Invalid entity string! Does not contain @!");
            System.exit(1);
        }

        return false;

    }

    public LinkedHashMap<String, String> getPartitionKeysAndValues(String partitionName){
        //Name Should be beginning with @

        LinkedHashMap<String, String> partitionKeyValuePairs = new LinkedHashMap<>();

        if(partitionName.contains("@")){
            String[] partitionDirs = partitionName.split("@");
            for(String partition : partitionDirs){
                if(partition.length() == 0) continue;
                String[] keyValueTuple = partition.split("=");
                partitionKeyValuePairs.put(keyValueTuple[0], keyValueTuple[1]);
            }
        }
        else{
            System.out.println("PartitionName does not contain any @!");
            System.exit(0);
        }

        return partitionKeyValuePairs;

    }

    public String getTableNameOfEntity(String entityString){
        String tbName = "";

        if(entityString.contains("@")){
            String[] entityParts = entityString.split("@");
            tbName = entityParts[1];
        }
        else{
            System.out.println("Invalid entity string! Does not contain @!");
            System.exit(1);
        }

        return tbName;

    }

    public String getPartitionNameOfEntity(String entityString){
        String partName = "@";

        if(entityString.contains("@")){
            String[] entityParts = entityString.split("@");
            if(entityParts.length > 2){
                for(int i=2; i < entityParts.length; i++){
                    if(i == entityParts.length - 1)
                        partName=partName+entityParts[i];
                    else
                        partName=partName+entityParts[i]+"@";
                }
            }
            else{
                System.out.println("Invalid entity string! Does not contain more than 2 @ for partition locating...!");
                System.exit(1);
            }
        }
        else{
            System.out.println("Invalid entity string! Does not contain @!");
            System.exit(1);
        }

        return partName;

    }

    public List<AdpDBSelectOperator> createDummyExaremeOperatorsOneNodeNoPartitions(List<OperatorQuery> opQueriesList){

        List<AdpDBSelectOperator> selectOperatorList = new LinkedList<>();

        System.out.println("Creating Dummy Exareme Operators for One Node No Partitions...");

        int i = 0;
        for(OperatorQuery opQuery : opQueriesList){ //All RunQueries

            /*---Create SQLSelect---*/
            SQLSelect sqlSelect = new SQLSelect();
            if(i == 0) {
                System.out.println("Setting InputDataPattern=CARTESIAN_PRODUCT for SQLSelect...");
                sqlSelect.setInputDataPattern(DataPattern.cartesian_product);
                System.out.println("Setting OutputDataPattern=SAME for SQLSelect...");
                sqlSelect.setOutputDataPattern(DataPattern.same);
                System.out.println("Setting NumberOfOutputParts=-1 for SQLSelect...");
                sqlSelect.setNumberOfOutputPartitions(-1); //means no partitions
                System.out.println("Setting ResultTableName for SQLSelect...");
                //sqlSelect.setResultTable(opQuery.getOutputTableNames().get(0), true, false);
                System.out.println("Setting PartsDefn=null for SQLSelect...");
                sqlSelect.setPartsDefn(null);
                System.out.println("Setting sqlQuery for SQLSelect...");
                sqlSelect.setSql(opQuery.getExaremeQueryString());
                Comments someComments = new Comments();
                someComments.addLine("test_comment");
                System.out.println("Setting sqlQuery for COmments...");
                sqlSelect.setComments(someComments);

            }
            else if(i == 1){
                System.out.println("Setting InputDataPattern=CARTESIAN_PRODUCT for SQLSelect...");
                sqlSelect.setInputDataPattern(DataPattern.cartesian_product);
                System.out.println("Setting OutputDataPattern=SAME for SQLSelect...");
                sqlSelect.setOutputDataPattern(DataPattern.same);
                System.out.println("Setting NumberOfOutputParts=-1 for SQLSelect...");
                sqlSelect.setNumberOfOutputPartitions(-1); //means no partitions
                System.out.println("Setting ResultTableName for SQLSelect...");
                //sqlSelect.setResultTable(opQuery.getOutputTableNames().get(0), false, false);
                System.out.println("Setting PartsDefn=null for SQLSelect...");
                sqlSelect.setPartsDefn(null);
                System.out.println("Setting sqlQuery for SQLSelect...");
                sqlSelect.setSql(opQuery.getExaremeQueryString());
                Comments someComments = new Comments();
                someComments.addLine("test_comment");
                System.out.println("Setting sqlQuery for COmments...");
                sqlSelect.setComments(someComments);
            }
            System.out.println("Testing created SQLSelect...\nSQLSelect:\n\t"+sqlSelect.toString());

            /*---Create Select---*/
            List<TableView> inputTableViews = new LinkedList<>();

            //madgik.exareme.common.schema.Table inputTable = new madgik.exareme.common.schema.Table(opQuery.getInputTableNames().get(0));

            /*if(i == 0) {
                System.out.println("Setting inputTable SqlDefinition... - i="+i);
                inputTable.setSqlDefinition("CREATE TABLE customer_demographics(\n" +
                        "  cd_demo_sk INT,\n" +
                        "  cd_gender TEXT,\n" +
                        "  cd_marital_status TEXT,\n" +
                        "  cd_education_status TEXT,\n" +
                        "  cd_purchase_estimate INT,\n" +
                        "  cd_credit_rating TEXT,\n" +
                        "  cd_dep_count INT,\n" +
                        "  cd_dep_employed_count INT,\n" +
                        "  cd_dep_college_count INT\n" +
                        ")");
                System.out.println("Setting inputTable setTemp... - i="+i);
                inputTable.setTemp(false);
                System.out.println("Setting inputTable setLevel=-1 - i="+i);
                inputTable.setLevel(-1);

                System.out.println("Setting Initialising InputTableView - i="+i);
                TableView inputTableView = new TableView(inputTable);
                System.out.println("Setting inputTableView DataPattern=CARTESIAN_PRODUCT - i="+i);
                inputTableView.setPattern(DataPattern.cartesian_product);
                System.out.println("Setting inputTableView setNumOfPartitions=-1 - i="+i);
                inputTableView.setNumOfPartitions(-1);
                System.out.println("Setting inputTableView addUsedColumn=cd_demo_sk - i="+i);
                inputTableView.addUsedColumn("cd_demo_sk");
                System.out.println("Setting inputTableView addUsedColumn=cd_education_status - i="+i);
                inputTableView.addUsedColumn("cd_education_status");

                inputTableViews.add(inputTableView);

            }
            else if(i == 1){
                System.out.println("Setting inputTable SqlDefinition... - i="+i);
                inputTable.setSqlDefinition("CREATE TABLE RS_15(\n" +
                        "  cd_demo_sk INT\n" +
                        ")");
                System.out.println("Setting inputTable setTemp... - i="+i);
                inputTable.setTemp(true);
                System.out.println("Setting inputTable setLevel=1 - i="+i);
                inputTable.setLevel(1);

                System.out.println("Setting Initialising InputTableView - i="+i);
                TableView inputTableView = new TableView(inputTable);
                System.out.println("Setting inputTableView DataPattern=CARTESIAN_PRODUCT - i="+i);
                inputTableView.setPattern(DataPattern.cartesian_product);
                System.out.println("Setting inputTableView setNumOfPartitions=-1 - i="+i);
                inputTableView.setNumOfPartitions(-1);
                System.out.println("Setting inputTableView addUsedColumn=cd_demo_sk - i="+i);
                inputTableView.addUsedColumn("cd_demo_sk");

                inputTableViews.add(inputTableView);
            }*/

            /*madgik.exareme.common.schema.Table outputTable = new madgik.exareme.common.schema.Table(opQuery.getOutputTableNames().get(0));
            TableView outputTableView;

            if(i == 0){
                System.out.println("Setting outputTable SqlDefinition... - i="+i);
                outputTable.setSqlDefinition("CREATE TABLE RS_15(\n" +
                        "  cd_demo_sk INT\n" +
                        ")");
                System.out.println("Setting outputTable setTemp... - i="+i);
                outputTable.setTemp(true);
                System.out.println("Setting outputTable setLevel=1 - i="+i);
                outputTable.setLevel(1);

                System.out.println("Setting Initialising OutputTableView - i="+i);
                outputTableView = new TableView(outputTable);
                System.out.println("Setting outputTableView DataPattern=SAME - i="+i);
                outputTableView.setPattern(DataPattern.same);
                System.out.println("Setting outputTableView setNumOfPartitions=-1 - i="+i);
                outputTableView.setNumOfPartitions(-1);

                System.out.println("Setting initialising SelectQuery - i="+i);
                Select selectQuery = new Select(i, sqlSelect, outputTableView);
                System.out.println("Adding InputTableViews to Select...");
                for(TableView inputV : inputTableViews){
                    selectQuery.addInput(inputV);
                }
                System.out.println("Adding QueryStatement to Select...");
                selectQuery.addQueryStatement(opQuery.getExaremeQueryString());
                System.out.println("Setting Query of Select...");
                selectQuery.setQuery(opQuery.getExaremeQueryString());
                System.out.println("Setting Mappings of Select...");
                selectQuery.setMappings(null);
                System.out.println("Setting DatabaseDir of Select...");
                selectQuery.setDatabaseDir("/base/warehouse/tpcds.db");

                System.out.println("Printing created SELECT for Debugging...\n\t"+selectQuery.toString());

                System.out.println("Setting initialising AdpDBSelectOperator - i="+i);
                AdpDBSelectOperator exaremeSelectOperator = new AdpDBSelectOperator(AdpDBOperatorType.runQuery, selectQuery, i);

                //exaremeSelectOperator.addInput(opQuery.getInputTableNames().get(0), 0);
                //exaremeSelectOperator.addOutput(opQuery.getOutputTableNames().get(0), 0);

                selectOperatorList.add(exaremeSelectOperator);

                System.out.println("Testing exaremeSelectOperator...\n\t"+exaremeSelectOperator.toString());

            }
            else if(i == 1){
                System.out.println("Setting outputTable SqlDefinition... - i="+i);
                outputTable.setSqlDefinition("CREATE TABLE OP_21(\n" +
                        "  cd_demo_sk INT\n" +
                        ")");
                System.out.println("Setting outputTable setTemp... - i="+i);
                outputTable.setTemp(false);
                System.out.println("Setting outputTable setLevel=2 - i="+i);
                outputTable.setLevel(2);

                System.out.println("Setting Initialising OutputTableView - i="+i);
                outputTableView = new TableView(outputTable);
                System.out.println("Setting outputTableView DataPattern=SAME - i="+i);
                outputTableView.setPattern(DataPattern.same);
                System.out.println("Setting outputTableView setNumOfPartitions=-1 - i="+i);
                outputTableView.setNumOfPartitions(1);

                System.out.println("Setting initialising SelectQuery - i="+i);
                Select selectQuery = new Select(i, sqlSelect, outputTableView);

                System.out.println("Adding InputTableViews to Select...");
                for(TableView inputV : inputTableViews){
                    selectQuery.addInput(inputV);
                }
                System.out.println("Adding QueryStatement to Select...");
                selectQuery.addQueryStatement(opQuery.getExaremeQueryString());
                System.out.println("Setting Query of Select...");
                selectQuery.setQuery(opQuery.getExaremeQueryString());
                System.out.println("Setting Mappings of Select...");
                selectQuery.setMappings(null);
                System.out.println("Setting DatabaseDir of Select...");
                selectQuery.setDatabaseDir("/base/warehouse/tpcds.db");

                System.out.println("Printing created SELECT for Debugging...\n\t"+selectQuery.toString());

                System.out.println("Setting initialising AdpDBSelectOperator - i="+i);
                AdpDBSelectOperator exaremeSelectOperator = new AdpDBSelectOperator(AdpDBOperatorType.runQuery, selectQuery, i);

                //exaremeSelectOperator.addInput(opQuery.getInputTableNames().get(0), 0);
                //exaremeSelectOperator.addOutput(opQuery.getOutputTableNames().get(0), 0);

                selectOperatorList.add(exaremeSelectOperator);

                System.out.println("Testing exaremeSelectOperator...\n\t"+exaremeSelectOperator.toString());

                //Create tableUnion
                System.out.println("Setting initialising TableUnion SelectQuery - i="+i);
                Select unionSelect = new Select(i+1, sqlSelect, outputTableView);

                System.out.println("Adding InputTableViews to Select...");
                for(TableView inputV : inputTableViews){
                    unionSelect.addInput(inputV);
                }
                System.out.println("Adding QueryStatement to Select...");
                unionSelect.addQueryStatement(opQuery.getExaremeQueryString());
                System.out.println("Setting Query of Select...");
                unionSelect.setQuery("select * from op_21");
                System.out.println("Setting Mappings of Select...");
                unionSelect.setMappings(null);
                System.out.println("Setting DatabaseDir of Select...");
                unionSelect.setDatabaseDir("/base/warehouse/tpcds.db");

                System.out.println("Printing created SELECT for Debugging...\n\t"+unionSelect.toString());


                System.out.println("Setting initialising AdpDBSelectOperator(TableUnion) - i="+i+1);
                AdpDBSelectOperator exaremeUnionOperator = new AdpDBSelectOperator(AdpDBOperatorType.tableUnionReplicator, unionSelect, i+1);

                //exaremeUnionOperator.addInput(opQuery.getInputTableNames().get(0), 0);
                //exaremeUnionOperator.addOutput(opQuery.getOutputTableNames().get(0), 0);

                selectOperatorList.add(exaremeUnionOperator);

                System.out.println("Testing exaremeUnionOperator...\n\t"+exaremeUnionOperator.toString());
            }*/

            i++;

        }

        return selectOperatorList;

    }

    public void createExaremeOutputFromExec(List<org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable>> roots, PrintWriter outputFile, QueryPlan queryPlan, List<org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable>> stagesList, String exaremePlanPath){

        ExaremeGraph exaremeGraphSimpler = new ExaremeGraph("Hive Native(Simplified)");

        List<Task> visitedStagesSimpler = new LinkedList<>();

        List<org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable>> newRoots = new LinkedList<>();

        List<MyTable> inputTables = new LinkedList<>();
        List<MyPartition> inputPartitions = new LinkedList<>();
        List<MyTable> outputTables = new LinkedList<>();
        List<MyPartition> outputPartitions = new LinkedList<>();

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
                MyTable inputTable = new MyTable();
                MyPartition inputPartition = new MyPartition();

                outputFile.println("\t\t\tEntity in InputSet (to String): " + readEntity.toString());
                outputFile.flush();

                if(readEntity.toString().contains("database:") == false) {
                    if(readEntity.toString().contains("file:")){
                        System.out.println("InputEntity is a file!");
                        inputTable = new MyTable(readEntity.toString(), true);
                    }
                    else {
                        if (isPartition(readEntity.toString())) {
                            inputPartition.setBelongingDatabaseName(getDataBaseOfEntity(readEntity.toString()));
                            inputPartition.setBelongingTableName(getTableNameOfEntity(readEntity.toString()));
                            inputPartition.setPartitionName(getPartitionNameOfEntity(readEntity.toString()));
                            System.out.println("Detected new InputPartition! - DB: " + inputPartition.getBelongingDataBaseName() + " Table: " + inputPartition.getBelogingTableName() + " - Partition: " + inputPartition.getPartitionName());
                        } else {
                            System.out.println("Detected new InputTable!");
                            inputTable.setBelongingDatabaseName(getDataBaseOfEntity(readEntity.toString()));
                            inputTable.setTableName(getTableNameOfEntity(readEntity.toString()));
                            System.out.println("Detected new InputTable! - DB: " + inputTable.getBelongingDataBaseName() + " Table: " + inputTable.getTableName());
                        }
                    }
                }
                else{
                    try{
                        currentDatabasePath = readEntity.getLocation().getPath();
                        System.out.println("CurrentDataBasePath="+currentDatabasePath);
                    }
                    catch(java.lang.Exception ex){
                        System.out.println("Exception while extracting database path...");
                        System.exit(0);
                    }
                }

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

                        if(readEntity.toString().contains("database:") == false) {
                            if(readEntity.toString().contains("file:") == false){
                                if (isPartition(readEntity.toString())) {
                                    inputPartition.setURIdetails(location);
                                    System.out.println("Added URI details to inputPartition!");
                                }
                                else{
                                    inputTable.setURIdetails(location);
                                    System.out.println("Added URI details to inputTable!");
                                }
                            }
                        }

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

                    inputPartition.setBucketColsList(bucketColsList);
                    inputPartition.setBucketCount(partition.getBucketCount());
                    inputPartition.setAllFields(partition.getCols());

                    inputPartition.setPartitionHDFSPath(partitionPath);

                    System.out.println("Added extra partition information to InputPartition!");
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

                    if(readEntity.toString().contains("database:") == false) {
                        if (readEntity.toString().contains("file:") == false) {
                            if (isPartition(readEntity.toString())) {
                                System.out.println("Using PartitionName to locate PartitionKeys and PartitionValues for this Partition...");
                                LinkedHashMap<String, String> keysAndValuesPairs = getPartitionKeysAndValues(getPartitionNameOfEntity(readEntity.toString()));
                                inputPartition.setKeyValuePairs(keysAndValuesPairs);
                                List<FieldSchema> partitionKeysToAdd = new LinkedList<>();
                                List<String> partitionValuesToAdd = new LinkedList<>();
                                for (Map.Entry<String, String> entry : keysAndValuesPairs.entrySet()) {
                                    if (entry != null) {
                                        for (FieldSchema f : partitionKeys) {
                                            if (f.getName().equals(entry.getKey())) {
                                                partitionKeysToAdd.add(f);
                                                partitionValuesToAdd.add(entry.getValue());
                                                System.out.println("Adding Key: " + entry.getKey() + " of Type: " + f.getType() + " and Value: " + entry.getValue() + " to inputPartition!");
                                            }
                                        }
                                    }
                                }

                                inputPartition.setAllPartitionKeys(partitionKeysToAdd);
                                inputPartition.setAllPartitionValues(partitionValuesToAdd);

                                System.out.println("Added key-value pairs to Partition!");

                            } else {
                                inputTable.setAllCols(allCols);
                                inputTable.setAllFields(allFields);
                                inputTable.setTableHDFSPath(dL);
                                inputTable.setAllPartitionKeys(partitionKeys);

                                if (partitionKeys == null) {
                                    inputTable.setHasPartitions(false);
                                } else {
                                    if (partitionKeys.size() > 0) {
                                        inputTable.setHasPartitions(true);
                                    } else {
                                        inputTable.setHasPartitions(false);
                                    }
                                }
                                System.out.println("Set AllCols, AllFields, HDFS Path, PartitionKeys for inputTable!");
                            }
                        }
                    }

                }

                outputFile.println("\t\t\tIsDirect: " + readEntity.isDirect());
                outputFile.flush();

                if(readEntity.toString().contains("database:") == false) {
                    if(readEntity.toString().contains("file:")){
                        inputTables.add(inputTable);
                        System.out.println("Added InputFile to List!");
                    }
                    else {
                        if (isPartition(readEntity.toString())) {
                            inputPartitions.add(inputPartition);
                            String rootPartitionPath = inputPartition.getBelogingTableName() + inputPartition.getPartitionName().replace("@", "/") + "/" + "000000_0";
                            System.out.println("Is Root Hive PARTITION Input - Setting Input Partition hivePath: " + rootPartitionPath);
                            inputPartition.setRootHiveLocationPath(rootPartitionPath);
                            System.out.println("Added InputPartition to List!");
                        } else {
                            System.out.println("Is Root Hive Input - Setting Input Table hivePath: " + inputTable.getTableName() + "/" + inputTable.getTableName() + ".dat");
                            inputTable.setRootHiveLocationPath(inputTable.getTableName() + "/" + inputTable.getTableName() + ".dat");
                            inputTables.add(inputTable);
                            System.out.println("Added InputTable to List!");
                        }
                    }
                }

            }
        }

        outputFile.println("\t\tAccessing OutputSet...");
        outputFile.flush();
        HashSet<WriteEntity> outputSet = queryPlan.getOutputs();
        if(outputSet != null) {
            for (WriteEntity writeEntity : outputSet) {
                MyTable outputTable = new MyTable();
                MyPartition outputPartition = new MyPartition();

                outputFile.println("\t\t\tEntity in OutputSet (to String): " + writeEntity.toString());
                outputFile.flush();

                if(writeEntity.toString().contains("database:") == false) {
                    if(writeEntity.toString().contains("file:") == false) {
                        if (isPartition(writeEntity.toString())) {
                            outputPartition.setBelongingDatabaseName(getDataBaseOfEntity(writeEntity.toString()));
                            outputPartition.setBelongingTableName(getTableNameOfEntity(writeEntity.toString()));
                            outputPartition.setPartitionName(getPartitionNameOfEntity(writeEntity.toString()));
                            System.out.println("Detected new Output! - DB: " + outputPartition.getBelongingDataBaseName() + " Table: " + outputPartition.getBelogingTableName() + " - Partition: " + outputPartition.getPartitionName());
                        } else {
                            System.out.println("Detected new OutputTable!");
                            outputTable.setBelongingDatabaseName(getDataBaseOfEntity(writeEntity.toString()));
                            outputTable.setTableName(getTableNameOfEntity(writeEntity.toString()));
                            System.out.println("Detected new OutputTable! - DB: " + outputTable.getBelongingDataBaseName() + " Table: " + outputTable.getTableName());
                        }
                    }
                    else{
                        outputTable = new MyTable(writeEntity.toString(), true);
                        System.out.println("OutputEntity is a file!");
                    }
                }

                Path pathD = writeEntity.getD();
                if(pathD != null){
                    outputFile.println("\t\t\t\tgetD: "+pathD.toString());
                    outputFile.flush();
                }

                try {
                    URI location = writeEntity.getLocation();
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

                        if(writeEntity.toString().contains("database:") == false) {
                            if(writeEntity.toString().contains("file:") == false) {
                                if (isPartition(writeEntity.toString())) {
                                    outputPartition.setURIdetails(location);
                                    System.out.println("Added URI details to outputPartition!");
                                } else {
                                    outputTable.setURIdetails(location);
                                    System.out.println("Added URI details to outputTable!");
                                }
                            }
                        }

                    }
                }
                catch(java.lang.Exception ex){
                    outputFile.println("\t\t\t\tURI: Failed to get URI!");
                    outputFile.flush();
                }

                Partition partition = writeEntity.getP();
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

                    outputPartition.setBucketColsList(bucketColsList);
                    outputPartition.setBucketCount(partition.getBucketCount());
                    outputPartition.setAllFields(partition.getCols());
                    outputPartition.setPartitionHDFSPath(partitionPath);

                    System.out.println("Added extra partition information to OutputPartition!");
                }

                Table table = writeEntity.getTable();
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

                    if(writeEntity.toString().contains("database:") == false) {
                        if (writeEntity.toString().contains("file:") == false) {
                            if (isPartition(writeEntity.toString())) {
                                System.out.println("Using PartitionName to locate PartitionKeys and PartitionValues for this Partition...");
                                LinkedHashMap<String, String> keysAndValuesPairs = getPartitionKeysAndValues(getPartitionNameOfEntity(writeEntity.toString()));
                                outputPartition.setKeyValuePairs(keysAndValuesPairs);
                                List<FieldSchema> partitionKeysToAdd = new LinkedList<>();
                                List<String> partitionValuesToAdd = new LinkedList<>();
                                for (Map.Entry<String, String> entry : keysAndValuesPairs.entrySet()) {
                                    if (entry != null) {
                                        for (FieldSchema f : partitionKeys) {
                                            if (f.getName().equals(entry.getKey())) {
                                                partitionKeysToAdd.add(f);
                                                partitionValuesToAdd.add(entry.getValue());
                                                System.out.println("Adding Key: " + entry.getKey() + " of Type: " + f.getType() + " and Value: " + entry.getValue() + " to inputPartition!");
                                            }
                                        }
                                    }
                                }

                                outputPartition.setAllPartitionKeys(partitionKeysToAdd);
                                outputPartition.setAllPartitionValues(partitionValuesToAdd);

                                System.out.println("Added key-value pairs to Partition!");

                            } else {
                                outputTable.setAllCols(allCols);
                                outputTable.setAllFields(allFields);
                                outputTable.setTableHDFSPath(dL);
                                outputTable.setAllPartitionKeys(partitionKeys);

                                if (partitionKeys == null) {
                                    outputTable.setHasPartitions(false);
                                } else {
                                    if (partitionKeys.size() > 0) {
                                        outputTable.setHasPartitions(true);
                                    } else {
                                        outputTable.setHasPartitions(false);
                                    }
                                }
                                System.out.println("Set AllCols, AllFields, HDFS Path, PartitionKeys for outputTable!");
                            }
                        }
                    }
                }

                if(writeEntity.toString().contains("database:") == false) {
                    if(writeEntity.toString().contains("file:") == false) {
                        if (isPartition(writeEntity.toString())) {
                            outputPartitions.add(outputPartition);
                            System.out.println("Added OutputPartition to List!");
                        } else {
                            outputTables.add(outputTable);
                            System.out.println("Added OutputTable to List!");
                        }
                    }
                    else{
                        outputTables.add(outputTable);
                        System.out.println("Added OutputFile to List!");
                    }
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

            exaremeGraphSimpler.setPlanStages(newStagesList);

            System.out.println("\nGathering all Operators and given connections...\n");

            for (org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable> root : newRoots) {
                System.out.println("createExaremeOutputFromExec: Diving from Root Stage: " + root.getId());
                diveInStageFromRootExec(root, null, exaremeGraphSimpler, visitedStagesSimpler);
            }

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

        //Creating exaremePlan
        if(queryPlan.getQueryString().equals("use tpcds_db")) {
            System.out.println("Skipping DATABASE command query...");
        }
        else{

            //Build Queries for Exareme Operators
            QueryBuilder queryBuilder = new QueryBuilder(exaremeGraphSimpler, inputTables, inputPartitions, outputTables, outputPartitions, currentDatabasePath);
            queryBuilder.createExaOperators(outputFile);
            List<AdpDBSelectOperator> exaremeOperators = queryBuilder.translateToExaremeOps();
            List<OpLink> opLinks = queryBuilder.getOpLinksList();

            List<ExaremeOperator> finalExaOps = new LinkedList<>();
            for(int l = 0; l < exaremeOperators.size(); l++){
                AdpDBSelectOperator exaOp = exaremeOperators.get(l);
                List<Parameter> parameterList = new LinkedList<>();
                StringParameter behaviourP = new StringParameter("behavior", "store_and_forward");
                parameterList.add(behaviourP);
                if(l == exaremeOperators.size() - 1) {
                    StringParameter categoryP = new StringParameter("category", "tab_"+exaOp.getQuery().getOutputTable().getTable().getName());
                    parameterList.add(categoryP);
                }
                else{
                    StringParameter categoryP = new StringParameter("category", "exe_"+exaOp.getQuery().getOutputTable().getTable().getName());
                    parameterList.add(categoryP);
                }
                NumParameter memoryP = new NumParameter("memoryPercentage", 1);
                parameterList.add(memoryP);

                ExaremeOperator finalOp;

                if(l == exaremeOperators.size() - 1){
                    finalOp = new ExaremeOperator("c0", "madgik.exareme.master.engine.executor.remote.operator.data.TableUnionReplicator", "TR_"+exaremeOperators.get(l).getQuery().getOutputTable().getTable().getName()+"_P_0", exaOp, parameterList, l);
                }
                else {
                    finalOp = new ExaremeOperator("c0", "madgik.exareme.master.engine.executor.remote.operator.process.ExecuteSelect", queryBuilder.getQueryList().get(l).getExaremeOutputTableName(), exaOp, parameterList, l);
                }
                finalExaOps.add(finalOp);
            }

            System.out.println("===============================Constructing Exareme Plan...=====================================");
            outputFile.println("===============================Constructing Exareme Plan...=====================================");
            outputFile.flush();
            //Build Containers for Exareme Plan
            List<Container> containers = new LinkedList<>();
            Container singleNode = new Container("c0", "192.168.1.6_container_192.168.1.6", 1098, 8088);
            containers.add(singleNode);

            System.out.println("\n\t------Containers------");
            outputFile.println("\n\t------Containers------");
            outputFile.flush();
            for(Container c : containers) {
                System.out.println("\t\tName: "+c.getName()+" - IP: "+c.getIP()+" - Data_Transfer_Port: "+c.getData_transfer_port()+" Port: "+c.getPort());
                outputFile.println("\t\tName: "+c.getName()+" - IP: "+c.getIP()+" - Data_Transfer_Port: "+c.getData_transfer_port()+" Port: "+c.getPort());
                outputFile.flush();
            }

            //Print Exareme Plan Operators Section
            System.out.println("\n\t------Operators------");
            outputFile.println("\n\t------Operators------");
            outputFile.flush();
            for(ExaremeOperator ex : finalExaOps){

                System.out.println("\t\tContainer: "+ex.getContainerName());
                outputFile.println("\t\tContainer: "+ex.getContainerName());
                outputFile.flush();
                System.out.println("\t\tOperatorName: "+ex.getOperatorName());
                outputFile.println("\t\tOperatorName: "+ex.getOperatorName());
                outputFile.flush();
                System.out.println("\t\tResultName: "+ex.getResultsName());
                outputFile.println("\t\tResultName: "+ex.getResultsName());
                outputFile.flush();
                System.out.println("\t\tQueryString: \n\t\t\t"+ex.getQueryString());
                outputFile.println("\t\tQueryString: \n\t\t\t"+ex.getQueryString());
                outputFile.flush();
                System.out.println("\t\tParameters: ");
                outputFile.println("\t\tParameters: ");
                outputFile.flush();
                for(Parameter p : ex.getParameters()){
                    if(p instanceof NumParameter){
                        NumParameter nP = (NumParameter) p;
                        System.out.println("\t\t\tParameterType: "+nP.getParemeterType()+" - Value: "+ nP.getValue());
                        outputFile.println("\t\t\tParameterType: "+nP.getParemeterType()+" - Value: "+ nP.getValue());
                        outputFile.flush();
                    }
                    else{
                        StringParameter sP = (StringParameter) p;
                        System.out.println("\t\t\tParameterType: "+sP.getParemeterType()+" - Value: "+ sP.getValue());
                        outputFile.println("\t\t\tParameterType: "+sP.getParemeterType()+" - Value: "+ sP.getValue());
                        outputFile.flush();
                    }
                }
            }

            System.out.println("\n\t------OpLinks------");
            outputFile.println("\n\t------OpLinks------");
            outputFile.flush();

            for(OpLink opLink : opLinks){
                System.out.println("\t\tContainerName: "+opLink.getContainerName());
                outputFile.println("\t\tContainerName: "+opLink.getContainerName());
                outputFile.flush();
                System.out.println("\t\tFromTable: "+opLink.getFromTable());
                outputFile.println("\t\tFromTable: "+opLink.getFromTable());
                outputFile.flush();
                System.out.println("\t\tToTable: "+opLink.getToTable());
                outputFile.println("\t\tToTable: "+opLink.getToTable());
                outputFile.flush();
                System.out.println("\t\tParameters: ");
                outputFile.println("\t\tParameters: ");
                outputFile.flush();
                for(Parameter p : opLink.getParameters()){
                    if(p instanceof NumParameter){
                        NumParameter nP = (NumParameter) p;
                        System.out.println("\t\t\tParameterType: "+nP.getParemeterType()+" - Value: "+nP.getValue());
                        outputFile.println("\t\t\tParameterType: "+nP.getParemeterType()+" - Value: "+nP.getValue());
                        outputFile.flush();
                    }
                    else{
                        StringParameter sP = (StringParameter) p;
                        System.out.println("\t\t\tParameterType: "+sP.getParemeterType()+" - Value: "+sP.getValue());
                        outputFile.println("\t\t\tParameterType: "+sP.getParemeterType()+" - Value: "+sP.getValue());
                        outputFile.flush();
                    }
                }
            }

            File f3 = new File(exaremePlanPath);
            if(f3.exists() && !f3.isDirectory()) {
                f3.delete();
            }

            PrintWriter exaremePlanFile;
            try {
                exaremePlanFile = new PrintWriter(f3);
            } catch (FileNotFoundException var8) {
                throw new RuntimeException("Failed to open FileOutputStream for outputQuery.txt", var8);
            }

            //Build ExaremePlan
            ExaremePlan exaremePlan = new ExaremePlan(containers, finalExaOps, opLinks);
            if(exaremePlanFile != null){
                exaremePlan.printExaremePlan(exaremePlanFile);
            }

            exaremePlanFile.close();

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

    private List<String> processStatement(String statement, PrintWriter compileLogFile, PrintWriter resultsLogFile, String exaremePlanPath, String flag, long i) {
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
                    Scanner stringScanner = new Scanner(System.in);
                    choice = -1;
                    do{
                       System.out.println("1. Compile & Extract OperatorGraph");
                       System.out.println("2. Compile & Run Query");
                       System.out.println("3. Skip");
                       System.out.println("4. ListStatus Metastore Files/Dirs");
                       System.out.println("5. Exit");
                       choice = intScanner.nextInt();
                    } while((choice != 1) && (choice != 2) && (choice != 3) && (choice != 4) && (choice != 5));

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

                                createExaremeOutputFromExec(rootTasks, compileLogFile, queryPlan, trueStagesList, exaremePlanPath);

                            }
                            else if(queryPlan.getFetchTask() != null) {
                                System.out.println("\nFetchTask exists and needs to be added...");
                                FetchTask fetchTask = queryPlan.getFetchTask();
                                rootTasks = new LinkedList<>();
                                trueStagesList = new LinkedList<>();
                                createExaremeOutputFromExec(rootTasks, compileLogFile, queryPlan, trueStagesList, exaremePlanPath);
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
                    else if(choice == 3){
                        System.out.println("\nSkipping Query...");
                        break;
                    }
                    else if(choice == 4){
                        do {
                            System.out.print("\nType the Path to use ls on: ");
                            String lsPath = stringScanner.next();
                            System.out.println();

                            if (lsPath.contains("\n")) lsPath = lsPath.replace("\n", "");

                            Path thePath = new Path(lsPath);

                            System.out.print("\nPrinting FileStatuses for given path: ");
                            FileStatus[] fileStatusArray = miniHS2.getDfs().getFileSystem().listStatus(thePath);
                            if (fileStatusArray != null) {
                                for (FileStatus fileStatus : fileStatusArray) {
                                    System.out.println("FileStatus: " + fileStatus.toString());
                                }
                            }

                            int choiceLS = -1;
                            System.out.println("\nRun another LS Command?");

                            do {
                                System.out.println("1. Yes");
                                System.out.println("2. No");
                                choiceLS = intScanner.nextInt();
                            } while ((choiceLS != 1) && (choiceLS != 2));

                            if (choiceLS == 2) break;
                        } while(true);


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
