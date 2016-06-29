package com.inmobi.hive.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.exec.mr.MapredLocalTask;
import org.apache.hadoop.hive.ql.index.IndexMetadataChangeTask;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.Counters;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.*;
/**
 * Created by panos on 21/4/2016.
 */
public class ExaremeGraph {

    private List<OperatorNode> roots;
    private List<OperatorNode> leaves;
    private List<OperatorNode> nodesList;
    private List<DirectedEdge> edges;
    private List<org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable>> planStages;
    private String queryString;
    private int numberOfWorkers;
    private String label;

    public ExaremeGraph(String l) {
        roots = new LinkedList<OperatorNode>();
        leaves = new LinkedList<OperatorNode>();
        nodesList = new LinkedList<OperatorNode>();
        edges = new LinkedList<DirectedEdge>();
        label = l;
    }

    public void setPlanStages(List<org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable>> stages) {
        planStages = stages;
    }

    public List<org.apache.hadoop.hive.ql.exec.Task<? extends java.io.Serializable>> getPlanStages() {
        return planStages;
    }

    public void addOperatorAndDiscoverChildren(Operator<? extends Serializable> op, Task<? extends Serializable> t, OperatorNode previousFather){

        if(op != null){
            for(OperatorNode o : nodesList){
                if(o.getOperatorName().equals(op.getOperatorId())){
                    return;
                }
            }
            OperatorNode fatherNode = new OperatorNode(op, t);
            addNode(fatherNode);
            if(previousFather != null){
                DirectedEdge e = new DirectedEdge(previousFather.getOperatorName(), fatherNode.getOperatorName(), "normal");
                for(DirectedEdge ed : edges){
                    if(ed.isEqualTo(e)){
                        return;
                    }
                }
                edges.add(e);
            }
            if(op.getChildOperators() != null){
                if(op.getChildOperators().size() > 0){
                    for(Operator<? extends OperatorDesc> c : op.getChildOperators()){
                        if(c != null){
                            addOperatorAndDiscoverChildren(c, t, fatherNode);
                        }
                    }
                }
            }
            return;
        }
    }

    public void linkLeavesToOperatorNode(OperatorNode newLeaf){

        if(leaves != null){
            if(leaves.size() > 0){
                if(newLeaf != null) {
                    for (OperatorNode op : leaves) {
                        DirectedEdge newEdge = new DirectedEdge(op.getOperatorName(), newLeaf.getOperatorName(), "normal");
                        this.addDirectedEdge(newEdge);
                    }
                    discoverCurrentLeaves();
                }
                else{
                    System.out.println("NewLeaf is null!");
                    System.exit(0);
                }
            }
            else{
               System.out.println("ERROR NO LEAVES EXIST!");
                System.exit(0);
            }
        }

        return;
    }

    public void linkOperatorNodes(String name1, String name2){

        if(name1 != null){
            if(name2 != null){
                for(OperatorNode op1 : nodesList){
                    if(op1.getOperatorName().equals(name1)){
                        for(OperatorNode op2 : nodesList){
                            if(op2.getOperatorName().equals(name2)){
                                DirectedEdge edge = new DirectedEdge(name1, name2, "normal");
                                this.addDirectedEdge(edge);
                                System.out.println("Successfully added Edge from "+name1+" to "+name2);
                                return;
                            }
                        }
                    }
                }
            }
        }
        System.out.println("Failed to add Edge from "+name1+" to "+name2);
    }

    public OperatorNode getOperatorNodeByName(String name){
        if(nodesList.size() > 0){
            for(OperatorNode la : nodesList){
                if(la.getOperatorName().equals(name)){
                    return la;
                }
            }
        }
        return null;
    }

    public void addNode(OperatorNode op1) {
        for(OperatorNode o : nodesList){
            if(o.getOperatorName().equals(op1.getOperatorName())){
                System.out.println("addNode - OperatorNode: " + op1.getOperatorName() + " already exists in Graph!");
                return;
            }
        }
        System.out.println("addNode - OperatorNode: " + op1.getOperatorName() + " successfully added!");
        nodesList.add(op1);
    }

    public void addRoot(OperatorNode op1) {
        if (roots.contains(op1)) {
            //for(OperatorNode op : roots){
            //if(op.compareOperatorNames(op1) == true){
            System.out.println("addRoot - OperatorNode: " + op1.getOperatorName() + " already exists in Graph Roots!");
            return;
            //}
        }
        System.out.println("addRoot - OperatorNode: " + op1.getOperatorName() + " successfully added!");
        roots.add(op1);
    }

    public void addLeaf(OperatorNode op1) {
        if (leaves.contains(op1)) {
            //for(OperatorNode op : roots){
            //if(op.compareOperatorNames(op1) == true){
            System.out.println("addLeaf - OperatorNode: " + op1.getOperatorName() + " already exists in Graph Leaves!");
            return;
            //}
        }
        System.out.println("addLeaf - OperatorNode: " + op1.getOperatorName() + " successfully added!");
        leaves.add(op1);

    }

    public boolean checkEdgeVerticesExist(DirectedEdge e1) {

        String fromVertex = e1.getFromVertex();
        String toVertex = e1.getToVertex();

        boolean fromVertexFound = false;
        for (OperatorNode op : nodesList) {
            if (op.getOperatorName().equals(fromVertex)) {
                fromVertexFound = true;
                break;
            }
        }

        boolean toVertexFound = false;
        for (OperatorNode op : nodesList) {
            if (op.getOperatorName().equals(toVertex)) {
                toVertexFound = true;
                break;
            }
        }

        if ((fromVertexFound == true) && (toVertexFound == true)) {
            return true;
        }

        return false;
    }

    public void addDirectedEdge(DirectedEdge e1) {

        for (DirectedEdge e : edges) {
            if (e.isEqualTo(e1) == true) {
                System.out.println("addDirectedEdge - Edge: [FROM: " + e1.getFromVertex() + " TO: " + e1.getToVertex() + "] already exists in Graph!");
                return;
            }
        }
        edges.add(e1);
        System.out.println("addDirectedEdge - Edge: [FROM: " + e1.getFromVertex() + " TO: " + e1.getToVertex() + "] successfully added in Graph!");

    }

    public List<OperatorNode> getRoots() {
        return roots;
    }

    public List<OperatorNode> getLeaves() {
        return leaves;
    }

    public List<OperatorNode> getNodesList() {
        return nodesList;
    }

    public List<DirectedEdge> getEdges() {
        return edges;
    }

    public void discoverRoots() {

        if (roots.size() > 0) {
            roots.clear();
            roots = new LinkedList<>();
        }

        boolean hasParent;
        for (OperatorNode op : nodesList) {
            hasParent = false;
            for (DirectedEdge e : edges) {
                if (e.getToVertex().equals(op.getOperatorName())) { //Discovered father
                    hasParent = true;
                    break;
                }
            }

            if (hasParent == false) {
                roots.add(op); //Add Operator to roots
            }
        }

    }

    public void discoverCurrentLeaves() {

        if (leaves.size() > 0) leaves = new LinkedList<>();

        for (OperatorNode op : nodesList) {
            if ((op.getOperator().getChildOperators() == null) || ((op.getOperator().getChildOperators() != null) && (op.getOperator().getChildOperators().size() == 0))) {
                addLeaf(op);
            }
        }
    }

    public int countColXOccurencesInString(String subString, String fullString) {

        char[] subCharArray = subString.toCharArray();
        char[] fullCharArray = fullString.toCharArray();
        int count = 0;

        int i = 0;
        int j = 0;
        boolean streak = false;
        boolean foundNumber = false;
        while (i < fullCharArray.length) {
            if (fullCharArray[i] == subCharArray[j]) { //Characters match aka c,o,l
                if (streak == false) { //Beginning match streak
                    streak = true;
                }
                j++;
            } else {
                if (streak == true) { //Streak was ongoing
                    if (subCharArray[j] == 'x') { //Time to check for number after col
                        if ((fullCharArray[i] >= '0') && (fullCharArray[i] <= '9')) { //Found number after col
                            if (foundNumber == false) {
                                foundNumber = true;
                            }
                        } else {
                            if (foundNumber == true) {
                                foundNumber = false;
                                streak = false;
                                j = 0;
                                count++;
                            } else {
                                j = 0;
                                streak = false;
                            }
                        }
                    } else { //Streak was broken before col was formed
                        streak = false;
                        j = 0;
                    }
                }
            }
            i++;
        }

        System.out.println("SubString: " + subString + " exists " + count + " times in String" + fullString);
        return count;
    }

    public void linkRootsAndLeaves() {

        if (roots != null) {
            if (roots.size() > 0) {
                for (OperatorNode opNode : roots) {
                    for (OperatorNode opLeaf : leaves) {
                        if (opLeaf != null) {
                            Operator<? extends OperatorDesc> leaf = opLeaf.getOperator();
                            if (leaf != null) {
                                if (leaf.getSchema() != null) {
                                    if (opNode.getOperator().getSchema() != null) {
                                        if (leaf.getSchema().toString().equals(opNode.getOperator().getSchema().toString())) {
                                            DirectedEdge e = new DirectedEdge(leaf.getOperatorId(), opNode.getOperator().getOperatorId(), "LEAF TO ROOT");
                                            addDirectedEdge(e);
                                            System.out.println("Added Edge from Leaf: " + leaf.getOperatorId() + " to Root: " + opNode.getOperator().getOperatorId());
                                            List<Operator<? extends OperatorDesc>> rootParents;
                                            if (opNode.getOperator().getParentOperators() != null) {
                                                rootParents = opNode.getOperator().getParentOperators();
                                            } else {
                                                rootParents = new LinkedList<>();
                                            }
                                            rootParents.add(leaf);
                                            opNode.getOperator().setParentOperators(rootParents);
                                            if ((leaf.getChildOperators() == null) || ((leaf.getChildOperators() != null) && (leaf.getChildOperators().size() == 0))) {
                                                List<Operator<? extends OperatorDesc>> leafChildren = new LinkedList<>();
                                                leafChildren.add(opNode.getOperator());
                                                leaf.setChildOperators(leafChildren);
                                            } else {
                                                List<Operator<? extends OperatorDesc>> leafChildren = leaf.getChildOperators();
                                                if (!leafChildren.contains(opNode.getOperator())) {
                                                    leafChildren.add(opNode.getOperator());
                                                    leaf.setChildOperators(leafChildren);
                                                    System.out.println("WARNING: This leaf has now more than one child check if this is correct! Children: " + leafChildren.toString());
                                                    System.exit(1);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                for (OperatorNode opNode : roots) {
                    Operator<? extends OperatorDesc> currentRootOp = opNode.getOperator();
                    if ((currentRootOp.getParentOperators() == null) || ((currentRootOp.getParentOperators() != null) && (currentRootOp.getParentOperators().size() == 0))) {
                        if (currentRootOp.getSchema() != null) {
                            if (currentRootOp.getSchema().toString().contains("col") == true) {
                                System.out.println("RootOperator: " + currentRootOp.getOperatorId() + " receives input from some previous operator...");
                                Task<? extends Serializable> RootOwner = opNode.getOwnerStage();
                                for (OperatorNode leafNode : leaves) {
                                    Operator<? extends OperatorDesc> currentLeafOp = leafNode.getOperator();
                                    Task<? extends Serializable> LeafOwner = leafNode.getOwnerStage();
                                    List<Task<? extends Serializable>> ownerChildren = LeafOwner.getDependentTasks();
                                    if (ownerChildren != null) {
                                        if (ownerChildren.size() > 0) {
                                            for (Task<? extends Serializable> tempStage : ownerChildren) {
                                                if (tempStage == RootOwner) {
                                                    if (currentLeafOp.getSchema() == null) continue;
                                                    if (currentRootOp.getSchema() == null) continue;
                                                    int countCol1 = countColXOccurencesInString("colx", currentLeafOp.getSchema().toString());
                                                    int countCol2 = countColXOccurencesInString("colx", currentRootOp.getSchema().toString());
                                                    if (countCol1 != countCol2) {
                                                        System.out.println("Root: " + currentRootOp.getOperatorId() + " and Leaf: " + currentLeafOp.getOperatorId() + " have different number of cols in Schema...");
                                                        if (currentLeafOp.getOperatorId().contains("RS") && currentRootOp.getOperatorId().contains("TS")) {
                                                            if ((currentLeafOp.getChildOperators() == null) || ((currentLeafOp.getChildOperators() != null) && (currentLeafOp.getChildOperators().size() == 0))) {
                                                                System.out.println("However...Leaf is RS and Root is TS with no children for RS! Difference: " + (countCol1 - countCol2));
                                                            } else {
                                                                System.out.println("Leaf is RS but with already at least 1 child..sorry!");
                                                                break;
                                                            }
                                                        } else {
                                                            break;
                                                        }
                                                    }
                                                    System.out.println("Root: " + currentRootOp.getOperatorId() + " and Leaf: " + currentLeafOp.getOperatorId() + " have matching Schema!");

                                                    DirectedEdge e = new DirectedEdge(currentLeafOp.getOperatorId(), currentRootOp.getOperatorId(), "LEAF TO ROOT");
                                                    addDirectedEdge(e);
                                                    System.out.println("Added Edge from Leaf: " + currentLeafOp.getOperatorId() + " to Root: " + currentRootOp.getOperatorId());
                                                    List<Operator<? extends OperatorDesc>> rootParents;
                                                    if (currentRootOp.getParentOperators() != null) {
                                                        rootParents = currentRootOp.getParentOperators();
                                                    } else {
                                                        rootParents = new LinkedList<>();
                                                    }
                                                    rootParents.add(currentLeafOp);
                                                    currentRootOp.setParentOperators(rootParents);
                                                    if ((currentLeafOp.getChildOperators() == null) || ((currentLeafOp.getChildOperators() != null) && (currentLeafOp.getChildOperators().size() == 0))) {
                                                        List<Operator<? extends OperatorDesc>> leafChildren = new LinkedList<>();
                                                        leafChildren.add(currentRootOp);
                                                        currentLeafOp.setChildOperators(leafChildren);
                                                    } else {
                                                        List<Operator<? extends OperatorDesc>> leafChildren = currentLeafOp.getChildOperators();
                                                        if (!leafChildren.contains(currentRootOp)) {
                                                            leafChildren.add(currentRootOp);
                                                            currentLeafOp.setChildOperators(leafChildren);
                                                            System.out.println("WARNING: This leaf has now more than one child check if this is correct! Children: " + leafChildren.toString());
                                                            //System.exit(1);
                                                        }
                                                    }
                                                    break;
                                                }
                                            }
                                        } else {
                                            continue;
                                        }
                                    } else {
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public void linkMapJoins() {

        for (OperatorNode op : nodesList) {
            if (op.getOperator().getOperatorId().contains("MAPJOIN")) {
                System.out.println("Found Operator: " + op.getOperator().getOperatorId());
                List<org.apache.hadoop.hive.ql.exec.Operator<? extends OperatorDesc>> opParents = op.getOperator().getParentOperators();
                if (opParents != null) {
                    for (Operator<? extends OperatorDesc> o1 : opParents) {
                        if (o1 != null) {
                            if (o1.getOperatorId().contains("HASHTABLEDUMMY")) {
                                String arr[] = o1.getOperatorId().trim().split("[_ ]+");
                                if (arr.length > 2) {
                                    System.out.println("linkMapJoins: More than 3 Tokens?!");
                                    System.exit(1);
                                }
                                int id = Integer.parseInt(arr[1]);
                                id = id - 1;
                                String newName = "HASHTABLESINK_" + String.valueOf(id);
                                o1.setOperatorId(newName);
                                System.out.println("Set as parent of: " + op.getOperator().getOperatorId() + " the Operator: " + o1.getOperatorId());
                                boolean found = false;
                                for (OperatorNode op2 : nodesList) {
                                    if (op2.getOperator().getOperatorId().equals(newName)) {
                                        System.out.println("Located: " + newName);
                                        found = true;
                                        List<org.apache.hadoop.hive.ql.exec.Operator<? extends OperatorDesc>> opChildren = op2.getOperator().getChildOperators();
                                        if (opChildren != null) {
                                            opChildren.add(op.getOperator());
                                            op2.getOperator().setChildOperators(opChildren);
                                            System.out.println("Set as child of: " + op2.getOperator().getOperatorId() + " the Operator: " + op.getOperator().getOperatorId());
                                        } else {
                                            opChildren = new LinkedList<>();
                                            opChildren.add(op.getOperator());
                                            op2.getOperator().setChildOperators(opChildren);
                                            System.out.println("Set as ONLY child of: " + op2.getOperator().getOperatorId() + " the Operator: " + op.getOperator().getOperatorId());
                                        }
                                        break;
                                    }
                                }
                                if (found == false) {
                                    System.out.println("Operator: " + newName + " doesn't exist!");
                                    System.exit(0);
                                }


                                DirectedEdge newEdge = new DirectedEdge(o1.getOperatorId(), op.getOperator().getOperatorId(), "? NO IDEA");

                                addDirectedEdge(newEdge);

                            }
                        }
                    }
                } else {
                    System.out.println("No MAPJOIN Parents?");
                    System.exit(1);
                }
            }
        }
    }

    public void printGraph(PrintWriter outputFile) {

        // Printing ROOTS
        System.out.println("\n\n=================EXAREME GRAPH (" + label + ")========================");
        outputFile.println("\n\t----------------------EXAREME GRAPH (" + label + ")-----------------------------");
        outputFile.flush();
        System.out.print("Root Operators: [");
        if (outputFile != null) {
            outputFile.print("\t\tRoot Operators: [");
            outputFile.flush();
        }
        int i = 0;
        OperatorNode op;
        while (i < roots.size()) {
            op = roots.get(i);
            System.out.print(op.getOperatorName());
            if (outputFile != null) {
                outputFile.print(op.getOperatorName());
                outputFile.flush();
            }
            if (i != roots.size() - 1) {
                System.out.print(", ");
                if (outputFile != null) {
                    outputFile.print(", ");
                    outputFile.flush();
                }
            }
            i++;
        }
        System.out.println("]\n");
        if (outputFile != null) {
            outputFile.println("]\n");
            outputFile.flush();
        }

        // Printing All OperatorNodes
        System.out.print("All Operators: [");
        if (outputFile != null) {
            outputFile.print("\t\tAll Operators: [");
            outputFile.flush();
        }

        i = 0;
        while (i < nodesList.size()) {
            op = nodesList.get(i);
            System.out.print(op.getOperatorName());
            if (outputFile != null) {
                outputFile.print(op.getOperatorName());
                outputFile.flush();
            }
            if (i != nodesList.size() - 1) {
                System.out.print(", ");
                if (outputFile != null) {
                    outputFile.print(", ");
                    outputFile.flush();
                }
            }
            i++;
        }
        System.out.println("]\n");
        if (outputFile != null) {
            outputFile.println("]\n");
            outputFile.flush();
        }

        // Printing All Edges (to:from notation)
        System.out.print("Op_Links (TO:FROM) = [");
        if (outputFile != null) {
            outputFile.print("\t\tOp_Links (TO:FROM) =  [");
            outputFile.flush();
        }

        i = 0;
        DirectedEdge e;
        while (i < edges.size()) {
            e = edges.get(i);
            System.out.print("{" + e.getToVertex() + ":" + e.getFromVertex() + "}");
            if (outputFile != null) {
                outputFile.print("{" + e.getToVertex() + ":" + e.getFromVertex() + "}");
                outputFile.flush();
            }
            if (i != edges.size() - 1) {
                System.out.print(", ");
                if (outputFile != null) {
                    outputFile.print(", ");
                    outputFile.flush();
                }
            }
            i++;
        }
        System.out.println("]\n");
        if (outputFile != null) {
            outputFile.println("]\n");
            outputFile.flush();
        }

        // Printing All Edges (from:to notation)
        System.out.print("Op_Links (FROM:TO) = [");
        if (outputFile != null) {
            outputFile.print("\t\tOp_Links (FROM:TO) =  [");
            outputFile.flush();
        }

        i = 0;
        while (i < edges.size()) {
            e = edges.get(i);
            System.out.print("{" + e.getFromVertex() + ":" + e.getToVertex() + "}");
            if (outputFile != null) {
                outputFile.print("{" + e.getFromVertex() + ":" + e.getToVertex() + "}");
                outputFile.flush();
            }
            if (i != edges.size() - 1) {
                System.out.print(", ");
                if (outputFile != null) {
                    outputFile.print(", ");
                    outputFile.flush();
                }
            }
            i++;
        }
        System.out.println("]\n");
        if (outputFile != null) {
            outputFile.println("]\n");
            outputFile.flush();
        }

        System.out.print("Leaf Operators: [");
        if (outputFile != null) {
            outputFile.print("\t\tLeaf Operators: [");
            outputFile.flush();
        }
        i = 0;
        while (i < leaves.size()) {
            op = leaves.get(i);
            System.out.print(op.getOperatorName());
            if (outputFile != null) {
                outputFile.print(op.getOperatorName());
                outputFile.flush();
            }
            if (i != leaves.size() - 1) {
                System.out.print(", ");
                if (outputFile != null) {
                    outputFile.print(", ");
                    outputFile.flush();
                }
            }
            i++;
        }
        System.out.println("]\n");
        if (outputFile != null) {
            outputFile.println("]\n");
            outputFile.flush();
        }

        outputFile.println("\n");
        outputFile.flush();

    }

    public void printOperatorList(PrintWriter outputFile){

        List<OperatorNode> operatorNodeListSimpler = getNodesList();

        outputFile.println("\t++++++++++++++++++++++++++++++++++++++++++++++++++++ ALL OPERATORS ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
        outputFile.flush();

        for(OperatorNode opNode : operatorNodeListSimpler){
            opNode.printOperatorInstance(outputFile);
        }

        outputFile.println("\n\n");
        outputFile.flush();

    }

    public void printStageInstance(PrintWriter outputFile, Task<? extends Serializable> t){

        outputFile.println("\t\t------------------------------Stage-ID: " + t.getId() + " ----------------------------------------");
        outputFile.flush();
        outputFile.println("\t\t\tTo String: " + t.toString());
        outputFile.flush();
        outputFile.println("\t\t\tJobID: " + t.getJobID());
        outputFile.flush();
        outputFile.println("\t\t\tNumChild: " + t.getNumChild());
        outputFile.flush();
        HashMap<String, Long> stageCounters = ((HashMap<String, Long>) t.getCounters());
        if (stageCounters != null) {
            outputFile.println("\t\t\tPrinting Stage Counters...");
            outputFile.flush();
            for (HashMap.Entry<String, Long> entry : stageCounters.entrySet()) {
                outputFile.println("\t\t\t\t" + entry.getKey() + " : " + entry.getValue());
                outputFile.flush();
            }
        }

        Serializable s = t.getWork();
        if (s != null) {
            outputFile.println("\t\t\tGetWork: " + s.toString());
            outputFile.flush();
        }

        outputFile.println("\t\t\tChildren...");
        outputFile.flush();
        List<Task<? extends Serializable>> dependentTasks = t.getChildTasks();
        if (dependentTasks != null) {
            for (Task<? extends Serializable> task : dependentTasks) {
                outputFile.println("\t\t\t\tHas Child: " + task.getId());
                outputFile.flush();
            }
        }

        outputFile.println("\t\t\tParents...");
        outputFile.flush();
        List<Task<? extends Serializable>> parentTasks = t.getParentTasks();
        if (parentTasks != null) {
            for (Task<? extends Serializable> task : parentTasks) {
                outputFile.println("\t\t\t\tHas Parent: " + task.getId());
                outputFile.flush();
            }
        }

        outputFile.println("\t\t\tBackup Task...");
        outputFile.flush();
        Task<? extends Serializable> backupTask = t.getBackupTask();
        if (backupTask != null) {
            outputFile.println("\t\t\t\t" + backupTask.getId());
            outputFile.flush();
        }

        outputFile.println("\t\t\tTop Operators...");
        outputFile.flush();
        if (t.getTopOperators() != null) {
            if (t.getTopOperators().size() > 0) {
                for (Object o : t.getTopOperators()) {
                    if (o != null) {
                        outputFile.println("\t\t\t\tOperator: " + ((org.apache.hadoop.hive.ql.exec.Operator<? extends Serializable>) o).getOperatorId());
                        outputFile.flush();
                    } else {
                        outputFile.println("\t\t\t\tOperator is NULL!");
                        outputFile.flush();
                    }
                }
            }
        }

        List<Task<? extends Serializable>> feedSubscribers = t.getFeedSubscribers();
        if (feedSubscribers != null) {
            outputFile.println("\t\t\tAccessing Feed Subscribers...");
            outputFile.flush();
            for (Task<? extends Serializable> f : feedSubscribers) {
                if (f != null) {
                    outputFile.println("\t\t\t\t" + f.getId());
                    outputFile.flush();
                }
            }
        }

        TaskHandle taskHandle = t.getTaskHandle();

        if (taskHandle != null) {
            outputFile.println("\t\t\tAccessing Task Handle...");
            outputFile.flush();

            Counters counters;

            try {
                counters = taskHandle.getCounters();
                outputFile.println("\t\t\t\tAccessing Task Handle Counters...");
                outputFile.flush();
                if (counters != null) {
                    outputFile.println("\t\t\t\t\tCounters(ToString): " + counters.toString());
                }
            } catch (java.io.IOException ex) {
                outputFile.println("\t\t\t\tCaught IOException Accessing Counters");
                outputFile.flush();
            }

        }

        if(t instanceof FetchTask ){
            outputFile.println("\n\t\t\tStageType: FetchTask");
            outputFile.flush();
            FetchTask fetchTask = (FetchTask) t;
            if(fetchTask != null){
                outputFile.println("\t\t\t\tMaxRows: "+fetchTask.getMaxRows());
                outputFile.flush();
                TableDesc tableDesc = fetchTask.getTblDesc();
                if(tableDesc != null){
                    outputFile.println("\t\t\t\tTableDesc(toString): "+tableDesc.toString());
                    outputFile.flush();
                }
            }
        }
        else if(t instanceof ColumnStatsTask){
            outputFile.println("\n\t\t\tStageType: ColumnStatsTask");
            outputFile.flush();
            ColumnStatsTask columnStatsTask = (ColumnStatsTask) t;
            if(columnStatsTask != null){
                outputFile.println("\t\t\t\tName: "+columnStatsTask.getName());
                outputFile.flush();
            }
        }
        else if(t instanceof ColumnStatsUpdateTask){
            outputFile.println("\n\t\t\tStageType: ColumnStatsUpdateTask");
            outputFile.flush();
            ColumnStatsUpdateTask columnStatsUpdateTask = (ColumnStatsUpdateTask) t;
            if(columnStatsUpdateTask != null){
                outputFile.println("\t\t\t\tName: "+columnStatsUpdateTask.getName());
                outputFile.flush();
            }
        }
        else if(t instanceof ConditionalTask){
            outputFile.println("\n\t\t\tStageType: ConditionalTask");
            outputFile.flush();
            ConditionalTask conditionalTask = (ConditionalTask) t;
            if(conditionalTask != null){
                outputFile.println("\t\t\t\tName: "+conditionalTask.getName());
                outputFile.flush();
                if(conditionalTask.getDependentTasks() != null){
                    outputFile.println("\t\t\t\tDependentTasks: "+conditionalTask.getDependentTasks().toString());
                    outputFile.flush();
                }
                if(conditionalTask.getListTasks() != null){
                    outputFile.println("\t\t\t\tListTasks: "+conditionalTask.getListTasks().toString());
                    outputFile.flush();
                }
                ConditionalResolver conditionalResolver = conditionalTask.getResolver();
                if(conditionalResolver != null){
                    outputFile.println("\t\t\t\tConditionalResolver(toString): "+conditionalResolver.toString());
                    outputFile.flush();
                }
                outputFile.println("\t\t\t\thasReduce: "+conditionalTask.hasReduce());
                outputFile.flush();
                outputFile.println("\t\t\t\tisMapRedTask: "+conditionalTask.isMapRedTask());
                outputFile.flush();
            }
        }
        else if(t instanceof CopyTask){
            outputFile.println("\n\t\t\tStageType: CopyTask");
            outputFile.flush();
        }
        else if(t instanceof DDLTask){
            outputFile.println("\n\t\t\tStageType: DDLTask");
            outputFile.flush();
            DDLTask ddlTask = (DDLTask) t;
            if(ddlTask != null){
                outputFile.println("\t\t\t\tName: "+ddlTask.getName());
                outputFile.flush();
                outputFile.println("\t\t\t\tRequireLock: "+ddlTask.requireLock());
                outputFile.flush();
            }
        }
        else if(t instanceof DependencyCollectionTask){
            outputFile.println("\n\t\t\tStageType: DependencyCollectionTask");
            outputFile.flush();
            DependencyCollectionTask dependencyCollectionTask = (DependencyCollectionTask) t;
            if(dependencyCollectionTask != null){
                outputFile.println("\t\t\t\tName: "+dependencyCollectionTask.getName());
                outputFile.flush();
            }
        }
        else if(t instanceof ExplainSQRewriteTask){
            outputFile.println("\n\t\t\tStageType: ExplainSQRewriteTask");
            outputFile.flush();
            ExplainSQRewriteTask explainSQRewriteTask = (ExplainSQRewriteTask) t;
            if(explainSQRewriteTask != null){
                outputFile.println("\t\t\t\tName: "+explainSQRewriteTask.getName());
                outputFile.flush();
                List<FieldSchema> listFieldSchema = explainSQRewriteTask.getResultSchema();
                outputFile.println("\t\t\t\tList of FieldSchemas: ");
                outputFile.flush();
                if(listFieldSchema != null){
                    for(FieldSchema f : listFieldSchema){
                        if(f != null){
                            outputFile.println("\t\t\t\t\tFieldSchema: "+f.toString());
                            outputFile.flush();
                        }
                    }
                }
            }
        }
        else if(t instanceof ExplainTask){
            outputFile.println("\n\t\t\tStageType: ExplainTask");
            outputFile.flush();
            ExplainTask explainTask = (ExplainTask) t;
            if(explainTask != null){
                outputFile.println("\t\t\t\tName: "+explainTask.getName());
                outputFile.flush();
                List<FieldSchema> listFieldSchema = explainTask.getResultSchema();
                outputFile.println("\t\t\t\tList of FieldSchemas: ");
                outputFile.flush();
                if(listFieldSchema != null){
                    for(FieldSchema f : listFieldSchema){
                        if(f != null){
                            outputFile.println("\t\t\t\t\tFieldSchema: "+f.toString());
                            outputFile.flush();
                        }
                    }
                }
            }
        }
        else if(t instanceof FunctionTask){
            outputFile.println("\n\t\t\tStageType: FunctionTask");
            outputFile.flush();
            FunctionTask functionTask = (FunctionTask) t;
            if(functionTask != null){
                outputFile.println("\t\t\t\tName: "+functionTask.getName());
                outputFile.flush();
            }
        }
        else if(t instanceof MoveTask){
            outputFile.println("\n\t\t\tStageType: MoveTask");
            outputFile.flush();
            MoveTask moveTask = (MoveTask) t;
            if(moveTask != null){
                outputFile.println("\t\t\t\tName: "+moveTask.getName());
                outputFile.flush();
                outputFile.println("\t\t\t\tisLocal: "+moveTask.isLocal());
                outputFile.flush();
            }
        }
        else if(t instanceof StatsNoJobTask){
            outputFile.println("\n\t\t\tStageType: StatsNoJobTask");
            outputFile.flush();
            StatsNoJobTask statsNoJobTask = (StatsNoJobTask) t;
            if(statsNoJobTask != null){
                outputFile.println("\t\t\t\tName: "+statsNoJobTask.getName());
                outputFile.flush();
            }
        }
        else if(t instanceof MapRedTask){
            outputFile.println("\n\t\t\tStageType: MapRedTask");
            outputFile.flush();
            MapRedTask mapRedTask = (MapRedTask) t;
            if(mapRedTask != null){
                outputFile.println("\t\t\t\tName: "+mapRedTask.getName());
                outputFile.flush();
                outputFile.println("\t\t\t\tmapDone: "+mapRedTask.mapDone());
                outputFile.flush();
                outputFile.println("\t\t\t\tmapStarted: "+mapRedTask.mapStarted());
                outputFile.flush();
                outputFile.println("\t\t\t\treduceDone: "+mapRedTask.reduceDone());
                outputFile.flush();
                outputFile.println("\t\t\t\treduceStarted: "+mapRedTask.reduceStarted());
                outputFile.flush();
            }
        }
        else if(t instanceof MapredLocalTask){
            outputFile.println("\n\t\t\tStageType: MapredLocalTask");
            outputFile.flush();
            MapredLocalTask mapRedLocalTask = (MapredLocalTask) t;
            if(mapRedLocalTask != null){
                outputFile.println("\t\t\t\tName: "+mapRedLocalTask.getName());
                outputFile.flush();
                outputFile.println("\t\t\t\trequireLock: "+mapRedLocalTask.requireLock());
                outputFile.flush();
                outputFile.println("\t\t\t\tisMapRedLocalTask: "+mapRedLocalTask.isMapRedLocalTask());
                outputFile.flush();
            }
        }
        else if(t instanceof IndexMetadataChangeTask){
            outputFile.println("\n\t\t\tStageType: IndexMetadataChangeTask");
            outputFile.flush();
            IndexMetadataChangeTask indexMetadataChangeTask = (IndexMetadataChangeTask) t;
            if(indexMetadataChangeTask != null){
                outputFile.println("\t\t\t\tName: "+indexMetadataChangeTask.getName());
                outputFile.flush();
            }
        }
        else{
            outputFile.println("\n\t\t\tStageType: Unknown");
            outputFile.flush();
        }

    }

    public void printStagesList(PrintWriter outputFile) {

        outputFile.println("\t++++++++++++++++++++++++++++++++++++++++++++++++++ ALL STAGES ++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
        outputFile.flush();

        for (Task t : planStages) {
            printStageInstance(outputFile, t);
        }

        outputFile.println("\n");
        outputFile.flush();

    }

}
