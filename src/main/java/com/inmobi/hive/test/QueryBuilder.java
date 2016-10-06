package com.inmobi.hive.test;

import madgik.exareme.common.app.engine.AdpDBOperatorType;
import madgik.exareme.common.app.engine.AdpDBSelectOperator;
import madgik.exareme.common.app.engine.scheduler.elasticTree.system.data.Table;
import madgik.exareme.common.schema.Select;
import madgik.exareme.common.schema.TableView;
import madgik.exareme.common.schema.expression.Comments;
import madgik.exareme.common.schema.expression.DataPattern;
import madgik.exareme.common.schema.expression.SQLSelect;
import org.apache.commons.httpclient.URI;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created by panos on 19/7/2016.
 */

//TODO CHECK NOT ONLY KEY BUT VALUE ALSO IF ENTRIES ARE EQUAL
//TODO INCORPORATE ALTERNATE ALIASES FOR JOIN RETURN PROBLEM

/* This class undertakes the task of taking an Exareme Graph
   and turning it into a set of OperatorQueries (basically Exareme Operators)
   and OpLinks. After its work is done all that is left is to actually
   create an AdpDBSelectOperator object for every OperatorQuery object
   and then print the ExaremePlan.

   CURRENT CONVERSION STRATEGY: We explore the ExaremeGraph DFS style.
   One loop begins for every Root of the Exareme Graph. Once we finish
   extracting the required information from an OperatorNode of the Graph
   we recursively move to its child and so on. This recursive process
   stops when we reach a leaf or a node that a second parent (such as a
   JoinOperator)

   As we explore from one operator to another we fill OperatorQuery objects
   which contains the InputTables, OutputTable, used columns and the Query
   Exareme will run on the Input Tables to form an OutputTable.

   Multiple Hive M/R Operators can be used to form a single OperatorQuery (aka Exareme Operator)

   FOR EXAMPLE:
   TableScanOperator--->FilterOperator--->GroupByOperator---->SelectOperator---->LimitOperator

   The above set of Operator Nodes in the ExaremeGraph will translate to a single Exareme Operator
   with the following query contained:

   select [columns] from [table] where [filters] group by [group keys] limit [#number];

   When we fully explore the ExaremeGraph and create all OperatorQuery objects and OpLinks
   we need we must invoke the translateToExaremeOps method in order to convert
   every OperatorQuery into an AdpDBSelectOperator for Exareme.

   HOW MULTIPLE TABLES WITH PARTITIONS ARE DEALT WITH:
   Exareme provides various ways to combine the input partitions of two or more tables and form
   a new table. Due to time constraints, the strategy we currently follow in order to combine
   input Partitions is the Cartesian Product to 1 strategy. That means that all partitions
   of different Hive Tables are combined in the way of Cartesian Product and then the results
   are combined into an OutputTable table with only 1 Partition.
   
   NOTE:
   This is the way currently a set of Hive Operators translates to an Exareme Operator.
   Note that due to the MapReduce Logic of the OperatorNodes some Exareme Operators might contain Queries
   that are essentially the same Query as the Exareme Operator before them. As of now, no steps
   have been taken to optimise this behaviour and remove queries that can be ommited due to the
   above behaviour.

*/

public class QueryBuilder {

    ExaremeGraph exaremeGraph; //A Hive Operator Graph ready to be translated
    MyMap columnAndTypeMap; //_col0 - int .etc
    List<OperatorQuery> allQueries;
    List<MyTable> inputTables;
    List<MyTable> outputTables;
    List<OpLink> opLinksList;
    String currentDatabasePath;

    List<JoinPoint> joinPointList = new LinkedList<>();

    int numberOfNodes;

    public QueryBuilder(ExaremeGraph graph, List<MyTable> inputT, List<MyPartition> inputP, List<MyTable> outputT, List<MyPartition> outputP, String databasePath){
        exaremeGraph = graph;
        allQueries = new LinkedList<>();
        columnAndTypeMap = new MyMap();
        inputTables = inputT;
        outputTables = outputT;
        opLinksList = new LinkedList<>();
        currentDatabasePath = databasePath;

        System.out.println("Initialising QueryBuilder with DataBasePath="+databasePath);
        System.out.println("Added InputTables to QueryBuilder!");
        System.out.println("Added OutputTables to QueryBuilder!");

        System.out.println("Adding InputPartitions...");
        for(MyPartition inputPartition : inputP){
            for(MyTable inputTable : inputTables){
                if(inputTable.getIsAFile() == false) {
                    if (inputTable.getBelongingDataBaseName().equals(inputPartition.getBelongingDataBaseName())) {
                        if (inputPartition.getBelogingTableName().equals(inputTable.getTableName())) {
                            System.out.println("Adding Partition: " + inputPartition.getPartitionName() + " to Table: " + inputTable.getTableName());
                            inputTable.addPartition(inputPartition);
                        }
                    }
                }
            }
        }

        System.out.println("Adding OutputPartitions...");
        for(MyPartition outputPartition : outputP){
            for(MyTable outputTable : outputTables){
                if(outputTable.getIsAFile() == false) {
                    if (outputTable.getBelongingDataBaseName().equals(outputPartition.getBelongingDataBaseName())) {
                        if (outputPartition.getBelogingTableName().equals(outputTable.getTableName())) {
                            System.out.println("Adding Partition: " + outputPartition.getPartitionName() + " to Table: " + outputTable.getTableName());
                            outputTable.addPartition(outputPartition);
                        }
                    }
                }
            }
        }

        System.out.println("Printing InputTables for DEBUGGING...");
        for(MyTable inputTable : inputTables){
            if(inputTable.getIsAFile() == false) {
                System.out.println("Input Table: " + inputTable.getTableName());
                System.out.println("\tBelongingDataBase: " + inputTable.getBelongingDataBaseName());
                System.out.println("\tTableHDFSPath: " + inputTable.getTableHDFSPath());
                System.out.println("\tHasPartitions: " + inputTable.getHasPartitions());

                if (inputTable.getAllCols() != null) {
                    System.out.println("\tAllCols: ");
                    if (inputTable.getAllCols().size() > 0) {
                        for (FieldSchema f : inputTable.getAllCols()) {
                            System.out.println("\t\tColName: " + f.getName() + " - ColType: " + f.getType());
                        }
                    } else {
                        System.out.println("InputTable has no Columns!");
                        System.exit(0);
                    }
                } else {
                    System.out.println("InputTable has no Columns!");
                    System.exit(0);
                }

                if (inputTable.getAllFields() != null) {
                    System.out.println("\tAllFields: ");
                    if (inputTable.getAllFields().size() > 0) {
                        for (StructField f : inputTable.getAllFields()) {
                            System.out.println("\t\tFieldName: " + f.getFieldName());
                        }
                    } else {
                        System.out.println("InputTable has no Fields!");
                        System.exit(0);
                    }
                } else {
                    System.out.println("InputTable has no Fields!");
                    System.exit(0);
                }

                if (inputTable.getHasPartitions() == true) {
                    System.out.println("\tInputTable IS partitioned!");
                } else {
                    System.out.println("\tInputTable IS NOT partitioned!");
                }

                if (inputTable.getAllPartitionKeys() != null) {
                    System.out.println("\tAllPartitionKeys: ");
                    if (inputTable.getAllPartitionKeys().size() > 0) {
                        for (FieldSchema f : inputTable.getAllPartitionKeys()) {
                            System.out.println("\t\tColName: " + f.getName() + " - ColType: " + f.getType());
                        }
                        LinkedHashMap<List<FieldSchema>, LinkedHashMap<List<String>, MyPartition>> mapFieldValueCombos = inputTable.getPartitionKeysValuesMap();
                        System.out.println("\tAllPartitions: ");
                        for (Map.Entry<List<FieldSchema>, LinkedHashMap<List<String>, MyPartition>> entry : mapFieldValueCombos.entrySet()) {
                            System.out.print("\t\tFieldCombination: [");
                            List<FieldSchema> allFieldCombos = entry.getKey();
                            for (int i = 0; i < allFieldCombos.size(); i++) {
                                if (i != allFieldCombos.size() - 1)
                                    System.out.print(allFieldCombos.get(i).getName() + ", ");
                                else
                                    System.out.println(allFieldCombos.get(i).getName() + " ]");
                            }

                            LinkedHashMap<List<String>, MyPartition> valueCombos = entry.getValue();
                            for (Map.Entry<List<String>, MyPartition> entry2 : valueCombos.entrySet()) {
                                System.out.print("\t\t\tValueCombination: [");
                                List<String> allValueCombos = entry2.getKey();
                                for (int i = 0; i < allValueCombos.size(); i++) {
                                    if (i != allValueCombos.size() - 1)
                                        System.out.print(allValueCombos.get(i) + ", ");
                                    else
                                        System.out.print(allValueCombos.get(i) + " ]");
                                }
                                System.out.println(" - PartitionName: " + entry2.getValue().getPartitionName());
                            }
                        }
                    } else {
                        System.out.println("InputTable has no PartitionKeys!");
                    }
                } else {
                    System.out.println("InputTable has no PartitionKeys!");
                }
            }
            else{
                System.out.println("Input File: "+inputTable.getTableName());
            }
        }

        System.out.println("Printing OutputTables for DEBUGGING...");
        for(MyTable outputTable : outputTables) {
            if (outputTable.getTableName().contains("file:")) {
                System.out.println("Output File: "+outputTable.getTableName());
            }
            else{
                System.out.println("Output Table: " + outputTable.getTableName());
            }
        }
    }

    public List<OperatorQuery> getQueryList(){
        return allQueries;
    }

    public void printColumnAndTypeMap(){

        columnAndTypeMap.printMap();

    }

    public List<MyTable> getInputTables() { return inputTables; }

    public void setInputTables(List<MyTable> iList) { inputTables = iList; }

    public void addInputTable(MyTable input) {

        if(inputTables.size() > 0){
            for(MyTable t : inputTables){
                if(t.getBelongingDataBaseName().equals(input.getBelongingDataBaseName())){
                    if(t.getTableName().equals(input.getTableName())){
                        return;
                    }
                }
            }
        }

        inputTables.add(input);

    }

    public List<OpLink> getOpLinksList() { return opLinksList; }

    public List<MyTable> getOutputTables() { return outputTables; }

    public void setOutputTables(List<MyTable> iList) { outputTables = iList; }

    public void addOutputTable(MyTable output) {

        if(outputTables.size() > 0){
            for(MyTable t : outputTables){
                if(t.getBelongingDataBaseName().equals(output.getBelongingDataBaseName())){
                    if(t.getTableName().equals(output.getTableName())){
                        return;
                    }
                }
            }
        }

        outputTables.add(output);

    }

    public void addInputTablePartition(MyPartition inputPart){
        if(inputTables.size() > 0){
            for(MyTable inputTable : inputTables){
                if(inputTable.getBelongingDataBaseName().equals(inputPart.getBelongingDataBaseName())){
                    if(inputTable.getTableName().equals(inputPart.getBelogingTableName())){
                        inputTable.addPartition(inputPart);
                    }
                }
            }
        }
        else{
            System.out.println("Can't add Partition to non existent Table! No Tables in List!");
            System.exit(0);
        }
    }

    public void addOutputTablePartition(MyPartition outputPart){
        if(outputTables.size() > 0){
            for(MyTable outputTable : outputTables){
                if(outputTable.getBelongingDataBaseName().equals(outputPart.getBelongingDataBaseName())){
                    if(outputTable.getTableName().equals(outputPart.getBelogingTableName())){
                        outputTable.addPartition(outputPart);
                    }
                }
            }
        }
        else{
            System.out.println("Can't add Partition to non existent Table! No Tables in List!");
            System.exit(0);
        }
    }

    public void addQueryToList(OperatorQuery opQuery) { allQueries.add(opQuery); }

    public ExaremeGraph getExaremeGraph(){
        return exaremeGraph;
    }

    //WARNING STRING ARE IMMUTABLE YOU CAN RETURN OR WRAP OR STRINGBUILD ONLY, NOT CHANGE THE VALUE OF THE REFERENCE

    public MyMap getColumnAndTypeMap(){ return columnAndTypeMap; }

    public void setColumnAndTypeMap(MyMap newMap) { columnAndTypeMap = newMap; }

    public void addNewColsFromSchema(String givenSchema){

        System.out.println("Given Schema: ["+givenSchema+"]");

        System.out.println("Before...addNewColsFromSchema...columnAndType has become: ");
        printColumnAndTypeMap();

        if((givenSchema.toCharArray()[0] == '(') && (givenSchema.toCharArray()[givenSchema.length() - 1] == ')')){ //REMOVING Starting ( and ) from Schema
            System.out.println("Contains (,)");
            char[] typeToChar = givenSchema.toCharArray();
            char[] newTypeChar = new char[givenSchema.length()-1];
            for(int i = 1; i < typeToChar.length; i++){
                newTypeChar[i-1] = typeToChar[i];
            }
            String newTypeName = new String(newTypeChar);
            char[] typeToChar2 = newTypeName.toCharArray();
            char[] newTypeChar2 = new char[newTypeName.length() - 1];
            for(int i = 0; i < typeToChar2.length - 1; i++){
                newTypeChar2[i] = typeToChar2[i];
            }
            givenSchema = new String(newTypeChar2);
            System.out.println("givenSchema has become: ["+givenSchema+"]");
        }
        else if(givenSchema.contains("struct<")){
            System.out.println("Contains struct");
            givenSchema = givenSchema.replace("struct", "");
            System.out.println("Given Schema has become: ["+givenSchema+"]");

            System.out.println("Contains <,>");
            char[] typeToChar = givenSchema.toCharArray();
            char[] newTypeChar = new char[givenSchema.length()-1];
            for(int i = 1; i < typeToChar.length; i++){
                newTypeChar[i-1] = typeToChar[i];
            }
            String newTypeName = new String(newTypeChar);
            char[] typeToChar2 = newTypeName.toCharArray();
            char[] newTypeChar2 = new char[newTypeName.length() - 1];
            for(int i = 0; i < typeToChar2.length - 1; i++){
                newTypeChar2[i] = typeToChar2[i];
            }
            givenSchema = new String(newTypeChar2);

            System.out.println("Given Schema has become: ["+givenSchema+"]");
            givenSchema = givenSchema.replace(":", ": ");
            System.out.println("Given Schema has become: ["+givenSchema+"]");
        }
        else{
            System.out.println("addNewColsFromSchema: Failed to find <,>,(,)!");
            System.exit(0);
        }

        System.out.println("Splitting on every Comma(,) found...");
        String[] parts = givenSchema.split(",");
        System.out.println("Created: "+parts.length+" parts!");
        for(String tuple : parts){
            if(tuple != null){
                if(tuple.contains(":")){
                    String[] tupleParts = tuple.split(":");
                    boolean exists = false;
                    List<ColumnTypePair> columnTypePairList = columnAndTypeMap.getColumnAndTypeList();
                    for(ColumnTypePair pair : columnTypePairList){
                        if(pair.getColumnName().equals(tupleParts[0])){
                            if(pair.getColumnType().equals(tupleParts[1])) {
                                exists = true;
                                break;
                            }
                            else{
                                if(pair.getColumnType().equals("Unknown-Type")){
                                    System.out.println("Fixing Column-Value Part located...Key: " + tupleParts[0] + " Value: " + tupleParts[1]);
                                    pair.setColumnType(tupleParts[1]);
                                    exists = true;
                                    break;
                                }
                            }
                        }
                    }
                    if(exists == false) {
                        System.out.println("New Column-Value Part located...Key: " + tupleParts[0] + " Value: " + tupleParts[1]);
                        ColumnTypePair pair = new ColumnTypePair(tupleParts[0], tupleParts[1]);
                        columnAndTypeMap.addPair(pair);
                    }
                }
                else{
                    System.out.println("addNewColsFromSchema: Failed to :!");
                    System.exit(0);
                }
            }
        }

        System.out.println("After...addNewColsFromSchema...columnAndType has become: ");
        printColumnAndTypeMap();

    }

    public void updateColumnAndTypesFromTableScan(String givenSchema, String tableAlias){

        System.out.println("Given Schema: ["+givenSchema+"]");

        System.out.println("Before...updateColumnAndTypesFromTableScan...columnAndType has become: ");
        printColumnAndTypeMap();

        if((givenSchema.toCharArray()[0] == '(') && (givenSchema.toCharArray()[givenSchema.length() - 1] == ')')){ //REMOVING Starting ( and ) from Schema
            System.out.println("Contains (,)");
            char[] typeToChar = givenSchema.toCharArray();
            char[] newTypeChar = new char[givenSchema.length()-1];
            for(int i = 1; i < typeToChar.length; i++){
                newTypeChar[i-1] = typeToChar[i];
            }
            String newTypeName = new String(newTypeChar);
            char[] typeToChar2 = newTypeName.toCharArray();
            char[] newTypeChar2 = new char[newTypeName.length() - 1];
            for(int i = 0; i < typeToChar2.length - 1; i++){
                newTypeChar2[i] = typeToChar2[i];
            }
            givenSchema = new String(newTypeChar2);
            System.out.println("givenSchema has become: ["+givenSchema+"]");
        }
        else if(givenSchema.contains("struct<")){
            System.out.println("Contains struct");
            givenSchema = givenSchema.replace("struct", "");
            System.out.println("Given Schema has become: ["+givenSchema+"]");

            System.out.println("Contains <,>");
            char[] typeToChar = givenSchema.toCharArray();
            char[] newTypeChar = new char[givenSchema.length()-1];
            for(int i = 1; i < typeToChar.length; i++){
                newTypeChar[i-1] = typeToChar[i];
            }
            String newTypeName = new String(newTypeChar);
            char[] typeToChar2 = newTypeName.toCharArray();
            char[] newTypeChar2 = new char[newTypeName.length() - 1];
            for(int i = 0; i < typeToChar2.length - 1; i++){
                newTypeChar2[i] = typeToChar2[i];
            }
            givenSchema = new String(newTypeChar2);

            System.out.println("Given Schema has become: ["+givenSchema+"]");
            givenSchema = givenSchema.replace(":", ": ");
            System.out.println("Given Schema has become: ["+givenSchema+"]");
        }
        else{
            System.out.println("updateColumnAndTypesFromTableScan: Failed to find <,>,(,)!");
            System.exit(0);
        }

        System.out.println("Splitting on every Comma(,) found...");
        String[] parts = givenSchema.split(",");
        System.out.println("Created: "+parts.length+" parts!");
        MyMap theNewMap = new MyMap();
        for(String tuple : parts){
            if(tuple != null){
                if(tuple.contains(":")){
                    String[] tupleParts = tuple.split(":");
                    boolean exists = false;
                    List<ColumnTypePair> columnTypePairList = columnAndTypeMap.getColumnAndTypeList();
                    for(ColumnTypePair pair : columnTypePairList){
                        if(pair.getColumnName().equals(tupleParts[0])){
                            if(pair.getColumnType().equals(tupleParts[1])) {
                                exists = true;
                                break;
                            }
                            else{
                                if(pair.getColumnType().equals("Unknown-Type")){
                                    System.out.println("Fixing Column-Value Part located...Key: " + tupleParts[0] + " Value: " + tupleParts[1]);
                                    pair.setColumnType(tupleParts[1]);
                                    exists = true;
                                    break;
                                }
                            }
                        }
                    }
                    if(exists == false) {
                        if(tupleParts[0].equals("BLOCK__OFFSET__INSIDE__FILE") || tupleParts[0].equals("INPUT__FILE__NAME") || tupleParts[0].equals("ROW__ID") || tupleParts[0].equals("bucketid") || tupleParts[0].equals("rowid") ){
                            continue;
                        }
                        System.out.println("New Column-Value Part located...Key: " + tupleParts[0] + " Value: " + tupleParts[1]);
                        ColumnTypePair pair = new ColumnTypePair(tupleParts[0], tupleParts[1]);
                        columnAndTypeMap.addPair(pair);
                    }
                }
                else{
                    System.out.println("addNewColsFromSchema: Failed to :!");
                    System.exit(0);
                }
            }
        }

        System.out.println("After...updateColumnAndTypesFromTableScan...columnAndType has become: ");
        printColumnAndTypeMap();

    }

    public String extractColsFromTypeName(String typeName, MyMap aMap, String schema,boolean keepKeys){

        System.out.println("Given typeName: ["+typeName+"]");

        if((typeName.toCharArray()[0] == '(') && (typeName.toCharArray()[typeName.length() - 1] == ')')){ //REMOVING Starting ( and ) from Schema
            System.out.println("Contains (,)");
            schema = typeName;
            char[] typeToChar = typeName.toCharArray();
            char[] newTypeChar = new char[typeName.length()-1];
            for(int i = 1; i < typeToChar.length; i++){
                newTypeChar[i-1] = typeToChar[i];
            }
            String newTypeName = new String(newTypeChar);
            char[] typeToChar2 = newTypeName.toCharArray();
            char[] newTypeChar2 = new char[newTypeName.length() - 1];
            for(int i = 0; i < typeToChar2.length - 1; i++){
                newTypeChar2[i] = typeToChar2[i];
            }
            typeName = new String(newTypeChar2);
            System.out.println("Schema has become: ["+schema+"]");
            System.out.println("TypeName has become: ["+typeName+"]");
        }
        else if(typeName.contains("struct<")){
            System.out.println("Contains struct");
            typeName = typeName.replace("struct", "");
            System.out.println("TypeName has become: ["+typeName+"]");

            System.out.println("Contains <,>");
            char[] typeToChar = typeName.toCharArray();
            char[] newTypeChar = new char[typeName.length()-1];
            for(int i = 1; i < typeToChar.length; i++){
                newTypeChar[i-1] = typeToChar[i];
            }
            String newTypeName = new String(newTypeChar);
            char[] typeToChar2 = newTypeName.toCharArray();
            char[] newTypeChar2 = new char[newTypeName.length() - 1];
            for(int i = 0; i < typeToChar2.length - 1; i++){
                newTypeChar2[i] = typeToChar2[i];
            }
            typeName = new String(newTypeChar2);

            System.out.println("TypeName has become: ["+typeName+"]");
            typeName = typeName.replace(":", ": ");
            System.out.println("TypeName has become: ["+typeName+"]");
            schema = schema.concat("(");
            schema = schema.concat(typeName);
            schema = schema.concat(")");
            System.out.println("Schema has become: ["+schema+"]");
        }
        else{
            System.out.println("extractColsFromTypeName: Failed to find <,>,(,)!");
            System.exit(0);
        }

        System.out.println("Splitting on every Comma(,) found...");
        String[] parts = typeName.split(",");
        System.out.println("Created: "+parts.length+" parts!");
        for(String tuple : parts){
            if(tuple != null){
                if(tuple.contains(":")){
                    String[] tupleParts = tuple.split(":");
                    if(keepKeys == false) {
                        if (tupleParts[0].contains("KEY.")) {
                            tupleParts[0] = tupleParts[0].replace("KEY.", "");
                        }
                        if (tupleParts[0].contains("VALUE.")) {
                            tupleParts[0] = tupleParts[0].replace("VALUE.", "");
                        }
                    }
                    System.out.println("New Column-Value Part located...Key: "+tupleParts[0]+" Value: "+tupleParts[1]);
                    String typeCorrect = tupleParts[1].trim();

                    ColumnTypePair pair = new ColumnTypePair(tupleParts[0], typeCorrect);
                    aMap.addPair(pair);
                }
                else{
                    System.out.println("extractColsFromTypeName: Failed to :!");
                    System.exit(0);
                }
            }
        }

        return schema;

    }

    public String buildColumnNamesFromMap(MyMap columns){

        String output = "";

        if(columns != null){
            List<ColumnTypePair> blaList = columns.getColumnAndTypeList();
            for(ColumnTypePair entry : blaList){
                if(!output.isEmpty()){
                    output = output.concat(", ");
                }
                if(entry != null){
                    if(entry.getColumnName() != null){
                        output = output.concat(entry.getColumnName());
                    }
                }
            }
        }

        System.out.println("buildColumnNamesFromMap: "+output);

        return output;

    }

    public String addNewPossibleAliases(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorNode fatherOperatorNode2){

        Map<String, ExprNodeDesc> exprNodeDescMap = currentOperatorNode.getOperator().getColumnExprMap();
        if(exprNodeDescMap == null) return currentOperatorNode.getOperator().getSchema().toString();

        if(currentOperatorNode.getOperator().getSchema() == null){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Schema is NULL!");
            System.exit(0);
        }

        String schemaString = currentOperatorNode.getOperator().getSchema().toString();

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Before...addNewPossibleAliases...columnAndType has become: ");
        printColumnAndTypeMap();

        System.out.println("Schema currently is : "+schemaString);

        for(Map.Entry<String, ExprNodeDesc> entry : currentOperatorNode.getOperator().getColumnExprMap().entrySet()){
            if(entry.getKey().equals("ROW__ID") || entry.getKey().equals("BLOCK__OFFSET__INSIDE__FILE") || entry.getKey().equals("INPUT__FILE__NAME")) continue;
            ExprNodeDesc oldValue = entry.getValue();
            if(oldValue == null){
                continue;
            }
            if(oldValue.getName().contains("Const ")) continue;
            if(oldValue.toString().contains("Const ")) continue;

            String oldColumnName = oldValue.getCols().get(0);

            List<ColumnTypePair> columnTypePairs = columnAndTypeMap.getColumnAndTypeList();
            for(ColumnTypePair pair : columnTypePairs){
                if(pair.getColumnName().equals(oldColumnName)){ //Old ColumnName is a normal Alias
                    pair.addAltAlias(currentOperatorNode.getOperatorName(), entry.getKey());
                    if(schemaString.contains(entry.getKey())){
                        schemaString = schemaString.replace(entry.getKey(), pair.getColumnName());
                    }
                    else if(schemaString.contains(oldColumnName)){
                        schemaString = schemaString.replace(oldColumnName, pair.getColumnName());
                    }

                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": (old Alias)= "+oldColumnName+" matched with (new Alias)="+entry.getKey()+" for Operator= "+currentOperatorNode.getOperatorName());
                }
                else{ //Check if old ColumnName is an alternate alias belonging to father
                    List<StringParameter> altAliases = pair.getAltAliasPairs();
                    for(StringParameter sP : altAliases){
                        if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                            if(sP.getValue().equals(oldColumnName)){
                                pair.addAltAlias(currentOperatorNode.getOperatorName(), entry.getKey());

                                if(schemaString.contains(entry.getKey())){
                                    schemaString = schemaString.replace(entry.getKey(), pair.getColumnName());
                                }
                                else if(schemaString.contains(oldColumnName)){
                                    schemaString = schemaString.replace(oldColumnName, pair.getColumnName());
                                }

                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": (old Alias)= "+oldColumnName+" matched with (new Alias)="+entry.getKey()+" for Operator= "+currentOperatorNode.getOperatorName()+" through fatherNode= "+fatherOperatorNode.getOperatorName());
                                break;
                            }
                        }
                        else{
                            if(fatherOperatorNode2 != null){
                                if(sP.getParemeterType().equals(fatherOperatorNode2.getOperatorName())){
                                    if(sP.getValue().equals(oldColumnName)){
                                        pair.addAltAlias(currentOperatorNode.getOperatorName(), entry.getKey());

                                        if(schemaString.contains(entry.getKey())){
                                            schemaString = schemaString.replace(entry.getKey(), pair.getColumnName());
                                        }
                                        else if(schemaString.contains(oldColumnName)){
                                            schemaString = schemaString.replace(oldColumnName, pair.getColumnName());
                                        }

                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": (old Alias)= "+oldColumnName+" matched with (new Alias)="+entry.getKey()+" for Operator= "+currentOperatorNode.getOperatorName()+" through fatherNode= "+fatherOperatorNode2.getOperatorName());
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": After...addNewPossibleAliases...columnAndType has become: ");
        printColumnAndTypeMap();

        System.out.println("Updated schema is : "+schemaString);

        return schemaString;

    }

    public void createExaOperators(PrintWriter outputFile){

        /*----Get Hive Operator Graph Leaves----*/
        List<OperatorNode> roots = exaremeGraph.getRoots();

        if(roots.size() == 1){ //One InputTable
            if(inputTables.size() > 1){
                System.out.println("InputTables more than 1 but only 1 TableScan?!");
                System.exit(0);
            }

            OperatorNode rootNode = roots.get(0);
            if(rootNode.getOperator() instanceof TableScanOperator){

                if(inputTables.get(0).getHasPartitions() == false) { //No Partitions for InputTable
                    System.out.println(rootNode.getOperator().getOperatorId() + ": InputTable: " + inputTables.get(0) + " has no Partitions!");
                }
                else{
                    System.out.println(rootNode.getOperator().getOperatorId() + ": InputTable: "+inputTables.get(0)+" HAS Partitions!");

                    int countPartitions = inputTables.get(0).getAllPartitions().size();

                    System.out.println(rootNode.getOperator().getOperatorId() + " Current TableScanOperator needs access to: "+countPartitions+" partitions...");
                    System.out.println(rootNode.getOperator().getOperatorId() + " Listing PartitionNames for easier debugging...");
                    for(MyPartition m : inputTables.get(0).getAllPartitions()){
                        System.out.println(rootNode.getOperator().getOperatorId() + " PartitionName: "+m.getPartitionName());
                    }

                    System.out.println(rootNode.getOperator().getOperatorId() + " Current Strategy: Treat each Partition as seperate table and Output Table will have only one 1 Partition");
                    System.out.println(rootNode.getOperator().getOperatorId() + " Since every Partition will follow the same exactly Operator Tree it is pointless to run multiple OperatorQuery recursive Trees...");
                    System.out.println(rootNode.getOperator().getOperatorId() + " Partitions will be dealt with in the translateToExaremeOps function...");
                }

                    /*----Begin an Operator Query----*/
                    OperatorQuery opQuery = new OperatorQuery();
                    opQuery.setDataBasePath(currentDatabasePath);
                    opQuery.setLocalQueryString("");
                    opQuery.setExaremeQueryString("");
                    opQuery.addInputTable(inputTables.get(0));
                    opQuery.setAssignedContainer("c0");
                    opQuery.setLocalQueryString(" from " + inputTables.get(0).getTableName().toLowerCase());
                    opQuery.setExaremeQueryString(" from " + inputTables.get(0).getTableName().toLowerCase());

                /*---Extract Columns from TableScan---*/
                    System.out.println(rootNode.getOperator().getOperatorId() + ": Beginning Query Construction from root...");
                    System.out.println(rootNode.getOperator().getOperatorId() + ": Adding Cols to Map from Input Table...");
                    MyTable inputTable = inputTables.get(0);
                    List<FieldSchema> allCols = inputTable.getAllCols();
                    for (FieldSchema field : allCols) {
                        ColumnTypePair somePair = new ColumnTypePair(field.getName(), field.getType());
                        System.out.println(rootNode.getOperator().getOperatorId() + ": Discovered new Pair: Name= " + field.getName() + " - Type= " + field.getType());
                        columnAndTypeMap.addPair(somePair);
                    }
                    TableScanOperator tbsOp = (TableScanOperator) rootNode.getOperator();
                    TableScanDesc tableScanDesc = tbsOp.getConf();

                /*---Access select needed columns---*/
                    System.out.println(rootNode.getOperator().getOperatorId() + ": Accessing needed Columns...");
                    List<String> neededColumns = tableScanDesc.getNeededColumns();

                /*---Access Child of TableScan(must be only 1)---*/
                    List<Operator<?>> children = tbsOp.getChildOperators();

                    if (children.size() == 1) {
                        Operator<?> child = children.get(0);
                        if ((child instanceof FilterOperator) || (child instanceof ReduceSinkOperator)) {
                            if(child instanceof FilterOperator)
                                System.out.println(rootNode.getOperator().getOperatorId() + ": TS--->FIL connection discovered! Child is a FilterOperator!");
                            else if(child instanceof ReduceSinkOperator)
                                System.out.println(rootNode.getOperator().getOperatorId() + ": TS--->RS connection discovered! Child is a ReduceSinkOperator!");

                            System.out.println(rootNode.getOperator().getOperatorId() + ": Adding needed columns as select Columns if possible...");
                            if (neededColumns.size() > 0) {
                                String expression = "";
                                int i = 0;
                                for (i = 0; i < neededColumns.size(); i++) {
                                    if (i == neededColumns.size() - 1) {
                                        expression = expression.concat(" " + neededColumns.get(i));
                                    } else {
                                        expression = expression.concat(" " + neededColumns.get(i) + ",");
                                    }
                                }

                                /*---Locate new USED columns---*/
                                for (String n : neededColumns) {
                                    opQuery.addUsedColumn(n, tableScanDesc.getAlias());
                                }

                                opQuery.setLocalQueryString(" select " + expression + " from " + inputTables.get(0).getTableName().toLowerCase());
                                opQuery.setExaremeQueryString(" select " + expression + " from " + inputTables.get(0).getTableName().toLowerCase());

                                MyTable outputTable = new MyTable();
                                outputTable.setIsAFile(false);
                                outputTable.setBelongingDatabaseName(inputTable.getBelongingDataBaseName());
                                outputTable.setTableName("temp_name");
                                outputTable.setHasPartitions(false);

                                List<FieldSchema> outputCols = new LinkedList<>();
                                /*for (String neededCol : neededColumns) {
                                    for (ColumnTypePair pair : columnAndTypeMap.getColumnAndTypeList()) {
                                        if (pair.getColumnName().equals(neededCol)) {
                                            FieldSchema outputField = new FieldSchema();
                                            outputField.setName(pair.getColumnName());
                                            outputField.setType(pair.getColumnType());
                                            outputCols.add(outputField);
                                        }
                                    }
                                }*/
                                outputTable.setAllCols(outputCols);
                                opQuery.setOutputTable(outputTable);
                            } else {
                                System.out.println(rootNode.getOperator().getOperatorId() + ": Error no needed Columns Exist!");
                            }
                            System.out.println(rootNode.getOperator().getOperatorId() + ": Moving to ChildOperator...");

                            /*---Move to Child---*/
                            OperatorNode targetChildNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());
                            goToChildOperator(targetChildNode, rootNode, opQuery, rootNode.getOperator().getSchema().toString(), opQuery.getInputTables().get(0).getTableName().toLowerCase(), null, null);

                            System.out.println(rootNode.getOperator().getOperatorId() + ": Returned from Child...");
                        }
                        else if(child instanceof SelectOperator) {

                            System.out.println(rootNode.getOperator().getOperatorId() + ": TS--->SEL connection discovered! Child is a SelectOperator!");


                            System.out.println(rootNode.getOperator().getOperatorId() + ": Adding needed columns as select Columns if possible...");
                            if (neededColumns.size() > 0) {
                                String expression = "";
                                int i = 0;
                                for (i = 0; i < neededColumns.size(); i++) {
                                    if (i == neededColumns.size() - 1) {
                                        expression = expression.concat(" " + neededColumns.get(i));
                                    } else {
                                        expression = expression.concat(" " + neededColumns.get(i) + ",");
                                    }
                                }

                                /*---Locate new USED columns---*/
                                for (String n : neededColumns) {
                                    opQuery.addUsedColumn(n, tableScanDesc.getAlias());
                                }

                                //opQuery.setLocalQueryString(" select " + expression + " from " + inputTables.get(0).getTableName().toLowerCase());
                                //opQuery.setExaremeQueryString(" select " + expression + " from " + inputTables.get(0).getTableName().toLowerCase());

                                MyTable outputTable = new MyTable();
                                outputTable.setIsAFile(false);
                                outputTable.setBelongingDatabaseName(inputTable.getBelongingDataBaseName());
                                outputTable.setTableName("temp_name");
                                outputTable.setHasPartitions(false);

                                List<FieldSchema> outputCols = new LinkedList<>();
                                /*for (String neededCol : neededColumns) {
                                    for (ColumnTypePair pair : columnAndTypeMap.getColumnAndTypeList()) {
                                        if (pair.getColumnName().equals(neededCol)) {
                                            FieldSchema outputField = new FieldSchema();
                                            outputField.setName(pair.getColumnName());
                                            outputField.setType(pair.getColumnType());
                                            outputCols.add(outputField);
                                        }
                                    }
                                }*/
                                outputTable.setAllCols(outputCols);
                                opQuery.setOutputTable(outputTable);
                            } else {
                                System.out.println(rootNode.getOperator().getOperatorId() + ": Error no needed Columns Exist!");
                            }
                            System.out.println(rootNode.getOperator().getOperatorId() + ": Moving to ChildOperator...");

                            /*---Move to Child---*/
                            OperatorNode targetChildNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());
                            goToChildOperator(targetChildNode, rootNode, opQuery, rootNode.getOperator().getSchema().toString(), opQuery.getInputTables().get(0).getTableName().toLowerCase(), null, null);

                            System.out.println(rootNode.getOperator().getOperatorId() + ": Returned from Child...");
                        }
                        else
                        {
                            System.out.println(rootNode.getOperator().getOperatorId() + ": Child of TS is not FIL! Not supported yet!");
                            System.exit(0);
                        }
                    } else {
                        System.out.println("TableScan must have exactly one child! Error!");
                        System.exit(0);
                    }
            }
            else{
                System.out.println("Root Operator is not TableScan! Error!");
                System.exit(0);
            }
        }
        else{ //Multiple Input Tables
            System.out.println("More than one Input Table Exists!");

            System.out.println("One loop for every operator tree!");

            for(OperatorNode root : roots){ //For Every Root Operator

                Operator<?> rootOp = root.getOperator();

                if(rootOp instanceof TableScanOperator){
                    TableScanDesc tableDesc = (TableScanDesc) rootOp.getConf();
                    String tableName = tableDesc.getAlias();

                    boolean tableExists = false;
                    MyTable wantedTable = new MyTable();
                    for(MyTable inputTable : inputTables){
                        if(inputTable.getTableName().equals(tableName)){
                            wantedTable = inputTable;
                            tableExists = true;
                            break;
                        }
                    }

                    if(tableExists == false){
                        System.out.println(root.getOperator().getOperatorId() + ": InputTable: "+tableName+" does not exist in QueryBuilder InputTables!");
                        System.exit(0);
                    }

                    if(wantedTable.getHasPartitions()){
                        System.out.println(root.getOperator().getOperatorId() + ": InputTable: "+tableName+" has Partitions! Partitions are not supported as of now!");
                        int countPartitions = wantedTable.getAllPartitions().size();

                        System.out.println(root.getOperator().getOperatorId() + " Current TableScanOperator needs access to: "+countPartitions+" partitions...");
                        System.out.println(root.getOperator().getOperatorId() + " Listing PartitionNames for easier debugging...");
                        for(MyPartition m : wantedTable.getAllPartitions()){
                            System.out.println(root.getOperator().getOperatorId() + " PartitionName: "+m.getPartitionName());
                        }

                        System.out.println(root.getOperator().getOperatorId() + " Current Strategy: Treat each Partition as seperate table and Output Table will have only one 1 Partition");
                        System.out.println(root.getOperator().getOperatorId() + " Since every Partition will follow the same exactly Operator Tree it is pointless to run multiple OperatorQuery recursive Trees...");
                        System.out.println(root.getOperator().getOperatorId() + " Partitions will be dealt with in the translateToExaremeOps function...");
                    }
                    else {
                        System.out.println(root.getOperator().getOperatorId() + ": InputTable: " + tableName + " does not have Partitions...");
                    }
                        /*----Begin an Operator Query----*/
                        OperatorQuery opQuery = new OperatorQuery();
                        opQuery.setDataBasePath(currentDatabasePath);
                        opQuery.setLocalQueryString("");
                        opQuery.setExaremeQueryString("");
                        opQuery.addInputTable(wantedTable);
                        opQuery.setAssignedContainer("c0");
                        opQuery.setLocalQueryString(" from " + wantedTable.getTableName().toLowerCase());
                        opQuery.setExaremeQueryString(" from " + wantedTable.getTableName().toLowerCase());

                        /*---Extract Columns from TableScan---*/
                        System.out.println(root.getOperator().getOperatorId() + ": Beginning Query Construction from root...");
                        System.out.println(root.getOperator().getOperatorId() + ": Adding Cols to Map from Input Table...");
                        MyTable inputTable = wantedTable;
                        List<FieldSchema> allCols = inputTable.getAllCols();
                        for (FieldSchema field : allCols) {
                            ColumnTypePair somePair = new ColumnTypePair(field.getName(), field.getType());
                            System.out.println(root.getOperator().getOperatorId() + ": Discovered new Pair: Name= " + field.getName() + " - Type= " + field.getType());
                            columnAndTypeMap.addPair(somePair);
                        }
                        TableScanOperator tbsOp = (TableScanOperator) root.getOperator();
                        TableScanDesc tableScanDesc = tbsOp.getConf();

                        /*---Access select needed columns---*/
                        System.out.println(root.getOperator().getOperatorId() + ": Accessing needed Columns...");
                        List<String> neededColumns = tableScanDesc.getNeededColumns();

                        /*---Access Child of TableScan(must be only 1)---*/
                        List<Operator<?>> children = tbsOp.getChildOperators();

                        if (children.size() == 1) {
                            Operator<?> child = children.get(0);
                            if ((child instanceof FilterOperator) || (child instanceof ReduceSinkOperator)) {
                                if(child instanceof FilterOperator)
                                    System.out.println(root.getOperator().getOperatorId() + ": TS--->FIL connection discovered! Child is a FilterOperator!");
                                else if(child instanceof ReduceSinkOperator)
                                    System.out.println(root.getOperator().getOperatorId() + ": TS--->RS connection discovered! Child is a ReduceSinkOperator!");

                                System.out.println(root.getOperator().getOperatorId() + ": Adding needed columns as select Columns if possible...");
                                if (neededColumns.size() > 0) {
                                    String expression = "";
                                    int i = 0;
                                    for (i = 0; i < neededColumns.size(); i++) {
                                        if (i == neededColumns.size() - 1) {
                                            expression = expression.concat(" " + neededColumns.get(i));
                                        } else {
                                            expression = expression.concat(" " + neededColumns.get(i) + ",");
                                        }
                                    }

                                    /*---Locate new USED columns---*/
                                    for (String n : neededColumns) {
                                        opQuery.addUsedColumn(n, inputTable.getTableName());
                                    }

                                    opQuery.setLocalQueryString(" select " + expression + " from " + inputTable.getTableName().toLowerCase());
                                    opQuery.setExaremeQueryString(" select " + expression + " from " + inputTable.getTableName().toLowerCase());

                                    MyTable outputTable = new MyTable();
                                    outputTable.setIsAFile(false);
                                    outputTable.setBelongingDatabaseName(inputTable.getBelongingDataBaseName());
                                    outputTable.setTableName("temp_name");
                                    outputTable.setHasPartitions(false);


                                    List<FieldSchema> outputCols = new LinkedList<>();
                                    /*for (String neededCol : neededColumns) {
                                        for (ColumnTypePair pair : columnAndTypeMap.getColumnAndTypeList()) {
                                            if (pair.getColumnName().equals(neededCol)) {
                                                FieldSchema outputField = new FieldSchema();
                                                outputField.setName(pair.getColumnName());
                                                outputField.setType(pair.getColumnType());
                                                outputCols.add(outputField);
                                            }
                                        }
                                    }*/
                                    outputTable.setAllCols(outputCols);
                                    opQuery.setOutputTable(outputTable);
                                } else {
                                    System.out.println(root.getOperator().getOperatorId() + ": Error no needed Columns Exist!");
                                }
                                System.out.println(root.getOperator().getOperatorId() + ": Moving to ChildOperator...");

                                /*---Move to Child---*/
                                OperatorNode targetChildNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());
                                goToChildOperator(targetChildNode, root, opQuery, root.getOperator().getSchema().toString(), inputTable.getTableName().toLowerCase(), null, null);

                                System.out.println(root.getOperator().getOperatorId() + ": Returned from Child...");
                            }
                            else if(child instanceof SelectOperator) {

                                System.out.println(root.getOperator().getOperatorId() + ": TS--->SEL connection discovered! Child is a SelectOperator!");


                                System.out.println(root.getOperator().getOperatorId() + ": Adding needed columns as select Columns if possible...");
                                if (neededColumns.size() > 0) {
                                    String expression = "";
                                    int i = 0;
                                    for (i = 0; i < neededColumns.size(); i++) {
                                        if (i == neededColumns.size() - 1) {
                                            expression = expression.concat(" " + neededColumns.get(i));
                                        } else {
                                            expression = expression.concat(" " + neededColumns.get(i) + ",");
                                        }
                                    }

                                /*---Locate new USED columns---*/
                                    for (String n : neededColumns) {
                                        opQuery.addUsedColumn(n, tableScanDesc.getAlias());
                                    }

                                    //opQuery.setLocalQueryString(" select " + expression + " from " + inputTables.get(0).getTableName().toLowerCase());
                                    //opQuery.setExaremeQueryString(" select " + expression + " from " + inputTables.get(0).getTableName().toLowerCase());

                                    MyTable outputTable = new MyTable();
                                    outputTable.setIsAFile(false);
                                    outputTable.setBelongingDatabaseName(inputTable.getBelongingDataBaseName());
                                    outputTable.setTableName("temp_name");
                                    outputTable.setHasPartitions(false);

                                    List<FieldSchema> outputCols = new LinkedList<>();
                                    /*for (String neededCol : neededColumns) {
                                        for (ColumnTypePair pair : columnAndTypeMap.getColumnAndTypeList()) {
                                            if (pair.getColumnName().equals(neededCol)) {
                                                FieldSchema outputField = new FieldSchema();
                                                outputField.setName(pair.getColumnName());
                                                outputField.setType(pair.getColumnType());
                                                outputCols.add(outputField);
                                            }
                                        }
                                    }*/
                                    outputTable.setAllCols(outputCols);
                                    opQuery.setOutputTable(outputTable);
                                } else {
                                    System.out.println(root.getOperator().getOperatorId() + ": Error no needed Columns Exist!");
                                }
                                System.out.println(root.getOperator().getOperatorId() + ": Moving to ChildOperator...");

                            /*---Move to Child---*/
                                OperatorNode targetChildNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());
                                goToChildOperator(targetChildNode, root, opQuery, root.getOperator().getSchema().toString(), opQuery.getInputTables().get(0).getTableName().toLowerCase(), null, null);

                                System.out.println(root.getOperator().getOperatorId() + ": Returned from Child...");
                            }
                            else {
                                System.out.println(root.getOperator().getOperatorId() + ": Child of TS is not FIL! Not supported yet!");
                                System.exit(0);
                            }
                        } else {
                            System.out.println("TableScan must have exactly one child! Error!");
                            System.exit(0);
                        }



                }
                else{
                    System.out.println("Root Operator is not TableScan! Error!");
                    System.exit(0);
                }

            }

        }

    }

    public void goToChildOperator(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorQuery currentOpQuery, String latestAncestorSchema, String latestAncestorTableName1, String latestAncestorTableName2, OperatorNode otherFatherNode){

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Beginning work from here...");
        if(currentOperatorNode.getOperator() instanceof FilterOperator){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Operator is a FilterOperator...");
            if((fatherOperatorNode.getOperator() instanceof TableScanOperator) || (fatherOperatorNode.getOperator() instanceof JoinOperator)){
                if(fatherOperatorNode.getOperator() instanceof TableScanOperator)
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a TableScanOperator: "+fatherOperatorNode.getOperatorName());
                else
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a JoinOperator: "+fatherOperatorNode.getOperatorName());

                if(fatherOperatorNode.getOperator() instanceof TableScanOperator) {
                /*----Extracting predicate of FilterOp----*/
                    if ((fatherOperatorNode.getOperator().getParentOperators() == null) || (fatherOperatorNode.getOperator().getParentOperators().size() == 0)) {
                        FilterOperator filtOp = (FilterOperator) currentOperatorNode.getOperator();
                        FilterDesc filterDesc = filtOp.getConf();

                        /*---Locate new USED columns---*/
                        List<String> predCols = filterDesc.getPredicate().getCols();
                        if (predCols != null) {
                            for (String p : predCols) {
                                currentOpQuery.addUsedColumn(p, latestAncestorTableName1);
                            }
                        }

                        ExprNodeDesc predicate = filterDesc.getPredicate();
                        if (predicate != null) {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Extracting columns of predicate...");
                            List<String> filterColumns = predicate.getCols();

                            String predicateString = predicate.getExprString();

                            currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString().concat(" where " + predicateString + " "));
                            currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString().concat(" where " + predicateString + " "));

                        } else {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Predicate is NULL");
                            System.exit(0);
                        }
                    } else {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Father TableScanOperator is not ROOT! Unsupported yet!");
                        System.exit(0);
                    }

                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a JoinOperator! Use ancestral schema to add Columns and transform current Schema!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Ancestral Schema: "+latestAncestorSchema);

                    MyMap ancestorMap = new MyMap();
                    latestAncestorSchema = extractColsFromTypeName(latestAncestorSchema, ancestorMap, latestAncestorSchema, false);

                    MyMap descendentMap = new MyMap();
                    String descendentSchema = extractColsFromTypeName(currentOperatorNode.getOperator().getSchema().toString(), descendentMap, currentOperatorNode.getOperator().getSchema().toString(), true);

                    if(ancestorMap.getColumnAndTypeList().size() != descendentMap.getColumnAndTypeList().size()){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Maps do not have equal size!");
                        System.exit(0);
                    }

                    FilterOperator filtOp = (FilterOperator) currentOperatorNode.getOperator();
                    FilterDesc filterDesc = filtOp.getConf();

                    List<String> usedCols = new LinkedList<>();
                    List<String> predCols = filterDesc.getPredicate().getCols();
                    ExprNodeDesc predicate = filterDesc.getPredicate();
                    String predicateString = predicate.getExprString();

                    for(int i = 0; i < ancestorMap.getColumnAndTypeList().size(); i++){
                        ColumnTypePair ancestorPair = ancestorMap.getColumnAndTypeList().get(i);
                        ColumnTypePair descendentPair = descendentMap.getColumnAndTypeList().get(i);

                        if(ancestorPair.getColumnType().equals(descendentPair.getColumnType())){
                            for(ColumnTypePair p : columnAndTypeMap.getColumnAndTypeList()){
                                if(p.getColumnName().equals(ancestorPair.getColumnName())){
                                    p.addAltAlias(currentOperatorNode.getOperatorName(), descendentPair.getColumnName());
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Match found! - Pair: "+ancestorPair.getColumnName()+" - "+descendentPair.getColumnName());
                                    if(predCols != null){
                                        if(predCols.size() > 0){
                                            for(String c : predCols){
                                                if(c.equals(descendentPair.getColumnName())){
                                                    if(predicateString != null){
                                                        if(predicateString.contains(c)){
                                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": This column also exists in the predicate! Replacing!");
                                                            predicateString = predicateString.replace(c, ancestorPair.getColumnName());
                                                        }
                                                    }

                                                    //Add used column
                                                    currentOpQuery.addUsedColumn(ancestorPair.getColumnName(), latestAncestorTableName1);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Possible match Pair does not have same Type - Pair: "+ancestorPair.getColumnName()+" - "+descendentPair.getColumnName());
                            System.exit(0);
                        }
                    }

                    if (predicateString != null) {
                        currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString().concat(" where " + predicateString + " "));
                        currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString().concat(" where " + predicateString + " "));

                    } else {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Predicate is NULL");
                        System.exit(0);
                    }

                }

                /*---Check Type of Child---*/
                List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
                if(children != null){
                    if(children.size() == 1){
                        Operator<?> child = children.get(0);
                        if(child instanceof SelectOperator) {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered FIL--->SEL Connection! Child is select: " + child.getOperatorId());
                            if (fatherOperatorNode.getOperator() instanceof JoinOperator) {
                                if (currentOpQuery.getLocalQueryString().contains("select ") == false) {
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Constructing SELECT statement columns...");
                                    MyMap aMap = new MyMap();
                                    latestAncestorSchema = extractColsFromTypeName(latestAncestorSchema, aMap, latestAncestorSchema, false);

                                    String selectString = "";
                                    List<ColumnTypePair> pairs = aMap.getColumnAndTypeList();
                                    for (int i = 0; i < pairs.size(); i++) {
                                        if (i == pairs.size() - 1) {
                                            selectString = selectString + " " + pairs.get(i).getColumnName() + " ";
                                        } else {
                                            selectString = selectString + " " + pairs.get(i).getColumnName() + ",";
                                        }
                                    }

                                    /*---Locate new USED columns---*/
                                    for (ColumnTypePair pair : aMap.getColumnAndTypeList()) {
                                        currentOpQuery.addUsedColumn(pair.getColumnName(), latestAncestorTableName1);
                                    }

                                    currentOpQuery.setLocalQueryString("select" + selectString + currentOpQuery.getLocalQueryString());
                                    currentOpQuery.setExaremeQueryString("select" + selectString + currentOpQuery.getExaremeQueryString());

                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + currentOpQuery.getLocalQueryString() + "]");

                                }
                            }

                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Current Query will be ending here...");

                            if (fatherOperatorNode.getOperator() instanceof TableScanOperator){

                                /*---Finalising outputTable---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");
                                MyTable outputTable = new MyTable();
                                outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                                outputTable.setIsAFile(false);

                                MyMap someMap = new MyMap();

                                String updatedSchemaString = addNewPossibleAliases(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, null);

                                updatedSchemaString = extractColsFromTypeName(updatedSchemaString, someMap, updatedSchemaString, false);

                                List<FieldSchema> newCols = new LinkedList<>();
                                List<ColumnTypePair> pairsList = someMap.getColumnAndTypeList();
                                for(ColumnTypePair p : pairsList){
                                    if(p.getColumnName().contains("BLOCK__OFFSET__INSIDE__FILE")){
                                        continue;
                                    }
                                    else if(p.getColumnName().contains("INPUT__FILE__NAME")){
                                        continue;
                                    }
                                    else if(p.getColumnName().contains("ROW__ID")){
                                        continue;
                                    }
                                    else if(p.getColumnName().contains("rowid")){
                                        continue;
                                    }
                                    else if(p.getColumnName().contains("bucketid")){
                                        continue;
                                    }

                                    FieldSchema f = new FieldSchema();
                                    f.setName(p.getColumnName());
                                    f.setType(p.getColumnType());
                                    newCols.add(f);
                                }

                                outputTable.setTableName(child.getOperatorId().toLowerCase());
                                outputTable.setAllCols(newCols);
                                outputTable.setHasPartitions(false);

                                currentOpQuery.setOutputTable(outputTable);
                                currentOpQuery.setExaremeOutputTableName("R_"+outputTable.getTableName().toLowerCase()+"_0");

                                /*---Finalize local part of Query---*/
                                currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

                                /*---Check if OpLink is to be created---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
                                for(MyTable inputT : currentOpQuery.getInputTables()){
                                    boolean isRoot = false;
                                    for(MyTable rootInputT : inputTables){
                                        if(rootInputT.getTableName().equals(inputT.getTableName())){
                                            isRoot = true;
                                            break;
                                        }
                                    }
                                    if(isRoot == false){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Creating new OpLink...");
                                        OpLink operatorLink = new OpLink();
                                        operatorLink.setContainerName("c0");
                                        operatorLink.setFromTable("R_"+inputT.getTableName().toLowerCase()+"_0");
                                        operatorLink.setToTable("R_"+outputTable.getTableName().toLowerCase()+"_0");

                                        StringParameter sP = new StringParameter("table", inputT.getTableName());
                                        NumParameter nP = new NumParameter("part", 0);

                                        List<Parameter> opParams = new LinkedList<>();
                                        opParams.add(sP);
                                        opParams.add(nP);

                                        operatorLink.setParameters(opParams);
                                        opLinksList.add(operatorLink);

                                    }
                                }

                                /*----Adding Finished Query to List----*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
                                allQueries.add(currentOpQuery);

                                /*----Begin a new Query----*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
                                OperatorQuery opQuery = new OperatorQuery();
                                opQuery.setDataBasePath(currentDatabasePath);
                                opQuery.setLocalQueryString("");
                                opQuery.setExaremeQueryString("");
                                opQuery.addInputTable(currentOpQuery.getOutputTable());
                                opQuery.setAssignedContainer("c0");
                                opQuery.setLocalQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());
                                opQuery.setExaremeQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());

                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + opQuery.getLocalQueryString() + "]");

                                OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                                /*---Moving to Child Operator---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Moving to Child: " + child.getOperatorId());
                                goToChildOperator(childNode, currentOperatorNode, opQuery, latestAncestorSchema, child.getOperatorId().toLowerCase(), latestAncestorTableName2, null);
                            }
                            else if(fatherOperatorNode.getOperator() instanceof JoinOperator){ //This differs from above case because the above case refers to ROOT TableScan ending

                                /*---Finalising outputTable---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");
                                MyTable outputTable = new MyTable();
                                outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                                outputTable.setIsAFile(false);

                                MyMap someMap = new MyMap();

                                latestAncestorSchema = extractColsFromTypeName(latestAncestorSchema, someMap, latestAncestorSchema, false);

                                List<FieldSchema> newCols = new LinkedList<>();
                                List<ColumnTypePair> pairsList = someMap.getColumnAndTypeList();
                                for(ColumnTypePair p : pairsList){
                                    if(p.getColumnName().contains("BLOCK__OFFSET__INSIDE__FILE")){
                                        continue;
                                    }
                                    else if(p.getColumnName().contains("INPUT__FILE__NAME")){
                                        continue;
                                    }
                                    else if(p.getColumnName().contains("ROW__ID")){
                                        continue;
                                    }
                                    else if(p.getColumnName().contains("rowid")){
                                        continue;
                                    }
                                    else if(p.getColumnName().contains("bucketid")){
                                        continue;
                                    }

                                    FieldSchema f = new FieldSchema();
                                    f.setName(p.getColumnName());
                                    f.setType(p.getColumnType());
                                    newCols.add(f);
                                }

                                outputTable.setTableName(child.getOperatorId().toLowerCase());
                                outputTable.setAllCols(newCols);
                                outputTable.setHasPartitions(false);

                                currentOpQuery.setOutputTable(outputTable);
                                currentOpQuery.setExaremeOutputTableName("R_"+outputTable.getTableName().toLowerCase()+"_0");

                                /*---Finalize local part of Query---*/
                                currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

                                /*---Check if OpLink is to be created---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
                                for(MyTable inputT : currentOpQuery.getInputTables()){
                                    boolean isRoot = false;
                                    for(MyTable rootInputT : inputTables){
                                        if(rootInputT.getTableName().equals(inputT.getTableName())){
                                            isRoot = true;
                                            break;
                                        }
                                    }
                                    if(isRoot == false){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Creating new OpLink...");
                                        OpLink operatorLink = new OpLink();
                                        operatorLink.setContainerName("c0");
                                        operatorLink.setFromTable("R_"+inputT.getTableName().toLowerCase()+"_0");
                                        operatorLink.setToTable("R_"+outputTable.getTableName().toLowerCase()+"_0");

                                        StringParameter sP = new StringParameter("table", inputT.getTableName());
                                        NumParameter nP = new NumParameter("part", 0);

                                        List<Parameter> opParams = new LinkedList<>();
                                        opParams.add(sP);
                                        opParams.add(nP);

                                        operatorLink.setParameters(opParams);
                                        opLinksList.add(operatorLink);

                                    }
                                }

                                /*----Adding Finished Query to List----*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
                                allQueries.add(currentOpQuery);

                                /*----Begin a new Query----*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
                                OperatorQuery opQuery = new OperatorQuery();
                                opQuery.setDataBasePath(currentDatabasePath);
                                opQuery.setLocalQueryString("");
                                opQuery.setExaremeQueryString("");
                                opQuery.addInputTable(currentOpQuery.getOutputTable());
                                opQuery.setAssignedContainer("c0");
                                opQuery.setLocalQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());
                                opQuery.setExaremeQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());

                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + opQuery.getLocalQueryString() + "]");

                                OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                                /*---Moving to Child Operator---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Moving to Child: " + child.getOperatorId());
                                goToChildOperator(childNode, currentOperatorNode, opQuery, latestAncestorSchema, child.getOperatorId().toLowerCase(), latestAncestorTableName2, null);

                            }
                            else {
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Unsupported type of father for ending Query!");
                                System.exit(0);
                            }


                        }
                        else if(child instanceof GroupByOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered FIL--->GBY Connection! Child is select: " + child.getOperatorId());
                            if(currentOpQuery.getLocalQueryString().contains("select ") == false) {
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Constructing SELECT statement columns...");
                                MyMap aMap = new MyMap();
                                latestAncestorSchema = extractColsFromTypeName(latestAncestorSchema, aMap, latestAncestorSchema, false);

                                String selectString = "";
                                List<ColumnTypePair> pairs = aMap.getColumnAndTypeList();
                                for (int i = 0; i < pairs.size(); i++) {
                                    if (i == pairs.size() - 1) {
                                        selectString = selectString + " " + pairs.get(i).getColumnName() + " ";
                                    } else {
                                        selectString = selectString + " " + pairs.get(i).getColumnName() + ",";
                                    }
                                }

                                    /*---Locate new USED columns---*/
                                for (ColumnTypePair pair : aMap.getColumnAndTypeList()) {
                                    currentOpQuery.addUsedColumn(pair.getColumnName(), latestAncestorTableName1);
                                }

                                currentOpQuery.setLocalQueryString("select" + selectString + currentOpQuery.getLocalQueryString());
                                currentOpQuery.setExaremeQueryString("select" + selectString + currentOpQuery.getExaremeQueryString());

                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + currentOpQuery.getLocalQueryString() + "]");

                            }

                             /*---Moving to Child Operator---*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Moving to Child: " + child.getOperatorId());
                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, currentOperatorNode.getOperator().getSchema().toString(), latestAncestorTableName1, latestAncestorTableName2, null);

                        }
                        else if(child instanceof ReduceSinkOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered FIL--->RS Connection! Child is select: " + child.getOperatorId());
                            if (fatherOperatorNode.getOperator() instanceof JoinOperator) {
                                if (currentOpQuery.getLocalQueryString().contains("select ") == false) {
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Constructing SELECT statement columns...");
                                    MyMap aMap = new MyMap();
                                    latestAncestorSchema = extractColsFromTypeName(latestAncestorSchema, aMap, latestAncestorSchema, false);

                                    String selectString = "";
                                    List<ColumnTypePair> pairs = aMap.getColumnAndTypeList();
                                    for (int i = 0; i < pairs.size(); i++) {
                                        if (i == pairs.size() - 1) {
                                            selectString = selectString + " " + pairs.get(i).getColumnName() + " ";
                                        } else {
                                            selectString = selectString + " " + pairs.get(i).getColumnName() + ",";
                                        }
                                    }

                                    /*---Locate new USED columns---*/
                                    for (ColumnTypePair pair : aMap.getColumnAndTypeList()) {
                                        currentOpQuery.addUsedColumn(pair.getColumnName(), latestAncestorTableName1);
                                    }

                                    currentOpQuery.setLocalQueryString("select" + selectString + currentOpQuery.getLocalQueryString());
                                    currentOpQuery.setExaremeQueryString("select" + selectString + currentOpQuery.getExaremeQueryString());

                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + currentOpQuery.getLocalQueryString() + "]");

                                }
                            }

                             /*---Moving to Child Operator---*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Moving to Child: " + child.getOperatorId());
                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, currentOperatorNode.getOperator().getSchema().toString(), latestAncestorTableName1, latestAncestorTableName2, null);

                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Child is of unsupported type!");
                            System.exit(0);
                        }
                    }
                    else{
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": More than 1 Child!");
                        System.exit(0);
                    }
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Children are null!");
                    System.exit(0);
                }
            }
            else{
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported Type of Parent Operator for FilterOperator...");
                System.exit(0);
            }
        }
        else if(currentOperatorNode.getOperator() instanceof TableScanOperator){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Operator is a TableScanOperator...");
            if(fatherOperatorNode.getOperator() instanceof FileSinkOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a FileSinkOperator!");

                if(currentOperatorNode.getOperator().getColumnExprMap() == null){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": FileSink has null columnExprMap! Assumming ancestor's schema...");
                    List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
                    if(children != null){
                        if(children.size() == 1){
                            Operator<?> child = children.get(0);

                            if(child instanceof ReduceSinkOperator){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered FS-->TS-->RS connection!");
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": We can't risk adding Select statement to Query yet...");
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": We will ommit this TableScan completely as a father of ReduceSink...");

                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child Operator...");

                                //TableScan is ommited as father
                                goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), fatherOperatorNode, currentOpQuery, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2, null);
                            }
                            else{
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported type of Child for FS-->TS-->?");
                                System.exit(0);
                            }
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Children are SIZE != 1!");
                            System.exit(0);
                        }
                    }
                    else{
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Children are NULL!");
                        System.exit(0);
                    }
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": TS with father FS with non null ColumnExprMap! Unsupported for now!");
                    System.exit(0);
                }

            }
            else{
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported type of Father for TableScanOperator");
                System.exit(0);
            }
        }
        else if(currentOperatorNode.getOperator() instanceof SelectOperator){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Operator is a SelectOperator...");
            if((fatherOperatorNode.getOperator() instanceof FilterOperator) || (fatherOperatorNode.getOperator() instanceof JoinOperator) || (fatherOperatorNode.getOperator() instanceof ReduceSinkOperator) || (fatherOperatorNode.getOperator() instanceof TableScanOperator)){
                if(fatherOperatorNode.getOperator() instanceof FilterOperator) {
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Parent Operator is FilterOperator!");
                }
                else if(fatherOperatorNode.getOperator() instanceof JoinOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Parent Operator is JoinOperator!");
                }
                else if(fatherOperatorNode.getOperator() instanceof ReduceSinkOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Parent Operator is ReduceSinkOperator!");
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Parent Operator is TableScanOperator!");
                }

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Adding new possible Aliases...");
                String updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);

                if(currentOpQuery.getLocalQueryString().contains("select ") == false) {
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Constructing SELECT statement columns...");
                    MyMap aMap = new MyMap();
                    updatedSchemaString = extractColsFromTypeName(updatedSchemaString, aMap, updatedSchemaString, false);

                    String selectString = "";
                    List<ColumnTypePair> pairs = aMap.getColumnAndTypeList();
                    for (int i = 0; i < pairs.size(); i++) {
                        if (i == pairs.size() - 1) {
                            selectString = selectString + " " + pairs.get(i).getColumnName() + " ";
                        } else {
                            selectString = selectString + " " + pairs.get(i).getColumnName() + ",";
                        }
                    }

                    /*---Locate new USED columns---*/
                    for (ColumnTypePair pair : aMap.getColumnAndTypeList()) {
                        currentOpQuery.addUsedColumn(pair.getColumnName(), latestAncestorTableName1);
                    }

                    currentOpQuery.setLocalQueryString("select" + selectString + currentOpQuery.getLocalQueryString());
                    currentOpQuery.setExaremeQueryString("select" + selectString + currentOpQuery.getExaremeQueryString());

                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + currentOpQuery.getLocalQueryString() + "]");

                }
                /*---Check Type of Child---*/
                List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
                if(children != null){
                    if(children.size() == 1){
                        Operator<?> child = children.get(0);
                        if(child instanceof GroupByOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered SEL--->GBY Connection! Child is : "+child.getOperatorId());
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child...");
                            OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                            /*---Move to Child---*/
                            goToChildOperator(childNode, currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);
                        }
                        else if(child instanceof LimitOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered SEL--->LIM Connection! Child is : "+child.getOperatorId());
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child...");
                            OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                            /*---Move to Child---*/
                            goToChildOperator(childNode, currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);
                        }
                        else if(child instanceof FileSinkOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered SEL--->FS Connection! Child is : "+child.getOperatorId());
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child...");
                            OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                            /*---Move to Child---*/
                            goToChildOperator(childNode, currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);
                        }
                        else if(child instanceof ReduceSinkOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered SEL--->RS Connection! Child is : "+child.getOperatorId());
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child...");
                            OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                            /*---Move to Child---*/
                            goToChildOperator(childNode, currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Child is of unsupported type!");
                            System.exit(0);
                        }
                    }
                    else{
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": More than 1 Child!");
                        System.exit(0);
                    }
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Children are null!");
                    System.exit(0);
                }
            }
            else if(fatherOperatorNode.getOperator() instanceof GroupByOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Parent Operator is GroupByOperator!");
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Adding new possible Aliases...");
                String updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);

                if(currentOpQuery.getLocalQueryString().contains("select ") == false) {
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Constructing SELECT statement columns...");
                    MyMap aMap = new MyMap();
                    updatedSchemaString = extractColsFromTypeName(updatedSchemaString, aMap, updatedSchemaString, false);

                    String selectString = "";
                    List<ColumnTypePair> pairs = aMap.getColumnAndTypeList();
                    for (int i = 0; i < pairs.size(); i++) {
                        if (i == pairs.size() - 1) {
                            selectString = selectString + " " + pairs.get(i).getColumnName() + " ";
                        } else {
                            selectString = selectString + " " + pairs.get(i).getColumnName() + ",";
                        }
                    }

                /*---Locate new USED columns---*/
                    for (ColumnTypePair pair : aMap.getColumnAndTypeList()) {
                        currentOpQuery.addUsedColumn(pair.getColumnName(), latestAncestorTableName1);
                    }

                    currentOpQuery.setLocalQueryString("select" + selectString + currentOpQuery.getLocalQueryString());
                    currentOpQuery.setExaremeQueryString("select" + selectString + currentOpQuery.getExaremeQueryString());

                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + currentOpQuery.getLocalQueryString() + "]");

                }

                /*---Check Type of Child---*/
                List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
                if(children != null){
                    if(children.size() == 1){
                        Operator<?> child = children.get(0);
                        if(child instanceof LimitOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered SEL--->LIM Connection! Child is : "+child.getOperatorId());
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child...");
                            OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                            /*---Move to Child---*/
                            goToChildOperator(childNode, currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Child is of unsupported type!");
                            System.exit(0);
                        }
                    }
                    else{
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": More than 1 Child!");
                        System.exit(0);
                    }
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Children are null!");
                    System.exit(0);
                }
            }
            else{
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported Type of Parent Operator for SelectOperator...");
                System.exit(0);
            }
        }
        else if(currentOperatorNode.getOperator() instanceof GroupByOperator){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Operator is a GroupByOperator...");
            if(fatherOperatorNode.getOperator() instanceof SelectOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is SelectOperator: "+fatherOperatorNode.getOperatorName());
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Adding new possible Aliases...");
                String updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);

                GroupByOperator groupByParent = (GroupByOperator) currentOperatorNode.getOperator();
                GroupByDesc groupByDesc = groupByParent.getConf();

                if(groupByParent == null){
                    System.out.println("GroupByDesc is null!");
                    System.exit(0);
                }

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Discovering Group By Keys...");
                List<String> groupByKeys = new LinkedList<>();
                MyMap changeMap = new MyMap();
                if(groupByDesc != null){
                    ArrayList<ExprNodeDesc> keys = groupByDesc.getKeys();
                    if (keys != null) {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Keys: ");
                        for (ExprNodeDesc k : keys) {
                            if (k != null) {
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"\tKey: ");
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"\t\tName: " + k.getName());
                                if (k.getCols() != null) {
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"\t\tCols: " + k.getCols().toString());
                                    if(k.getCols().size() > 1){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Key for more than one column?! GROUP BY");
                                        System.exit(9);
                                    }
                                    else if(k.getCols().size() == 1){
                                        String col = k.getCols().get(0);
                                        //if(col.contains("KEY.")){
                                        //    col = col.replace("KEY.", "");
                                        //}
                                        //else if(col.contains("VALUE.")){
                                        //   col = col.replace("VALUE.", "");
                                        //}


                                        boolean fg = false;
                                        if(groupByKeys.size() == 0) groupByKeys.add(col);
                                        else {
                                            for (String g : groupByKeys) {
                                                if (g.equals(col)) {
                                                    fg = true;
                                                    break;
                                                }
                                            }
                                            if (fg == false)
                                                groupByKeys.add(col);
                                        }
                                    }
                                } else {
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"\t\tCols: NULL");
                                }
                            }
                        }

                        List<String> realColumnAliases = new LinkedList<>();
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finding real column Aliases for GroupBy Keys...");
                        for(String s : groupByKeys){
                            boolean matchFound = false;
                            List<ColumnTypePair> pairs = columnAndTypeMap.getColumnAndTypeList();
                            for(ColumnTypePair c : pairs){
                                if(c.getColumnName().equals(s)){
                                    realColumnAliases.add(c.getColumnName());
                                    matchFound = true;
                                    break;
                                }
                                else{
                                    List<StringParameter> altAliases = c.getAltAliasPairs();
                                    if(altAliases != null){
                                        if(altAliases.size() > 0){
                                            for(StringParameter sP : altAliases){
                                                if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                                    if((sP.getValue().equals(s)) || ("KEY.".concat(sP.getValue()).equals(s))){
                                                        realColumnAliases.add(c.getColumnName());
                                                        matchFound = true;
                                                        break;
                                                    }
                                                }
                                            }

                                            if(matchFound == true){
                                                break;
                                            }
                                        }
                                    }
                                }
                            }

                            if(matchFound == false){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Match does not exist! For Group By Key: "+s);
                            }
                        }

                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real GroupBy Keys: "+realColumnAliases.toString());

                        String expression = "";
                        int i = 0;
                        for(i=0; i < realColumnAliases.size(); i++){
                            if(i == realColumnAliases.size() - 1){
                                expression = expression.concat(" "+realColumnAliases.get(i));
                            }
                            else{
                                expression = expression.concat(" "+realColumnAliases.get(i)+",");
                            }
                        }

                        /*---Locate new USED Columns----*/
                        for(String s : realColumnAliases){
                            currentOpQuery.addUsedColumn(s, latestAncestorTableName1);
                        }

                        currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString() + " group by "+expression);
                        currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString() + " group by "+expression);

                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");
                    }
                }

                List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
                if(children != null){
                    if(children.size() == 1){
                        Operator<?> child = children.get(0);
                        if(child instanceof ReduceSinkOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered GBY--->RS connection!");
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                            /*---Moving to child---*/
                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);
                        }
                        else if(child instanceof FileSinkOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered GBY--->FS connection!");
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                            /*---Moving to child---*/
                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported child of GroupByOperator!");
                            System.exit(0);
                        }
                    }
                    else{
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": children are not size=1");
                        System.exit(0);
                    }
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": children are null");
                    System.exit(0);
                }
            }
            else if(fatherOperatorNode.getOperator() instanceof ReduceSinkOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is ReduceSinkOperator: "+fatherOperatorNode.getOperatorName());
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Adding new possible Aliases...");
                String updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);

                GroupByOperator groupByParent = (GroupByOperator) currentOperatorNode.getOperator();
                GroupByDesc groupByDesc = groupByParent.getConf();

                if(groupByParent == null){
                    System.out.println("GroupByDesc is null!");
                    System.exit(0);
                }

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Discovering Group By Keys...");
                List<String> groupByKeys = new LinkedList<>();
                MyMap changeMap = new MyMap();
                if(groupByDesc != null){
                    ArrayList<ExprNodeDesc> keys = groupByDesc.getKeys();
                    if (keys != null) {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Keys: ");
                        for (ExprNodeDesc k : keys) {
                            if (k != null) {
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"\tKey: ");
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"\t\tName: " + k.getName());
                                if (k.getCols() != null) {
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"\t\tCols: " + k.getCols().toString());
                                    if(k.getCols().size() > 1){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Key for more than one column?! GROUP BY");
                                        System.exit(9);
                                    }
                                    else if(k.getCols().size() == 1){
                                        String col = k.getCols().get(0);
                                        //if(col.contains("KEY.")){
                                        //    col = col.replace("KEY.", "");
                                        //}
                                        //else if(col.contains("VALUE.")){
                                        //    col = col.replace("VALUE.", "");
                                        //}


                                        boolean fg = false;
                                        if(groupByKeys.size() == 0) groupByKeys.add(col);
                                        else {
                                            for (String g : groupByKeys) {
                                                if (g.equals(col)) {
                                                    fg = true;
                                                    break;
                                                }
                                            }
                                            if (fg == false)
                                                groupByKeys.add(col);
                                        }
                                    }
                                } else {
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"\t\tCols: NULL");
                                }
                            }
                        }

                        List<String> realColumnAliases = new LinkedList<>();
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finding real column Aliases for GroupBy Keys...");
                        for(String s : groupByKeys){
                            boolean matchFound = false;
                            List<ColumnTypePair> pairs = columnAndTypeMap.getColumnAndTypeList();
                            for(ColumnTypePair c : pairs){
                                if(c.getColumnName().equals(s)){
                                    realColumnAliases.add(c.getColumnName());
                                    matchFound = true;
                                    break;
                                }
                                else{
                                    List<StringParameter> altAliases = c.getAltAliasPairs();
                                    if(altAliases != null){
                                        if(altAliases.size() > 0){
                                            for(StringParameter sP : altAliases){
                                                if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                                    if((sP.getValue().equals(s)) || ("KEY.".concat(sP.getValue()).equals(s))){
                                                        realColumnAliases.add(c.getColumnName());
                                                        matchFound = true;
                                                        break;
                                                    }
                                                }
                                            }

                                            if(matchFound == true){
                                                break;
                                            }
                                        }
                                    }
                                }
                            }

                            if(matchFound == false){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Match does not exist! For Group By Key: "+s);
                            }
                        }

                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real GroupBy Keys: "+realColumnAliases.toString());

                        String expression = "";
                        int i = 0;
                        for(i=0; i < realColumnAliases.size(); i++){
                            if(i == realColumnAliases.size() - 1){
                                expression = expression.concat(" "+realColumnAliases.get(i));
                            }
                            else{
                                expression = expression.concat(" "+realColumnAliases.get(i)+",");
                            }
                        }

                        /*---Locate new USED Columns----*/
                        for(String s : realColumnAliases){
                            currentOpQuery.addUsedColumn(s, latestAncestorTableName1);
                        }

                        currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString() + " group by "+expression);
                        currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString() + " group by "+expression);

                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");
                    }
                }

                List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
                if(children != null){
                    if(children.size() == 1){
                        Operator<?> child = children.get(0);
                        if(child instanceof SelectOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered GBY--->SEL connection!");
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                            /*---Moving to child---*/
                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);
                        }
                        else if(child instanceof LimitOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered GBY--->LIM connection!");
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                            /*---Moving to child---*/
                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported type of Child for RS-->GBY-->? Connection!");
                            System.exit(0);
                        }
                    }
                    else{
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Children != 1!: ");
                        System.exit(0);
                    }
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Children == NULL!: ");
                    System.exit(0);
                }
            }
            else if(fatherOperatorNode.getOperator() instanceof FilterOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is FilterOperator: "+fatherOperatorNode.getOperatorName());
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Adding new possible Aliases...");
                String updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);

                GroupByOperator groupByParent = (GroupByOperator) currentOperatorNode.getOperator();
                GroupByDesc groupByDesc = groupByParent.getConf();

                if(groupByParent == null){
                    System.out.println("GroupByDesc is null!");
                    System.exit(0);
                }

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Discovering Group By Keys...");
                List<String> groupByKeys = new LinkedList<>();
                MyMap changeMap = new MyMap();
                if(groupByDesc != null){
                    ArrayList<ExprNodeDesc> keys = groupByDesc.getKeys();
                    if (keys != null) {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Keys: ");
                        for (ExprNodeDesc k : keys) {
                            if (k != null) {
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"\tKey: ");
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"\t\tName: " + k.getName());
                                if (k.getCols() != null) {
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"\t\tCols: " + k.getCols().toString());
                                    if(k.getCols().size() > 1){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Key for more than one column?! GROUP BY");
                                        System.exit(9);
                                    }
                                    else if(k.getCols().size() == 1){
                                        String col = k.getCols().get(0);
                                        //if(col.contains("KEY.")){
                                        //    col = col.replace("KEY.", "");
                                        //}
                                        //else if(col.contains("VALUE.")){
                                        //    col = col.replace("VALUE.", "");
                                        //}


                                        boolean fg = false;
                                        if(groupByKeys.size() == 0) groupByKeys.add(col);
                                        else {
                                            for (String g : groupByKeys) {
                                                if (g.equals(col)) {
                                                    fg = true;
                                                    break;
                                                }
                                            }
                                            if (fg == false)
                                                groupByKeys.add(col);
                                        }
                                    }
                                } else {
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"\t\tCols: NULL");
                                }
                            }
                        }

                        List<String> realColumnAliases = new LinkedList<>();
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finding real column Aliases for GroupBy Keys...");
                        for(String s : groupByKeys){
                            boolean matchFound = false;
                            List<ColumnTypePair> pairs = columnAndTypeMap.getColumnAndTypeList();
                            for(ColumnTypePair c : pairs){
                                if(c.getColumnName().equals(s)){
                                    realColumnAliases.add(c.getColumnName());
                                    matchFound = true;
                                    break;
                                }
                                else{
                                    List<StringParameter> altAliases = c.getAltAliasPairs();
                                    if(altAliases != null){
                                        if(altAliases.size() > 0){
                                            for(StringParameter sP : altAliases){
                                                if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                                    if((sP.getValue().equals(s)) || ("KEY.".concat(sP.getValue()).equals(s))){
                                                        realColumnAliases.add(c.getColumnName());
                                                        matchFound = true;
                                                        break;
                                                    }
                                                }
                                            }

                                            if(matchFound == true){
                                                break;
                                            }
                                        }
                                    }
                                }
                            }

                            if(matchFound == false){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Match does not exist! For Group By Key: "+s);
                            }
                        }

                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real GroupBy Keys: "+realColumnAliases.toString());

                        String expression = "";
                        int i = 0;
                        for(i=0; i < realColumnAliases.size(); i++){
                            if(i == realColumnAliases.size() - 1){
                                expression = expression.concat(" "+realColumnAliases.get(i));
                            }
                            else{
                                expression = expression.concat(" "+realColumnAliases.get(i)+",");
                            }
                        }

                        /*---Locate new USED Columns----*/
                        for(String s : realColumnAliases){
                            currentOpQuery.addUsedColumn(s, latestAncestorTableName1);
                        }

                        currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString() + " group by "+expression);
                        currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString() + " group by "+expression);

                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");
                    }
                }

                //Now check for aggregate functions ie HAVING
                /*ArrayList<AggregationDesc> aggregationDescs = groupByDesc.getAggregators();
                boolean partialAggregation = false;
                if(aggregationDescs != null){
                    if(aggregationDescs.size() > 0){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Aggregations exist!");
                        if(aggregationDescs.size() > 1){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": More than 1 Aggregation! Unsupported for now!");
                            System.exit(0);
                        }

                        AggregationDesc aggDesc = aggregationDescs.get(0);
                        if(aggDesc.getGenericUDAFName().equals("count")){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Count Aggregation Exists!");
                            if(aggDesc.getParameters().size() > 1){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": More than 1 Count Parameters! Unsupported for Now!");
                                System.exit(0);
                            }

                            //Get Col Parameter
                            String colName = aggDesc.getParameters().get(0).getCols().get(0);
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Count Parameter: "+colName);

                            //Form having expression
                            currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString().concat(" having count("+colName+") "));
                            currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString().concat(" having count("+colName+") "));

                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query currently is: "+currentOpQuery.getLocalQueryString());

                            aggDesc.getParameters().get(0).getTypeInfo(
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Aggregation : "+currentOpQuery.getLocalQueryString());
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Check if current Schema contains bigint...and replace with count expression...");
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported type of Aggregation!");
                            System.exit(0);
                        }
                    }
                }*/

                List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
                if(children != null){
                    if(children.size() == 1){
                        Operator<?> child = children.get(0);
                        if(child instanceof SelectOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered GBY--->SEL connection!");
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                            /*---Moving to child---*/
                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);
                        }
                        else if(child instanceof LimitOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered GBY--->LIM connection!");
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                            /*---Moving to child---*/
                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported type of Child for FIL-->GBY-->? Connection!");
                            System.exit(0);
                        }
                    }
                    else{
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Children != 1!: ");
                        System.exit(0);
                    }
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Children == NULL!: ");
                    System.exit(0);
                }
            }
            else if(fatherOperatorNode.getOperator() instanceof JoinOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is JoinOperator: "+fatherOperatorNode.getOperatorName());
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Adding new possible Aliases...");
                String updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);

                GroupByOperator groupByParent = (GroupByOperator) currentOperatorNode.getOperator();
                GroupByDesc groupByDesc = groupByParent.getConf();

                if(groupByParent == null){
                    System.out.println("GroupByDesc is null!");
                    System.exit(0);
                }

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Discovering Group By Keys...");
                List<String> groupByKeys = new LinkedList<>();
                MyMap changeMap = new MyMap();
                if(groupByDesc != null){
                    ArrayList<ExprNodeDesc> keys = groupByDesc.getKeys();
                    if (keys != null) {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Keys: ");
                        for (ExprNodeDesc k : keys) {
                            if (k != null) {
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"\tKey: ");
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"\t\tName: " + k.getName());
                                if (k.getCols() != null) {
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"\t\tCols: " + k.getCols().toString());
                                    if(k.getCols().size() > 1){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Key for more than one column?! GROUP BY");
                                        System.exit(9);
                                    }
                                    else if(k.getCols().size() == 1){
                                        String col = k.getCols().get(0);
                                        //if(col.contains("KEY.")){
                                        //    col = col.replace("KEY.", "");
                                        //}
                                        //else if(col.contains("VALUE.")){
                                        //    col = col.replace("VALUE.", "");
                                        //}


                                        boolean fg = false;
                                        if(groupByKeys.size() == 0) groupByKeys.add(col);
                                        else {
                                            for (String g : groupByKeys) {
                                                if (g.equals(col)) {
                                                    fg = true;
                                                    break;
                                                }
                                            }
                                            if (fg == false)
                                                groupByKeys.add(col);
                                        }
                                    }
                                } else {
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"\t\tCols: NULL");
                                }
                            }
                        }

                        List<String> realColumnAliases = new LinkedList<>();
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finding real column Aliases for GroupBy Keys...");
                        for(String s : groupByKeys){
                            boolean matchFound = false;
                            List<ColumnTypePair> pairs = columnAndTypeMap.getColumnAndTypeList();
                            for(ColumnTypePair c : pairs){
                                if(c.getColumnName().equals(s)){
                                    realColumnAliases.add(c.getColumnName());
                                    matchFound = true;
                                    break;
                                }
                                else{
                                    List<StringParameter> altAliases = c.getAltAliasPairs();
                                    if(altAliases != null){
                                        if(altAliases.size() > 0){
                                            for(StringParameter sP : altAliases){
                                                if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                                    if((sP.getValue().equals(s)) || ("KEY.".concat(sP.getValue()).equals(s))){
                                                        realColumnAliases.add(c.getColumnName());
                                                        matchFound = true;
                                                        break;
                                                    }
                                                }
                                            }

                                            if(matchFound == true){
                                                break;
                                            }
                                        }
                                    }
                                }
                            }

                            if(matchFound == false){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Match does not exist! For Group By Key: "+s);
                            }
                        }

                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real GroupBy Keys: "+realColumnAliases.toString());

                        String expression = "";
                        int i = 0;
                        for(i=0; i < realColumnAliases.size(); i++){
                            if(i == realColumnAliases.size() - 1){
                                expression = expression.concat(" "+realColumnAliases.get(i));
                            }
                            else{
                                expression = expression.concat(" "+realColumnAliases.get(i)+",");
                            }
                        }

                        /*---Locate new USED Columns----*/
                        for(String s : realColumnAliases){
                            currentOpQuery.addUsedColumn(s, latestAncestorTableName1);
                        }

                        currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString() + " group by "+expression);
                        currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString() + " group by "+expression);

                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");
                    }
                }

                //Now check for aggregate functions ie HAVING
                /*ArrayList<AggregationDesc> aggregationDescs = groupByDesc.getAggregators();
                boolean partialAggregation = false;
                if(aggregationDescs != null){
                    if(aggregationDescs.size() > 0){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Aggregations exist!");
                        if(aggregationDescs.size() > 1){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": More than 1 Aggregation! Unsupported for now!");
                            System.exit(0);
                        }

                        AggregationDesc aggDesc = aggregationDescs.get(0);
                        if(aggDesc.getGenericUDAFName().equals("count")){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Count Aggregation Exists!");
                            if(aggDesc.getParameters().size() > 1){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": More than 1 Count Parameters! Unsupported for Now!");
                                System.exit(0);
                            }

                            //Get Col Parameter
                            String colName = aggDesc.getParameters().get(0).getCols().get(0);
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Count Parameter: "+colName);

                            //Form having expression
                            currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString().concat(" having count("+colName+") "));
                            currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString().concat(" having count("+colName+") "));

                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query currently is: "+currentOpQuery.getLocalQueryString());

                            aggDesc.getParameters().get(0).getTypeInfo(
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Aggregation : "+currentOpQuery.getLocalQueryString());
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Check if current Schema contains bigint...and replace with count expression...");
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported type of Aggregation!");
                            System.exit(0);
                        }
                    }
                }*/

                List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
                if(children != null){
                    if(children.size() == 1){
                        Operator<?> child = children.get(0);
                        if(child instanceof SelectOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered GBY--->SEL connection!");
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                            /*---Moving to child---*/
                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);
                        }
                        else if(child instanceof LimitOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered GBY--->LIM connection!");
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                            /*---Moving to child---*/
                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);
                        }
                        else if(child instanceof FileSinkOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered GBY--->FS connection!");
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                            /*---Moving to child---*/
                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported type of Child for JOIN-->GBY-->? Connection!");
                            System.exit(0);
                        }
                    }
                    else{
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Children != 1!: ");
                        System.exit(0);
                    }
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Children == NULL!: ");
                    System.exit(0);
                }
            }
            else{
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported Type of Parent Operator for GroupByOperator...");
                System.exit(0);
            }
        }
        else if(currentOperatorNode.getOperator() instanceof ReduceSinkOperator){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Operator is a ReduceSinkOperator...");
            if(fatherOperatorNode.getOperator() instanceof GroupByOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is GroupByOperator: "+fatherOperatorNode.getOperatorName());
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Adding new possible Aliases...");
                String updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
                if(currentOpQuery.getLocalQueryString().contains("select ") == false){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query does not contain SELECT statement (needs to be added)...");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Constructing SELECT statement columns...");
                    MyMap aMap = new MyMap();
                    updatedSchemaString = extractColsFromTypeName(latestAncestorSchema, aMap, latestAncestorSchema, false);

                    String selectString = "";
                    List<ColumnTypePair> pairs = aMap.getColumnAndTypeList();
                    for(int i = 0; i < pairs.size(); i++){
                        if(i == pairs.size() - 1){
                            selectString = selectString + " " + pairs.get(i).getColumnName()+ " ";
                        }
                        else{
                            selectString = selectString + " " + pairs.get(i).getColumnName() + ",";
                        }

                        //Attempt to add used column if not existing
                        currentOpQuery.addUsedColumn(pairs.get(i).getColumnName(), currentOpQuery.getInputTables().get(0).getTableName().toLowerCase());
                    }

                    currentOpQuery.setLocalQueryString("select"+selectString+currentOpQuery.getLocalQueryString());
                    currentOpQuery.setExaremeQueryString("select"+selectString+currentOpQuery.getExaremeQueryString());

                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

                }

                //Check for order by
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if ORDER BY exists in this RS...");
                ReduceSinkOperator reduceSinkOperator = (ReduceSinkOperator) currentOperatorNode.getOperator();
                ReduceSinkDesc reduceSinkDesc = reduceSinkOperator.getConf();

                if(reduceSinkDesc.getOrder() != null){
                    if((reduceSinkDesc.getOrder().contains("+")) || (reduceSinkDesc.getOrder().contains("-"))){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": + and - exist! Order by statement exists!");
                        List<String> orderByCols = new LinkedList<>();
                        if(reduceSinkDesc.getKeyCols() != null) {
                            ArrayList<ExprNodeDesc> exprCols = reduceSinkDesc.getKeyCols();
                            if(exprCols.size() > 0){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Key Cols will be used to build order by clause!");
                                if(exprCols.size() == reduceSinkDesc.getOrder().length()){

                                    String orderByString = " order by ";
                                    MyMap keysMap = new MyMap();
                                    String[] partsPairs = reduceSinkDesc.getKeyColString().split(",");
                                    if(partsPairs.length != exprCols.size()){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": PartsPairs length and exprCols size ARE NOT equal! Error! partsPair.length="+partsPairs.length);
                                        System.exit(0);
                                    }

                                    for(String partPair : partsPairs){ //Add all keys to a map
                                        String[] parts = partPair.split(" ");
                                        String currentColName = "";
                                        String type = "";
                                        if(parts.length == 3){
                                            currentColName = parts[0];
                                            type = parts[2].replace(")", "");
                                        }
                                        else if(parts.length == 4){
                                            currentColName = parts[1];
                                            type = parts[3].replace(")", "");
                                        }
                                        else{
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Parts.length must be 3! Error! parts.length="+parts.length);
                                            System.exit(0);
                                        }

                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Key: "+currentColName+" - Type: "+type);

                                        ColumnTypePair somePair = new ColumnTypePair(currentColName, type);

                                        keysMap.addPair(somePair);
                                    }

                                    //Use the map to locate the real aliases and form the Order by clause
                                    for(int i = 0; i < keysMap.getColumnAndTypeList().size(); i++){
                                        ColumnTypePair keyPair = keysMap.getColumnAndTypeList().get(i);

                                        String realAliasOfKey = "";
                                        boolean aliasFound = false;
                                        String fatherName = fatherOperatorNode.getOperatorName();
                                        String order = "";

                                        if(reduceSinkDesc.getOrder().toCharArray()[i] == '+'){
                                            order = "ASC";
                                        }
                                        else if(reduceSinkDesc.getOrder().toCharArray()[i] == '-'){
                                            order = "DESC";
                                        }
                                        else{
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unknown symbol in order string (+/-) allowed only");
                                            System.exit(0);
                                        }

                                        for(ColumnTypePair cP : columnAndTypeMap.getColumnAndTypeList()){
                                            if(cP.getColumnType().equals(keyPair.getColumnType())){
                                                if(cP.getColumnName().equals(keyPair.getColumnName())){
                                                    aliasFound = true;
                                                    realAliasOfKey = cP.getColumnName();
                                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+"is: "+realAliasOfKey);
                                                    break;
                                                }
                                                else{
                                                    if(cP.getAltAliasPairs() != null){
                                                        if(cP.getAltAliasPairs().size() > 0){
                                                            for(StringParameter sP : cP.getAltAliasPairs()){
                                                                if(sP.getParemeterType().equals(fatherName)){
                                                                    if(sP.getValue().equals(keyPair.getColumnName())){
                                                                        aliasFound = true;
                                                                        realAliasOfKey = cP.getColumnName();
                                                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+"is: "+realAliasOfKey);
                                                                        break;
                                                                    }
                                                                }
                                                            }

                                                            if(aliasFound == true){
                                                                break;
                                                            }

                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        if(aliasFound == false){
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+" was never found! Error!");
                                            System.exit(0);
                                        }

                                        if(i == keysMap.getColumnAndTypeList().size() - 1){
                                            orderByString = orderByString.concat(" "+realAliasOfKey+" "+order+" ");
                                        }
                                        else{
                                            orderByString = orderByString.concat(" "+realAliasOfKey+" "+order+", ");
                                        }

                                        //Add used column order by
                                        currentOpQuery.addUsedColumn(realAliasOfKey, latestAncestorTableName1);
                                    }

                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Formed ORDER BY string: "+orderByString);
                                    currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString()+" "+orderByString);
                                    currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString()+" "+orderByString);

                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");



                                }
                                else{
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Key Cols And Order(+/-) string do not have the same size!");
                                    System.exit(0);
                                }
                            }
                            else{
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": No Key Cols exist!");
                                System.exit(0);
                            }
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": No Key Cols exist!");
                            System.exit(0);
                        }
                    }
                }

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

                /*---Finalising outputTable---*/
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");
                MyTable outputTable = new MyTable();
                outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                outputTable.setIsAFile(false);

                MyMap someMap = new MyMap();

                if(currentOperatorNode.getOperator().getSchema().toString().equals("()")){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is empty! Assuming older Schema!");
                    updatedSchemaString = latestAncestorSchema;
                }

                updatedSchemaString = extractColsFromTypeName(updatedSchemaString, someMap, updatedSchemaString, false);

                List<FieldSchema> newCols = new LinkedList<>();
                List<ColumnTypePair> pairsList = someMap.getColumnAndTypeList();
                for(ColumnTypePair p : pairsList){
                    if(p.getColumnName().contains("BLOCK__OFFSET__INSIDE__FILE")){
                        continue;
                    }
                    else if(p.getColumnName().contains("INPUT__FILE__NAME")){
                        continue;
                    }
                    else if(p.getColumnName().contains("ROW__ID")){
                        continue;
                    }
                    else if(p.getColumnName().contains("rowid")){
                        continue;
                    }
                    else if(p.getColumnName().contains("bucketid")){
                        continue;
                    }

                    FieldSchema f = new FieldSchema();
                    f.setName(p.getColumnName());
                    f.setType(p.getColumnType());
                    newCols.add(f);
                }

                outputTable.setTableName(currentOperatorNode.getOperator().getOperatorId().toLowerCase());
                outputTable.setAllCols(newCols);
                outputTable.setHasPartitions(false);

                currentOpQuery.setOutputTable(outputTable);
                currentOpQuery.setExaremeOutputTableName("R_"+outputTable.getTableName().toLowerCase()+"_0");

                /*---Finalize local part of Query---*/
                currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

                /*---Check if OpLink is to be created---*/
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
                for(MyTable inputT : currentOpQuery.getInputTables()){
                    boolean isRoot = false;
                    for(MyTable rootInputT : inputTables){
                        if(rootInputT.getTableName().equals(inputT.getTableName())){
                            isRoot = true;
                            break;
                        }
                    }
                    if(isRoot == false){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Creating new OpLink...");
                        OpLink operatorLink = new OpLink();
                        operatorLink.setContainerName("c0");
                        operatorLink.setFromTable("R_"+inputT.getTableName().toLowerCase()+"_0");
                        operatorLink.setToTable("R_"+outputTable.getTableName().toLowerCase()+"_0");

                        StringParameter sP = new StringParameter("table", inputT.getTableName());
                        NumParameter nP = new NumParameter("part", 0);

                        List<Parameter> opParams = new LinkedList<>();
                        opParams.add(sP);
                        opParams.add(nP);

                        operatorLink.setParameters(opParams);
                        opLinksList.add(operatorLink);

                    }
                }

                /*----Adding Finished Query to List----*/
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
                allQueries.add(currentOpQuery);

                /*----Begin a new Query----*/
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
                OperatorQuery opQuery = new OperatorQuery();
                opQuery.setDataBasePath(currentDatabasePath);
                opQuery.setLocalQueryString("");
                opQuery.setExaremeQueryString("");
                opQuery.addInputTable(currentOpQuery.getOutputTable());
                opQuery.setAssignedContainer("c0");
                opQuery.setLocalQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());
                opQuery.setExaremeQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+opQuery.getLocalQueryString()+"]");

                /*---Moving to Child Operator---*/
                List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
                if(children != null){
                    if(children.size() == 1){
                        Operator<?> child = children.get(0);
                        if(child instanceof GroupByOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered RS--->GBY connection!");
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                            OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                            goToChildOperator(childNode, currentOperatorNode, opQuery, updatedSchemaString, currentOpQuery.getOutputTable().getTableName().toLowerCase(), latestAncestorTableName2, null);
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported child for ReduceSinkOperator!");
                            System.exit(0);
                        }
                    }
                    else{
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": children are NOT SIZE==1!");
                        System.exit(0);
                    }
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": children are null");
                    System.exit(0);
                }
            }
            else if(fatherOperatorNode.getOperator() instanceof FilterOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is FilterOperator: "+fatherOperatorNode.getOperatorName());

                String updatedSchemaString = "";

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if GrandFather is Root...");
                OperatorNode grandFatherOperatorNode = exaremeGraph.getOperatorNodeByName(fatherOperatorNode.getOperator().getParentOperators().get(0).getOperatorId());

                if((grandFatherOperatorNode.getOperator().getParentOperators() == null) || (grandFatherOperatorNode.getOperator().getParentOperators().size() == 0)){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": GrandFather is root...TableScan!");

                    if(currentOperatorNode.getOperator().getSchema().toString().equals("()") || (currentOperatorNode.getOperator().getSchema() == null)){
                        if(currentOperatorNode.getOperator().getColumnExprMap() != null){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": RowSchema is empty! We will build Schema from ColumnExprMap...");

                            MyMap someMap = new MyMap();

                            String pastSchema = extractColsFromTypeName(grandFatherOperatorNode.getOperator().getSchema().toString(), someMap, grandFatherOperatorNode.getOperator().getSchema().toString(), false);

                            updatedSchemaString = "(";
                            int i = 0;
                            for(Map.Entry<String, ExprNodeDesc> entry : currentOperatorNode.getOperator().getColumnExprMap().entrySet()){
                                String newColName = entry.getKey();
                                String pastName = entry.getValue().getCols().get(0);

                                boolean matchFound = false;
                                String nameForSchema = "";
                                String typeForSchema = "";

                                for(ColumnTypePair oldPair : someMap.getColumnAndTypeList()){ //Search all the parent operator Map
                                    if(oldPair.getColumnName().equals(pastName)){
                                        for(ColumnTypePair mapPair : columnAndTypeMap.getColumnAndTypeList()){ //Search all the total map
                                            if(mapPair.getColumnType().contains(oldPair.getColumnType())){
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": TypeOldPair: "+oldPair.getColumnType()+" - TypeNewPair: "+mapPair.getColumnType());
                                                if(mapPair.getColumnName().equals(oldPair.getColumnName())){
                                                    mapPair.addAltAlias(currentOperatorNode.getOperatorName(), newColName);
                                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": NewColName: "+newColName+" - was added in map as altAlias for: "+mapPair.getColumnName());
                                                    matchFound = true;
                                                    nameForSchema = mapPair.getColumnName();
                                                    typeForSchema = mapPair.getColumnType();
                                                    break;
                                                }
                                            }
                                        }

                                        if(matchFound == true) break;
                                    }

                                }

                                if(matchFound == false){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": NewColName: "+newColName+" has no match!");
                                    System.exit(0);
                                }

                                if(i == currentOperatorNode.getOperator().getColumnExprMap().entrySet().size() - 1){
                                    updatedSchemaString = updatedSchemaString.concat(nameForSchema + ": " + typeForSchema + ")");
                                }
                                else{
                                    updatedSchemaString = updatedSchemaString.concat(nameForSchema + ": " + typeForSchema + ",");
                                }

                                i++;
                            }

                        }
                        else{
                            updatedSchemaString = latestAncestorSchema;
                        }
                    }
                    else {
                        if(currentOperatorNode.getOperator().getSchema() == null){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is empty! Assuming older Schema!");
                            updatedSchemaString = latestAncestorSchema;
                        }
                        else {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...");
                            updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);
                        }
                    }
                }
                else{
                    if(currentOperatorNode.getOperator().getSchema() == null){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is empty! Assuming older Schema!");
                        updatedSchemaString = latestAncestorSchema;
                    }
                    else {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...");
                        updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);
                    }
                }

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
                if(currentOpQuery.getLocalQueryString().contains("select ") == false){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query does not contain SELECT statement (needs to be added)...");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Constructing SELECT statement columns...");
                    MyMap aMap = new MyMap();
                    updatedSchemaString = extractColsFromTypeName(latestAncestorSchema, aMap, latestAncestorSchema, false);

                    String selectString = "";
                    List<ColumnTypePair> pairs = aMap.getColumnAndTypeList();
                    for(int i = 0; i < pairs.size(); i++){
                        if(i == pairs.size() - 1){
                            selectString = selectString + " " + pairs.get(i).getColumnName()+ " ";
                        }
                        else{
                            selectString = selectString + " " + pairs.get(i).getColumnName() + ",";
                        }

                        //Attempt to add used column if not existing
                        currentOpQuery.addUsedColumn(pairs.get(i).getColumnName(), currentOpQuery.getInputTables().get(0).getTableName().toLowerCase());
                    }

                    currentOpQuery.setLocalQueryString("select"+selectString+currentOpQuery.getLocalQueryString());
                    currentOpQuery.setExaremeQueryString("select"+selectString+currentOpQuery.getExaremeQueryString());

                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

                }

                //Check for order by
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if ORDER BY exists in this RS...");
                ReduceSinkOperator reduceSinkOperator = (ReduceSinkOperator) currentOperatorNode.getOperator();
                ReduceSinkDesc reduceSinkDesc = reduceSinkOperator.getConf();

                if(reduceSinkDesc.getOrder() != null){
                    if((reduceSinkDesc.getOrder().contains("+")) || (reduceSinkDesc.getOrder().contains("-"))){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": + and - exist! Order by statement exists!");
                        List<String> orderByCols = new LinkedList<>();
                        if(reduceSinkDesc.getKeyCols() != null) {
                            ArrayList<ExprNodeDesc> exprCols = reduceSinkDesc.getKeyCols();
                            if(exprCols.size() > 0){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Key Cols will be used to build order by clause!");
                                if(exprCols.size() == reduceSinkDesc.getOrder().length()){

                                    String orderByString = " order by ";
                                    MyMap keysMap = new MyMap();
                                    String[] partsPairs = reduceSinkDesc.getKeyColString().split(",");
                                    if(partsPairs.length != exprCols.size()){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": PartsPairs length and exprCols size ARE NOT equal! Error! partsPair.length="+partsPairs.length);
                                        System.exit(0);
                                    }

                                    for(String partPair : partsPairs){ //Add all keys to a map
                                        String[] parts = partPair.split(" ");
                                        String currentColName = "";
                                        String type = "";
                                        if(parts.length == 3){
                                            currentColName = parts[0];
                                            type = parts[2].replace(")", "");
                                        }
                                        else if(parts.length == 4){
                                            currentColName = parts[1];
                                            type = parts[3].replace(")", "");
                                        }
                                        else{
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Parts.length must be 3! Error! parts.length="+parts.length);
                                            System.exit(0);
                                        }

                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Key: "+currentColName+" - Type: "+type);

                                        ColumnTypePair somePair = new ColumnTypePair(currentColName, type);

                                        keysMap.addPair(somePair);
                                    }

                                    //Use the map to locate the real aliases and form the Order by clause
                                    for(int i = 0; i < keysMap.getColumnAndTypeList().size(); i++){
                                        ColumnTypePair keyPair = keysMap.getColumnAndTypeList().get(i);

                                        String realAliasOfKey = "";
                                        boolean aliasFound = false;
                                        String fatherName = fatherOperatorNode.getOperatorName();
                                        String order = "";

                                        if(reduceSinkDesc.getOrder().toCharArray()[i] == '+'){
                                            order = "ASC";
                                        }
                                        else if(reduceSinkDesc.getOrder().toCharArray()[i] == '-'){
                                            order = "DESC";
                                        }
                                        else{
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unknown symbol in order string (+/-) allowed only");
                                            System.exit(0);
                                        }

                                        for(ColumnTypePair cP : columnAndTypeMap.getColumnAndTypeList()){
                                            if(cP.getColumnType().equals(keyPair.getColumnType())){
                                                if(cP.getColumnName().equals(keyPair.getColumnName())){
                                                    aliasFound = true;
                                                    realAliasOfKey = cP.getColumnName();
                                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+"is: "+realAliasOfKey);
                                                    break;
                                                }
                                                else{
                                                    if(cP.getAltAliasPairs() != null){
                                                        if(cP.getAltAliasPairs().size() > 0){
                                                            for(StringParameter sP : cP.getAltAliasPairs()){
                                                                if(sP.getParemeterType().equals(fatherName)){
                                                                    if(sP.getValue().equals(keyPair.getColumnName())){
                                                                        aliasFound = true;
                                                                        realAliasOfKey = cP.getColumnName();
                                                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+"is: "+realAliasOfKey);
                                                                        break;
                                                                    }
                                                                }
                                                            }

                                                            if(aliasFound == true){
                                                                break;
                                                            }

                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        if(aliasFound == false){
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+" was never found! Error!");
                                            System.exit(0);
                                        }

                                        if(i == keysMap.getColumnAndTypeList().size() - 1){
                                            orderByString = orderByString.concat(" "+realAliasOfKey+" "+order+" ");
                                        }
                                        else{
                                            orderByString = orderByString.concat(" "+realAliasOfKey+" "+order+", ");
                                        }

                                        //Add used column order by
                                        currentOpQuery.addUsedColumn(realAliasOfKey, latestAncestorTableName1);

                                    }

                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Formed ORDER BY string: "+orderByString);
                                    currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString()+" "+orderByString);
                                    currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString()+" "+orderByString);

                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");



                                }
                                else{
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Key Cols And Order(+/-) string do not have the same size!");
                                    System.exit(0);
                                }
                            }
                            else{
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": No Key Cols exist!");
                                System.exit(0);
                            }
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": No Key Cols exist!");
                            System.exit(0);
                        }
                    }
                }

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

                /*---Finalising outputTable---*/
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");
                MyTable outputTable = new MyTable();
                outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                outputTable.setIsAFile(false);

                MyMap someMap = new MyMap();

                if(currentOperatorNode.getOperator().getSchema().toString().equals("()")){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is empty! Assuming older Schema!");
                    updatedSchemaString = latestAncestorSchema;
                }

                updatedSchemaString = extractColsFromTypeName(updatedSchemaString, someMap, updatedSchemaString, false);

                List<FieldSchema> newCols = new LinkedList<>();
                List<ColumnTypePair> pairsList = someMap.getColumnAndTypeList();
                for(ColumnTypePair p : pairsList){
                    if(p.getColumnName().contains("BLOCK__OFFSET__INSIDE__FILE")){
                        continue;
                    }
                    else if(p.getColumnName().contains("INPUT__FILE__NAME")){
                        continue;
                    }
                    else if(p.getColumnName().contains("ROW__ID")){
                        continue;
                    }
                    else if(p.getColumnName().contains("rowid")){
                        continue;
                    }
                    else if(p.getColumnName().contains("bucketid")){
                        continue;
                    }
                    FieldSchema f = new FieldSchema();
                    f.setName(p.getColumnName());
                    f.setType(p.getColumnType());
                    newCols.add(f);
                }

                outputTable.setTableName(currentOperatorNode.getOperator().getOperatorId().toLowerCase());
                outputTable.setAllCols(newCols);
                outputTable.setHasPartitions(false);

                currentOpQuery.setOutputTable(outputTable);
                currentOpQuery.setExaremeOutputTableName("R_"+outputTable.getTableName().toLowerCase()+"_0");

                /*---Finalize local part of Query---*/
                currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

                /*---Check if OpLink is to be created---*/
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
                for(MyTable inputT : currentOpQuery.getInputTables()){
                    boolean isRoot = false;
                    for(MyTable rootInputT : inputTables){
                        if(rootInputT.getTableName().equals(inputT.getTableName())){
                            isRoot = true;
                            break;
                        }
                    }
                    if(isRoot == false){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Creating new OpLink...");
                        OpLink operatorLink = new OpLink();
                        operatorLink.setContainerName("c0");
                        operatorLink.setFromTable("R_"+inputT.getTableName().toLowerCase()+"_0");
                        operatorLink.setToTable("R_"+outputTable.getTableName().toLowerCase()+"_0");

                        StringParameter sP = new StringParameter("table", inputT.getTableName());
                        NumParameter nP = new NumParameter("part", 0);

                        List<Parameter> opParams = new LinkedList<>();
                        opParams.add(sP);
                        opParams.add(nP);

                        operatorLink.setParameters(opParams);
                        opLinksList.add(operatorLink);
                    }
                }

                /*----Adding Finished Query to List----*/
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
                allQueries.add(currentOpQuery);


                //TODO JOIN
                /*---Moving to Child Operator---*/
                List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
                if(children != null){
                    if(children.size() == 1){
                        Operator<?> child = children.get(0);
                        if(child instanceof JoinOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered RS--->JOIN connection!");
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if JOIN_POINT: "+child.getOperatorId()+" exists...");

                            if(joinPointList.size() > 0){
                                boolean joinPExists = false;
                                for(JoinPoint jP : joinPointList){
                                    if(jP.getId().equals(child.getOperatorId())){
                                        joinPExists = true;

                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" Exists! Removing it from List!");

                                        OperatorQuery associatedQuery = jP.getAssociatedOpQuery();
                                        String otherParent = jP.getCreatedById();
                                        latestAncestorTableName2 = jP.getLatestAncestorTableName();
                                        OperatorNode secondFather = jP.getOtherFatherNode();
                                        String joinPhrase = jP.getJoinPhrase();

                                        joinPointList.remove(jP);

                                        associatedQuery.addInputTable(currentOpQuery.getOutputTable());

                                        associatedQuery.setLocalQueryString(associatedQuery.getLocalQueryString().concat(" "+joinPhrase+" "+currentOpQuery.getOutputTable().getTableName().toLowerCase()+" on "));
                                        associatedQuery.setExaremeQueryString(associatedQuery.getExaremeQueryString().concat(" "+joinPhrase+" "+currentOpQuery.getOutputTable().getTableName().toLowerCase()+" on "));

                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                                        OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                                        goToChildOperator(childNode, currentOperatorNode, associatedQuery, updatedSchemaString, currentOpQuery.getOutputTable().getTableName().toLowerCase(), latestAncestorTableName2, secondFather);

                                        return;
                                    }
                                }

                                if(joinPExists == false){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                                    /*----Begin a new Query----*/
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
                                    OperatorQuery opQuery = new OperatorQuery();
                                    opQuery.setDataBasePath(currentDatabasePath);
                                    opQuery.setLocalQueryString("");
                                    opQuery.setExaremeQueryString("");
                                    opQuery.addInputTable(currentOpQuery.getOutputTable());
                                    opQuery.setAssignedContainer("c0");
                                    opQuery.setLocalQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());
                                    opQuery.setExaremeQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());

                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+opQuery.getLocalQueryString()+"]");

                                    JoinOperator joinOp = (JoinOperator) child;
                                    JoinDesc joinDesc = joinOp.getConf();

                                    JoinCondDesc[] joinCondDescs = joinDesc.getConds();
                                    if(joinCondDescs.length > 1){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": More than 1 JoinCondDesc! Error!");
                                        System.exit(0);
                                    }

                                    String joinPhrase = "";
                                    if(joinCondDescs[0].getType() == 0){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Type=0 - JoinString: "+joinCondDescs[0].getJoinCondString());
                                        joinPhrase = " inner join ";
                                    }
                                    else if(joinCondDescs[0].getType() == 1){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Type=1 - JoinString: "+joinCondDescs[0].getJoinCondString());
                                        joinPhrase = " left join ";
                                    }
                                    else if(joinCondDescs[0].getType() == 2){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Type=2 - JoinString: "+joinCondDescs[0].getJoinCondString());
                                        joinPhrase = " right join ";
                                    }
                                    else if(joinCondDescs[0].getType() == 3){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Type=3 - JoinString: "+joinCondDescs[0].getJoinCondString());
                                        joinPhrase = " full outer join ";
                                    }
                                    else{
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": UNSUPPORTED JOIN TYPE! JoinString: "+joinCondDescs[0].getJoinCondString());
                                        System.exit(0);
                                        //joinPhrase = " full outer join ";
                                    }

                                    JoinPoint joinP = new JoinPoint(child.getOperatorId(), currentOperatorNode.getOperatorName(), opQuery, latestAncestorTableName1, currentOperatorNode, joinPhrase);

                                    joinPointList.add(joinP);

                                    return;
                                }
                            }
                            else{ //No JoinPoint Exists
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                                /*----Begin a new Query----*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
                                OperatorQuery opQuery = new OperatorQuery();
                                opQuery.setDataBasePath(currentDatabasePath);
                                opQuery.setLocalQueryString("");
                                opQuery.setExaremeQueryString("");
                                opQuery.addInputTable(currentOpQuery.getOutputTable());
                                opQuery.setAssignedContainer("c0");
                                opQuery.setLocalQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());
                                opQuery.setExaremeQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());

                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");
                                JoinOperator joinOp = (JoinOperator) child;
                                JoinDesc joinDesc = joinOp.getConf();

                                JoinCondDesc[] joinCondDescs = joinDesc.getConds();
                                if(joinCondDescs.length > 1){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": More than 1 JoinCondDesc! Error!");
                                    System.exit(0);
                                }

                                String joinPhrase = "";
                                if(joinCondDescs[0].getType() == 0){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Type=0 - JoinString: "+joinCondDescs[0].getJoinCondString());
                                    joinPhrase = " inner join ";
                                }
                                else if(joinCondDescs[0].getType() == 1){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Type=1 - JoinString: "+joinCondDescs[0].getJoinCondString());
                                    joinPhrase = " left join ";
                                }
                                else if(joinCondDescs[0].getType() == 2){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Type=2 - JoinString: "+joinCondDescs[0].getJoinCondString());
                                    joinPhrase = " right join ";
                                }
                                else if(joinCondDescs[0].getType() == 3){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Type=3 - JoinString: "+joinCondDescs[0].getJoinCondString());
                                    joinPhrase = " full outer join ";
                                }
                                else{
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": UNSUPPORTED JOIN TYPE! JoinString: "+joinCondDescs[0].getJoinCondString());
                                    System.exit(0);
                                    //joinPhrase = " full outer join ";
                                }

                                JoinPoint joinP = new JoinPoint(child.getOperatorId(), currentOperatorNode.getOperatorName(), opQuery, latestAncestorTableName1, currentOperatorNode, joinPhrase);

                                joinPointList.add(joinP);

                                return;
                            }
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported child for ReduceSinkOperator!");
                            System.exit(0);
                        }
                    }
                    else{
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": children are NOT SIZE==1!");
                        System.exit(0);
                    }
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": children are null");
                    System.exit(0);
                }

            }
            else if(fatherOperatorNode.getOperator() instanceof FileSinkOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a FileSinkOperator: "+fatherOperatorNode.getOperatorName());

                String updatedSchemaString = "";

                if(currentOperatorNode.getOperator().getParentOperators().get(0) instanceof TableScanOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": True father was a TableScanOperator and was ommited! Use ancestral schema to add Columns and transform current Schema!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Ancestral Schema: "+latestAncestorSchema);

                    MyMap ancestorMap = new MyMap();
                    latestAncestorSchema = extractColsFromTypeName(latestAncestorSchema, ancestorMap, latestAncestorSchema, false);

                    MyMap descendentMap = new MyMap();
                    String descendentSchema = extractColsFromTypeName(currentOperatorNode.getOperator().getSchema().toString(), descendentMap, currentOperatorNode.getOperator().getSchema().toString(), true);

                    if(ancestorMap.getColumnAndTypeList().size() != descendentMap.getColumnAndTypeList().size()){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Maps do not have equal size!");
                        System.exit(0);
                    }

                    for(int i = 0; i < ancestorMap.getColumnAndTypeList().size(); i++){
                        ColumnTypePair ancestorPair = ancestorMap.getColumnAndTypeList().get(i);
                        ColumnTypePair descendentPair = descendentMap.getColumnAndTypeList().get(i);

                        if(ancestorPair.getColumnType().equals(descendentPair.getColumnType())){
                            for(ColumnTypePair p : columnAndTypeMap.getColumnAndTypeList()){
                                if(p.getColumnName().equals(ancestorPair.getColumnName())){
                                    p.addAltAlias(currentOperatorNode.getOperatorName(), descendentPair.getColumnName());
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Match found! - Pair: "+ancestorPair.getColumnName()+" - "+descendentPair.getColumnName());
                                }
                            }
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Possible match Pair does not have same Type - Pair: "+ancestorPair.getColumnName()+" - "+descendentPair.getColumnName());
                            System.exit(0);
                        }
                    }

                    updatedSchemaString = latestAncestorSchema;
                }
                else {

                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...");
                    updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);

                }

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
                if(currentOpQuery.getLocalQueryString().contains("select ") == false){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query does not contain SELECT statement (needs to be added)...");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Constructing SELECT statement columns...");
                    MyMap aMap = new MyMap();
                    updatedSchemaString = extractColsFromTypeName(latestAncestorSchema, aMap, latestAncestorSchema, false);

                    String selectString = "";
                    List<ColumnTypePair> pairs = aMap.getColumnAndTypeList();
                    for(int i = 0; i < pairs.size(); i++){
                        if(i == pairs.size() - 1){
                            selectString = selectString + " " + pairs.get(i).getColumnName()+ " ";
                        }
                        else{
                            selectString = selectString + " " + pairs.get(i).getColumnName() + ",";
                        }

                        //Attempt to add used column if not existing
                        currentOpQuery.addUsedColumn(pairs.get(i).getColumnName(), currentOpQuery.getInputTables().get(0).getTableName().toLowerCase());
                    }

                    currentOpQuery.setLocalQueryString("select"+selectString+currentOpQuery.getLocalQueryString());
                    currentOpQuery.setExaremeQueryString("select"+selectString+currentOpQuery.getExaremeQueryString());

                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

                }

                //Check for order by
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if ORDER BY exists in this RS...");
                ReduceSinkOperator reduceSinkOperator = (ReduceSinkOperator) currentOperatorNode.getOperator();
                ReduceSinkDesc reduceSinkDesc = reduceSinkOperator.getConf();

                if(reduceSinkDesc.getOrder() != null){
                    if((reduceSinkDesc.getOrder().contains("+")) || (reduceSinkDesc.getOrder().contains("-"))){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": + and - exist! Order by statement exists!");
                        List<String> orderByCols = new LinkedList<>();
                        if(reduceSinkDesc.getKeyCols() != null) {
                            ArrayList<ExprNodeDesc> exprCols = reduceSinkDesc.getKeyCols();
                            if(exprCols.size() > 0){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Key Cols will be used to build order by clause!");
                                if(exprCols.size() == reduceSinkDesc.getOrder().length()){

                                    String orderByString = " order by ";
                                    MyMap keysMap = new MyMap();
                                    String[] partsPairs = reduceSinkDesc.getKeyColString().split(",");
                                    if(partsPairs.length != exprCols.size()){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": PartsPairs length and exprCols size ARE NOT equal! Error! partsPair.length="+partsPairs.length);
                                        System.exit(0);
                                    }

                                    for(String partPair : partsPairs){ //Add all keys to a map
                                        String[] parts = partPair.split(" ");
                                        String currentColName = "";
                                        String type = "";
                                        if(parts.length == 3){
                                            currentColName = parts[0];
                                            type = parts[2].replace(")", "");
                                        }
                                        else if(parts.length == 4){
                                            currentColName = parts[1];
                                            type = parts[3].replace(")", "");
                                        }
                                        else{
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Parts.length must be 3! Error! parts.length="+parts.length);
                                            System.exit(0);
                                        }

                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Key: "+currentColName+" - Type: "+type);

                                        ColumnTypePair somePair = new ColumnTypePair(currentColName, type);

                                        keysMap.addPair(somePair);
                                    }

                                    //Use the map to locate the real aliases and form the Order by clause
                                    for(int i = 0; i < keysMap.getColumnAndTypeList().size(); i++){
                                        ColumnTypePair keyPair = keysMap.getColumnAndTypeList().get(i);

                                        String realAliasOfKey = "";
                                        boolean aliasFound = false;
                                        String fatherName = fatherOperatorNode.getOperatorName();
                                        String order = "";

                                        if(reduceSinkDesc.getOrder().toCharArray()[i] == '+'){
                                            order = "ASC";
                                        }
                                        else if(reduceSinkDesc.getOrder().toCharArray()[i] == '-'){
                                            order = "DESC";
                                        }
                                        else{
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unknown symbol in order string (+/-) allowed only");
                                            System.exit(0);
                                        }

                                        for(ColumnTypePair cP : columnAndTypeMap.getColumnAndTypeList()){
                                            if(cP.getColumnType().equals(keyPair.getColumnType())){
                                                if(cP.getColumnName().equals(keyPair.getColumnName())){
                                                    aliasFound = true;
                                                    realAliasOfKey = cP.getColumnName();
                                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+"is: "+realAliasOfKey);
                                                    break;
                                                }
                                                else{
                                                    if(cP.getAltAliasPairs() != null){
                                                        if(cP.getAltAliasPairs().size() > 0){
                                                            for(StringParameter sP : cP.getAltAliasPairs()){
                                                                if(sP.getParemeterType().equals(fatherName)){
                                                                    if(sP.getValue().equals(keyPair.getColumnName())){
                                                                        aliasFound = true;
                                                                        realAliasOfKey = cP.getColumnName();
                                                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+"is: "+realAliasOfKey);
                                                                        break;
                                                                    }
                                                                }
                                                            }

                                                            if(aliasFound == true){
                                                                break;
                                                            }

                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        if(aliasFound == false){
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+" was never found! Error!");
                                            System.exit(0);
                                        }

                                        if(i == keysMap.getColumnAndTypeList().size() - 1){
                                            orderByString = orderByString.concat(" "+realAliasOfKey+" "+order+" ");
                                        }
                                        else{
                                            orderByString = orderByString.concat(" "+realAliasOfKey+" "+order+", ");
                                        }

                                        //Add used column order by
                                        currentOpQuery.addUsedColumn(realAliasOfKey, latestAncestorTableName1);

                                    }

                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Formed ORDER BY string: "+orderByString);
                                    currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString()+" "+orderByString);
                                    currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString()+" "+orderByString);

                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");



                                }
                                else{
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Key Cols And Order(+/-) string do not have the same size!");
                                    System.exit(0);
                                }
                            }
                            else{
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": No Key Cols exist!");
                                System.exit(0);
                            }
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": No Key Cols exist!");
                            System.exit(0);
                        }
                    }
                }

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

                /*---Finalising outputTable---*/
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");
                MyTable outputTable = new MyTable();
                outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                outputTable.setIsAFile(false);

                MyMap someMap = new MyMap();

                if(currentOperatorNode.getOperator().getSchema().toString().equals("()")){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is empty! Assuming older Schema!");
                    updatedSchemaString = latestAncestorSchema;
                }

                updatedSchemaString = extractColsFromTypeName(updatedSchemaString, someMap, updatedSchemaString, false);

                List<FieldSchema> newCols = new LinkedList<>();
                List<ColumnTypePair> pairsList = someMap.getColumnAndTypeList();
                for(ColumnTypePair p : pairsList){
                    if(p.getColumnName().contains("BLOCK__OFFSET__INSIDE__FILE")){
                        continue;
                    }
                    else if(p.getColumnName().contains("INPUT__FILE__NAME")){
                        continue;
                    }
                    else if(p.getColumnName().contains("ROW__ID")){
                        continue;
                    }
                    else if(p.getColumnName().contains("rowid")){
                        continue;
                    }
                    else if(p.getColumnName().contains("bucketid")){
                        continue;
                    }
                    FieldSchema f = new FieldSchema();
                    f.setName(p.getColumnName());
                    f.setType(p.getColumnType());
                    newCols.add(f);
                }

                outputTable.setTableName(currentOperatorNode.getOperator().getOperatorId().toLowerCase());
                outputTable.setAllCols(newCols);
                outputTable.setHasPartitions(false);

                currentOpQuery.setOutputTable(outputTable);
                currentOpQuery.setExaremeOutputTableName("R_"+outputTable.getTableName().toLowerCase()+"_0");

                /*---Finalize local part of Query---*/
                currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

                /*---Check if OpLink is to be created---*/
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
                for(MyTable inputT : currentOpQuery.getInputTables()){
                    boolean isRoot = false;
                    for(MyTable rootInputT : inputTables){
                        if(rootInputT.getTableName().equals(inputT.getTableName())){
                            isRoot = true;
                            break;
                        }
                    }
                    if(isRoot == false){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Creating new OpLink...");
                        OpLink operatorLink = new OpLink();
                        operatorLink.setContainerName("c0");
                        operatorLink.setFromTable("R_"+inputT.getTableName().toLowerCase()+"_0");
                        operatorLink.setToTable("R_"+outputTable.getTableName().toLowerCase()+"_0");

                        StringParameter sP = new StringParameter("table", inputT.getTableName());
                        NumParameter nP = new NumParameter("part", 0);

                        List<Parameter> opParams = new LinkedList<>();
                        opParams.add(sP);
                        opParams.add(nP);

                        operatorLink.setParameters(opParams);
                        opLinksList.add(operatorLink);

                    }
                }

                /*----Adding Finished Query to List----*/
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
                allQueries.add(currentOpQuery);

                /*----Begin a new Query----*/
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
                OperatorQuery opQuery = new OperatorQuery();
                opQuery.setDataBasePath(currentDatabasePath);
                opQuery.setLocalQueryString("");
                opQuery.setExaremeQueryString("");
                opQuery.addInputTable(currentOpQuery.getOutputTable());
                opQuery.setAssignedContainer("c0");
                opQuery.setLocalQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());
                opQuery.setExaremeQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+opQuery.getLocalQueryString()+"]");

                //TODO: CHECK OP_LINKS AND GOTOPARENTNEXT

                /*---Moving to Child Operator---*/
                List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
                if(children != null){
                    if(children.size() == 1){
                        Operator<?> child = children.get(0);
                        if(child instanceof GroupByOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered RS--->GBY connection!");
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                            OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                            goToChildOperator(childNode, currentOperatorNode, opQuery, updatedSchemaString, currentOpQuery.getOutputTable().getTableName(), latestAncestorTableName2, null);
                        }
                        else if(child instanceof SelectOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered RS--->SEL connection!");
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                            OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                            goToChildOperator(childNode, currentOperatorNode, opQuery, updatedSchemaString, currentOpQuery.getOutputTable().getTableName(), latestAncestorTableName2, null);
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported child for ReduceSinkOperator!");
                            System.exit(0);
                        }
                    }
                    else{
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": children are NOT SIZE==1!");
                        System.exit(0);
                    }
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": children are null");
                    System.exit(0);
                }
            }
            else if(fatherOperatorNode.getOperator() instanceof TableScanOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is TableScanOperator: "+fatherOperatorNode.getOperatorName());
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if Father is Root...");
                if((fatherOperatorNode.getOperator().getParentOperators() == null) || (fatherOperatorNode.getOperator().getParentOperators().size() == 0)){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is root...OK!");
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is not ROOT! Error!");
                    System.exit(0);
                }

                String updatedSchemaString = "";

                if(currentOperatorNode.getOperator().getSchema().toString().equals("()") || (currentOperatorNode.getOperator().getSchema() == null)){
                    if(currentOperatorNode.getOperator().getColumnExprMap() != null){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": RowSchema is empty! We will build Schema from ColumnExprMap...");

                        MyMap someMap = new MyMap();

                        String pastSchema = extractColsFromTypeName(latestAncestorSchema, someMap, latestAncestorSchema, false);

                        updatedSchemaString = "(";
                        int i = 0;
                        for(Map.Entry<String, ExprNodeDesc> entry : currentOperatorNode.getOperator().getColumnExprMap().entrySet()){
                            String newColName = entry.getKey();
                            String pastName = entry.getValue().getCols().get(0);

                            boolean matchFound = false;
                            String nameForSchema = "";
                            String typeForSchema = "";

                            for(ColumnTypePair oldPair : someMap.getColumnAndTypeList()){ //Search all the parent operator Map
                                if(oldPair.getColumnName().equals(pastName)){
                                    for(ColumnTypePair mapPair : columnAndTypeMap.getColumnAndTypeList()){ //Search all the total map
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": TypeOldPair: "+oldPair.getColumnType()+" - TypeNewPair: "+mapPair.getColumnType());
                                        if(mapPair.getColumnType().contains(oldPair.getColumnType())){
                                            if(mapPair.getColumnName().equals(oldPair.getColumnName())){
                                                mapPair.addAltAlias(currentOperatorNode.getOperatorName(), newColName);
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": NewColName: "+newColName+" - was added in map as altAlias for: "+mapPair.getColumnName());
                                                matchFound = true;
                                                nameForSchema = mapPair.getColumnName();
                                                typeForSchema = mapPair.getColumnType();
                                                break;
                                            }
                                        }
                                    }

                                    if(matchFound == true) break;
                                }

                            }

                            if(matchFound == false){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": NewColName: "+newColName+" has no match!");
                                System.exit(0);
                            }

                            if(i == currentOperatorNode.getOperator().getColumnExprMap().entrySet().size() - 1){
                                updatedSchemaString = updatedSchemaString.concat(nameForSchema + ": " + typeForSchema + ")");
                            }
                            else{
                                updatedSchemaString = updatedSchemaString.concat(nameForSchema + ": " + typeForSchema + ",");
                            }

                            i++;
                        }

                    }
                    else{
                        updatedSchemaString = latestAncestorSchema;
                    }
                }
                else {
                    if(currentOperatorNode.getOperator().getSchema() == null){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is empty! Assuming older Schema!");
                        updatedSchemaString = latestAncestorSchema;
                    }
                    else {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...");
                        updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);
                    }
                }

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
                if(currentOpQuery.getLocalQueryString().contains("select ") == false){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query does not contain SELECT statement (needs to be added)...");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Constructing SELECT statement columns...");
                    MyMap aMap = new MyMap();
                    updatedSchemaString = extractColsFromTypeName(updatedSchemaString, aMap, updatedSchemaString, false);

                    String selectString = "";
                    List<ColumnTypePair> pairs = aMap.getColumnAndTypeList();
                    for(int i = 0; i < pairs.size(); i++){
                        if(i == pairs.size() - 1){
                            selectString = selectString + " " + pairs.get(i).getColumnName()+ " ";
                        }
                        else{
                            selectString = selectString + " " + pairs.get(i).getColumnName() + ",";
                        }

                        //Attempt to add used column if not existing
                        currentOpQuery.addUsedColumn(pairs.get(i).getColumnName(), currentOpQuery.getInputTables().get(0).getTableName().toLowerCase());
                    }

                    currentOpQuery.setLocalQueryString("select"+selectString+currentOpQuery.getLocalQueryString());
                    currentOpQuery.setExaremeQueryString("select"+selectString+currentOpQuery.getExaremeQueryString());

                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

                }

                //Check for order by
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if ORDER BY exists in this RS...");
                ReduceSinkOperator reduceSinkOperator = (ReduceSinkOperator) currentOperatorNode.getOperator();
                ReduceSinkDesc reduceSinkDesc = reduceSinkOperator.getConf();

                if(reduceSinkDesc.getOrder() != null){
                    if((reduceSinkDesc.getOrder().contains("+")) || (reduceSinkDesc.getOrder().contains("-"))){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": + and - exist! Order by statement exists!");
                        List<String> orderByCols = new LinkedList<>();
                        if(reduceSinkDesc.getKeyCols() != null) {
                            ArrayList<ExprNodeDesc> exprCols = reduceSinkDesc.getKeyCols();
                            if(exprCols.size() > 0){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Key Cols will be used to build order by clause!");
                                if(exprCols.size() == reduceSinkDesc.getOrder().length()){

                                    String orderByString = " order by ";
                                    MyMap keysMap = new MyMap();
                                    String[] partsPairs = reduceSinkDesc.getKeyColString().split(",");
                                    if(partsPairs.length != exprCols.size()){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": PartsPairs length and exprCols size ARE NOT equal! Error! partsPair.length="+partsPairs.length);
                                        System.exit(0);
                                    }

                                    for(String partPair : partsPairs){ //Add all keys to a map
                                        String[] parts = partPair.split(" ");
                                        String currentColName = "";
                                        String type = "";
                                        if(parts.length == 3){
                                            currentColName = parts[0];
                                            type = parts[2].replace(")", "");
                                        }
                                        else if(parts.length == 4){
                                            currentColName = parts[1];
                                            type = parts[3].replace(")", "");
                                        }
                                        else{
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Parts.length must be 3! Error! parts.length="+parts.length);
                                            System.exit(0);
                                        }

                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Key: "+currentColName+" - Type: "+type);

                                        ColumnTypePair somePair = new ColumnTypePair(currentColName, type);

                                        keysMap.addPair(somePair);
                                    }

                                    //Use the map to locate the real aliases and form the Order by clause
                                    for(int i = 0; i < keysMap.getColumnAndTypeList().size(); i++){
                                        ColumnTypePair keyPair = keysMap.getColumnAndTypeList().get(i);

                                        String realAliasOfKey = "";
                                        boolean aliasFound = false;
                                        String fatherName = fatherOperatorNode.getOperatorName();
                                        String order = "";

                                        if(reduceSinkDesc.getOrder().toCharArray()[i] == '+'){
                                            order = "ASC";
                                        }
                                        else if(reduceSinkDesc.getOrder().toCharArray()[i] == '-'){
                                            order = "DESC";
                                        }
                                        else{
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unknown symbol in order string (+/-) allowed only");
                                            System.exit(0);
                                        }

                                        for(ColumnTypePair cP : columnAndTypeMap.getColumnAndTypeList()){
                                            if(cP.getColumnType().equals(keyPair.getColumnType())){
                                                if(cP.getColumnName().equals(keyPair.getColumnName())){
                                                    aliasFound = true;
                                                    realAliasOfKey = cP.getColumnName();
                                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+"is: "+realAliasOfKey);
                                                    break;
                                                }
                                                else{
                                                    if(cP.getAltAliasPairs() != null){
                                                        if(cP.getAltAliasPairs().size() > 0){
                                                            for(StringParameter sP : cP.getAltAliasPairs()){
                                                                if(sP.getParemeterType().equals(fatherName)){
                                                                    if(sP.getValue().equals(keyPair.getColumnName())){
                                                                        aliasFound = true;
                                                                        realAliasOfKey = cP.getColumnName();
                                                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+"is: "+realAliasOfKey);
                                                                        break;
                                                                    }
                                                                }
                                                            }

                                                            if(aliasFound == true){
                                                                break;
                                                            }

                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        if(aliasFound == false){
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+" was never found! Error!");
                                            System.exit(0);
                                        }

                                        if(i == keysMap.getColumnAndTypeList().size() - 1){
                                            orderByString = orderByString.concat(" "+realAliasOfKey+" "+order+" ");
                                        }
                                        else{
                                            orderByString = orderByString.concat(" "+realAliasOfKey+" "+order+", ");
                                        }

                                        //Add used column order by
                                        currentOpQuery.addUsedColumn(realAliasOfKey, latestAncestorTableName1);

                                    }

                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Formed ORDER BY string: "+orderByString);
                                    currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString()+" "+orderByString);
                                    currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString()+" "+orderByString);

                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");



                                }
                                else{
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Key Cols And Order(+/-) string do not have the same size!");
                                    System.exit(0);
                                }
                            }
                            else{
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": No Key Cols exist!");
                                System.exit(0);
                            }
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": No Key Cols exist!");
                            System.exit(0);
                        }
                    }
                }

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

                /*---Finalising outputTable---*/
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");
                MyTable outputTable = new MyTable();
                outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                outputTable.setIsAFile(false);

                MyMap someMap = new MyMap();

                updatedSchemaString = extractColsFromTypeName(updatedSchemaString, someMap, updatedSchemaString, false);

                List<FieldSchema> newCols = new LinkedList<>();
                List<ColumnTypePair> pairsList = someMap.getColumnAndTypeList();
                for(ColumnTypePair p : pairsList){
                    if(p.getColumnName().contains("BLOCK__OFFSET__INSIDE__FILE")){
                        continue;
                    }
                    else if(p.getColumnName().contains("INPUT__FILE__NAME")){
                        continue;
                    }
                    else if(p.getColumnName().contains("ROW__ID")){
                        continue;
                    }
                    else if(p.getColumnName().contains("rowid")){
                        continue;
                    }
                    else if(p.getColumnName().contains("bucketid")){
                        continue;
                    }
                    FieldSchema f = new FieldSchema();
                    f.setName(p.getColumnName());
                    f.setType(p.getColumnType());
                    newCols.add(f);
                }

                outputTable.setTableName(currentOperatorNode.getOperator().getOperatorId().toLowerCase());
                outputTable.setAllCols(newCols);
                outputTable.setHasPartitions(false);

                currentOpQuery.setOutputTable(outputTable);
                currentOpQuery.setExaremeOutputTableName("R_"+outputTable.getTableName().toLowerCase()+"_0");

                /*---Finalize local part of Query---*/
                currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

                /*---Check if OpLink is to be created---*/
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
                for(MyTable inputT : currentOpQuery.getInputTables()){
                    boolean isRoot = false;
                    for(MyTable rootInputT : inputTables){
                        if(rootInputT.getTableName().equals(inputT.getTableName())){
                            isRoot = true;
                            break;
                        }
                    }
                    if(isRoot == false){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Creating new OpLink...");
                        OpLink operatorLink = new OpLink();
                        operatorLink.setContainerName("c0");
                        operatorLink.setFromTable("R_"+inputT.getTableName().toLowerCase()+"_0");
                        operatorLink.setToTable("R_"+outputTable.getTableName().toLowerCase()+"_0");

                        StringParameter sP = new StringParameter("table", inputT.getTableName());
                        NumParameter nP = new NumParameter("part", 0);

                        List<Parameter> opParams = new LinkedList<>();
                        opParams.add(sP);
                        opParams.add(nP);

                        operatorLink.setParameters(opParams);
                        opLinksList.add(operatorLink);

                    }
                }

                /*----Adding Finished Query to List----*/
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
                allQueries.add(currentOpQuery);


                //TODO JOIN
                /*---Moving to Child Operator---*/
                List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
                if(children != null){
                    if(children.size() == 1){
                        Operator<?> child = children.get(0);
                        if(child instanceof JoinOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered RS--->JOIN connection!");
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if JOIN_POINT: "+child.getOperatorId()+" exists...");

                            if(joinPointList.size() > 0){
                                boolean joinPExists = false;
                                for(JoinPoint jP : joinPointList){
                                    if(jP.getId().equals(child.getOperatorId())){
                                        joinPExists = true;

                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" Exists! Removing it from List!");

                                        OperatorQuery associatedQuery = jP.getAssociatedOpQuery();
                                        String otherParent = jP.getCreatedById();
                                        latestAncestorTableName2 = jP.getLatestAncestorTableName();
                                        OperatorNode secondFather = jP.getOtherFatherNode();
                                        String joinPhrase = jP.getJoinPhrase();

                                        joinPointList.remove(jP);

                                        associatedQuery.addInputTable(currentOpQuery.getOutputTable());

                                        associatedQuery.setLocalQueryString(associatedQuery.getLocalQueryString().concat(" "+joinPhrase+" "+currentOpQuery.getOutputTable().getTableName().toLowerCase()+" on "));
                                        associatedQuery.setExaremeQueryString(associatedQuery.getExaremeQueryString().concat(" "+joinPhrase+" "+currentOpQuery.getOutputTable().getTableName().toLowerCase()+" on "));

                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                                        OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                                        goToChildOperator(childNode, currentOperatorNode, associatedQuery, updatedSchemaString, currentOpQuery.getOutputTable().getTableName().toLowerCase(), latestAncestorTableName2, secondFather);

                                        return;
                                    }
                                }

                                if(joinPExists == false){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                                    /*----Begin a new Query----*/
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
                                    OperatorQuery opQuery = new OperatorQuery();
                                    opQuery.setDataBasePath(currentDatabasePath);
                                    opQuery.setLocalQueryString("");
                                    opQuery.setExaremeQueryString("");
                                    opQuery.addInputTable(currentOpQuery.getOutputTable());
                                    opQuery.setAssignedContainer("c0");
                                    opQuery.setLocalQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());
                                    opQuery.setExaremeQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());

                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+opQuery.getLocalQueryString()+"]");
                                    JoinOperator joinOp = (JoinOperator) child;
                                    JoinDesc joinDesc = joinOp.getConf();

                                    JoinCondDesc[] joinCondDescs = joinDesc.getConds();
                                    if(joinCondDescs.length > 1){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": More than 1 JoinCondDesc! Error!");
                                        System.exit(0);
                                    }

                                    String joinPhrase = "";
                                    if(joinCondDescs[0].getType() == 0){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Type=0 - JoinString: "+joinCondDescs[0].getJoinCondString());
                                        joinPhrase = " inner join ";
                                    }
                                    else if(joinCondDescs[0].getType() == 1){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Type=1 - JoinString: "+joinCondDescs[0].getJoinCondString());
                                        joinPhrase = " left join ";
                                    }
                                    else if(joinCondDescs[0].getType() == 2){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Type=2 - JoinString: "+joinCondDescs[0].getJoinCondString());
                                        joinPhrase = " right join ";
                                    }
                                    else if(joinCondDescs[0].getType() == 3){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Type=3 - JoinString: "+joinCondDescs[0].getJoinCondString());
                                        joinPhrase = " full outer join ";
                                    }
                                    else{
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": UNSUPPORTED JOIN TYPE! JoinString: "+joinCondDescs[0].getJoinCondString());
                                        System.exit(0);
                                        //joinPhrase = " full outer join ";
                                    }

                                    JoinPoint joinP = new JoinPoint(child.getOperatorId(), currentOperatorNode.getOperatorName(), opQuery, latestAncestorTableName1, currentOperatorNode, joinPhrase);

                                    joinPointList.add(joinP);

                                    return;
                                }
                            }
                            else{ //No JoinPoint Exists
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                                /*----Begin a new Query----*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
                                OperatorQuery opQuery = new OperatorQuery();
                                opQuery.setDataBasePath(currentDatabasePath);
                                opQuery.setLocalQueryString("");
                                opQuery.setExaremeQueryString("");
                                opQuery.addInputTable(currentOpQuery.getOutputTable());
                                opQuery.setAssignedContainer("c0");
                                opQuery.setLocalQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());
                                opQuery.setExaremeQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());

                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");
                                JoinOperator joinOp = (JoinOperator) child;
                                JoinDesc joinDesc = joinOp.getConf();

                                JoinCondDesc[] joinCondDescs = joinDesc.getConds();
                                if(joinCondDescs.length > 1){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": More than 1 JoinCondDesc! Error!");
                                    System.exit(0);
                                }

                                String joinPhrase = "";
                                if(joinCondDescs[0].getType() == 0){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Type=0 - JoinString: "+joinCondDescs[0].getJoinCondString());
                                    joinPhrase = " inner join ";
                                }
                                else if(joinCondDescs[0].getType() == 1){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Type=1 - JoinString: "+joinCondDescs[0].getJoinCondString());
                                    joinPhrase = " left join ";
                                }
                                else if(joinCondDescs[0].getType() == 2){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Type=2 - JoinString: "+joinCondDescs[0].getJoinCondString());
                                    joinPhrase = " right join ";
                                }
                                else if(joinCondDescs[0].getType() == 3){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Type=3 - JoinString: "+joinCondDescs[0].getJoinCondString());
                                    joinPhrase = " full outer join ";
                                }
                                else{
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": UNSUPPORTED JOIN TYPE! JoinString: "+joinCondDescs[0].getJoinCondString());
                                    System.exit(0);
                                    //joinPhrase = " full outer join ";
                                }

                                JoinPoint joinP = new JoinPoint(child.getOperatorId(), currentOperatorNode.getOperatorName(), opQuery, latestAncestorTableName1, currentOperatorNode, joinPhrase);

                                joinPointList.add(joinP);

                                return;
                            }
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported child for ReduceSinkOperator!");
                            System.exit(0);
                        }
                    }
                    else{
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": children are NOT SIZE==1!");
                        System.exit(0);
                    }
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": children are null");
                    System.exit(0);
                }

            }
            else if(fatherOperatorNode.getOperator() instanceof SelectOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a SelectOperator: "+fatherOperatorNode.getOperatorName());

                String updatedSchemaString = "";

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if GrandFather is Root...");
                OperatorNode grandFatherOperatorNode = exaremeGraph.getOperatorNodeByName(fatherOperatorNode.getOperator().getParentOperators().get(0).getOperatorId());

                if((grandFatherOperatorNode.getOperator().getParentOperators() == null) || (grandFatherOperatorNode.getOperator().getParentOperators().size() == 0)){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": GrandFather is root...TableScan!");

                    if(currentOperatorNode.getOperator().getSchema().toString().equals("()") || (currentOperatorNode.getOperator().getSchema() == null)){
                        if(currentOperatorNode.getOperator().getColumnExprMap() != null){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": RowSchema is empty! We will build Schema from ColumnExprMap...");

                            MyMap someMap = new MyMap();

                            String pastSchema = extractColsFromTypeName(grandFatherOperatorNode.getOperator().getSchema().toString(), someMap, grandFatherOperatorNode.getOperator().getSchema().toString(), false);

                            List<FieldSchema> newCols = new LinkedList<>();
                            List<ColumnTypePair> pairsList = someMap.getColumnAndTypeList();
                            for(ColumnTypePair p : pairsList){
                                if(p.getColumnName().contains("BLOCK__OFFSET__INSIDE__FILE")){
                                    continue;
                                }
                                else if(p.getColumnName().contains("INPUT__FILE__NAME")){
                                    continue;
                                }
                                else if(p.getColumnName().contains("ROW__ID")){
                                    continue;
                                }
                                else if(p.getColumnName().contains("rowid")){
                                    continue;
                                }
                                else if(p.getColumnName().contains("bucketid")){
                                    continue;
                                }
                                FieldSchema f = new FieldSchema();
                                f.setName(p.getColumnName());
                                f.setType(p.getColumnType());
                                newCols.add(f);
                            }

                            updatedSchemaString = "(";
                            int i = 0;
                            for(Map.Entry<String, ExprNodeDesc> entry : currentOperatorNode.getOperator().getColumnExprMap().entrySet()){
                                String newColName = entry.getKey();
                                String pastName = entry.getValue().getCols().get(0);

                                boolean matchFound = false;
                                String nameForSchema = "";
                                String typeForSchema = "";

                                for(ColumnTypePair oldPair : someMap.getColumnAndTypeList()){ //Search all the parent operator Map
                                    if(oldPair.getColumnName().equals(pastName)){
                                        for(ColumnTypePair mapPair : columnAndTypeMap.getColumnAndTypeList()){ //Search all the total map
                                            if(mapPair.getColumnType().equals(oldPair.getColumnType())){
                                                if(mapPair.getColumnName().equals(oldPair.getColumnName())){
                                                    mapPair.addAltAlias(currentOperatorNode.getOperatorName(), newColName);
                                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": NewColName: "+newColName+" - was added in map as altAlias for: "+mapPair.getColumnName());
                                                    matchFound = true;
                                                    nameForSchema = mapPair.getColumnName();
                                                    typeForSchema = mapPair.getColumnType();
                                                    break;
                                                }
                                            }
                                        }

                                        if(matchFound == true) break;
                                    }

                                }

                                if(matchFound == false){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": NewColName: "+newColName+" has no match!");
                                    System.exit(0);
                                }

                                if(i == currentOperatorNode.getOperator().getColumnExprMap().entrySet().size() - 1){
                                    updatedSchemaString = updatedSchemaString.concat(nameForSchema + ": " + typeForSchema + ")");
                                }
                                else{
                                    updatedSchemaString = updatedSchemaString.concat(nameForSchema + ": " + typeForSchema + ",");
                                }

                                i++;
                            }

                        }
                        else{
                            updatedSchemaString = latestAncestorSchema;
                        }
                    }
                    else {
                        if(currentOperatorNode.getOperator().getSchema() == null){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is empty! Assuming older Schema!");
                            updatedSchemaString = latestAncestorSchema;
                        }
                        else {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...");
                            updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);
                        }
                    }
                }
                else{
                    if(currentOperatorNode.getOperator().getSchema() == null){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is empty! Assuming older Schema!");
                        updatedSchemaString = latestAncestorSchema;
                    }
                    else {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...");
                        updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);
                    }
                }

                //Check for order by
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if ORDER BY exists in this RS...");
                ReduceSinkOperator reduceSinkOperator = (ReduceSinkOperator) currentOperatorNode.getOperator();
                ReduceSinkDesc reduceSinkDesc = reduceSinkOperator.getConf();

                if(reduceSinkDesc.getOrder() != null){
                    if((reduceSinkDesc.getOrder().contains("+")) || (reduceSinkDesc.getOrder().contains("-"))){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": + and - exist! Order by statement exists!");
                        List<String> orderByCols = new LinkedList<>();
                        if(reduceSinkDesc.getKeyCols() != null) {
                            ArrayList<ExprNodeDesc> exprCols = reduceSinkDesc.getKeyCols();
                            if(exprCols.size() > 0){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Key Cols will be used to build order by clause!");
                                if(exprCols.size() == reduceSinkDesc.getOrder().length()){

                                    String orderByString = " order by ";
                                    MyMap keysMap = new MyMap();
                                    String[] partsPairs = reduceSinkDesc.getKeyColString().split(",");
                                    if(partsPairs.length != exprCols.size()){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": PartsPairs length and exprCols size ARE NOT equal! Error! partsPair.length="+partsPairs.length);
                                        System.exit(0);
                                    }

                                    for(String partPair : partsPairs){ //Add all keys to a map
                                        String[] parts = partPair.split(" ");
                                        String currentColName = "";
                                        String type = "";
                                        if(parts.length == 3){
                                            currentColName = parts[0];
                                            type = parts[2].replace(")", "");
                                        }
                                        else if(parts.length == 4){
                                            currentColName = parts[1];
                                            type = parts[3].replace(")", "");
                                        }
                                        else{
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Parts.length must be 3! Error! parts.length="+parts.length);
                                            System.exit(0);
                                        }

                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Key: "+currentColName+" - Type: "+type);

                                        ColumnTypePair somePair = new ColumnTypePair(currentColName, type);

                                        keysMap.addPair(somePair);
                                    }

                                    //Use the map to locate the real aliases and form the Order by clause
                                    for(int i = 0; i < keysMap.getColumnAndTypeList().size(); i++){
                                        ColumnTypePair keyPair = keysMap.getColumnAndTypeList().get(i);

                                        String realAliasOfKey = "";
                                        boolean aliasFound = false;
                                        String fatherName = fatherOperatorNode.getOperatorName();
                                        String order = "";

                                        if(reduceSinkDesc.getOrder().toCharArray()[i] == '+'){
                                            order = "ASC";
                                        }
                                        else if(reduceSinkDesc.getOrder().toCharArray()[i] == '-'){
                                            order = "DESC";
                                        }
                                        else{
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unknown symbol in order string (+/-) allowed only");
                                            System.exit(0);
                                        }

                                        for(ColumnTypePair cP : columnAndTypeMap.getColumnAndTypeList()){
                                            if(cP.getColumnType().equals(keyPair.getColumnType())){
                                                if(cP.getColumnName().equals(keyPair.getColumnName())){
                                                    aliasFound = true;
                                                    realAliasOfKey = cP.getColumnName();
                                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+"is: "+realAliasOfKey);
                                                    break;
                                                }
                                                else{
                                                    if(cP.getAltAliasPairs() != null){
                                                        if(cP.getAltAliasPairs().size() > 0){
                                                            for(StringParameter sP : cP.getAltAliasPairs()){
                                                                if(sP.getParemeterType().equals(fatherName)){
                                                                    if(sP.getValue().equals(keyPair.getColumnName())){
                                                                        aliasFound = true;
                                                                        realAliasOfKey = cP.getColumnName();
                                                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+"is: "+realAliasOfKey);
                                                                        break;
                                                                    }
                                                                }
                                                            }

                                                            if(aliasFound == true){
                                                                break;
                                                            }

                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        if(aliasFound == false){
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+" was never found! Error!");
                                            System.exit(0);
                                        }

                                        if(i == keysMap.getColumnAndTypeList().size() - 1){
                                            orderByString = orderByString.concat(" "+realAliasOfKey+" "+order+" ");
                                        }
                                        else{
                                            orderByString = orderByString.concat(" "+realAliasOfKey+" "+order+", ");
                                        }

                                        //Add used column order by
                                        currentOpQuery.addUsedColumn(realAliasOfKey, latestAncestorTableName1);

                                    }


                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Formed ORDER BY string: "+orderByString);
                                    currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString()+" "+orderByString);
                                    currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString()+" "+orderByString);

                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");



                                }
                                else{
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Key Cols And Order(+/-) string do not have the same size!");
                                    System.exit(0);
                                }
                            }
                            else{
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": No Key Cols exist!");
                                System.exit(0);
                            }
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": No Key Cols exist!");
                            System.exit(0);
                        }
                    }
                }

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

                /*---Finalising outputTable---*/
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");
                MyTable outputTable = new MyTable();
                outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                outputTable.setIsAFile(false);

                MyMap someMap = new MyMap();

                if(currentOperatorNode.getOperator().getSchema().toString().equals("()")){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is empty! Assuming older Schema!");
                    updatedSchemaString = latestAncestorSchema;
                }

                updatedSchemaString = extractColsFromTypeName(updatedSchemaString, someMap, updatedSchemaString, false);

                List<FieldSchema> newCols = new LinkedList<>();
                List<ColumnTypePair> pairsList = someMap.getColumnAndTypeList();
                for(ColumnTypePair p : pairsList){
                    if(p.getColumnName().contains("BLOCK__OFFSET__INSIDE__FILE")){
                        continue;
                    }
                    else if(p.getColumnName().contains("INPUT__FILE__NAME")){
                        continue;
                    }
                    else if(p.getColumnName().contains("ROW__ID")){
                        continue;
                    }
                    else if(p.getColumnName().contains("rowid")){
                        continue;
                    }
                    else if(p.getColumnName().contains("bucketid")){
                        continue;
                    }
                    FieldSchema f = new FieldSchema();
                    f.setName(p.getColumnName());
                    f.setType(p.getColumnType());
                    newCols.add(f);
                }

                outputTable.setTableName(currentOperatorNode.getOperator().getOperatorId().toLowerCase());
                outputTable.setAllCols(newCols);
                outputTable.setHasPartitions(false);

                currentOpQuery.setOutputTable(outputTable);
                currentOpQuery.setExaremeOutputTableName("R_"+outputTable.getTableName().toLowerCase()+"_0");

                /*---Finalize local part of Query---*/
                currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

                /*---Check if OpLink is to be created---*/
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
                for(MyTable inputT : currentOpQuery.getInputTables()){
                    boolean isRoot = false;
                    for(MyTable rootInputT : inputTables){
                        if(rootInputT.getTableName().equals(inputT.getTableName())){
                            isRoot = true;
                            break;
                        }
                    }
                    if(isRoot == false){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Creating new OpLink...");
                        OpLink operatorLink = new OpLink();
                        operatorLink.setContainerName("c0");
                        operatorLink.setFromTable("R_"+inputT.getTableName().toLowerCase()+"_0");
                        operatorLink.setToTable("R_"+outputTable.getTableName().toLowerCase()+"_0");

                        StringParameter sP = new StringParameter("table", inputT.getTableName());
                        NumParameter nP = new NumParameter("part", 0);

                        List<Parameter> opParams = new LinkedList<>();
                        opParams.add(sP);
                        opParams.add(nP);

                        operatorLink.setParameters(opParams);
                        opLinksList.add(operatorLink);
                    }
                }

                /*----Adding Finished Query to List----*/
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
                allQueries.add(currentOpQuery);


                //TODO JOIN
                /*---Moving to Child Operator---*/
                List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
                if(children != null){
                    if(children.size() == 1){
                        Operator<?> child = children.get(0);
                        if(child instanceof SelectOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered RS--->SEL connection!");

                            /*----Begin a new Query----*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
                            OperatorQuery opQuery = new OperatorQuery();
                            opQuery.setDataBasePath(currentDatabasePath);
                            opQuery.setLocalQueryString("");
                            opQuery.setExaremeQueryString("");
                            opQuery.addInputTable(currentOpQuery.getOutputTable());
                            opQuery.setAssignedContainer("c0");
                            opQuery.setLocalQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());
                            opQuery.setExaremeQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());

                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+opQuery.getLocalQueryString()+"]");


                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                            OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                            goToChildOperator(childNode, currentOperatorNode, opQuery, updatedSchemaString, currentOpQuery.getOutputTable().getTableName().toLowerCase(), latestAncestorTableName2, null);

                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported child for ReduceSinkOperator!");
                            System.exit(0);
                        }
                    }
                    else{
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": children are NOT SIZE==1!");
                        System.exit(0);
                    }
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": children are null");
                    System.exit(0);
                }

            }
            else{
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported Type of Parent Operator for ReduceSinkOperator...");
                System.exit(0);
            }
        }
        else if(currentOperatorNode.getOperator() instanceof JoinOperator) {
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Operator is a JoinOperator...");
            if(otherFatherNode == null){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": otherFather is NULL! Error!");
                System.exit(0);
            }
            if((fatherOperatorNode.getOperator() instanceof ReduceSinkOperator) && (otherFatherNode.getOperator() instanceof ReduceSinkOperator)){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JoinOperator: "+currentOperatorNode.getOperatorName()+" has parents: "+fatherOperatorNode.getOperatorName()+" , "+otherFatherNode.getOperatorName());

                String updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);

                //TODO ADD KEY SEARCH AND OUTPUT SELECT COLUMNS
                JoinOperator joinOp = (JoinOperator) currentOperatorNode.getOperator();
                JoinDesc joinDesc = joinOp.getConf();

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing keys of Inner Join...");
                Map<Byte, String> keys = joinDesc.getKeysString();
                List<String> joinColumns = new LinkedList<>();
                if (keys != null) {
                     //TODO: HANDLE COLS WITH SAME NAME ON DIFFERENT TABLES

                    for (Map.Entry<Byte, String> entry : keys.entrySet()) {
                        //System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing keys of Inner Join...");
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Key: " + entry.getKey() + " : Value: " + entry.getValue());

                        String[] parts = entry.getValue().split(" ");
                        if(parts.length != 3){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Reading value info...Failed! Parts size: "+parts.length);
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Parts: "+parts);
                            System.exit(0);
                        }
                        String columnName = parts[0];
                        String columnType = "";
                        if(parts[2].contains(")"))
                            columnType = parts[2].replace(")", "");
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": ColumnType does not contain )");
                            System.exit(0);
                        }

                        List<ColumnTypePair> allPairs = columnAndTypeMap.getColumnAndTypeList();

                        //Located Value
                        boolean valueLocated = false;
                        String realValueName = "";
                        for(ColumnTypePair p : allPairs){
                            if(p.getColumnType().equals(columnType)) {
                                if (p.getColumnName().equals(columnName)) { //Check for direct match
                                    valueLocated = true;
                                    realValueName = columnName;
                                    joinColumns.add(columnName);

                                    //Locate the father that last used the column
                                    String lastFather = "";
                                    boolean altAliasExists = false;

                                    List<StringParameter> altAliases = p.getAltAliasPairs();
                                    if (altAliases.size() > 0) {
                                        for (StringParameter sP : altAliases) {
                                            if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Located Father of Used Column for Join Column - Father: " + fatherOperatorNode.getOperatorName());
                                                lastFather = fatherOperatorNode.getOperatorName();
                                                altAliasExists = true;
                                                break;
                                            } else if (sP.getParemeterType().equals(otherFatherNode.getOperatorName())) {
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Located Father of Used Column for Join Column - Father: " + otherFatherNode.getOperatorName());
                                                lastFather = otherFatherNode.getOperatorName();
                                                altAliasExists = true;
                                                break;
                                            }
                                        }

                                        if (altAliasExists == false) {
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Can't locate add JOIN COLUMN TO USED COLUMNS because AltAlias and Previous Father Entry does not exist!1");
                                            System.exit(0);
                                        }

                                        //Add JOIN USED Column
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + lastFather);
                                        currentOpQuery.addUsedColumn(p.getColumnName(), lastFather);

                                    } else {
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Can't locate add JOIN COLUMN TO USED COLUMNS because AltAlias and Previous Father Entry does not exist!2");
                                        System.exit(0);
                                    }
                                    break;
                                } else {
                                    List<StringParameter> altAliases = p.getAltAliasPairs();
                                    if (altAliases != null) {
                                        if (altAliases.size() > 0) {
                                            for (StringParameter sP : altAliases) {
                                                if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) { //Check for possible match through father1
                                                    if (sP.getValue().equals(columnName)) {
                                                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Located match through: " + fatherOperatorNode.getOperatorName());
                                                        realValueName = p.getColumnName();
                                                        valueLocated = true;
                                                        joinColumns.add(columnName);
                                                        break;
                                                    }
                                                } else if (sP.getParemeterType().equals(otherFatherNode.getOperatorName())) { //Check for possible match through father2
                                                    if (sP.getValue().equals(columnName)) {
                                                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Located match through: " + otherFatherNode.getOperatorName());
                                                        realValueName = p.getColumnName();
                                                        valueLocated = true;
                                                        joinColumns.add(columnName);
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if(valueLocated == false){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Failed to locate the real alias of: "+columnName);
                            System.exit(0);
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real alias of: "+columnName+" is: "+realValueName);
                        }

                    }
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Join Keys are null!");
                    System.exit(0);
                }
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Now we will use keys to form the join expression...");

                //Add Join col1 = col2 part of expression
                currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString().concat(" "+joinColumns.get(0)+" = "+joinColumns.get(1)+" "));
                currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString().concat(" "+joinColumns.get(0)+" = "+joinColumns.get(1)+" "));

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

                /*---Moving to Child Operator---*/
                List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
                if(children != null) {
                    if (children.size() == 1) {
                        Operator<?> child = children.get(0);
                        if (child instanceof SelectOperator) {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered JOIN--->SELECT connection!");
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Current Query will be ending here with Select statement addition...");

                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Constructing SELECT statement columns...");
                            MyMap aMap = new MyMap();
                            updatedSchemaString = extractColsFromTypeName(updatedSchemaString, aMap, updatedSchemaString, false);

                            String selectString = "";
                            List<ColumnTypePair> pairs = aMap.getColumnAndTypeList();
                            for(int i = 0; i < pairs.size(); i++){
                                if(i == pairs.size() - 1){
                                    selectString = selectString + " " + pairs.get(i).getColumnName()+ " ";
                                }
                                else{
                                    selectString = selectString + " " + pairs.get(i).getColumnName() + ",";
                                }
                            }

                            currentOpQuery.setLocalQueryString("select "+selectString+" "+currentOpQuery.getLocalQueryString());
                            currentOpQuery.setExaremeQueryString("select "+selectString+" "+currentOpQuery.getExaremeQueryString());

                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

                            /*---Finalising outputTable---*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Finalising OutputTable...");
                            MyTable outputTable = new MyTable();
                            outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                            outputTable.setIsAFile(false);

                            MyMap someMap = new MyMap();
                            updatedSchemaString = extractColsFromTypeName(updatedSchemaString, someMap, updatedSchemaString, false);

                            List<FieldSchema> newCols = new LinkedList<>();
                            List<ColumnTypePair> pairsList = someMap.getColumnAndTypeList();
                            for (ColumnTypePair p : pairsList) {
                                FieldSchema f = new FieldSchema();
                                f.setName(p.getColumnName());
                                f.setType(p.getColumnType());
                                newCols.add(f);
                            }

                            outputTable.setTableName(currentOperatorNode.getOperator().getOperatorId().toLowerCase());
                            outputTable.setAllCols(newCols);
                            outputTable.setHasPartitions(false);

                            currentOpQuery.setOutputTable(outputTable);
                            currentOpQuery.setExaremeOutputTableName("R_" + outputTable.getTableName().toLowerCase() + "_0");

                            /*---Finalize local part of Query---*/
                            currentOpQuery.setLocalQueryString("create table " + currentOperatorNode.getOperator().getOperatorId().toLowerCase() + " as " + currentOpQuery.getLocalQueryString());

                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + currentOpQuery.getLocalQueryString() + "]");

                            /*---Check if OpLink is to be created---*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Checking if OpLink can be created...");
                            for (MyTable inputT : currentOpQuery.getInputTables()) {
                                boolean isRoot = false;
                                for (MyTable rootInputT : inputTables) {
                                    if (rootInputT.getTableName().equals(inputT.getTableName())) {
                                        isRoot = true;
                                        break;
                                    }
                                }
                                if(isRoot == false){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Creating new OpLink...");
                                    OpLink operatorLink = new OpLink();
                                    operatorLink.setContainerName("c0");
                                    operatorLink.setFromTable("R_" + inputT.getTableName().toLowerCase() + "_0");
                                    operatorLink.setToTable("R_" + outputTable.getTableName().toLowerCase() + "_0");

                                    StringParameter sP = new StringParameter("table", inputT.getTableName());
                                    NumParameter nP = new NumParameter("part", 0);

                                    List<Parameter> opParams = new LinkedList<>();
                                    opParams.add(sP);
                                    opParams.add(nP);

                                    operatorLink.setParameters(opParams);
                                    opLinksList.add(operatorLink);

                                }
                            }

                            /*----Adding Finished Query to List----*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Adding OperatorQuery to QueryList...");
                            allQueries.add(currentOpQuery);

                            /*----Begin a new Query----*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
                            OperatorQuery opQuery = new OperatorQuery();
                            opQuery.setDataBasePath(currentDatabasePath);
                            opQuery.setLocalQueryString("");
                            opQuery.setExaremeQueryString("");
                            opQuery.addInputTable(currentOpQuery.getOutputTable());
                            opQuery.setAssignedContainer("c0");
                            opQuery.setLocalQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());
                            opQuery.setExaremeQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());

                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+opQuery.getLocalQueryString()+"]");

                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, opQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);


                        }
                        else if(child instanceof FilterOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered JOIN--->FIL connection!");
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Moving to child operator...");

                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);

                        }
                        else if(child instanceof GroupByOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered JOIN--->GBY connection!");
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Moving to child operator...");

                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);

                        }
                        else {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported type of Child for Join...");
                            System.exit(0);
                        }
                    } else {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": has != 1 children");
                        System.exit(0);
                    }
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Join Operator has NULL Children...");
                    System.exit(0);
                }

            }
            else{
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported Type of Parent Operator for ReduceSinkOperator...");
                System.exit(0);
            }
        }
        else if(currentOperatorNode.getOperator() instanceof LimitOperator){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Operator is a LimitOperator...");
            if((fatherOperatorNode.getOperator() instanceof SelectOperator) || (fatherOperatorNode.getOperator() instanceof GroupByOperator)){
                if(fatherOperatorNode.getOperator() instanceof SelectOperator) {
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Father is a SelectOperator...");
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Father is a GroupByOperator...");
                }

                if(currentOperatorNode.getOperator().getColumnExprMap() == null){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Assume the schema of FatherOperator(Select)...");

                    LimitOperator limOp = (LimitOperator) currentOperatorNode.getOperator();
                    LimitDesc limDesc = (LimitDesc) limOp.getConf();

                    if(limDesc != null){
                        int theLimit = limDesc.getLimit();

                        currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString() + " limit "+theLimit);
                        currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString() + " limit "+theLimit);

                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

                        List<Operator<?>> children = limOp.getChildOperators();
                        if(children != null){
                            if(children.size() == 1){
                                Operator<?> child = children.get(0);
                                if(child instanceof FileSinkOperator){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered LIM--->FS connection!");
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                                    OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                                    goToChildOperator(childNode, currentOperatorNode, currentOpQuery, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2, null);
                                }
                                else{
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported type of Child for LimitOperator!");
                                    System.exit(0);
                                }
                            }
                            else{
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Children ARE NOT SIZE==1!");
                                System.exit(0);
                            }
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Children are NULL!");
                            System.exit(0);
                        }
                    }
                    else{
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": LimDesc is null!");
                        System.exit(0);
                    }
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": LimitOperator with NON NULL ColumnExprMap! Unsupported yet!");
                    System.exit(0);
                }
            }
            else{
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported Type of Parent Operator for LimitOperator...");
                System.exit(0);
            }
        }
        else if(currentOperatorNode.getOperator() instanceof FileSinkOperator){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Operator is a FileSinkOperator...");
            if((fatherOperatorNode.getOperator() instanceof LimitOperator) || (fatherOperatorNode.getOperator() instanceof GroupByOperator) || (fatherOperatorNode.getOperator() instanceof SelectOperator)){
                if(fatherOperatorNode.getOperator() instanceof LimitOperator) {
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Father is a LimitOperator...");
                }
                else if(fatherOperatorNode.getOperator() instanceof GroupByOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Father is a GroupByOperator...");
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Father is a SelectOperator...");
                }
                if(currentOperatorNode.getOperator().getColumnExprMap() == null){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": FileSink has null columnExprMap! Assumming ancestor's schema...");

                    if(currentOperatorNode.getOperator().getSchema() != null){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": We will use the currentSchema however to fill new Aliases for children...");
                        MyMap ancestorMap = new MyMap();
                        latestAncestorSchema = extractColsFromTypeName(latestAncestorSchema, ancestorMap, latestAncestorSchema, false);

                        MyMap descendentMap = new MyMap();
                        String descendentSchema = extractColsFromTypeName(currentOperatorNode.getOperator().getSchema().toString(), descendentMap, currentOperatorNode.getOperator().getSchema().toString(), true);

                        if(ancestorMap.getColumnAndTypeList().size() != descendentMap.getColumnAndTypeList().size()){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Maps do not have equal size!");
                            System.exit(0);
                        }

                        for(int i = 0; i < ancestorMap.getColumnAndTypeList().size(); i++){
                            ColumnTypePair ancestorPair = ancestorMap.getColumnAndTypeList().get(i);
                            ColumnTypePair descendentPair = descendentMap.getColumnAndTypeList().get(i);

                            if(ancestorPair.getColumnType().equals(descendentPair.getColumnType())){
                                for(ColumnTypePair p : columnAndTypeMap.getColumnAndTypeList()){
                                    if(p.getColumnName().equals(ancestorPair.getColumnName())){
                                        p.addAltAlias(currentOperatorNode.getOperatorName(), descendentPair.getColumnName());
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Match found! - Pair: "+ancestorPair.getColumnName()+" - "+descendentPair.getColumnName());
                                    }
                                }
                            }
                            else{
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Possible match Pair does not have same Type - Pair: "+ancestorPair.getColumnName()+" - "+descendentPair.getColumnName());
                                System.exit(0);
                            }
                        }

                    }

                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
                    if(currentOpQuery.getLocalQueryString().contains("select ") == false){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query does not contain SELECT statement (needs to be added)...");
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Constructing SELECT statement columns...");
                        MyMap aMap = new MyMap();
                        String updatedSchemaString = extractColsFromTypeName(latestAncestorSchema, aMap, latestAncestorSchema, false);

                        String selectString = "";
                        List<ColumnTypePair> pairs = aMap.getColumnAndTypeList();
                        for(int i = 0; i < pairs.size(); i++){
                            if(i == pairs.size() - 1){
                                selectString = selectString + " " + pairs.get(i).getColumnName()+ " ";
                            }
                            else{
                                selectString = selectString + " " + pairs.get(i).getColumnName() + ",";
                            }

                            //Attempt to add used column if not existing
                            currentOpQuery.addUsedColumn(pairs.get(i).getColumnName(), currentOpQuery.getInputTables().get(0).getTableName().toLowerCase());
                        }

                        currentOpQuery.setLocalQueryString("select"+selectString+currentOpQuery.getLocalQueryString());
                        currentOpQuery.setExaremeQueryString("select"+selectString+currentOpQuery.getExaremeQueryString());

                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

                    }

                    List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
                    if(children != null){
                        if((children.size() == 1) || (children.size() == 2)){
                            Operator<?> child = null;
                            boolean childFound = false;
                            if(children.size() == 2){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current FileSink has 2 Children! Undefined behaviour! Seeking TableScan child only! Check this!");
                                for(Operator<?> someChild : children){
                                    if(someChild.getOperatorId().contains("TS_")){
                                        child = someChild;
                                        childFound = true;
                                    }
                                    else{
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": The other Child: "+someChild.getOperatorId());
                                    }
                                }

                                if(childFound == false){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Non TableScanOperator child exists! Error");
                                    System.exit(0);
                                }
                            }
                            else{
                                child = children.get(0);
                            }

                            if(child instanceof ListSinkOperator){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered FS--->OP connection!");
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                                OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                                goToChildOperator(childNode, currentOperatorNode, currentOpQuery, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2, null);
                            }
                            else if(child instanceof TableScanOperator){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered FS--->TS connection!");
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current query will be ending here!");

                                /*---Finalising outputTable---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Finalising OutputTable...");
                                MyTable outputTable = new MyTable();
                                outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                                outputTable.setIsAFile(false);

                                MyMap someMap = new MyMap();
                                String updatedSchemaString = extractColsFromTypeName(latestAncestorSchema, someMap, latestAncestorSchema, false);

                                List<FieldSchema> newCols = new LinkedList<>();
                                List<ColumnTypePair> pairsList = someMap.getColumnAndTypeList();
                                for (ColumnTypePair p : pairsList) {
                                    FieldSchema f = new FieldSchema();
                                    f.setName(p.getColumnName());
                                    f.setType(p.getColumnType());
                                    newCols.add(f);
                                }

                                outputTable.setTableName(currentOperatorNode.getOperator().getOperatorId().toLowerCase());
                                outputTable.setAllCols(newCols);
                                outputTable.setHasPartitions(false);

                                currentOpQuery.setOutputTable(outputTable);
                                currentOpQuery.setExaremeOutputTableName("R_" + outputTable.getTableName().toLowerCase() + "_0");

                                /*---Finalize local part of Query---*/
                                currentOpQuery.setLocalQueryString("create table " + currentOperatorNode.getOperator().getOperatorId().toLowerCase() + " as " + currentOpQuery.getLocalQueryString());

                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + currentOpQuery.getLocalQueryString() + "]");

                                /*---Check if OpLink is to be created---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Checking if OpLink can be created...");
                                for (MyTable inputT : currentOpQuery.getInputTables()) {
                                    boolean isRoot = false;
                                    for (MyTable rootInputT : inputTables) {
                                        if (rootInputT.getTableName().equals(inputT.getTableName())) {
                                            isRoot = true;
                                            break;
                                        }
                                    }
                                    if(isRoot == false){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Creating new OpLink...");
                                        OpLink operatorLink = new OpLink();
                                        operatorLink.setContainerName("c0");
                                        operatorLink.setFromTable("R_" + inputT.getTableName().toLowerCase() + "_0");
                                        operatorLink.setToTable("R_" + outputTable.getTableName().toLowerCase() + "_0");

                                        StringParameter sP = new StringParameter("table", inputT.getTableName());
                                        NumParameter nP = new NumParameter("part", 0);

                                        List<Parameter> opParams = new LinkedList<>();
                                        opParams.add(sP);
                                        opParams.add(nP);

                                        operatorLink.setParameters(opParams);
                                        opLinksList.add(operatorLink);


                                    }
                                }

                                /*----Adding Finished Query to List----*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Adding OperatorQuery to QueryList...");
                                allQueries.add(currentOpQuery);

                                /*----Begin a new Query----*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
                                OperatorQuery opQuery = new OperatorQuery();
                                opQuery.setDataBasePath(currentDatabasePath);
                                opQuery.setLocalQueryString("");
                                opQuery.setExaremeQueryString("");
                                opQuery.addInputTable(currentOpQuery.getOutputTable());
                                opQuery.setAssignedContainer("c0");
                                opQuery.setLocalQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());
                                opQuery.setExaremeQueryString(" from "+currentOpQuery.getOutputTable().getTableName().toLowerCase());

                                //Add used columns to new Query because they will be needed by TableScan
                                for(FieldSchema f : newCols){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"USED Column for New Query: "+f.getName());
                                    opQuery.addUsedColumn(f.getName(), currentOpQuery.getOutputTable().getTableName().toLowerCase());
                                }

                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+opQuery.getLocalQueryString()+"]");

                                goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, opQuery, updatedSchemaString, opQuery.getInputTables().get(0).getTableName().toLowerCase(), latestAncestorTableName2, null);

                            }
                            else{
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported type of Child for FS!");
                                System.exit(0);
                            }
                        }
                        else if(children.size() == 0){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Reached LEAF FS!!! We did it!");
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current query will be ending here!");

                            /*---Finalising outputTable---*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Finalising OutputTable...");
                            MyTable outputTable = new MyTable();
                            outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                            outputTable.setIsAFile(false);

                            MyMap someMap = new MyMap();
                            String updatedSchemaString = extractColsFromTypeName(latestAncestorSchema, someMap, latestAncestorSchema, false);

                            List<FieldSchema> newCols = new LinkedList<>();
                            List<ColumnTypePair> pairsList = someMap.getColumnAndTypeList();
                            for (ColumnTypePair p : pairsList) {
                                FieldSchema f = new FieldSchema();
                                f.setName(p.getColumnName());
                                f.setType(p.getColumnType());
                                newCols.add(f);
                            }

                            outputTable.setTableName(currentOperatorNode.getOperator().getOperatorId().toLowerCase());
                            outputTable.setAllCols(newCols);
                            outputTable.setHasPartitions(false);

                            currentOpQuery.setOutputTable(outputTable);
                            currentOpQuery.setExaremeOutputTableName("R_" + outputTable.getTableName().toLowerCase() + "_0");

                            /*---Finalize local part of Query---*/
                            currentOpQuery.setLocalQueryString("create table " + currentOperatorNode.getOperator().getOperatorId().toLowerCase() + " as " + currentOpQuery.getLocalQueryString());

                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + currentOpQuery.getLocalQueryString() + "]");

                                /*---Check if OpLink is to be created---*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Checking if OpLink can be created...");
                            for (MyTable inputT : currentOpQuery.getInputTables()) {
                                boolean isRoot = false;
                                for (MyTable rootInputT : inputTables) {
                                    if (rootInputT.getTableName().equals(inputT.getTableName())) {
                                        isRoot = true;
                                        break;
                                    }
                                }
                                if(isRoot == false){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Creating new OpLink...");
                                    OpLink operatorLink = new OpLink();
                                    operatorLink.setContainerName("c0");
                                    operatorLink.setFromTable("R_" + inputT.getTableName().toLowerCase() + "_0");
                                    operatorLink.setToTable("R_" + outputTable.getTableName().toLowerCase() + "_0");

                                    StringParameter sP = new StringParameter("table", inputT.getTableName());
                                    NumParameter nP = new NumParameter("part", 0);

                                    List<Parameter> opParams = new LinkedList<>();
                                    opParams.add(sP);
                                    opParams.add(nP);

                                    operatorLink.setParameters(opParams);
                                    opLinksList.add(operatorLink);

                                }
                            }

                                /*----Adding Finished Query to List----*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Adding OperatorQuery to QueryList...");
                            allQueries.add(currentOpQuery);
                        }
                        else{
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Children are SIZE != 1!");
                            System.exit(0);
                        }
                    }
                    else{
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Children are NULL!");
                        System.exit(0);
                    }
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": FS with non NULL columnExprMap! Unsupported for now!");
                    System.exit(0);
                }
            }
            else{
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported Type of Parent Operator for FileSinkOperator...");
                System.exit(0);
            }
        }
        else if(currentOperatorNode.getOperator() instanceof ListSinkOperator){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Operator is a ListSinkOperator...");
            if(fatherOperatorNode.getOperator() instanceof FileSinkOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a FileSinkOperator...");
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Check if ColumnExprMap is NULL...");
                if(currentOperatorNode.getOperator().getColumnExprMap() == null) {

                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Check if LEAF...");
                    List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
                    if ((children == null) || (children.size() == 0)) {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Reached a Leaf!");

                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Current Query will be ending here...");

                        /*---Finalising outputTable---*/
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Finalising OutputTable...");
                        MyTable outputTable = new MyTable();
                        outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                        outputTable.setIsAFile(false);

                        MyMap someMap = new MyMap();
                        latestAncestorSchema = extractColsFromTypeName(latestAncestorSchema, someMap, latestAncestorSchema, false);

                        List<FieldSchema> newCols = new LinkedList<>();
                        List<ColumnTypePair> pairsList = someMap.getColumnAndTypeList();
                        for (ColumnTypePair p : pairsList) {
                            FieldSchema f = new FieldSchema();
                            f.setName(p.getColumnName());
                            f.setType(p.getColumnType());
                            newCols.add(f);
                        }

                        outputTable.setTableName(currentOperatorNode.getOperator().getOperatorId().toLowerCase());
                        outputTable.setAllCols(newCols);
                        outputTable.setHasPartitions(false);

                        currentOpQuery.setOutputTable(outputTable);
                        currentOpQuery.setExaremeOutputTableName("R_" + outputTable.getTableName().toLowerCase() + "_0");

                        /*---Finalize local part of Query---*/
                        currentOpQuery.setLocalQueryString("create table " + currentOperatorNode.getOperator().getOperatorId().toLowerCase() + " as " + currentOpQuery.getLocalQueryString());

                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + currentOpQuery.getLocalQueryString() + "]");

                        /*---Check if OpLink is to be created---*/
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Checking if OpLink can be created...");
                        for (MyTable inputT : currentOpQuery.getInputTables()) {
                            boolean isRoot = false;
                            for (MyTable rootInputT : inputTables) {
                                if (rootInputT.getTableName().equals(inputT.getTableName())) {
                                    isRoot = true;
                                    break;
                                }
                            }
                            if(isRoot == false){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Creating new OpLink...");
                                OpLink operatorLink = new OpLink();
                                operatorLink.setContainerName("c0");
                                operatorLink.setFromTable("R_" + inputT.getTableName().toLowerCase() + "_0");
                                operatorLink.setToTable("R_" + outputTable.getTableName().toLowerCase() + "_0");

                                StringParameter sP = new StringParameter("table", inputT.getTableName());
                                NumParameter nP = new NumParameter("part", 0);

                                List<Parameter> opParams = new LinkedList<>();
                                opParams.add(sP);
                                opParams.add(nP);

                                operatorLink.setParameters(opParams);
                                opLinksList.add(operatorLink);

                            }
                        }

                        /*----Adding Finished Query to List----*/
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Adding OperatorQuery to QueryList...");
                        allQueries.add(currentOpQuery);

                    } else {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": ListSinkOperator is not LEAF! Unsupported!");
                        System.exit(0);
                    }
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": ListSink with Non Null ColumnExprMap!!");
                    System.exit(0);
                }
            }
            else{
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported Type of Parent Operator for FileSinkOperator...");
                System.exit(0);
            }
        }
        else{
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported Type of Operator...");
            System.exit(0);
        }

        return;
    }

    public String exaremeTableDefinition(MyTable someTable){
        String definition = "";

        List<FieldSchema> allCols = someTable.getAllCols();
        for(int i = 0; i < allCols.size(); i++){
            if(i == allCols.size() - 1){
                definition = definition.concat(" "+allCols.get(i).getName() + " ");
                String type = allCols.get(i).getType();
                if(type.contains("int")){
                    definition = definition.concat("INT");
                }
                else if(type.contains("string")){
                    definition = definition.concat("TEXT");
                }
                else if(type.contains("decimal")){
                    definition = definition.concat("NUM");
                }
                else if(type.contains("char")){
                    definition = definition.concat("TEXT");
                }
                else if(type.contains("float")){
                    definition = definition.concat("NUM");
                }
                else{
                    System.out.println("exaremeTableDefinition: Unsupported Hive Type! Type: "+type);
                    System.exit(0);
                }
            }
            else{
                definition = definition.concat(" "+allCols.get(i).getName() + " ");
                String type = allCols.get(i).getType();
                if(type.contains("int")){
                    definition = definition.concat("INT,");
                }
                else if(type.contains("string")){
                    definition = definition.concat("TEXT,");
                }
                else if(type.contains("decimal")){
                    definition = definition.concat("NUM,");
                }
                else if(type.contains("float")){
                    definition = definition.concat("NUM,");
                }
                else if(type.contains("char")){
                    definition = definition.concat("TEXT,");
                }
                else{
                    System.out.println("exaremeTableDefinition: Unsupported Hive Type: "+type);
                    System.exit(0);
                }
            }
        }

        definition = "create table "+someTable.getTableName().toLowerCase()+" ("+definition+" )";

        return definition;
    }

    public String exaremeTableDefinition(MyPartition somePartition){
        String definition = "";

        List<FieldSchema> allCols = somePartition.getAllFields();

        System.out.println("Partition Columns need to be added at the end of the definition!");
        for(FieldSchema partCol : somePartition.getAllPartitionKeys()){
            allCols.add(partCol);
        }

        for(int i = 0; i < allCols.size(); i++){
            if(i == allCols.size() - 1){
                definition = definition.concat(" "+allCols.get(i).getName() + " ");
                String type = allCols.get(i).getType();
                if(type.contains("int")){
                    definition = definition.concat("INT");
                }
                else if(type.contains("string")){
                    definition = definition.concat("TEXT");
                }
                else if(type.contains("decimal")){
                    definition = definition.concat("NUM");
                }
                else if(type.contains("char")){
                    definition = definition.concat("TEXT");
                }
                else{
                    System.out.println("exaremeTableDefinition: Unsupported Hive Type! Type: "+type);
                    System.exit(0);
                }
            }
            else{
                definition = definition.concat(" "+allCols.get(i).getName() + " ");
                String type = allCols.get(i).getType();
                if(type.contains("int")){
                    definition = definition.concat("INT,");
                }
                else if(type.contains("string")){
                    definition = definition.concat("TEXT,");
                }
                else if(type.contains("decimal")){
                    definition = definition.concat("NUM,");
                }
                else if(type.contains("char")){
                    definition = definition.concat("TEXT,");
                }
                else{
                    System.out.println("exaremeTableDefinition: Unsupported Hive Type: "+type);
                    System.exit(0);
                }
            }
        }

        definition = "create table "+somePartition.getBelogingTableName().toLowerCase()+" ("+definition+" )";

        return definition;
    }


    public Integer[][] allUniqueCombinations(List<List<Integer>> listOfPartitionLists){ //[ [0,1], [0], [0,1,2] ] etc.
        int n = listOfPartitionLists.size();                                            //Credits to: http://stackoverflow.com/questions/9591561/java-cartesian-product-of-a-list-of-lists
        int solutions = 1;

        for(List<Integer> vector : listOfPartitionLists) {
            solutions *= vector.size();
        }

        Integer[][] allCombinations = new Integer[solutions][];

        for(int i = 0; i < solutions; i++) {
            Vector<Integer> combination = new Vector<Integer>(n);
            int j = 1;
            for(List<Integer> vec : listOfPartitionLists) {
                combination.add(vec.get((i/j)%vec.size()));
                j *= vec.size();
            }
            allCombinations[i] = combination.toArray(new Integer[n]);
        }

        return allCombinations;
    }

    public List<List<Integer>> createCartesianCombinationsOfInputs(List<MyTable> inputTables){

        LinkedList<List<Integer>> listOfCombinations = new LinkedList<>();

        if(inputTables.size() == 0){
            System.out.println("createCartesianCombinationsOfInputs: Empty inputTables List...");
            System.exit(0);
        }

        if(inputTables.size() == 1){ //Only 1 possible combination...
            System.out.println("createCartesianCombinationsOfInputs: Only 1 InputTable case...");
            LinkedList<Integer> combination = new LinkedList<>();
            MyTable inputTable = inputTables.get(0);

            if(inputTable.getHasPartitions() == false){
                Integer number = 0;
                combination.add(number);
            }
            else{
                if(inputTable.getAllPartitions().size() == 1){
                    Integer number = 0;
                    combination.add(number);
                }
                else{
                    for(int i = 0; i < inputTable.getAllPartitions().size(); i++){
                        combination.add(i);
                    }
                }
            }

            listOfCombinations.add(combination);
        }
        else{
            System.out.println("createCartesianCombinationsOfInputs: Multiple Input Tables...");
            List<List<Integer>> listOfPartitionLists = new LinkedList<>();

            for(MyTable inputT : inputTables){
                List<Integer> combination = new LinkedList<>();
                if(inputT.getHasPartitions() == false){
                    Integer number = 0;
                    combination.add(number);
                }
                else{
                    if(inputT.getAllPartitions().size() == 1){
                        Integer number = 0;
                        combination.add(number);
                    }
                    else{
                        for(int i = 0; i < inputT.getAllPartitions().size(); i++){
                            combination.add(i);
                        }
                    }
                }

                listOfPartitionLists.add(combination);
            }

            Integer[][] allCombos = allUniqueCombinations(listOfPartitionLists);
            for(int i = 0; i < allCombos.length; i++){
                List currentCombo = new LinkedList<>();
                for(int j = 0; j < allCombos[i].length; j++){
                    currentCombo.add(allCombos[i][j]);
                }
                listOfCombinations.add(currentCombo);
            }

        }

        System.out.println("createCartesianCombinationsOfInputs: Printing all combinations...");
        for(List<Integer> combo : listOfCombinations){
            System.out.println("createCartesianCombinationsOfInputs: "+combo.toString());
        }

        return listOfCombinations;

    }

    public List<AdpDBSelectOperator> translateToExaremeOps(){ //This method translated an OperatorQuery to An AdpDBSelectOperator

        LinkedHashMap<String, Integer> outputTablesWithInputParts = new LinkedHashMap<>();

        List<AdpDBSelectOperator> exaremeOperators = new LinkedList<>();

        System.out.println("translateToExaremeOps: -----------All Exareme Query Strings ------------\n");
        System.out.flush();
        for(OperatorQuery opQuery : allQueries){
            System.out.println("translateToExaremeOps: \tOperatorQuery: ["+opQuery.getExaremeQueryString()+" ]");
            System.out.flush();
        }

        int currentInputLevel = -1;
        System.out.println("translateToExaremeOps: Now attempting to convert every OperatorQuery object to a AdpDBSelectOperator object...");
        System.out.flush();

        //int numberOfOutputPartsForNext = 1;

        for(int i = 0; i < allQueries.size(); i++){ //For Every Operator Query
            OperatorQuery opQuery = allQueries.get(i);
            System.out.flush();

            if(opQuery.getInputTables().size() == 1){ //OperatorQuery has only 1 InputTable

                System.out.println("translateToExaremeOps: Checking if the InputTable has Partitions...");
                System.out.flush();
                boolean partitionsExist = false;
                if(opQuery.getInputTables().get(0).getHasPartitions() == true){
                    partitionsExist = true;
                }

                if(partitionsExist == true){ //SINGLE TABLE - MULTIPLE PARTITIONS

                    System.out.println("translateToExaremeOps: InputTable has Partitions and each Partition will create a different Exareme Operator!\n\t");

                    MyTable inputTable = opQuery.getInputTables().get(0);
                    List<MyPartition> myPartitions = inputTable.getAllPartitions();

                    System.out.println("translateToExaremeOps: We have: "+myPartitions.size()+" input partitions!");

                    int j = 0;
                    for(MyPartition inputPartition : myPartitions){
                        List<TableView> inputTableViews = new LinkedList<>();

                        /*---Build SQLSelect---*/
                        System.out.println("translateToExaremeOps: \tOperatorQuery: [" + opQuery.getExaremeQueryString() + " ]");
                        System.out.flush();

                        SQLSelect sqlSelect;

                        sqlSelect = new SQLSelect();
                        System.out.println("translateToExaremeOps: Setting InputDataPattern=CARTESIAN_PRODUCT for SQLSelect...");
                        System.out.flush();
                        sqlSelect.setInputDataPattern(DataPattern.cartesian_product);
                        sqlSelect.setOutputDataPattern(DataPattern.same);

                        System.out.println("translateToExaremeOps: Setting NumberOfOutputParts=1 for SQLSelect...(Partitions exist)");
                        System.out.flush();
                        sqlSelect.setNumberOfOutputPartitions(1); //means partitions exist

                        System.out.println("translateToExaremeOps: Constructing Exareme Input Table Definition...");
                        System.out.flush();

                        if (i == allQueries.size() - 1) {
                            System.out.println("translateToExaremeOps: Setting ResultTableName for SQLSelect...(TABLE WILL BE NON TEMP)");
                            System.out.flush();
                            sqlSelect.setResultTable(opQuery.getOutputTable().getTableName(), false, false);
                        } else {
                            System.out.println("translateToExaremeOps: Setting ResultTableName for SQLSelect...(TABLE WILL BE TEMP)");
                            System.out.flush();
                            sqlSelect.setResultTable(opQuery.getOutputTable().getTableName(), true, false);
                        }

                        System.out.println("translateToExaremeOps: Setting PartsDefn=null for SQLSelect...");
                        System.out.flush();
                        sqlSelect.setPartsDefn(null);
                        System.out.println("translateToExaremeOps: Setting sqlQuery for SQLSelect...");
                        System.out.flush();
                        sqlSelect.setSql(opQuery.getExaremeQueryString());
                        Comments someComments = new Comments();
                        someComments.addLine("test_comment");
                        System.out.println("translateToExaremeOps: Setting sqlQuery for Comments...");
                        System.out.flush();
                        sqlSelect.setComments(someComments);

                        madgik.exareme.common.schema.Table exaInputTable = new madgik.exareme.common.schema.Table(inputPartition.getBelogingTableName().toLowerCase());
                        String exaremeInputTdef = exaremeTableDefinition(inputPartition);

                        System.out.println("translateToExaremeOps: SQL def: " + exaremeInputTdef);
                        System.out.flush();

                        exaInputTable.setSqlDefinition(exaremeInputTdef);
                        if (currentInputLevel == -1) {
                            System.out.println("translateToExaremeOps: Setting Input Table to NON TEMP!");
                            exaInputTable.setTemp(false);
                        } else {
                            System.out.println("translateToExaremeOps: Setting Input Table to temp!");
                            exaInputTable.setTemp(true);
                        }
                        System.out.println("translateToExaremeOps: Setting Input Table Level= " + currentInputLevel);
                        System.out.flush();
                        exaInputTable.setLevel(currentInputLevel);

                        System.out.println("translateToExaremeOps: Initialising InputTableView...");
                        System.out.flush();
                        TableView inputTableView = null;

                        if (inputTable.getIsRootInput()) {
                            System.out.println("translateToExaremeOps: This Hive Partition is a Root Input and we need more details...");
                            inputTableView = new TableView(exaInputTable, inputPartition.getRootHiveLocationPath(), inputPartition.getRootHiveTableDefinition(), inputPartition.getSecondaryNeededQueries());
                            System.out.println("translateToExaremeOps: RootHiveLocationPath: " + inputPartition.getRootHiveLocationPath() + " - RootHiveTableDefinition: " + inputPartition.getRootHiveTableDefinition());
                        } else {
                            inputTableView = new TableView(exaInputTable);
                        }

                        System.out.println("translateToExaremeOps: Setting inputTableView DataPattern=CARTESIAN_PRODUCT...");
                        System.out.flush();
                        inputTableView.setPattern(DataPattern.cartesian_product);
                        System.out.println("translateToExaremeOps: Setting inputTableView setNumOfPartitions=-1");
                        System.out.flush();
                        inputTableView.setNumOfPartitions(-1);

                        System.out.println("translateToExaremeOps: Adding used Columns...");
                        System.out.flush();

                        MyMap usedColumns = opQuery.getUsedColumns();
                        List<ColumnTypePair> usedColsList = usedColumns.getColumnAndTypeList();

                        for (ColumnTypePair colPair : usedColsList) {
                            if(colPair.getColumnType().equals(inputTable.getTableName())) {
                                inputTableView.addUsedColumn(colPair.getColumnName());
                                System.out.println("translateToExaremeOps: Table: "+colPair.getColumnType()+" - Column: "+colPair.getColumnName());
                                System.out.flush();
                            }
                        }

                        inputTableViews.add(inputTableView);

                        /*---Build OutputTableView of Select---*/
                        madgik.exareme.common.schema.Table exaremeOutputTable = new madgik.exareme.common.schema.Table(opQuery.getOutputTable().getTableName());
                        TableView outputTableView;

                        System.out.println("translateToExaremeOps: Constructing Exareme Output Table Definition...");
                        System.out.flush();
                        String exaremeOutputTdef = exaremeTableDefinition(opQuery.getOutputTable());
                        System.out.println("translateToExaremeOps: SQL def: " + exaremeOutputTdef);
                        System.out.flush();

                        exaremeOutputTable.setSqlDefinition(exaremeOutputTdef);
                        if (i == allQueries.size() - 1) {
                            System.out.println("translateToExaremeOps: Setting outputTable setTemp=FALSE (final Table) ");
                            System.out.flush();
                            exaremeOutputTable.setTemp(false);

                        } else {
                            System.out.println("translateToExaremeOps: Setting outputTable setTemp=TRUE");
                            System.out.flush();
                            exaremeOutputTable.setTemp(true);
                        }
                        if (currentInputLevel == -1) {
                            System.out.println("translateToExaremeOps: Setting outputTable setLevel= 1");
                            System.out.flush();
                            exaremeOutputTable.setLevel(currentInputLevel + 2);
                        } else {
                            System.out.println("translateToExaremeOps: Setting outputTable setLevel= " + (currentInputLevel + 1));
                            System.out.flush();
                            exaremeOutputTable.setLevel(currentInputLevel + 1);
                        }

                        System.out.println("translateToExaremeOps: Initialising OutputTableView");
                        System.out.flush();
                        outputTableView = new TableView(exaremeOutputTable);
                        System.out.println("translateToExaremeOps: Setting outputTableView DataPattern=SAME ");
                        System.out.flush();
                        outputTableView.setPattern(DataPattern.same);


                        System.out.println("translateToExaremeOps: Setting outputTableView setNumOfPartitions=1 (Partitions exist!)");
                        System.out.flush();
                        outputTableView.setNumOfPartitions(1);

                        /*---Finally initialising Select---*/
                        System.out.println("translateToExaremeOps: Initialising SelectQuery");
                        System.out.flush();
                        Select selectQuery = new Select(i, sqlSelect, outputTableView);
                        System.out.println("translateToExaremeOps: Adding InputTableViews to Select...");
                        System.out.flush();
                        for (TableView inputV : inputTableViews) {
                            selectQuery.addInput(inputV);
                        }

                        System.out.println("translateToExaremeOps: Adding QueryStatement to Select...");
                        System.out.flush();

                        selectQuery.addQueryStatement(opQuery.getExaremeQueryString());
                        System.out.println("translateToExaremeOps: Setting Query of Select...");
                        System.out.flush();
                        selectQuery.setQuery(opQuery.getExaremeQueryString());
                        System.out.println("translateToExaremeOps: Setting Mappings of Select...");
                        System.out.flush();
                        selectQuery.setMappings(null);
                        System.out.println("translateToExaremeOps: Setting DatabaseDir of Select...");
                        System.out.flush();
                        List<Integer> runOnParts = selectQuery.getRunOnParts();
                        selectQuery.setDatabaseDir(currentDatabasePath);
                        System.out.println("translateToExaremeOps: Testing DataBasePath Given: " + currentDatabasePath + " - Set: " + selectQuery.getDatabaseDir());
                        System.out.flush();

                        System.out.println("translateToExaremeOps: Printing created SELECT for Debugging...\n\t" + selectQuery.toString());
                        System.out.flush();

                        boolean tableIsPreviousOutput = false;
                        for(Map.Entry<String, Integer> entry : outputTablesWithInputParts.entrySet()){
                            if(entry.getKey().equals(inputPartition.getBelogingTableName())){
                                tableIsPreviousOutput = true;
                                System.out.println("translateToExaremeOps: Input Table: "+inputTable.getTableName()+" is previous Output with PartitionCount: "+entry.getValue() +" this case should not exist! Error!");
                                System.exit(0);
                            }
                        }

                        if(tableIsPreviousOutput == false) {
                            System.out.println("translateToExaremeOps: Initialising AdpDBSelectOperator - Input Combo: "+j);
                            System.out.flush();
                            AdpDBSelectOperator exaremeSelectOperator = new AdpDBSelectOperator(AdpDBOperatorType.runQuery, selectQuery, 0);

                            exaremeSelectOperator.addInput(inputPartition.getBelogingTableName().toLowerCase(), j);

                            exaremeSelectOperator.addOutput(opQuery.getOutputTable().getTableName(), 0);

                            exaremeOperators.add(exaremeSelectOperator);

                            System.out.println("translateToExaremeOps: Printing exaremeSelectOperator...\n\t");
                            System.out.flush();

                            exaremeSelectOperator.print();

                            if(j > 0){
                                System.out.println("translateToExaremeOps: We need to create an extra OpLinks for this Operator...\n\t");
                                for(OpLink opLink : opLinksList){
                                    if(opLink.getFromTable().equals("R_"+opQuery.getOutputTable().getTableName().toLowerCase()+"_0")){
                                        OpLink brotherLink = new OpLink();
                                        brotherLink.setFromTable("R_"+opQuery.getOutputTable().getTableName().toLowerCase()+"_"+j);
                                        brotherLink.setToTable(opLink.getToTable());
                                        brotherLink.setContainerName(opLink.getContainerName());
                                        brotherLink.setParameters(opLink.getParameters());

                                        opLink.addBrother(brotherLink);
                                    }
                                }
                            }

                        }

                        j++;
                    }

                    //If output table combines partitions keep record of it for next Operators
                    boolean exists = false;
                    for(Map.Entry<String, Integer> entry : outputTablesWithInputParts.entrySet()) {
                        if(entry.getKey().equals(opQuery.getOutputTable().getTableName())){
                            exists = true;
                            break;
                        }
                    }

                    if(exists == false)
                        outputTablesWithInputParts.put(opQuery.getOutputTable().getTableName(), myPartitions.size());

                }
                else{ //SINGLE TABLE - NO PARTITIONS
                    List<TableView> inputTableViews = new LinkedList<>();

                    /*---Build SQLSelect---*/
                    System.out.println("translateToExaremeOps: \tOperatorQuery: [" + opQuery.getExaremeQueryString() + " ]");
                    System.out.flush();

                    SQLSelect sqlSelect = new SQLSelect();

                    sqlSelect = new SQLSelect();
                    System.out.println("translateToExaremeOps: Setting InputDataPattern=CARTESIAN_PRODUCT for SQLSelect...");
                    System.out.flush();
                    sqlSelect.setInputDataPattern(DataPattern.cartesian_product);
                    sqlSelect.setOutputDataPattern(DataPattern.same);

                    System.out.println("translateToExaremeOps: Setting NumberOfOutputParts=-1 for SQLSelect...(Partitions do not exist)");
                    System.out.flush();
                    sqlSelect.setNumberOfOutputPartitions(-1); //means no partitions

                    if (i == allQueries.size() - 1) {
                        System.out.println("translateToExaremeOps: Setting ResultTableName for SQLSelect...(TABLE WILL BE NON TEMP)");
                        System.out.flush();
                        sqlSelect.setResultTable(opQuery.getOutputTable().getTableName(), false, false);
                    } else {
                        System.out.println("translateToExaremeOps: Setting ResultTableName for SQLSelect...(TABLE WILL BE TEMP)");
                        System.out.flush();
                        sqlSelect.setResultTable(opQuery.getOutputTable().getTableName(), true, false);
                    }

                    System.out.println("translateToExaremeOps: Setting PartsDefn=null for SQLSelect...");
                    System.out.flush();
                    sqlSelect.setPartsDefn(null);
                    System.out.println("translateToExaremeOps: Setting sqlQuery for SQLSelect...");
                    System.out.flush();
                    sqlSelect.setSql(opQuery.getExaremeQueryString());
                    Comments someComments = new Comments();
                    someComments.addLine("test_comment");
                    System.out.println("translateToExaremeOps: Setting sqlQuery for Comments...");
                    System.out.flush();
                    sqlSelect.setComments(someComments);

                    /*--------------Build Select----------------*/
                    MyTable inputTable = opQuery.getInputTables().get(0); //Get Input Table

                    System.out.println("translateToExaremeOps: InputTable: " + inputTable.getTableName() + " does not have any partitions...");

                    System.out.println("translateToExaremeOps: Constructing Exareme Input Table Definition...");
                    System.out.flush();
                    madgik.exareme.common.schema.Table exaInputTable = new madgik.exareme.common.schema.Table(inputTable.getTableName().toLowerCase());
                    String exaremeInputTdef = exaremeTableDefinition(inputTable);

                    System.out.println("translateToExaremeOps: SQL def: " + exaremeInputTdef);
                    System.out.flush();
                    exaInputTable.setSqlDefinition(exaremeInputTdef);
                    if (currentInputLevel == -1) {
                        System.out.println("translateToExaremeOps: Setting Input Table to NON TEMP!");
                        exaInputTable.setTemp(false);
                    } else {
                        System.out.println("translateToExaremeOps: Setting Input Table to temp!");
                        exaInputTable.setTemp(true);
                    }
                    System.out.println("translateToExaremeOps: Setting Input Table Level= " + currentInputLevel);
                    System.out.flush();
                    exaInputTable.setLevel(currentInputLevel);

                    System.out.println("translateToExaremeOps: Initialising InputTableView...");
                    System.out.flush();
                    TableView inputTableView = null;

                    if (inputTable.getIsRootInput()) {
                        System.out.println("translateToExaremeOps: This Hive Table is a Root Input and we need more details...");
                        inputTableView = new TableView(exaInputTable, inputTable.getRootHiveLocationPath(), inputTable.getRootHiveTableDefinition(), null);
                        System.out.println("translateToExaremeOps: RootHiveLocationPath: " + inputTable.getRootHiveLocationPath() + " - RootHiveTableDefinition: " + inputTable.getRootHiveTableDefinition());
                    } else {
                        inputTableView = new TableView(exaInputTable);
                    }

                    System.out.println("translateToExaremeOps: Setting inputTableView DataPattern=CARTESIAN_PRODUCT...");
                    System.out.flush();
                    inputTableView.setPattern(DataPattern.cartesian_product);
                    System.out.println("translateToExaremeOps: Setting inputTableView setNumOfPartitions=-1");
                    System.out.flush();
                    inputTableView.setNumOfPartitions(-1);

                    System.out.println("translateToExaremeOps: Adding used Columns...");
                    System.out.flush();

                    MyMap usedColumns = opQuery.getUsedColumns();
                    List<ColumnTypePair> usedColsList = usedColumns.getColumnAndTypeList();

                    for (ColumnTypePair colPair : usedColsList) {
                        if(colPair.getColumnType().equals(inputTable.getTableName())) {
                            inputTableView.addUsedColumn(colPair.getColumnName());
                            System.out.println("translateToExaremeOps: Table: "+colPair.getColumnType()+" - Column: "+colPair.getColumnName());
                            System.out.flush();
                        }
                    }

                    inputTableViews.add(inputTableView);

                     /*---Build OutputTableView of Select---*/
                    madgik.exareme.common.schema.Table exaremeOutputTable = new madgik.exareme.common.schema.Table(opQuery.getOutputTable().getTableName());
                    TableView outputTableView;

                    System.out.println("translateToExaremeOps: Constructing Exareme Output Table Definition...");
                    System.out.flush();
                    String exaremeOutputTdef = exaremeTableDefinition(opQuery.getOutputTable());
                    System.out.println("translateToExaremeOps: SQL def: " + exaremeOutputTdef);
                    System.out.flush();

                    exaremeOutputTable.setSqlDefinition(exaremeOutputTdef);
                    if (i == allQueries.size() - 1) {
                        System.out.println("translateToExaremeOps: Setting outputTable setTemp=FALSE (final Table) ");
                        System.out.flush();
                        exaremeOutputTable.setTemp(false);

                    } else {
                        System.out.println("translateToExaremeOps: Setting outputTable setTemp=TRUE");
                        System.out.flush();
                        exaremeOutputTable.setTemp(true);
                    }
                    if (currentInputLevel == -1) {
                        System.out.println("translateToExaremeOps: Setting outputTable setLevel= 1");
                        System.out.flush();
                        exaremeOutputTable.setLevel(currentInputLevel + 2);
                    } else {
                        System.out.println("translateToExaremeOps: Setting outputTable setLevel= " + (currentInputLevel + 1));
                        System.out.flush();
                        exaremeOutputTable.setLevel(currentInputLevel + 1);
                    }

                    System.out.println("translateToExaremeOps: Initialising OutputTableView");
                    System.out.flush();
                    outputTableView = new TableView(exaremeOutputTable);
                    System.out.println("translateToExaremeOps: Setting outputTableView DataPattern=SAME ");
                    System.out.flush();
                    outputTableView.setPattern(DataPattern.same);

                    System.out.println("translateToExaremeOps: Setting outputTableView setNumOfPartitions=-1 (Partitions do not exist!)");
                    System.out.flush();
                    outputTableView.setNumOfPartitions(-1);

                    /*---Finally initialising Select---*/
                    System.out.println("translateToExaremeOps: Initialising SelectQuery");
                    System.out.flush();
                    Select selectQuery = new Select(i, sqlSelect, outputTableView);
                    System.out.println("translateToExaremeOps: Adding InputTableViews to Select...");
                    System.out.flush();
                    for (TableView inputV : inputTableViews) {
                        selectQuery.addInput(inputV);
                    }
                    System.out.println("translateToExaremeOps: Adding QueryStatement to Select...");
                    System.out.flush();
                    selectQuery.addQueryStatement(opQuery.getExaremeQueryString());
                    System.out.println("translateToExaremeOps: Setting Query of Select...");
                    System.out.flush();
                    selectQuery.setQuery(opQuery.getExaremeQueryString());
                    System.out.println("translateToExaremeOps: Setting Mappings of Select...");
                    System.out.flush();
                    selectQuery.setMappings(null);
                    System.out.println("translateToExaremeOps: Setting DatabaseDir of Select...");
                    System.out.flush();
                    List<Integer> runOnParts = selectQuery.getRunOnParts();
                    selectQuery.setDatabaseDir(currentDatabasePath);
                    System.out.println("translateToExaremeOps: Testing DataBasePath Given: " + currentDatabasePath + " - Set: " + selectQuery.getDatabaseDir());
                    System.out.flush();

                    System.out.println("translateToExaremeOps: Printing created SELECT for Debugging...\n\t" + selectQuery.toString());
                    System.out.flush();

                    System.out.println("translateToExaremeOps: Initialising AdpDBSelectOperator - (No Input Partitions Case)");
                    System.out.flush();
                    AdpDBSelectOperator exaremeSelectOperator = new AdpDBSelectOperator(AdpDBOperatorType.runQuery, selectQuery, 0);

                    boolean tableIsPreviousOutput = false;
                    for(Map.Entry<String, Integer> entry : outputTablesWithInputParts.entrySet()){
                        if(entry.getKey().equals(opQuery.getInputTables().get(0).getTableName())){
                            tableIsPreviousOutput = true;
                            System.out.println("translateToExaremeOps: Input Table: "+opQuery.getInputTables().get(0).getTableName()+" is previous Output with PartitionCount: "+entry.getValue());
                            for(int k = 0; k < entry.getValue(); k++){
                                exaremeSelectOperator.addInput(opQuery.getInputTables().get(0).getTableName().toLowerCase(), 0);
                            }
                            break;
                        }
                    }

                    if(tableIsPreviousOutput == false) {
                        exaremeSelectOperator.addInput(opQuery.getInputTables().get(0).getTableName().toLowerCase(), 0);
                    }

                    exaremeSelectOperator.addOutput(opQuery.getOutputTable().getTableName(), 0);

                    exaremeOperators.add(exaremeSelectOperator);

                    System.out.println("translateToExaremeOps: Printing exaremeSelectOperator...\n\t");
                    System.out.flush();

                    exaremeSelectOperator.print();

                }

            }
            else { //OperatorQuery has multiple Input Tables
                System.out.println("translateToExaremeOps: More than 1 Input Table for OpQuery!");

                System.out.println("translateToExaremeOps: Checking if any of the InputTables has Partitions...");
                System.out.flush();
                boolean partitionsExist = false;
                for(MyTable inputTable : opQuery.getInputTables()){
                    if(inputTable.getHasPartitions() == true){
                        partitionsExist = true;
                        break;
                    }
                }

                if(partitionsExist == false){
                    List<TableView> inputTableViews = new LinkedList<>();

                    /*---Build SQLSelect---*/
                    System.out.println("translateToExaremeOps: \tOperatorQuery: [" + opQuery.getExaremeQueryString() + " ]");
                    System.out.flush();

                    SQLSelect sqlSelect = new SQLSelect();
                    System.out.println("translateToExaremeOps: Setting InputDataPattern=CARTESIAN_PRODUCT for SQLSelect...");
                    System.out.flush();
                    sqlSelect.setInputDataPattern(DataPattern.cartesian_product);
                    sqlSelect.setOutputDataPattern(DataPattern.same);

                    System.out.println("translateToExaremeOps: Setting NumberOfOutputParts=-1 for SQLSelect...(Since partitions do not exist)");
                    System.out.flush();
                    sqlSelect.setNumberOfOutputPartitions(-1); //means no partitions in input tables

                    if (i == allQueries.size() - 1) {
                        System.out.println("translateToExaremeOps: Setting ResultTableName for SQLSelect...(TABLE WILL BE NON TEMP)");
                        System.out.flush();
                        sqlSelect.setResultTable(opQuery.getOutputTable().getTableName(), false, false);
                    } else {
                        System.out.println("translateToExaremeOps: Setting ResultTableName for SQLSelect...(TABLE WILL BE TEMP)");
                        System.out.flush();
                        sqlSelect.setResultTable(opQuery.getOutputTable().getTableName(), true, false);
                    }

                    System.out.println("translateToExaremeOps: Setting PartsDefn=null for SQLSelect...");
                    System.out.flush();
                    sqlSelect.setPartsDefn(null);
                    System.out.println("translateToExaremeOps: Setting sqlQuery for SQLSelect...");
                    System.out.flush();
                    sqlSelect.setSql(opQuery.getExaremeQueryString());
                    Comments someComments = new Comments();
                    someComments.addLine("test_comment");
                    System.out.println("translateToExaremeOps: Setting sqlQuery for Comments...");
                    System.out.flush();
                    sqlSelect.setComments(someComments);

                    /*--------------Build Select----------------*/
                    for (MyTable inputTable : opQuery.getInputTables()) {
                        System.out.println("translateToExaremeOps: Constructing Exareme Input Table Definition...");
                        System.out.flush();
                        madgik.exareme.common.schema.Table exaInputTable = new madgik.exareme.common.schema.Table(inputTable.getTableName().toLowerCase());
                        String exaremeInputTdef = exaremeTableDefinition(inputTable);
                        System.out.println("translateToExaremeOps: SQL def: " + exaremeInputTdef);
                        System.out.flush();
                        exaInputTable.setSqlDefinition(exaremeInputTdef);
                        if (currentInputLevel == -1) {
                            System.out.println("translateToExaremeOps: Setting Input Table to NON TEMP!");
                            exaInputTable.setTemp(false);
                        } else {
                            System.out.println("translateToExaremeOps: Setting Input Table to temp!");
                            exaInputTable.setTemp(true);
                        }
                        System.out.println("translateToExaremeOps: Setting Input Table Level= " + currentInputLevel);
                        System.out.flush();
                        exaInputTable.setLevel(currentInputLevel);

                        System.out.println("translateToExaremeOps: Initialising InputTableView...");
                        System.out.flush();
                        TableView inputTableView = null;

                        if (inputTable.getIsRootInput()) {
                            System.out.println("translateToExaremeOps: This Hive Table is a Root Input and we need more details...");
                            inputTableView = new TableView(exaInputTable, inputTable.getRootHiveLocationPath(), inputTable.getRootHiveTableDefinition(), null);
                            System.out.println("translateToExaremeOps: RootHiveLocationPath: " + inputTable.getRootHiveLocationPath() + " - RootHiveTableDefinition: " + inputTable.getRootHiveTableDefinition());
                        } else {
                            inputTableView = new TableView(exaInputTable);
                        }

                        System.out.println("translateToExaremeOps: Setting inputTableView DataPattern=CARTESIAN_PRODUCT...");
                        System.out.flush();
                        inputTableView.setPattern(DataPattern.cartesian_product);
                        System.out.println("translateToExaremeOps: Setting inputTableView setNumOfPartitions=-1");
                        System.out.flush();
                        inputTableView.setNumOfPartitions(-1);


                        System.out.println("translateToExaremeOps: Adding used Columns...");
                        System.out.flush();

                        MyMap usedColumns = opQuery.getUsedColumns();
                        List<ColumnTypePair> usedColsList = usedColumns.getColumnAndTypeList();

                        for (ColumnTypePair colPair : usedColsList) {
                            if(colPair.getColumnType().equals(inputTable.getTableName())) {
                                inputTableView.addUsedColumn(colPair.getColumnName());
                                System.out.println("translateToExaremeOps: Table: "+colPair.getColumnType()+" - Column: "+colPair.getColumnName());
                                System.out.flush();
                            }
                        }

                        inputTableViews.add(inputTableView);

                    }

                    /*---Build OutputTableView of Select---*/
                    madgik.exareme.common.schema.Table exaremeOutputTable = new madgik.exareme.common.schema.Table(opQuery.getOutputTable().getTableName());
                    TableView outputTableView;

                    System.out.println("translateToExaremeOps: Constructing Exareme Output Table Definition...");
                    System.out.flush();
                    String exaremeOutputTdef = exaremeTableDefinition(opQuery.getOutputTable());
                    System.out.println("translateToExaremeOps: SQL def: " + exaremeOutputTdef);
                    System.out.flush();

                    exaremeOutputTable.setSqlDefinition(exaremeOutputTdef);
                    if (i == allQueries.size() - 1) {
                        System.out.println("translateToExaremeOps: Setting outputTable setTemp=FALSE (final Table) ");
                        System.out.flush();
                        exaremeOutputTable.setTemp(false);

                    } else {
                        System.out.println("translateToExaremeOps: Setting outputTable setTemp=TRUE");
                        System.out.flush();
                        exaremeOutputTable.setTemp(true);
                    }
                    if (currentInputLevel == -1) {
                        System.out.println("translateToExaremeOps: Setting outputTable setLevel= 1");
                        System.out.flush();
                        exaremeOutputTable.setLevel(currentInputLevel + 2);
                    } else {
                        System.out.println("translateToExaremeOps: Setting outputTable setLevel= " + (currentInputLevel + 1));
                        System.out.flush();
                        exaremeOutputTable.setLevel(currentInputLevel + 1);
                    }

                    System.out.println("translateToExaremeOps: Initialising OutputTableView");
                    System.out.flush();
                    outputTableView = new TableView(exaremeOutputTable);
                    System.out.println("translateToExaremeOps: Setting outputTableView DataPattern=SAME ");
                    System.out.flush();
                    outputTableView.setPattern(DataPattern.same);

                    System.out.println("translateToExaremeOps: Setting outputTableView setNumOfPartitions=-1 (NO Partitions exist)");
                    System.out.flush();
                    outputTableView.setNumOfPartitions(-1);

                    /*---Finally initialising Select---*/
                    System.out.println("translateToExaremeOps: Initialising SelectQuery");
                    System.out.flush();
                    Select selectQuery = new Select(i, sqlSelect, outputTableView);
                    System.out.println("translateToExaremeOps: Adding InputTableViews to Select...");
                    System.out.flush();
                    for (TableView inputV : inputTableViews) {
                        selectQuery.addInput(inputV);
                    }
                    System.out.println("translateToExaremeOps: Adding QueryStatement to Select...");
                    System.out.flush();
                    selectQuery.addQueryStatement(opQuery.getExaremeQueryString());
                    System.out.println("translateToExaremeOps: Setting Query of Select...");
                    System.out.flush();
                    selectQuery.setQuery(opQuery.getExaremeQueryString());
                    System.out.println("translateToExaremeOps: Setting Mappings of Select...");
                    System.out.flush();
                    selectQuery.setMappings(null);
                    System.out.println("translateToExaremeOps: Setting DatabaseDir of Select...");
                    System.out.flush();
                    List<Integer> runOnParts = selectQuery.getRunOnParts();
                    selectQuery.setDatabaseDir(currentDatabasePath);
                    System.out.println("translateToExaremeOps: Testing DataBasePath Given: " + currentDatabasePath + " - Set: " + selectQuery.getDatabaseDir());
                    System.out.flush();

                    System.out.println("translateToExaremeOps: Printing created SELECT for Debugging...\n\t" + selectQuery.toString());
                    System.out.flush();

                    System.out.println("translateToExaremeOps: Initialising AdpDBSelectOperator - (No Input Partitions Case)");
                    System.out.flush();
                    AdpDBSelectOperator exaremeSelectOperator = new AdpDBSelectOperator(AdpDBOperatorType.runQuery, selectQuery, 0);

                    for (MyTable inputT : opQuery.getInputTables()) {
                        boolean tableIsPreviousOutput = false;
                        for(Map.Entry<String, Integer> entry : outputTablesWithInputParts.entrySet()){
                            if(entry.getKey().equals(inputT.getTableName())){
                                tableIsPreviousOutput = true;
                                System.out.println("translateToExaremeOps: Input Table: "+inputT.getTableName()+" is previous Output with PartitionCount: "+entry.getValue());
                                for(int k = 0; k < entry.getValue(); k++){
                                    exaremeSelectOperator.addInput(inputT.getTableName().toLowerCase(), 0);
                                }
                                break;
                            }
                        }

                        if(tableIsPreviousOutput == false) {
                            exaremeSelectOperator.addInput(inputT.getTableName().toLowerCase(), 0);
                        }

                    }

                    exaremeSelectOperator.addOutput(opQuery.getOutputTable().getTableName(), 0);

                    exaremeOperators.add(exaremeSelectOperator);

                    System.out.println("translateToExaremeOps: Printing exaremeSelectOperator...\n\t");
                    System.out.flush();

                    exaremeSelectOperator.print();
                }
                else{ //Some Tables may have partitions

                    //Locate all possible combinations of Partitions
                    int partsToBeCreated = 1;
                    for(MyTable inputT : opQuery.getInputTables()){
                        if(inputT.getHasPartitions()){
                            partsToBeCreated = partsToBeCreated * inputT.getAllPartitions().size();
                        }
                    }

                    System.out.println("translateToExaremeOps: Since partitionsExist we must create: "+partsToBeCreated+" AdpDBSelectOperators...");

                    List<List<Integer>> listOfCombinations = createCartesianCombinationsOfInputs(opQuery.getInputTables());

                    if(listOfCombinations.size() != partsToBeCreated){
                        System.out.println("translateToExaremeOps: listOfCombinations.size() == "+listOfCombinations.size()+" does not equals partToBeCreated number!");
                        System.exit(0);
                    }

                    for(List<Integer> combination : listOfCombinations){ //Get every partition combination
                        System.out.println("translateToExaremeOps: Current combination: "+combination);

                        List<TableView> inputTableViews = new LinkedList<>();

                        /*---Build SQLSelect---*/
                        System.out.println("translateToExaremeOps: \tOperatorQuery: [" + opQuery.getExaremeQueryString() + " ]");
                        System.out.flush();

                        SQLSelect sqlSelect = new SQLSelect();
                        System.out.println("translateToExaremeOps: Setting InputDataPattern=CARTESIAN_PRODUCT for SQLSelect...");
                        System.out.flush();
                        sqlSelect.setInputDataPattern(DataPattern.cartesian_product);
                        sqlSelect.setOutputDataPattern(DataPattern.same);

                        System.out.println("translateToExaremeOps: Setting NumberOfOutputParts=-1 for SQLSelect...(Since partitions do not exist)");
                        System.out.flush();
                        sqlSelect.setNumberOfOutputPartitions(-1); //means no partitions in input tables

                        if (i == allQueries.size() - 1) {
                            System.out.println("translateToExaremeOps: Setting ResultTableName for SQLSelect...(TABLE WILL BE NON TEMP)");
                            System.out.flush();
                            sqlSelect.setResultTable(opQuery.getOutputTable().getTableName(), false, false);
                        } else {
                            System.out.println("translateToExaremeOps: Setting ResultTableName for SQLSelect...(TABLE WILL BE TEMP)");
                            System.out.flush();
                            sqlSelect.setResultTable(opQuery.getOutputTable().getTableName(), true, false);
                        }

                        System.out.println("translateToExaremeOps: Setting PartsDefn=null for SQLSelect...");
                        System.out.flush();
                        sqlSelect.setPartsDefn(null);
                        System.out.println("translateToExaremeOps: Setting sqlQuery for SQLSelect...");
                        System.out.flush();
                        sqlSelect.setSql(opQuery.getExaremeQueryString());
                        Comments someComments = new Comments();
                        someComments.addLine("test_comment");
                        System.out.println("translateToExaremeOps: Setting sqlQuery for Comments...");
                        System.out.flush();
                        sqlSelect.setComments(someComments);

                        /*--------------Build Select----------------*/
                        int j = 0;
                        for (MyTable inputTable : opQuery.getInputTables()) {
                            boolean usePartition = false;
                            MyPartition inputPartition = new MyPartition();

                            if (inputTable.getHasPartitions() == false) {
                                System.out.println("translateToExaremeOps: InputTable: " + inputTable.getTableName() + " does not have any partitions...");
                            } else {
                                System.out.println("translateToExaremeOps: InputTable: " + inputTable.getTableName() + " has Partitions... - CURRENT PARTITION: "+combination.get(j));
                                usePartition = true;
                            }

                            if(usePartition == true){
                                inputPartition = inputTable.getAllPartitions().get(j);
                            }

                            if(usePartition == true){
                                System.out.println("translateToExaremeOps: Constructing Exareme Input Table Definition...");
                                System.out.flush();
                                madgik.exareme.common.schema.Table exaInputTable = new madgik.exareme.common.schema.Table(inputPartition.getBelogingTableName().toLowerCase());
                                String exaremeInputTdef = exaremeTableDefinition(inputPartition);
                                System.out.println("translateToExaremeOps: SQL def: " + exaremeInputTdef);
                                System.out.flush();
                                exaInputTable.setSqlDefinition(exaremeInputTdef);
                                if (currentInputLevel == -1) {
                                    System.out.println("translateToExaremeOps: Setting Input Table to NON TEMP!");
                                    exaInputTable.setTemp(false);
                                } else {
                                    System.out.println("translateToExaremeOps: Setting Input Table to temp!");
                                    exaInputTable.setTemp(true);
                                }
                                System.out.println("translateToExaremeOps: Setting Input Table Level= " + currentInputLevel);
                                System.out.flush();
                                exaInputTable.setLevel(currentInputLevel);

                                System.out.println("translateToExaremeOps: Initialising InputTableView...");
                                System.out.flush();
                                TableView inputTableView = null;

                                if (inputTable.getIsRootInput()) {
                                    System.out.println("translateToExaremeOps: This Hive Table is a Root Input and we need more details...");
                                    inputTableView = new TableView(exaInputTable, inputPartition.getRootHiveLocationPath(), inputPartition.getRootHiveTableDefinition(), inputPartition.getSecondaryNeededQueries());
                                    System.out.println("translateToExaremeOps: RootHiveLocationPath: " + inputPartition.getRootHiveLocationPath() + " - RootHiveTableDefinition: " + inputPartition.getRootHiveTableDefinition());
                                } else {
                                    inputTableView = new TableView(exaInputTable);
                                }

                                System.out.println("translateToExaremeOps: Setting inputTableView DataPattern=CARTESIAN_PRODUCT...");
                                System.out.flush();
                                inputTableView.setPattern(DataPattern.cartesian_product);
                                System.out.println("translateToExaremeOps: Setting inputTableView setNumOfPartitions=-1");
                                System.out.flush();
                                inputTableView.setNumOfPartitions(-1);

                                System.out.println("translateToExaremeOps: Adding used Columns...");
                                System.out.flush();

                                MyMap usedColumns = opQuery.getUsedColumns();
                                List<ColumnTypePair> usedColsList = usedColumns.getColumnAndTypeList();

                                for (ColumnTypePair colPair : usedColsList) {
                                    if(colPair.getColumnType().equals(inputTable.getTableName())) {
                                        inputTableView.addUsedColumn(colPair.getColumnName());
                                        System.out.println("translateToExaremeOps: Table: "+colPair.getColumnType()+" - Column: "+colPair.getColumnName());
                                        System.out.flush();
                                    }
                                }

                                inputTableViews.add(inputTableView);
                            }
                            else{
                                System.out.println("translateToExaremeOps: Constructing Exareme Input Table Definition...");
                                System.out.flush();
                                madgik.exareme.common.schema.Table exaInputTable = new madgik.exareme.common.schema.Table(inputTable.getTableName().toLowerCase());
                                String exaremeInputTdef = exaremeTableDefinition(inputTable);
                                System.out.println("translateToExaremeOps: SQL def: " + exaremeInputTdef);
                                System.out.flush();
                                exaInputTable.setSqlDefinition(exaremeInputTdef);
                                if (currentInputLevel == -1) {
                                    System.out.println("translateToExaremeOps: Setting Input Table to NON TEMP!");
                                    exaInputTable.setTemp(false);
                                } else {
                                    System.out.println("translateToExaremeOps: Setting Input Table to temp!");
                                    exaInputTable.setTemp(true);
                                }
                                System.out.println("translateToExaremeOps: Setting Input Table Level= " + currentInputLevel);
                                System.out.flush();
                                exaInputTable.setLevel(currentInputLevel);

                                System.out.println("translateToExaremeOps: Initialising InputTableView...");
                                System.out.flush();
                                TableView inputTableView = null;

                                if (inputTable.getIsRootInput()) {
                                    System.out.println("translateToExaremeOps: This Hive Table is a Root Input and we need more details...");
                                    inputTableView = new TableView(exaInputTable, inputTable.getRootHiveLocationPath(), inputTable.getRootHiveTableDefinition(), null);
                                    System.out.println("translateToExaremeOps: RootHiveLocationPath: " + inputTable.getRootHiveLocationPath() + " - RootHiveTableDefinition: " + inputTable.getRootHiveTableDefinition());
                                } else {
                                    inputTableView = new TableView(exaInputTable);
                                }

                                System.out.println("translateToExaremeOps: Setting inputTableView DataPattern=CARTESIAN_PRODUCT...");
                                System.out.flush();
                                inputTableView.setPattern(DataPattern.cartesian_product);
                                System.out.println("translateToExaremeOps: Setting inputTableView setNumOfPartitions=-1");
                                System.out.flush();
                                inputTableView.setNumOfPartitions(-1);

                                System.out.println("translateToExaremeOps: Adding used Columns...");
                                System.out.flush();

                                MyMap usedColumns = opQuery.getUsedColumns();
                                List<ColumnTypePair> usedColsList = usedColumns.getColumnAndTypeList();

                                for (ColumnTypePair colPair : usedColsList) {
                                    if(colPair.getColumnType().equals(inputTable.getTableName())) {
                                        inputTableView.addUsedColumn(colPair.getColumnName());
                                        System.out.println("translateToExaremeOps: Table: "+colPair.getColumnType()+" - Column: "+colPair.getColumnName());
                                        System.out.flush();
                                    }
                                }

                                inputTableViews.add(inputTableView);
                            }

                            /*---Build OutputTableView of Select---*/
                            madgik.exareme.common.schema.Table exaremeOutputTable = new madgik.exareme.common.schema.Table(opQuery.getOutputTable().getTableName());
                            TableView outputTableView;

                            System.out.println("translateToExaremeOps: Constructing Exareme Output Table Definition...");
                            System.out.flush();
                            String exaremeOutputTdef = exaremeTableDefinition(opQuery.getOutputTable());
                            System.out.println("translateToExaremeOps: SQL def: " + exaremeOutputTdef);
                            System.out.flush();

                            exaremeOutputTable.setSqlDefinition(exaremeOutputTdef);
                            if (i == allQueries.size() - 1) {
                                System.out.println("translateToExaremeOps: Setting outputTable setTemp=FALSE (final Table) ");
                                System.out.flush();
                                exaremeOutputTable.setTemp(false);

                            } else {
                                System.out.println("translateToExaremeOps: Setting outputTable setTemp=TRUE");
                                System.out.flush();
                                exaremeOutputTable.setTemp(true);
                            }
                            if (currentInputLevel == -1) {
                                System.out.println("translateToExaremeOps: Setting outputTable setLevel= 1");
                                System.out.flush();
                                exaremeOutputTable.setLevel(currentInputLevel + 2);
                            } else {
                                System.out.println("translateToExaremeOps: Setting outputTable setLevel= " + (currentInputLevel + 1));
                                System.out.flush();
                                exaremeOutputTable.setLevel(currentInputLevel + 1);
                            }

                            System.out.println("translateToExaremeOps: Initialising OutputTableView");
                            System.out.flush();
                            outputTableView = new TableView(exaremeOutputTable);
                            System.out.println("translateToExaremeOps: Setting outputTableView DataPattern=SAME ");
                            System.out.flush();
                            outputTableView.setPattern(DataPattern.same);

                            System.out.println("translateToExaremeOps: Setting outputTableView setNumOfPartitions=1 (Partitions exist)");
                            System.out.flush();
                            outputTableView.setNumOfPartitions(1);

                            /*---Finally initialising Select---*/
                            System.out.println("translateToExaremeOps: Initialising SelectQuery");
                            System.out.flush();
                            Select selectQuery = new Select(i, sqlSelect, outputTableView);
                            System.out.println("translateToExaremeOps: Adding InputTableViews to Select...");
                            System.out.flush();
                            for (TableView inputV : inputTableViews) {
                                selectQuery.addInput(inputV);
                            }
                            System.out.println("translateToExaremeOps: Adding QueryStatement to Select...");
                            System.out.flush();
                            selectQuery.addQueryStatement(opQuery.getExaremeQueryString());
                            System.out.println("translateToExaremeOps: Setting Query of Select...");
                            System.out.flush();
                            selectQuery.setQuery(opQuery.getExaremeQueryString());
                            System.out.println("translateToExaremeOps: Setting Mappings of Select...");
                            System.out.flush();
                            selectQuery.setMappings(null);
                            System.out.println("translateToExaremeOps: Setting DatabaseDir of Select...");
                            System.out.flush();
                            List<Integer> runOnParts = selectQuery.getRunOnParts();
                            selectQuery.setDatabaseDir(currentDatabasePath);
                            System.out.println("translateToExaremeOps: Testing DataBasePath Given: " + currentDatabasePath + " - Set: " + selectQuery.getDatabaseDir());
                            System.out.flush();

                            System.out.println("translateToExaremeOps: Printing created SELECT for Debugging...\n\t" + selectQuery.toString());
                            System.out.flush();

                            System.out.println("translateToExaremeOps: Initialising AdpDBSelectOperator - (No Input Partitions Case)");
                            System.out.flush();
                            AdpDBSelectOperator exaremeSelectOperator = new AdpDBSelectOperator(AdpDBOperatorType.runQuery, selectQuery, 0);

                            int l = 0;
                            for (MyTable inputT : opQuery.getInputTables()) {
                                boolean tableIsPreviousOutput = false;
                                for(Map.Entry<String, Integer> entry : outputTablesWithInputParts.entrySet()){
                                    if(entry.getKey().equals(inputT.getTableName())){
                                        tableIsPreviousOutput = true;
                                        System.out.println("translateToExaremeOps: Input Table: "+inputT.getTableName()+" is previous Output with PartitionCount: "+entry.getValue());
                                        for(int k = 0; k < entry.getValue(); k++){
                                            exaremeSelectOperator.addInput(inputT.getTableName().toLowerCase(), 0);
                                        }
                                        break;
                                    }
                                }

                                if(tableIsPreviousOutput == false) {
                                    exaremeSelectOperator.addInput(inputT.getTableName().toLowerCase(), combination.get(l));
                                }

                                l++;
                            }

                            exaremeSelectOperator.addOutput(opQuery.getOutputTable().getTableName(), 0);

                            exaremeOperators.add(exaremeSelectOperator);

                            System.out.println("translateToExaremeOps: Printing exaremeSelectOperator...\n\t");
                            System.out.flush();

                            exaremeSelectOperator.print();

                            //Create extra OpLinks
                            if(partitionsExist){
                                if(combination.get(j) > 0){
                                    System.out.println("translateToExaremeOps: We need to create an extra OpLinks for this Operator...\n\t");
                                    for(OpLink opLink : opLinksList){
                                        if(opLink.getFromTable().equals("R_"+opQuery.getOutputTable().getTableName().toLowerCase()+"_0")){
                                            OpLink brotherLink = new OpLink();
                                            brotherLink.setFromTable("R_"+opQuery.getOutputTable().getTableName().toLowerCase()+"_"+combination.get(j));
                                            brotherLink.setToTable(opLink.getToTable());
                                            brotherLink.setContainerName(opLink.getContainerName());
                                            brotherLink.setParameters(opLink.getParameters());

                                            opLink.addBrother(brotherLink);
                                        }
                                    }
                                }
                            }

                            j++;
                        }

                    }

                    //If output table combines partitions keep record of it for next Operators
                    boolean exists = false;
                    for(Map.Entry<String, Integer> entry : outputTablesWithInputParts.entrySet()) {
                        if(entry.getKey().equals(opQuery.getOutputTable().getTableName())){
                            exists = true;
                            break;
                        }
                    }

                    if(exists == false)
                        outputTablesWithInputParts.put(opQuery.getOutputTable().getTableName(), partsToBeCreated);

                }

            }

            if(i == allQueries.size() - 1){ //TODO

                SQLSelect sqlSelect2 = new SQLSelect();
                if(opQuery.getInputTables().size() == 1){ //TableUnionReplicator with only 1 Input Table

                    /*---Build SQLSelect---*/
                    System.out.println("translateToExaremeOps: \tOperatorQuery: [" + opQuery.getExaremeQueryString() + " ]");
                    System.out.flush();
                    sqlSelect2 = new SQLSelect();
                    System.out.println("translateToExaremeOps: Setting InputDataPattern=CARTESIAN_PRODUCT for SQLSelect...");
                    System.out.flush();
                    sqlSelect2.setInputDataPattern(DataPattern.cartesian_product);
                    sqlSelect2.setOutputDataPattern(DataPattern.same);

                    boolean partitionsExist = false;
                    if(opQuery.getInputTables().get(0).getHasPartitions() == true){
                        partitionsExist = true;
                    }

                    if(partitionsExist == true){
                        System.out.println("translateToExaremeOps: Setting NumberOfOutputParts=1 for SQLSelect...(Partitions exist)");
                        System.out.flush();
                        sqlSelect2.setNumberOfOutputPartitions(1); //means partitions exist
                    }
                    else{
                        System.out.println("translateToExaremeOps: Setting NumberOfOutputParts=-1 for SQLSelect...(Partitions do not exist)");
                        System.out.flush();
                        sqlSelect2.setNumberOfOutputPartitions(-1); //means no partitions
                    }

                    System.out.println("translateToExaremeOps: Setting ResultTableName for SQLSelect...(NON TEMP TABLE)");
                    System.out.flush();
                    sqlSelect2.setResultTable(opQuery.getOutputTable().getTableName(), false, false);
                    System.out.println("translateToExaremeOps: Setting PartsDefn=null for SQLSelect...");
                    System.out.flush();
                    sqlSelect2.setPartsDefn(null);
                    System.out.println("translateToExaremeOps: Setting sqlQuery for SQLSelect...");
                    System.out.flush();
                    sqlSelect2.setSql(opQuery.getExaremeQueryString());
                    Comments someComments2 = new Comments();
                    someComments2.addLine("test_comment");
                    System.out.println("translateToExaremeOps: Setting sqlQuery for Comments...");
                    System.out.flush();
                    sqlSelect2.setComments(someComments2);

                    /*--------------Build Select----------------*/

                    /*---Build InputTableViews of Select---*/
                    List<TableView> inputTableViews2 = new LinkedList<>();
                    MyTable inputTable2 = opQuery.getInputTables().get(0);

                    //Now Check if InputTable has Partitions and treat Partition Case accordingly
                    if(inputTable2.getHasPartitions() == false) { //TableUnionReplicator for Single Table with no Partitions

                        System.out.println("translateToExaremeOps: Constructing Exareme Input Table Definition...");
                        System.out.flush();
                        madgik.exareme.common.schema.Table exaInputTable2 = new madgik.exareme.common.schema.Table(inputTable2.getTableName().toLowerCase());
                        String exaremeInputTdef2 = exaremeTableDefinition(inputTable2);
                        System.out.println("translateToExaremeOps: SQL def: " + exaremeInputTdef2);
                        System.out.flush();
                        exaInputTable2.setSqlDefinition(exaremeInputTdef2);

                        System.out.println("translateToExaremeOps: Setting Input Table to temp!");
                        exaInputTable2.setTemp(true);

                        System.out.println("translateToExaremeOps: Setting Input Table Level= " + currentInputLevel);
                        System.out.flush();
                        exaInputTable2.setLevel(currentInputLevel);

                        System.out.println("translateToExaremeOps: Initialising InputTableView...");
                        System.out.flush();
                        TableView inputTableView2;

                        //if(inputTable2.getIsRootInput()){
                            //System.out.println("translateToExaremeOps: This Hive Table is a Root Input and we need more details...");
                            //inputTableView2 = new TableView(exaInputTable2, inputTable2.getRootHiveLocationPath(), inputTable2.getRootHiveTableDefinition(), null);
                            //System.out.println("translateToExaremeOps: RootHiveLocationPath: "+inputTable2.getRootHiveLocationPath()+" - RootHiveTableDefinition: "+inputTable2.getRootHiveTableDefinition());
                        //}
                        //else{
                        inputTableView2 = new TableView(exaInputTable2);
                        //}

                        System.out.println("translateToExaremeOps: Setting inputTableView DataPattern=CARTESIAN_PRODUCT...");
                        System.out.flush();
                        inputTableView2.setPattern(DataPattern.cartesian_product);
                        System.out.println("translateToExaremeOps: Setting inputTableView setNumOfPartitions=-1");
                        System.out.flush();
                        inputTableView2.setNumOfPartitions(-1);

                        System.out.println("translateToExaremeOps: Adding used Columns..."); //TODO
                        System.out.flush();

                        MyMap usedColumns = opQuery.getUsedColumns();
                        List<ColumnTypePair> usedColsList = usedColumns.getColumnAndTypeList();

                        for (ColumnTypePair colPair : usedColsList) {
                            if(colPair.getColumnType().equals(inputTable2.getTableName())) {
                                inputTableView2.addUsedColumn(colPair.getColumnName());
                                System.out.println("translateToExaremeOps: Table: "+colPair.getColumnType()+" - Column: "+colPair.getColumnName());
                                System.out.flush();
                            }
                        }

                        inputTableViews2.add(inputTableView2);

                        /*---Build OutputTableView of Select---*/
                        madgik.exareme.common.schema.Table exaremeOutputTable2 = new madgik.exareme.common.schema.Table(opQuery.getOutputTable().getTableName());
                        TableView outputTableView2;

                        System.out.println("translateToExaremeOps: Constructing Exareme Output Table Definition...");
                        System.out.flush();
                        String exaremeOutputTdef2 = exaremeTableDefinition(opQuery.getOutputTable());
                        System.out.println("translateToExaremeOps: SQL def: " + exaremeOutputTdef2);
                        System.out.flush();

                        exaremeOutputTable2.setSqlDefinition(exaremeOutputTdef2);
                        System.out.println("translateToExaremeOps: Setting outputTable setTemp=FALSE (final Table) ");
                        System.out.flush();
                        exaremeOutputTable2.setTemp(false);

                        System.out.println("translateToExaremeOps: Setting outputTable setLevel= " + (currentInputLevel + 1));
                        System.out.flush();
                        exaremeOutputTable2.setLevel(currentInputLevel + 1);

                        System.out.println("translateToExaremeOps: Initialising OutputTableView");
                        System.out.flush();
                        outputTableView2 = new TableView(exaremeOutputTable2);
                        System.out.println("translateToExaremeOps: Setting outputTableView DataPattern=SAME ");
                        System.out.flush();
                        outputTableView2.setPattern(DataPattern.same);
                        System.out.println("translateToExaremeOps: Setting outputTableView setNumOfPartitions=1 ");
                        System.out.flush();
                        outputTableView2.setNumOfPartitions(1);

                        /*---Finally initialising Select---*/
                        System.out.println("translateToExaremeOps: Initialising SelectQuery");
                        System.out.flush();

                        Select unionSelect = new Select(i + 1, sqlSelect2, outputTableView2);

                        System.out.println("translateToExaremeOps: Adding InputTableViews to Select...");
                        System.out.flush();
                        for (TableView inputV : inputTableViews2) {
                            unionSelect.addInput(inputV);
                        }
                        System.out.println("translateToExaremeOps: Adding QueryStatement to Select...");
                        System.out.flush();
                        unionSelect.addQueryStatement(opQuery.getExaremeQueryString());
                        System.out.println("translateToExaremeOps: Setting Query of Select...");
                        System.out.flush();
                        unionSelect.setQuery("select * from " + opQuery.getOutputTable().getTableName().toLowerCase());
                        System.out.println("translateToExaremeOps: Setting Mappings of Select...");
                        System.out.flush();
                        List<Integer> runOnParts2 = unionSelect.getRunOnParts();
                        unionSelect.setMappings(null);
                        System.out.println("translateToExaremeOps: Setting DatabaseDir of Select...");
                        System.out.flush();
                        unionSelect.setDatabaseDir(currentDatabasePath);

                        System.out.println("translateToExaremeOps: Initialising AdpDBSelectOperator(TableUnion)");
                        System.out.flush();
                        AdpDBSelectOperator exaremeUnionOperator = new AdpDBSelectOperator(AdpDBOperatorType.tableUnionReplicator, unionSelect, 0);

                        exaremeUnionOperator.addInput(opQuery.getOutputTable().getTableName().toLowerCase(), 0);
                        exaremeUnionOperator.addOutput(opQuery.getOutputTable().getTableName().toLowerCase(), 0);

                        exaremeOperators.add(exaremeUnionOperator);

                        System.out.println("translateToExaremeOps: Printing TableUnionReplication for Debugging...\n\t");
                        System.out.flush();
                        exaremeUnionOperator.print();

                        /*---Create an Extra OpLink---*/
                        System.out.println("translateToExaremeOps: Create an Extra OpLink...\n\t");
                        System.out.flush();
                        OpLink opLink = new OpLink();
                        opLink.setContainerName("c0");
                        opLink.setFromTable(opQuery.getExaremeOutputTableName());
                        opLink.setToTable("TR_" + opQuery.getOutputTable().getTableName() + "_P_0");
                        StringParameter sP = new StringParameter("table", opQuery.getOutputTable().getTableName());
                        NumParameter nP = new NumParameter("part", 0);
                        List<Parameter> params = new LinkedList<>();
                        params.add(sP);
                        params.add(nP);
                        opLink.setParameters(params);

                        opLinksList.add(opLink);

                    }
                    else{ //TableUnionReplicator - 1 Table - Many Partitions
                        System.out.println("translateToExaremeOps: InputTable has Partitions !\n\t");

                        List<MyPartition> myPartitions = inputTable2.getAllPartitions();

                        System.out.println("translateToExaremeOps: We have: "+myPartitions.size()+" input partitions!");

                        System.out.println("translateToExaremeOps: Constructing Exareme Input Table Definition...");
                        System.out.flush();

                        madgik.exareme.common.schema.Table exaInputTable2 = new madgik.exareme.common.schema.Table(myPartitions.get(0).getBelogingTableName().toLowerCase());
                        String exaremeInputTdef2 = exaremeTableDefinition(myPartitions.get(0));

                        System.out.println("translateToExaremeOps: SQL def: " + exaremeInputTdef2);
                        System.out.flush();

                        exaInputTable2.setSqlDefinition(exaremeInputTdef2);
                        if (currentInputLevel == -1) {
                            System.out.println("translateToExaremeOps: Setting Input Table to NON TEMP!");
                            exaInputTable2.setTemp(false);
                        } else {
                            System.out.println("translateToExaremeOps: Setting Input Table to temp!");
                            exaInputTable2.setTemp(true);
                        }
                        System.out.println("translateToExaremeOps: Setting Input Table Level= " + currentInputLevel);
                        System.out.flush();
                        exaInputTable2.setLevel(currentInputLevel);

                        System.out.println("translateToExaremeOps: Initialising InputTableView...");
                        System.out.flush();
                        TableView inputTableView2 = null;

                        //if (inputTable2.getIsRootInput()) {
                        //    System.out.println("translateToExaremeOps: This Hive Partition is a Root Input and we need more details...");
                        //    inputTableView2 = new TableView(exaInputTable2, inputPartition.getRootHiveLocationPath(), inputPartition.getRootHiveTableDefinition(), inputPartition.getSecondaryNeededQueries());
                        //    System.out.println("translateToExaremeOps: RootHiveLocationPath: " + inputPartition.getRootHiveLocationPath() + " - RootHiveTableDefinition: " + inputPartition.getRootHiveTableDefinition());
                        //} else {
                        inputTableView2 = new TableView(exaInputTable2);
                        //}

                        System.out.println("translateToExaremeOps: Setting inputTableView DataPattern=CARTESIAN_PRODUCT...");
                        System.out.flush();
                        inputTableView2.setPattern(DataPattern.cartesian_product);
                        System.out.println("translateToExaremeOps: Setting inputTableView setNumOfPartitions=-1");
                        System.out.flush();
                        inputTableView2.setNumOfPartitions(-1);

                        System.out.println("translateToExaremeOps: Adding used Columns...");
                        System.out.flush();

                        MyMap usedColumns = opQuery.getUsedColumns();
                        List<ColumnTypePair> usedColsList = usedColumns.getColumnAndTypeList();

                        for (ColumnTypePair colPair : usedColsList) {
                            if (colPair.getColumnType().equals(inputTable2.getTableName())) {
                                inputTableView2.addUsedColumn(colPair.getColumnName());
                                System.out.println("translateToExaremeOps: Table: " + colPair.getColumnType() + " - Column: " + colPair.getColumnName());
                                System.out.flush();
                            }
                        }

                        inputTableViews2.add(inputTableView2);

                        /*---Build OutputTableView of Select---*/
                        madgik.exareme.common.schema.Table exaremeOutputTable2 = new madgik.exareme.common.schema.Table(opQuery.getOutputTable().getTableName());
                        TableView outputTableView2;

                        System.out.println("translateToExaremeOps: Constructing Exareme Output Table Definition...");
                        System.out.flush();
                        String exaremeOutputTdef2 = exaremeTableDefinition(opQuery.getOutputTable());
                        System.out.println("translateToExaremeOps: SQL def: " + exaremeOutputTdef2);
                        System.out.flush();

                        exaremeOutputTable2.setSqlDefinition(exaremeOutputTdef2);
                        System.out.println("translateToExaremeOps: Setting outputTable setTemp=FALSE (final Table) ");
                        System.out.flush();
                        exaremeOutputTable2.setTemp(false);

                        System.out.println("translateToExaremeOps: Setting outputTable setLevel= " + (currentInputLevel + 1));
                        System.out.flush();
                        exaremeOutputTable2.setLevel(currentInputLevel + 1);

                        System.out.println("translateToExaremeOps: Initialising OutputTableView");
                        System.out.flush();
                        outputTableView2 = new TableView(exaremeOutputTable2);
                        System.out.println("translateToExaremeOps: Setting outputTableView DataPattern=SAME ");
                        System.out.flush();
                        outputTableView2.setPattern(DataPattern.same);
                        System.out.println("translateToExaremeOps: Setting outputTableView setNumOfPartitions=1 ");
                        System.out.flush();
                        outputTableView2.setNumOfPartitions(1);

                        /*---Finally initialising Select---*/
                        System.out.println("translateToExaremeOps: Initialising SelectQuery");
                        System.out.flush();

                        Select unionSelect = new Select(i + 1, sqlSelect2, outputTableView2);

                        System.out.println("translateToExaremeOps: Adding InputTableViews to Select...");
                        System.out.flush();
                        for (TableView inputV : inputTableViews2) {
                            unionSelect.addInput(inputV);
                        }
                        System.out.println("translateToExaremeOps: Adding QueryStatement to Select...");
                        System.out.flush();
                        unionSelect.addQueryStatement(opQuery.getExaremeQueryString());
                        System.out.println("translateToExaremeOps: Setting Query of Select...");
                        System.out.flush();
                        unionSelect.setQuery("select * from " + opQuery.getOutputTable().getTableName().toLowerCase());
                        System.out.println("translateToExaremeOps: Setting Mappings of Select...");
                        System.out.flush();
                        List<Integer> runOnParts2 = unionSelect.getRunOnParts();
                        unionSelect.setMappings(null);
                        System.out.println("translateToExaremeOps: Setting DatabaseDir of Select...");
                        System.out.flush();
                        unionSelect.setDatabaseDir(currentDatabasePath);

                        boolean tableIsPreviousOutput = false;
                        for(Map.Entry<String, Integer> entry : outputTablesWithInputParts.entrySet()){
                            if(entry.getKey().equals(inputTable2.getTableName())){
                                tableIsPreviousOutput = true;
                                System.out.println("translateToExaremeOps: Input Table: "+inputTable2.getTableName()+" is previous Output with PartitionCount: "+entry.getValue() +" this case should not exist! Error!");
                                System.exit(0);
                                /*System.out.flush();
                                System.out.println("translateToExaremeOps: Initialising AdpDBSelectOperator(TableUnion)");
                                System.out.flush();
                                AdpDBSelectOperator exaremeUnionOperator = new AdpDBSelectOperator(AdpDBOperatorType.tableUnionReplicator, unionSelect, 0);

                                for(int k = 0; k < entry.getValue(); k++){
                                    exaremeUnionOperator.addInput(inputTable2.getTableName().toLowerCase(), 0);
                                }

                                exaremeUnionOperator.addOutput(opQuery.getOutputTable().getTableName(), 0);

                                exaremeOperators.add(exaremeUnionOperator);

                                System.out.println("translateToExaremeOps: Printing exaremeSelectOperator...\n\t");
                                System.out.flush();

                                exaremeUnionOperator.print();

                                break;*/
                            }
                        }

                        if(tableIsPreviousOutput == false) {
                            System.out.println("translateToExaremeOps: Initialising AdpDBSelectOperator(TableUnion)");
                            System.out.flush();
                            AdpDBSelectOperator exaremeUnionOperator = new AdpDBSelectOperator(AdpDBOperatorType.tableUnionReplicator, unionSelect, 0);

                            for(MyPartition inputPartition : inputTable2.getAllPartitions()) { //The Sum Number of Partitions participating in creating the part 0 of TableUnionReplicator
                                exaremeUnionOperator.addInput(opQuery.getOutputTable().getTableName(), 0);
                            }

                            exaremeUnionOperator.addOutput(opQuery.getOutputTable().getTableName(), 0);

                            exaremeOperators.add(exaremeUnionOperator);

                            System.out.println("translateToExaremeOps: Printing exaremeSelectOperator...\n\t");
                            System.out.flush();

                            exaremeUnionOperator.print();

                            /*---Create an Extra OpLink---*/
                            for(int l=0; l < inputTable2.getAllPartitions().size(); l++) {
                                System.out.println("translateToExaremeOps: Create an Extra OpLink...For Every Input Partition\n\t");
                                System.out.flush();
                                OpLink opLink = new OpLink();
                                opLink.setContainerName("c0");
                                opLink.setFromTable("R_"+opQuery.getOutputTable().getTableName().toLowerCase()+"_"+l);
                                opLink.setToTable("TR_" + opQuery.getOutputTable().getTableName() + "_P_0");
                                StringParameter sP = new StringParameter("table", opQuery.getOutputTable().getTableName());
                                NumParameter nP = new NumParameter("part", 0);
                                List<Parameter> params = new LinkedList<>();
                                params.add(sP);
                                params.add(nP);
                                opLink.setParameters(params);

                                opLinksList.add(opLink);
                            }

                        }

                    }
                }
                else{ //Multiple Tables
                    /*---Build SQLSelect---*/
                    System.out.println("translateToExaremeOps: \tOperatorQuery: [" + opQuery.getExaremeQueryString() + " ]");
                    System.out.flush();
                    sqlSelect2 = new SQLSelect();
                    System.out.println("translateToExaremeOps: Setting InputDataPattern=CARTESIAN_PRODUCT for SQLSelect...");
                    System.out.flush();
                    sqlSelect2.setInputDataPattern(DataPattern.cartesian_product);
                    sqlSelect2.setOutputDataPattern(DataPattern.same);

                    boolean partitionsExist = false;
                    if(opQuery.getInputTables().get(0).getHasPartitions() == true){
                        partitionsExist = true;
                    }

                    if(partitionsExist == true){
                        System.out.println("translateToExaremeOps: Setting NumberOfOutputParts=1 for SQLSelect...(Partitions exist)");
                        System.out.flush();
                        sqlSelect2.setNumberOfOutputPartitions(1); //means partitions exist
                    }
                    else{
                        System.out.println("translateToExaremeOps: Setting NumberOfOutputParts=-1 for SQLSelect...(Partitions do not exist)");
                        System.out.flush();
                        sqlSelect2.setNumberOfOutputPartitions(-1); //means no partitions
                    }

                    System.out.println("translateToExaremeOps: Setting ResultTableName for SQLSelect...(NON TEMP TABLE)");
                    System.out.flush();
                    sqlSelect2.setResultTable(opQuery.getOutputTable().getTableName(), false, false);
                    System.out.println("translateToExaremeOps: Setting PartsDefn=null for SQLSelect...");
                    System.out.flush();
                    sqlSelect2.setPartsDefn(null);
                    System.out.println("translateToExaremeOps: Setting sqlQuery for SQLSelect...");
                    System.out.flush();
                    sqlSelect2.setSql(opQuery.getExaremeQueryString());
                    Comments someComments2 = new Comments();
                    someComments2.addLine("test_comment");
                    System.out.println("translateToExaremeOps: Setting sqlQuery for Comments...");
                    System.out.flush();
                    sqlSelect2.setComments(someComments2);

                    /*--------------Build Select----------------*/

                    /*---Build InputTableViews of Select---*/
                    List<TableView> inputTableViews2 = new LinkedList<>();
                    for(MyTable inputTable2 : opQuery.getInputTables()){
                        if(inputTable2.getHasPartitions() == false){
                            System.out.println("translateToExaremeOps: Constructing Exareme Input Table Definition...");
                            System.out.flush();
                            madgik.exareme.common.schema.Table exaInputTable2 = new madgik.exareme.common.schema.Table(inputTable2.getTableName().toLowerCase());
                            String exaremeInputTdef2 = exaremeTableDefinition(inputTable2);
                            System.out.println("translateToExaremeOps: SQL def: " + exaremeInputTdef2);
                            System.out.flush();
                            exaInputTable2.setSqlDefinition(exaremeInputTdef2);

                            System.out.println("translateToExaremeOps: Setting Input Table to temp!");
                            exaInputTable2.setTemp(true);

                            System.out.println("translateToExaremeOps: Setting Input Table Level= " + currentInputLevel);
                            System.out.flush();
                            exaInputTable2.setLevel(currentInputLevel);

                            System.out.println("translateToExaremeOps: Initialising InputTableView...");
                            System.out.flush();
                            TableView inputTableView2;

                            //if(inputTable2.getIsRootInput()){
                            //System.out.println("translateToExaremeOps: This Hive Table is a Root Input and we need more details...");
                            //inputTableView2 = new TableView(exaInputTable2, inputTable2.getRootHiveLocationPath(), inputTable2.getRootHiveTableDefinition(), null);
                            //System.out.println("translateToExaremeOps: RootHiveLocationPath: "+inputTable2.getRootHiveLocationPath()+" - RootHiveTableDefinition: "+inputTable2.getRootHiveTableDefinition());
                            //}
                            //else{
                            inputTableView2 = new TableView(exaInputTable2);
                            //}

                            System.out.println("translateToExaremeOps: Setting inputTableView DataPattern=CARTESIAN_PRODUCT...");
                            System.out.flush();
                            inputTableView2.setPattern(DataPattern.cartesian_product);
                            System.out.println("translateToExaremeOps: Setting inputTableView setNumOfPartitions=-1");
                            System.out.flush();
                            inputTableView2.setNumOfPartitions(-1);

                            System.out.println("translateToExaremeOps: Adding used Columns..."); //TODO
                            System.out.flush();

                            MyMap usedColumns = opQuery.getUsedColumns();
                            List<ColumnTypePair> usedColsList = usedColumns.getColumnAndTypeList();

                            for (ColumnTypePair colPair : usedColsList) {
                                if(colPair.getColumnType().equals(inputTable2.getTableName())) {
                                    inputTableView2.addUsedColumn(colPair.getColumnName());
                                    System.out.println("translateToExaremeOps: Table: "+colPair.getColumnType()+" - Column: "+colPair.getColumnName());
                                    System.out.flush();
                                }
                            }

                            inputTableViews2.add(inputTableView2);
                        }
                        else{
                            System.out.println("translateToExaremeOps: InputTable has Partitions !\n\t");

                            List<MyPartition> myPartitions = inputTable2.getAllPartitions();

                            System.out.println("translateToExaremeOps: We have: "+myPartitions.size()+" input partitions!");

                            System.out.println("translateToExaremeOps: Constructing Exareme Input Table Definition...");
                            System.out.flush();

                            madgik.exareme.common.schema.Table exaInputTable2 = new madgik.exareme.common.schema.Table(myPartitions.get(0).getBelogingTableName().toLowerCase());
                            String exaremeInputTdef2 = exaremeTableDefinition(myPartitions.get(0));

                            System.out.println("translateToExaremeOps: SQL def: " + exaremeInputTdef2);
                            System.out.flush();

                            exaInputTable2.setSqlDefinition(exaremeInputTdef2);
                            if (currentInputLevel == -1) {
                                System.out.println("translateToExaremeOps: Setting Input Table to NON TEMP!");
                                exaInputTable2.setTemp(false);
                            } else {
                                System.out.println("translateToExaremeOps: Setting Input Table to temp!");
                                exaInputTable2.setTemp(true);
                            }
                            System.out.println("translateToExaremeOps: Setting Input Table Level= " + currentInputLevel);
                            System.out.flush();
                            exaInputTable2.setLevel(currentInputLevel);

                            System.out.println("translateToExaremeOps: Initialising InputTableView...");
                            System.out.flush();
                            TableView inputTableView2 = null;

                            //if (inputTable2.getIsRootInput()) {
                            //    System.out.println("translateToExaremeOps: This Hive Partition is a Root Input and we need more details...");
                            //    inputTableView2 = new TableView(exaInputTable2, inputPartition.getRootHiveLocationPath(), inputPartition.getRootHiveTableDefinition(), inputPartition.getSecondaryNeededQueries());
                            //    System.out.println("translateToExaremeOps: RootHiveLocationPath: " + inputPartition.getRootHiveLocationPath() + " - RootHiveTableDefinition: " + inputPartition.getRootHiveTableDefinition());
                            //} else {
                            inputTableView2 = new TableView(exaInputTable2);
                            //}

                            System.out.println("translateToExaremeOps: Setting inputTableView DataPattern=CARTESIAN_PRODUCT...");
                            System.out.flush();
                            inputTableView2.setPattern(DataPattern.cartesian_product);
                            System.out.println("translateToExaremeOps: Setting inputTableView setNumOfPartitions=-1");
                            System.out.flush();
                            inputTableView2.setNumOfPartitions(-1);

                            System.out.println("translateToExaremeOps: Adding used Columns...");
                            System.out.flush();

                            MyMap usedColumns = opQuery.getUsedColumns();
                            List<ColumnTypePair> usedColsList = usedColumns.getColumnAndTypeList();

                            for (ColumnTypePair colPair : usedColsList) {
                                if (colPair.getColumnType().equals(inputTable2.getTableName())) {
                                    inputTableView2.addUsedColumn(colPair.getColumnName());
                                    System.out.println("translateToExaremeOps: Table: " + colPair.getColumnType() + " - Column: " + colPair.getColumnName());
                                    System.out.flush();
                                }
                            }

                            inputTableViews2.add(inputTableView2);
                        }
                    }

                    /*---Build OutputTableView of Select---*/
                    madgik.exareme.common.schema.Table exaremeOutputTable2 = new madgik.exareme.common.schema.Table(opQuery.getOutputTable().getTableName());
                    TableView outputTableView2;

                    System.out.println("translateToExaremeOps: Constructing Exareme Output Table Definition...");
                    System.out.flush();
                    String exaremeOutputTdef2 = exaremeTableDefinition(opQuery.getOutputTable());
                    System.out.println("translateToExaremeOps: SQL def: " + exaremeOutputTdef2);
                    System.out.flush();

                    exaremeOutputTable2.setSqlDefinition(exaremeOutputTdef2);
                    System.out.println("translateToExaremeOps: Setting outputTable setTemp=FALSE (final Table) ");
                    System.out.flush();
                    exaremeOutputTable2.setTemp(false);

                    System.out.println("translateToExaremeOps: Setting outputTable setLevel= " + (currentInputLevel + 1));
                    System.out.flush();
                    exaremeOutputTable2.setLevel(currentInputLevel + 1);

                    System.out.println("translateToExaremeOps: Initialising OutputTableView");
                    System.out.flush();
                    outputTableView2 = new TableView(exaremeOutputTable2);
                    System.out.println("translateToExaremeOps: Setting outputTableView DataPattern=SAME ");
                    System.out.flush();
                    outputTableView2.setPattern(DataPattern.same);
                    System.out.println("translateToExaremeOps: Setting outputTableView setNumOfPartitions=1 ");
                    System.out.flush();
                    outputTableView2.setNumOfPartitions(1);

                        /*---Finally initialising Select---*/
                    System.out.println("translateToExaremeOps: Initialising SelectQuery");
                    System.out.flush();

                    Select unionSelect = new Select(i + 1, sqlSelect2, outputTableView2);

                    System.out.println("translateToExaremeOps: Adding InputTableViews to Select...");
                    System.out.flush();
                    for (TableView inputV : inputTableViews2) {
                        unionSelect.addInput(inputV);
                    }
                    System.out.println("translateToExaremeOps: Adding QueryStatement to Select...");
                    System.out.flush();
                    unionSelect.addQueryStatement(opQuery.getExaremeQueryString());
                    System.out.println("translateToExaremeOps: Setting Query of Select...");
                    System.out.flush();
                    unionSelect.setQuery("select * from " + opQuery.getOutputTable().getTableName().toLowerCase());
                    System.out.println("translateToExaremeOps: Setting Mappings of Select...");
                    System.out.flush();
                    List<Integer> runOnParts2 = unionSelect.getRunOnParts();
                    unionSelect.setMappings(null);
                    System.out.println("translateToExaremeOps: Setting DatabaseDir of Select...");
                    System.out.flush();
                    unionSelect.setDatabaseDir(currentDatabasePath);

                    System.out.println("translateToExaremeOps: Initialising AdpDBSelectOperator(TableUnion)");
                    System.out.flush();
                    AdpDBSelectOperator exaremeUnionOperator = new AdpDBSelectOperator(AdpDBOperatorType.tableUnionReplicator, unionSelect, 0);

                    int l = 0;
                    for (MyTable inputT : opQuery.getInputTables()) {
                        //boolean tableIsPreviousOutput = false;
                        //for(Map.Entry<String, Integer> entry : outputTablesWithInputParts.entrySet()){
                        //    if(entry.getKey().equals(inputT.getTableName())){
                        //        tableIsPreviousOutput = true;
                        //        System.out.println("translateToExaremeOps: Input Table: "+inputT.getTableName()+" is previous Output with PartitionCount: "+entry.getValue());
                        //        for(int k = 0; k < entry.getValue(); k++){
                        //            exaremeUnionOperator.addInput(opQuery.getOutputTable().getTableName(), 0);
                        //        }
                        //        break;
                        //    }
                        //}

                        if(inputT.getHasPartitions() == false) {
                            exaremeUnionOperator.addInput(opQuery.getOutputTable().getTableName(), 0);
                            System.out.println("translateToExaremeOps: Create an Extra OpLink...For This Input Table\n\t");
                            System.out.flush();

                            /*---Create an Extra OpLink---*/
                            OpLink opLink = new OpLink();
                            opLink.setContainerName("c0");
                            opLink.setFromTable("R_"+opQuery.getOutputTable().getTableName().toLowerCase()+"_"+l);
                            opLink.setToTable("TR_" + opQuery.getOutputTable().getTableName() + "_P_0");
                            StringParameter sP = new StringParameter("table", opQuery.getOutputTable().getTableName());
                            NumParameter nP = new NumParameter("part", 0);
                            List<Parameter> params = new LinkedList<>();
                            params.add(sP);
                            params.add(nP);
                            opLink.setParameters(params);

                            opLinksList.add(opLink);

                            l++;
                        }
                        else {
                            for(MyPartition myPart : inputT.getAllPartitions()){
                                exaremeUnionOperator.addInput(opQuery.getOutputTable().getTableName(), 0);

                                /*---Create an Extra OpLink---*/
                                System.out.println("translateToExaremeOps: Create an Extra OpLink...For Every Input Partition\n\t");
                                System.out.flush();
                                OpLink opLink = new OpLink();
                                opLink.setContainerName("c0");
                                opLink.setFromTable("R_"+opQuery.getOutputTable().getTableName().toLowerCase()+"_"+l);
                                opLink.setToTable("TR_" + opQuery.getOutputTable().getTableName() + "_P_0");
                                StringParameter sP = new StringParameter("table", opQuery.getOutputTable().getTableName());
                                NumParameter nP = new NumParameter("part", 0);
                                List<Parameter> params = new LinkedList<>();
                                params.add(sP);
                                params.add(nP);
                                opLink.setParameters(params);

                                opLinksList.add(opLink);

                                l++;
                            }

                        }

                    }

                }

            }

            if(currentInputLevel == -1) currentInputLevel = 1;
            else currentInputLevel++;

        }

        return exaremeOperators;

    }

}
