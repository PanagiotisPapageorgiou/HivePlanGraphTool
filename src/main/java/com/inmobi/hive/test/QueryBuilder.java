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

   This is the way currently a set of Hive Operators translates to an Exareme Operator.

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
   Due to the MapReduce Logic of the OperatorNodes some Exareme Operators might contain Queries
   that are essentially the same Query as the Exareme Operator before them. As of now, no steps
   have been taken to optimise this behaviour and remove queries that can be ommited due to the
   above behaviour.

*/

public class QueryBuilder {

    ExaremeGraph exaremeGraph; //A Hive Operator Graph ready to be translated
    TableRegistry tableRegistry;
    MyMap aggregationsMap;
    MyMap ommitedConstantsMap;
    List<OperatorQuery> allQueries;
    List<MyTable> inputTables;
    List<MyTable> outputTables;
    List<OpLink> opLinksList;
    String currentDatabasePath;

    List<JoinPoint> joinPointList = new LinkedList<>();
    List<UnionPoint> unionPointList = new LinkedList<>();

    int numberOfNodes;

    public QueryBuilder(ExaremeGraph graph, List<MyTable> inputT, List<MyPartition> inputP, List<MyTable> outputT, List<MyPartition> outputP, String databasePath){
        exaremeGraph = graph;
        allQueries = new LinkedList<>();
        aggregationsMap = new MyMap(false);
        ommitedConstantsMap = new MyMap(false);
        tableRegistry = new TableRegistry();
        //columnAndTypeMap = new MyMap();
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

    public void printTableRegistry(){

        tableRegistry.printRegistry();

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

    public void checkIfConstantMapBreaksRegistry(String constType, String operatorName, String wantedAlias){

        for(TableRegEntry tableRegEntry : tableRegistry.getEntries()){
            for(ColumnTypePair cP : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()){
                if(cP.getColumnType().equals(constType)){
                    //if(cP.getColumnName().equals(wantedAlias)){
                     //   for(StringParameter sP : cP.getAltAliasPairs()){
                    //        if(sP.getParemeterType().equals(operatorName)){
                     //           System.out.println("Constant: "+wantedAlias+" of type: "+constType+" of: "+operatorName+" is currently breaking the TableRegistry because same (Operator, Alias) pair also exists! Needs to be fixed!");
                     //           System.exit(0);
                    //        }
                    //    }
                    //}
                    //else{
                        for(StringParameter sP : cP.getAltAliasPairs()){
                            if(sP.getValue().equals(wantedAlias)) {
                                if (sP.getParemeterType().equals(operatorName)) {
                                    System.out.println("Constant: " + wantedAlias + " of type: " + constType + " of: " + operatorName + " is currently breaking the TableRegistry because same (Operator, Alias) pair also exists! Needs to be fixed!");
                                    System.exit(0);
                                }
                            }
                        }
                   // }
                }
            }
        }

    }

    public void addQueryToList(OperatorQuery opQuery) { allQueries.add(opQuery); }

    public ExaremeGraph getExaremeGraph(){
        return exaremeGraph;
    }

    //WARNING STRING ARE IMMUTABLE YOU CAN RETURN OR WRAP OR STRINGBUILD ONLY, NOT CHANGE THE VALUE OF THE REFERENCE

    public TableRegistry getTableRegistry(){ return tableRegistry; }

    public void setTableRegistry(TableRegistry tableReg) { tableRegistry = tableReg; }

    public String extractColsFromTypeName(String typeName, MyMap aMap, String schema,boolean keepKeys){

        System.out.println("Given typeName: ["+typeName+"]");

        typeName = fixSchemaContainingDecimals(typeName);

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

                    String realName = tupleParts[0];

                    /*if(tupleParts[0].contains("__aggregationColumn_")){
                        System.out.println("This is a special aggregation column searching for it... ");
                        String colNumber = tupleParts[0].replace("__aggregationColumn_", "");
                        int wantedIndex = new Integer(colNumber);
                        System.out.println("We are looking for index: "+wantedIndex);
                        int i = 0;
                        for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                            if(i == wantedIndex){
                                realName = aggPair.getColumnName();
                            }
                            i++;
                        }
                    }*/
                    ColumnTypePair pair = new ColumnTypePair(realName, typeCorrect);
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

    public String fixSchemaContainingStructs(String givenSchema){

        char[] schemaToCharArray = givenSchema.toCharArray();
        List<Character> validatedSchema = new LinkedList<>();

        boolean locatedStruct = false;
        for(int i = 0; i < schemaToCharArray.length; i++){
            if(locatedStruct == false){
                if(schemaToCharArray[i] == '<'){
                    locatedStruct = true;
                }
                else{
                    validatedSchema.add(new Character(schemaToCharArray[i]));
                }
            }
            else{
                if(schemaToCharArray[i] == '>'){
                    locatedStruct = false;
                }
            }
        }

        char[] newSchemaCharArray = new char[validatedSchema.size()];

        int i = 0;
        for(Character c : validatedSchema){
            newSchemaCharArray[i] = c.charValue();
            i++;
        }

        return new String(newSchemaCharArray);

    }

    public String fixSchemaContainingDecimals(String givenSchema){

        char[] schemaToCharArray = givenSchema.toCharArray();

        String comboToLocate = "decimal(";
        char[] comboToArray = comboToLocate.toCharArray();
        int neededCorrectChars = comboToLocate.length();

        int j = 0;
        boolean replaceMode = false;
        for(int i = 0; i < schemaToCharArray.length; i++){
            if(replaceMode == false) {
                if (schemaToCharArray[i] == comboToArray[j]) { //Located a correct char
                    j++;
                } else {
                    j = 0;
                }

                if (j == neededCorrectChars) { //Completely located decimal(
                    replaceMode = true;
                    schemaToCharArray[i] = '%';
                }
            }
            else{
                if(schemaToCharArray[i] == ')'){
                    replaceMode = false;
                    j = 0;
                }
                schemaToCharArray[i] = '%';
            }

        }

        String updatedString = new String(schemaToCharArray);

        updatedString = updatedString.replace("%", "");


        return updatedString;

    }

    public String extractColsFromTypeNameWithStructs(String typeName, MyMap aMap, String schema,boolean keepKeys){

        System.out.println("Given typeName: ["+typeName+"]");

        if(typeName.contains("struct")) {
            System.out.println("Typename contains structs and need to be fixed...");
        }
        else{
            System.out.println("Typename does not contain structs! Wrong method call!");
            System.exit(0);
        }

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

        typeName = fixSchemaContainingStructs(typeName);
        System.out.println("Fixed typeName: ["+typeName+"]");

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

                    String realName = tupleParts[0];

                    if(tupleParts[0].contains("__aggregationColumn_")){
                        System.out.println("This is a special aggregation column searching for it... ");
                        String colNumber = tupleParts[0].replace("__aggregationColumn_", "");
                        int wantedIndex = new Integer(colNumber);
                        System.out.println("We are looking for index: "+wantedIndex);
                        int i = 0;
                        for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                            if(i == wantedIndex){
                                realName = aggPair.getColumnName();
                            }
                            i++;
                        }
                    }
                    ColumnTypePair pair = new ColumnTypePair(realName, typeCorrect);
                    aMap.addPair(pair);

                }
                else{
                    System.out.println("extractColsFromTypeName: Failed to :!");
                    System.exit(0);
                }
            }
        }

        schema = typeName;

        return "("+schema+")";

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

        MyMap tempMap = new MyMap(true);
        if(schemaString.contains("struct<")){
            schemaString = extractColsFromTypeNameWithStructs(schemaString, tempMap, schemaString, true);
        }
        else{
            schemaString = extractColsFromTypeName(schemaString, tempMap, schemaString, true);
        }

        //System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Before...addNewPossibleAliases...TableRegistry was : ");
        //printTableRegistry();

        //System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Before...addNewPossibleAliases...AggregationMap was : ");
        //aggregationsMap.printMap();

        //System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Before...addNewPossibleAliases...ConstantsMap was : ");
        //ommitedConstantsMap.printMap();

        if(fatherOperatorNode != null) {
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+ ": First copying all altAliases of FatherOperatorNode1...");
            addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);
        }

        if(fatherOperatorNode2 != null){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+ ": First copying all altAliases of FatherOperatorNode2...");
            addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode2);
        }

        System.out.println("Schema currently is : "+schemaString);

        for(Map.Entry<String, ExprNodeDesc> entry : currentOperatorNode.getOperator().getColumnExprMap().entrySet()){
            if(entry.getKey().equals("ROW__ID") || entry.getKey().equals("BLOCK__OFFSET__INSIDE__FILE") || entry.getKey().equals("INPUT__FILE__NAME")) continue;
            ExprNodeDesc oldValue = entry.getValue();
            if(oldValue == null){
                continue;
            }
            if( (oldValue.getName().contains("Const ") || oldValue.toString().contains("Const "))) {
                String constType = "";
                if(oldValue.toString().contains(" int ")){
                    constType = "int";
                }
                else if(oldValue.toString().contains(" float ")){
                    constType = "float";
                }
                else if(oldValue.toString().contains(" decimal ")){
                    constType = oldValue.toString().split(" ")[1];
                }
                else if(oldValue.toString().contains(" string ")){
                    constType = "string";
                }
                else if(oldValue.toString().contains("char")){
                    constType = "char";
                }
                else if(oldValue.toString().contains("varchar")){
                    constType = "varchar";
                }
                else if(oldValue.toString().contains("date")){
                    constType = "date";
                }
                else if(oldValue.toString().contains("double")){
                    constType = "double";
                }
                else if(oldValue.toString().contains("double precision")){
                    constType = "double precision";
                }
                else if(oldValue.toString().contains("bigint")){
                    constType = "bigint";
                }
                else if(oldValue.toString().contains("smallint")){
                    constType = "smallint";
                }
                else if(oldValue.toString().contains("tinyint")){
                    constType = "tinyint";
                }
                else{
                    System.out.println("Unsupported Const type for : "+oldValue.toString());
                    System.exit(0);
                }

                boolean ommitAlias = false;
                for(TableRegEntry tableRegEntry : tableRegistry.getEntries()){
                    for(ColumnTypePair pair2 : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()){
                        if(pair2.getColumnType().equals(constType)){
                            if(pair2.getColumnName().equals(entry.getKey())){
                                System.out.println("Constant Value: "+pair2.getColumnName() + " also exists in tableRegistry! Omitting!");
                                ommitAlias = true;
                                break;
                            }
                        }
                    }
                    if(ommitAlias) break;
                }

                if(ommitAlias == false) {
                    ColumnTypePair theNewPair = new ColumnTypePair(oldValue.toString(), constType);
                    theNewPair.addAltAlias(currentOperatorNode.getOperatorName(), entry.getKey());
                    checkIfConstantMapBreaksRegistry(constType, currentOperatorNode.getOperatorName(), entry.getKey());
                    ommitedConstantsMap.addPair(theNewPair);
                }


                continue;
            }

            String oldColumnName = oldValue.getCols().get(0);

            List<TableRegEntry> tableEntries = tableRegistry.getEntries();

            boolean matchFound = false;

            //We might have more than one match due to not checking datatypes
            int numberOfMatches = 0;
            boolean hasMoreThan1Match = false;
            String targetType = "";

            if(fatherOperatorNode2 != null){ //Has second father too - allowedMatches == 2
                List<String> theMatchers = new LinkedList<>();
                if(entry.getKey().contains("_col")) {
                    for (TableRegEntry regEntry : tableEntries) {
                        for (ColumnTypePair pair : regEntry.getColumnTypeMap().getColumnAndTypeList()){
                            if(pair.getColumnName().equals(oldColumnName) == false){
                                for (StringParameter sP : pair.getAltAliasPairs()) {
                                    if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                        if (sP.getValue().equals(oldColumnName)) {

                                            boolean alreadyExists = false;
                                            for(String match : theMatchers){
                                                if(match.equals(pair.getColumnName())){
                                                    alreadyExists = true;
                                                    break;
                                                }
                                            }

                                            if(alreadyExists){
                                                boolean hasMapJoin = false;

                                                for(StringParameter sP2 : pair.getAltAliasPairs()){
                                                    if(sP2.getValue().equals(sP.getValue())){
                                                        if(sP2.getParemeterType().contains("MAPJOIN_") || (sP2.getParemeterType().contains("JOIN_"))){
                                                            hasMapJoin = true;
                                                            break;
                                                        }
                                                    }
                                                }

                                                if(hasMapJoin == false){
                                                    numberOfMatches++;
                                                    theMatchers.add(pair.getColumnName());
                                                    break;
                                                }

                                            }
                                            else {
                                                numberOfMatches++;
                                                theMatchers.add(pair.getColumnName());
                                                break;
                                            }

                                        }
                                    }
                                    else if(sP.getParemeterType().equals(fatherOperatorNode2.getOperatorName())){
                                        if (sP.getValue().equals(oldColumnName)) {

                                            boolean alreadyExists = false;
                                            for(String match : theMatchers){
                                                if(match.equals(pair.getColumnName())){
                                                    alreadyExists = true;
                                                    break;
                                                }
                                            }

                                            if(alreadyExists){
                                                boolean hasMapJoin = false;

                                                for(StringParameter sP2 : pair.getAltAliasPairs()){
                                                    if(sP2.getValue().equals(sP.getValue())){
                                                        if(sP2.getParemeterType().contains("MAPJOIN_") || (sP2.getParemeterType().contains("JOIN_"))){
                                                            hasMapJoin = true;
                                                            break;
                                                        }
                                                    }
                                                }

                                                if(hasMapJoin == false){
                                                    numberOfMatches++;
                                                    theMatchers.add(pair.getColumnName());
                                                    break;
                                                }

                                            }
                                            else {
                                                numberOfMatches++;
                                                theMatchers.add(pair.getColumnName());
                                                break;
                                            }

                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if( numberOfMatches > 2){
                    System.out.println("addNewPossibleAlias: entry.getKey(): "+entry.getKey()+" has: "+numberOfMatches+" matches! NOT SAFE! We must pick one only!");
                    System.out.println("addNewPossibleAlias: Schema is: "+schemaString);
                    System.out.println("Match Names: "+theMatchers.toString());
                    System.out.println("addNewPossibleAlias: We will attempt to find the entry in schema or the oldColumnName and use the type from there...");

                    boolean locatedInSchema = false; //If entry.getKey() is contained in  schema
                    for(ColumnTypePair tempPair : tempMap.getColumnAndTypeList()){
                        if(tempPair.getColumnName().equals(entry.getKey())){
                            locatedInSchema = true;
                            targetType = tempPair.getColumnType();
                            break;
                        }
                    }

                    //if(locatedInSchema == false){ //If old value is contained in schema
                    //    for(ColumnTypePair tempPair : tempMap.getColumnAndTypeList()){
                    //        if(tempPair.getColumnName().equals(oldColumnName)){
                    //            locatedInSchema = true;
                    //           targetType = tempPair.getColumnType();
                    //            break;
                    //        }
                    //    }
                    //}

                    //Last effort:
                    if(locatedInSchema == false){
                        boolean foundWithOlderName = false;
                        for(ColumnTypePair tempPair : tempMap.getColumnAndTypeList()){
                            for(TableRegEntry tableRegEntry : tableRegistry.getEntries()){
                                for(ColumnTypePair cP : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()){
                                    if(cP.getColumnName().equals(tempPair.getColumnName())){
                                        if(cP.getColumnType().equals(tempPair.getColumnType())){
                                            for(StringParameter sP : cP.getAltAliasPairs()){
                                                if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                                    if(sP.getValue().equals(entry.getKey())){
                                                        locatedInSchema = true;
                                                        targetType = cP.getColumnType();
                                                        foundWithOlderName = true;
                                                        break;
                                                    }
                                                    else if(sP.getValue().equals(oldColumnName)){
                                                        locatedInSchema = true;
                                                        targetType = cP.getColumnType();
                                                        foundWithOlderName = true;
                                                        break;
                                                    }
                                                }
                                            }

                                            if(foundWithOlderName) break;
                                        }
                                    }
                                }

                                if(foundWithOlderName) break;
                            }

                            if(foundWithOlderName) break;
                        }


                    }

                    //Last effort:
                    if(locatedInSchema == false){
                        boolean foundWithOlderName = false;
                        for(ColumnTypePair tempPair : tempMap.getColumnAndTypeList()){
                            for(TableRegEntry tableRegEntry : tableRegistry.getEntries()){
                                for(ColumnTypePair cP : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()){
                                    if(cP.getColumnName().equals(tempPair.getColumnName())){
                                        if(cP.getColumnType().equals(tempPair.getColumnType())){
                                            for(StringParameter sP : cP.getAltAliasPairs()){
                                                if(sP.getParemeterType().equals(fatherOperatorNode2.getOperatorName())){
                                                    if(sP.getValue().equals(entry.getKey())){
                                                        locatedInSchema = true;
                                                        targetType = cP.getColumnType();
                                                        foundWithOlderName = true;
                                                        break;
                                                    }
                                                    else if(sP.getValue().equals(oldColumnName)){
                                                        locatedInSchema = true;
                                                        targetType = cP.getColumnType();
                                                        foundWithOlderName = true;
                                                        break;
                                                    }
                                                }
                                            }

                                            if(foundWithOlderName) break;
                                        }
                                    }
                                }

                                if(foundWithOlderName) break;
                            }

                            if(foundWithOlderName) break;
                        }


                    }



                    if(locatedInSchema == false){
                        System.out.println("addNewPossibleAlias: Neither entry.getKey() nor oldColumnName are contained in schema: Shutting down to being unable to determine column alias..."+entry.getKey());
                        tableRegistry.printRegistry();
                        aggregationsMap.printMap();
                        ommitedConstantsMap.printMap();
                        System.exit(0);
                    }

                    hasMoreThan1Match = true;
                }
            }
            else{ //Has only one father - allowedMatches == 1
                List<String> theMatchers = new LinkedList<>();

                if(entry.getKey().contains("_col")) {
                    for (TableRegEntry regEntry : tableEntries) {
                        for (ColumnTypePair pair : regEntry.getColumnTypeMap().getColumnAndTypeList()){
                            if(pair.getColumnName().equals(oldColumnName) == false){
                                for (StringParameter sP : pair.getAltAliasPairs()) {
                                    if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                        if (sP.getValue().equals(oldColumnName)) {

                                            boolean alreadyExists = false;
                                            for(String match : theMatchers){
                                                if(match.equals(pair.getColumnName())){
                                                    alreadyExists = true;
                                                    break;
                                                }
                                            }

                                            if(alreadyExists){
                                                boolean hasMapJoin = false;

                                                for(StringParameter sP2 : pair.getAltAliasPairs()){
                                                    if(sP2.getValue().equals(sP.getValue())){
                                                        if(sP2.getParemeterType().contains("MAPJOIN_") || (sP2.getParemeterType().contains("JOIN_"))){
                                                            hasMapJoin = true;
                                                            break;
                                                        }
                                                    }
                                                }

                                                if(hasMapJoin == false){
                                                    numberOfMatches++;
                                                    theMatchers.add(pair.getColumnName());
                                                    break;
                                                }

                                            }
                                            else {
                                                numberOfMatches++;
                                                theMatchers.add(pair.getColumnName());
                                                break;
                                            }

                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if(numberOfMatches > 1){

                    System.out.println("addNewPossibleAlias: entry.getKey(): "+entry.getKey()+" has: "+numberOfMatches+" matches! NOT SAFE! We must pick one only!");
                    System.out.println("addNewPossibleAlias: Schema is: "+schemaString);
                    System.out.println("Match Names: "+theMatchers.toString());
                    System.out.println("addNewPossibleAlias: We will attempt to find the entry in schema or the oldColumnName and use the type from there...");

                    boolean locatedInSchema = false; //If entry.getKey() is contained in  schema
                    for(ColumnTypePair tempPair : tempMap.getColumnAndTypeList()){
                        if(tempPair.getColumnName().equals(entry.getKey())){
                            locatedInSchema = true;
                            targetType = tempPair.getColumnType();
                            break;
                        }
                    }

                    //if(locatedInSchema == false){ //If old value is contained in schema
                    //    for(ColumnTypePair tempPair : tempMap.getColumnAndTypeList()){
                    //        if(tempPair.getColumnName().equals(oldColumnName)){
                    //            locatedInSchema = true;
                    //            targetType = tempPair.getColumnType();
                    //            break;
                    //        }
                    //    }
                    //}

                    //Last effort:
                    if(locatedInSchema == false){
                        boolean foundWithOlderName = false;
                        for(ColumnTypePair tempPair : tempMap.getColumnAndTypeList()){
                            for(TableRegEntry tableRegEntry : tableRegistry.getEntries()){
                                for(ColumnTypePair cP : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()){
                                    if(cP.getColumnName().equals(tempPair.getColumnName())){
                                        if(cP.getColumnType().equals(tempPair.getColumnType())){
                                            for(StringParameter sP : cP.getAltAliasPairs()){
                                                if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                                    if(sP.getValue().equals(entry.getKey())){
                                                        locatedInSchema = true;
                                                        targetType = cP.getColumnType();
                                                        foundWithOlderName = true;
                                                        break;
                                                    }
                                                    else if(sP.getValue().equals(oldColumnName)){
                                                        locatedInSchema = true;
                                                        targetType = cP.getColumnType();
                                                        foundWithOlderName = true;
                                                        break;
                                                    }
                                                }
                                            }

                                            if(foundWithOlderName) break;
                                        }
                                    }
                                }

                                if(foundWithOlderName) break;
                            }

                            if(foundWithOlderName) break;
                        }


                    }

                    if(locatedInSchema == false){
                        System.out.println("addNewPossibleAlias: Neither entry.getKey() nor oldColumnName are contained in schema: Shutting down to being unable to determine column alias..."+entry.getKey());
                        tableRegistry.printRegistry();
                        aggregationsMap.printMap();
                        ommitedConstantsMap.printMap();
                        System.exit(0);
                    }

                    hasMoreThan1Match = true;
                }
            }


            //Seek possible type in schema
            boolean allowTypeComparisons = false;
            String possibleType = "";
            for(ColumnTypePair somePair : tempMap.getColumnAndTypeList()){
                if(somePair.getColumnName().equals(entry.getKey())){
                    allowTypeComparisons = true;
                    possibleType = somePair.getColumnType();
                }
            }

            for(TableRegEntry regEntry : tableEntries){ //Run through all Input Tables of Table Registry

                List<ColumnTypePair> columnTypePairs = regEntry.getColumnTypeMap().getColumnAndTypeList();

                for(ColumnTypePair pair : columnTypePairs){ //Run through the column Map of each table
                    List<StringParameter> altAliases = pair.getAltAliasPairs(); //Get the alt Aliases of a column

                    if(hasMoreThan1Match == true){
                        if(pair.getColumnType().equals(targetType) == false){
                            continue;
                        }
                    }

                    if(allowTypeComparisons == true){
                        if(pair.getColumnType().equals(possibleType) == false){
                            continue;
                        }
                    }

                    if(altAliases.size() == 0){
                        System.out.println("addNewPossibleAlias: AltAlias must always be > 0");
                        System.exit(0);
                    }

                    for(StringParameter sP : altAliases){ //Try to find father through the altAliases
                        if((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) ){ //Father1 Located
                            if(sP.getValue().equals(oldColumnName)){ //Column names match
                                matchFound = true;
                                if(tempMap.getColumnAndTypeList().size() > 1){
                                    if(schemaString.contains(","+entry.getKey()+":")){ //Update schema
                                        schemaString = schemaString.replace(","+entry.getKey()+":", ","+pair.getColumnName()+":");
                                        System.out.println("Schema becomes: "+schemaString);
                                    }
                                    else if(schemaString.contains(","+oldColumnName+":")){ //Update schema
                                        schemaString = schemaString.replace(","+oldColumnName+":", ","+pair.getColumnName()+":");
                                        System.out.println("Schema becomes: "+schemaString);
                                    }
                                    else{
                                        if(schemaString.contains("("+entry.getKey()+":")){ //Update schema
                                            schemaString = schemaString.replace("("+entry.getKey()+":", "("+pair.getColumnName()+":");
                                            System.out.println("Schema becomes: "+schemaString);
                                        }
                                        else if(schemaString.contains("("+oldColumnName+":")){ //Update schema
                                            schemaString = schemaString.replace("("+oldColumnName+":", "("+pair.getColumnName()+":");
                                            System.out.println("Schema becomes: "+schemaString);
                                        }
                                    }
                                }
                                else{
                                    if(schemaString.contains("("+entry.getKey()+":")){ //Update schema
                                        schemaString = schemaString.replace("("+entry.getKey()+":", "("+pair.getColumnName()+":");
                                        System.out.println("Schema becomes: "+schemaString);
                                    }
                                    else if(schemaString.contains("("+oldColumnName+":")){ //Update schema
                                        schemaString = schemaString.replace("("+oldColumnName+":", "("+pair.getColumnName()+":");
                                        System.out.println("Schema becomes: "+schemaString);
                                    }
                                    else{
                                        if(schemaString.contains("("+entry.getKey()+":")){ //Update schema
                                            schemaString = schemaString.replace("("+entry.getKey()+":", "("+pair.getColumnName()+":");
                                            System.out.println("Schema becomes: "+schemaString);
                                        }
                                        else if(schemaString.contains("("+oldColumnName+":")){ //Update schema
                                            schemaString = schemaString.replace("("+oldColumnName+":", "("+pair.getColumnName()+":");
                                            System.out.println("Schema becomes: "+schemaString);
                                        }
                                    }
                                }
                                pair.modifyAltAlias(currentOperatorNode.getOperatorName(), oldColumnName, entry.getKey()); //Modify and bring new alias
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": (old Alias)= "+oldColumnName+" matched with (new Alias)="+entry.getKey()+" for Operator= "+currentOperatorNode.getOperatorName()+" through fatherNode= "+fatherOperatorNode.getOperatorName());
                                break;
                            }
                        }
                        else if((fatherOperatorNode2 != null) && (sP.getParemeterType().equals(fatherOperatorNode2.getOperatorName())) ) {
                            if(sP.getValue().equals(oldColumnName)){ //Column names match
                                matchFound = true;
                                if(tempMap.getColumnAndTypeList().size() > 1){
                                    if(schemaString.contains(","+entry.getKey()+":")){ //Update schema
                                        schemaString = schemaString.replace(","+entry.getKey()+":", ","+pair.getColumnName()+":");
                                        System.out.println("Schema becomes: "+schemaString);
                                    }
                                    else if(schemaString.contains(","+oldColumnName+":")){ //Update schema
                                        schemaString = schemaString.replace(","+oldColumnName+":", ","+pair.getColumnName()+":");
                                        System.out.println("Schema becomes: "+schemaString);
                                    }
                                    else{
                                        if(schemaString.contains("("+entry.getKey()+":")){ //Update schema
                                            schemaString = schemaString.replace("("+entry.getKey()+":", "("+pair.getColumnName()+":");
                                            System.out.println("Schema becomes: "+schemaString);
                                        }
                                        else if(schemaString.contains("("+oldColumnName+":")){ //Update schema
                                            schemaString = schemaString.replace("("+oldColumnName+":", "("+pair.getColumnName()+":");
                                            System.out.println("Schema becomes: "+schemaString);
                                        }
                                    }
                                }
                                else{
                                    if(schemaString.contains("("+entry.getKey()+":")){ //Update schema
                                        schemaString = schemaString.replace("("+entry.getKey()+":", "("+pair.getColumnName()+":");
                                        System.out.println("Schema becomes: "+schemaString);
                                    }
                                    else if(schemaString.contains("("+oldColumnName+":")){ //Update schema
                                        schemaString = schemaString.replace("("+oldColumnName+":", "("+pair.getColumnName()+":");
                                        System.out.println("Schema becomes: "+schemaString);
                                    }
                                    else{
                                        if(schemaString.contains("("+entry.getKey()+":")){ //Update schema
                                            schemaString = schemaString.replace("("+entry.getKey()+":", "("+pair.getColumnName()+":");
                                            System.out.println("Schema becomes: "+schemaString);
                                        }
                                        else if(schemaString.contains("("+oldColumnName+":")){ //Update schema
                                            schemaString = schemaString.replace("("+oldColumnName+":", "("+pair.getColumnName()+":");
                                            System.out.println("Schema becomes: "+schemaString);
                                        }
                                    }
                                }
                                pair.modifyAltAlias(currentOperatorNode.getOperatorName(), oldColumnName, entry.getKey()); //Modify and bring new alias
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": (old Alias)= "+oldColumnName+" matched with (new Alias)="+entry.getKey()+" for Operator= "+currentOperatorNode.getOperatorName()+" through fatherNode= "+fatherOperatorNode2.getOperatorName());
                                break;
                            }
                        }
                    }

                }

            }

            if(matchFound == false){ //Search in constants list
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Searching for possible match in ConstantsList: ");
                for(ColumnTypePair constPair : ommitedConstantsMap.getColumnAndTypeList()){ //Run through the column Map of each table
                    List<StringParameter> altAliases = constPair.getAltAliasPairs(); //Get the alt Aliases of a column

                    if(altAliases.size() == 0){
                        System.out.println("addNewPossibleAlias: AltAlias must always be > 0");
                        System.exit(0);
                    }

                    for(StringParameter sP : altAliases){ //Try to find father through the altAliases
                        if((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) ){ //Father1 Located
                            if(sP.getValue().equals(oldColumnName)){ //Column names match
                                matchFound = true;
                                //if(schemaString.contains(entry.getKey()+":")){ //Update schema
                                //    schemaString = schemaString.replace(entry.getKey()+":", constPair.getColumnName()+":");
                                //}
                                //else if(schemaString.contains(oldColumnName+":")){ //Update schema
                                //    schemaString = schemaString.replace(oldColumnName+":", constPair.getColumnName()+":");
                                //}
                                constPair.modifyAltAlias(currentOperatorNode.getOperatorName(), oldColumnName, entry.getKey()); //Modify and bring new alias
                                checkIfConstantMapBreaksRegistry(constPair.getColumnType(), currentOperatorNode.getOperatorName(), entry.getKey());
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": (old Alias)= "+oldColumnName+" matched with (new Alias)="+entry.getKey()+" for Operator= "+currentOperatorNode.getOperatorName()+" through fatherNode= "+fatherOperatorNode.getOperatorName()+" in constantsList");
                                break;
                            }
                        }
                        else if((fatherOperatorNode2 != null) && (sP.getParemeterType().equals(fatherOperatorNode2.getOperatorName())) ) {
                            if(sP.getValue().equals(oldColumnName)){ //Column names match
                                matchFound = true;
                                //if(schemaString.contains(entry.getKey()+":")){ //Update schema
                                //    schemaString = schemaString.replace(entry.getKey()+":", pair.getColumnName()+":");
                                //}
                                //else if(schemaString.contains(oldColumnName+":")){ //Update schema
                                //    schemaString = schemaString.replace(oldColumnName+":", pair.getColumnName()+":");
                                //}
                                constPair.modifyAltAlias(currentOperatorNode.getOperatorName(), oldColumnName, entry.getKey()); //Modify and bring new alias
                                checkIfConstantMapBreaksRegistry(constPair.getColumnType(), currentOperatorNode.getOperatorName(), entry.getKey());
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": (old Alias)= "+oldColumnName+" matched with (new Alias)="+entry.getKey()+" for Operator= "+currentOperatorNode.getOperatorName()+" through fatherNode= "+fatherOperatorNode2.getOperatorName()+" in constantsList...");
                                break;
                            }
                        }
                    }

                }

            }

            if(matchFound == false){ //Search in aggr list finally
                //Standard search
                for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                    for(StringParameter altAlias : aggPair.getAltAliasPairs()){
                        if(altAlias.getParemeterType().equals(currentOperatorNode.getOperatorName())){
                            if(oldColumnName.equals(altAlias.getValue())){
                                matchFound = true;
                                aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), altAlias.getValue(), entry.getKey());
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": (old Alias)= "+oldColumnName+" matched with (new Alias)="+entry.getKey()+" for Operator= "+currentOperatorNode.getOperatorName()+" in aggregationMap!");
                                break;
                            }
                        }
                    }

                    if(matchFound == true) break;
                }

            }

            if(matchFound == false){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Alias: "+entry.getKey()+" never found a match...");
                //System.exit(0);
            }

        }

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": After...addNewPossibleAliases...TableRegistry has become: ");
        printTableRegistry();

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": After...addNewPossibleAliases...AggregationMap has become: ");
        aggregationsMap.printMap();

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": After...addNewPossibleAliases...ConstantsMap has become: ");
        ommitedConstantsMap.printMap();

        System.out.println("Updated schema is : "+schemaString);

        return schemaString;

    }

    /*
        Properly handles Input Tables and adds them to a Registry
        to locate the true names of columns in Graph Nodes later
     */

    public void extractInputInfoFromTableScan(OperatorNode root){

        if(root.getOperator() instanceof TableScanOperator){
            TableScanOperator tbsOp = (TableScanOperator) root.getOperator();
            TableScanDesc tbsDesc = (TableScanDesc) tbsOp.getConf();

            String alias = tbsDesc.getAlias();

            if(alias != null){
                System.out.println(root.getOperator().getOperatorId() + ": Root OperatorNode has Input with Alias: "+alias);

                RowSchema schema = tbsOp.getSchema();

                if(schema != null) {
                    System.out.println(root.getOperator().getOperatorId() + ": Input Schema: " + schema.toString());
                    System.out.println(root.getOperator().getOperatorId() + ": Searching for matching InputTable...");

                    MyMap tempMap = new MyMap(false);

                    String tempSchemaString = extractColsFromTypeName(schema.toString(), tempMap, schema.toString(), false);

                    //Build Name/Type Map
                    List<FieldSchema> newCols = new LinkedList<>();
                    List<ColumnTypePair> pairsList = tempMap.getColumnAndTypeList();
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

                    MyTable wantedTable = new MyTable();
                    boolean tableLocated = false;

                    for(MyTable inputT : inputTables){

                        if(inputT.getTableName().equals(alias)){
                            wantedTable = inputT;
                            tableLocated = true;
                            break;
                        }
                        else{ //Attempt to locate if Alias is known table with other name
                            System.out.println(root.getOperator()+ ": Comparing Alias: "+alias + " with Input Table: "+inputT.getTableName());
                            if(inputT.getAllCols().size() == newCols.size()){
                                System.out.println(root.getOperator()+ ": Tables have equal size...");
                                int matches = 0;
                                for(FieldSchema f : inputT.getAllCols()){
                                    boolean colExists = false;
                                    for(FieldSchema f2 : newCols){
                                        if(f.getName().equals(f2.getName())){
                                            if(f.getType().equals(f2.getType())){
                                                colExists = true;
                                                matches++;
                                                break;
                                            }
                                        }
                                    }

                                    if(colExists == false){
                                        System.out.println(root.getOperator()+ ": Tables do not have the same columns");
                                        break;
                                    }

                                }

                                if(matches == newCols.size()){
                                    System.out.println(root.getOperator().getOperatorId() + ": Table with Alias: "+alias+" is actually: "+inputT.getTableName()+"! Table found!");
                                    wantedTable = inputT;
                                    tableLocated = true;
                                    break;
                                }

                            }

                        }

                    }

                    if(tableLocated == false){
                        System.out.println(root.getOperator().getOperatorId() + ": Root OperatorNode has Input with Alias: "+alias + " was never Located!");
                        System.exit(0);
                    }

                    //Finally Add new Input to Table Registry
                    TableRegEntry regEntry = new TableRegEntry(alias, wantedTable);
                    tableRegistry.addEntry(regEntry, root.getOperatorName());

                }
                else{
                    System.out.println(root.getOperator().getOperatorId() + ": Root OperatorNode has Input with Alias: "+alias + " has null schema!");
                    System.exit(0);
                }

            }
            else{
                System.out.println(root.getOperator().getOperatorId() + ": Root OperatorNode has NULL input alias! Might not be root!");
                System.exit(0);
            }

        }
        else{
            System.out.println(root.getOperator().getOperatorId() + ": Given operatorNode is not a TableScanOperator!");
            System.exit(0);
        }

    }

    /*
        This method should be called
        when the OperatorNode has the same schema
        as its father or no schema at all

        It finds all column aliases of a parent and copies them for
        the child too
     */

    public void addAliasesBasedOnFather(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode){

        //System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Before...addAliasesBaseOnFather...TableRegistry was : ");
        //printTableRegistry();

        List<TableRegEntry> tableEntries = tableRegistry.getEntries();

        for(TableRegEntry regEntry : tableEntries){ //Run through all Input Tables of Table Registry

            List<ColumnTypePair> columnTypePairs = regEntry.getColumnTypeMap().getColumnAndTypeList();

            for(ColumnTypePair pair : columnTypePairs){ //Run through the column Map of each table
                List<StringParameter> altAliases = pair.getAltAliasPairs(); //Get the alt Aliases of a column

                boolean foundParent = false;
                String wantedAlias = "";

                if(altAliases.size() == 0){
                    System.out.println("addNewPossibleAlias: AltAlias must always be > 0");
                    System.exit(0);
                }

                for(StringParameter sP : altAliases){ //Try to find father through the altAliases
                    if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){ //Father1 Located
                        wantedAlias = sP.getValue();
                        foundParent = true;
                        //System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Added Column: "+sP.getValue()+" through fatherNode= "+fatherOperatorNode.getOperatorName());
                        break;
                    }
                }

                if(foundParent == true){
                    pair.addAltAlias(currentOperatorNode.getOperatorName(), wantedAlias);
                }

            }

        }

        //Do the same if father exists also in aggregationMap
        for(ColumnTypePair pair : aggregationsMap.getColumnAndTypeList()){ //Run through the column Map of each table
            List<StringParameter> altAliases = pair.getAltAliasPairs(); //Get the alt Aliases of a column

            boolean foundParent = false;
            String wantedAlias = "";

            if(altAliases.size() == 0){
                System.out.println("addNewPossibleAlias: AltAlias must always be > 0");
                System.exit(0);
            }

            for(StringParameter sP : altAliases){ //Try to find father through the altAliases
                if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){ //Father1 Located
                    wantedAlias = sP.getValue();
                    foundParent = true;
                    //System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Added Column: "+sP.getValue()+" through fatherNode= "+fatherOperatorNode.getOperatorName()+" in the aggregationsMap...");
                    break;
                }
            }

            if(foundParent == true){
                pair.addAltAlias(currentOperatorNode.getOperatorName(), wantedAlias);
            }

        }

        //The same for ConstantsMap
        for(ColumnTypePair pair : ommitedConstantsMap.getColumnAndTypeList()){ //Run through the column Map of each table
            List<StringParameter> altAliases = pair.getAltAliasPairs(); //Get the alt Aliases of a column

            boolean foundParent = false;
            String wantedAlias = "";

            if(altAliases.size() == 0){
                System.out.println("addNewPossibleAlias: AltAlias must always be > 0");
                System.exit(0);
            }

            for(StringParameter sP : altAliases){ //Try to find father through the altAliases
                if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){ //Father1 Located
                    wantedAlias = sP.getValue();
                    foundParent = true;
                    //System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Added Column: "+sP.getValue()+" through fatherNode= "+fatherOperatorNode.getOperatorName()+" in the constantsMap...");
                    break;
                }
            }

            if(foundParent == true){
                pair.addAltAlias(currentOperatorNode.getOperatorName(), wantedAlias);
            }

        }
       // System.out.println(currentOperatorNode.getOperator().getOperatorId()+": After...addAliasesBasedOnFather...TableRegistry has become: ");
        //printTableRegistry();

    }

    //TODO: FIX USED COLUMNS AND DUPLICATE COLUMN NAMES CASE
    //TODO: ENSURE EVERY OPERATOR LEAVES ITS MARK

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
                    System.out.println(rootNode.getOperator().getOperatorId() + ": InputTable: " + inputTables.get(0).getTableName() + " has no Partitions!");
                }
                else{
                    System.out.println(rootNode.getOperator().getOperatorId() + ": InputTable: "+inputTables.get(0).getTableName()+" HAS Partitions!");

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
                initialiseNewQuery(null, opQuery, inputTables.get(0));


                /*---Extract Columns from TableScan---*/
                System.out.println(rootNode.getOperator().getOperatorId() + ": Beginning Query Construction from root...");
                extractInputInfoFromTableScan(rootNode);

                TableScanOperator tbsOp = (TableScanOperator) rootNode.getOperator();
                TableScanDesc tableScanDesc = tbsOp.getConf();

                MyTable inputTable = tableRegistry.fetchTableByAlias(tableScanDesc.getAlias());

                /*---Access select needed columns---*/
                System.out.println(rootNode.getOperator().getOperatorId() + ": Accessing needed Columns...");
                List<String> neededColumns = tableScanDesc.getNeededColumns();

                /*---Access Child of TableScan(must be only 1)---*/
                    List<Operator<?>> children = tbsOp.getChildOperators();

                    if (children.size() == 1) {
                        Operator<?> child = children.get(0);
                        if ((child instanceof FilterOperator) || (child instanceof ReduceSinkOperator) || (child instanceof HashTableSinkOperator)) {
                            if(child instanceof FilterOperator)
                                System.out.println(rootNode.getOperator().getOperatorId() + ": TS--->FIL connection discovered! Child is a FilterOperator!");
                            else if(child instanceof ReduceSinkOperator)
                                System.out.println(rootNode.getOperator().getOperatorId() + ": TS--->RS connection discovered! Child is a ReduceSinkOperator!");
                            else if(child instanceof HashTableSinkOperator)
                                System.out.println(rootNode.getOperator().getOperatorId() + ": TS--->HASH connection discovered! Child is a HashTableSinkOperator!");

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
                        else if(child instanceof MapJoinOperator){
                            System.out.println(rootNode.getOperator().getOperatorId()+": Discovered TS-->MAP_JOIN connection!");

                            System.out.println(rootNode.getOperator().getOperatorId()+": Current Query will be ending here...");

                            String updatedSchemaString = rootNode.getOperator().getSchema().toString();

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


                            /*---Finalising outputTable---*/
                            System.out.println(rootNode.getOperator().getOperatorId()+": Finalising OutputTable...");

                            MyTable outputTable = new MyTable();
                            outputTable.setIsAFile(false);
                            outputTable.setBelongingDatabaseName(inputTable.getBelongingDataBaseName());
                            outputTable.setTableName(rootNode.getOperatorName().toLowerCase());
                            outputTable.setHasPartitions(false);

                            List<FieldSchema> outputCols = new LinkedList<>();
                            for(TableRegEntry regEntry : tableRegistry.getEntries()){
                                if(tableScanDesc.getAlias().equals(regEntry.getAlias())){
                                    for(ColumnTypePair pair : regEntry.getColumnTypeMap().getColumnAndTypeList()){
                                        for(String neededCol : neededColumns){
                                            if(pair.getColumnName().equals(neededCol)){
                                                FieldSchema outputField = new FieldSchema();
                                                outputField.setName(pair.getColumnName());
                                                outputField.setType(pair.getColumnType());
                                                outputCols.add(outputField);
                                            }
                                        }
                                    }
                                }
                            }

                            outputTable.setAllCols(outputCols);
                            opQuery.setOutputTable(outputTable);

                            /*---Finalize local part of Query---*/
                            opQuery.setLocalQueryString("create table "+rootNode.getOperator().getOperatorId().toLowerCase()+" as "+opQuery.getLocalQueryString());

                            System.out.println(rootNode.getOperator().getOperatorId()+": Query is currently: ["+opQuery.getLocalQueryString()+"]");

                            /*---Check if OpLink is to be created---*/
                            System.out.println(rootNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
                            checkForPossibleOpLinks(rootNode,  opQuery, outputTable);

                            /*----Adding Finished Query to List----*/
                            System.out.println(rootNode.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
                            allQueries.add(opQuery);

                            System.out.println(rootNode.getOperator().getOperatorId()+": Checking if JOIN_POINT: "+child.getOperatorId()+" exists...");

                            if(joinPointList.size() > 0){
                                boolean joinPExists = false;
                                for(JoinPoint jP : joinPointList){
                                    if(jP.getId().equals(child.getOperatorId())){
                                        joinPExists = true;

                                        System.out.println(rootNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" Exists! Removing it from List!");

                                        OperatorQuery associatedQuery = jP.getAssociatedOpQuery();
                                        String otherParent = jP.getCreatedById();
                                        String latestAncestorTableName2 = jP.getLatestAncestorTableName();
                                        OperatorNode secondFather = jP.getOtherFatherNode();
                                        String joinPhrase = jP.getJoinPhrase();

                                        joinPointList.remove(jP);

                                        associatedQuery.addInputTable(opQuery.getOutputTable());

                                        if(joinPhrase.equals("") == false) {
                                            associatedQuery.setLocalQueryString(associatedQuery.getLocalQueryString().concat(" " + joinPhrase + " " + opQuery.getOutputTable().getTableName().toLowerCase() + " on "));
                                            associatedQuery.setExaremeQueryString(associatedQuery.getExaremeQueryString().concat(" " + joinPhrase + " " + opQuery.getOutputTable().getTableName().toLowerCase() + " on "));
                                        }

                                        System.out.println(rootNode.getOperator().getOperatorId()+": Moving to child operator...");

                                        OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                                        goToChildOperator(childNode, rootNode, associatedQuery, updatedSchemaString, opQuery.getOutputTable().getTableName().toLowerCase(), latestAncestorTableName2, secondFather);

                                        return;
                                    }
                                }

                                if(joinPExists == false){
                                    System.out.println(rootNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                                    createMapJoinPoint(rootNode, opQuery, child, inputTable.getTableName());

                                    return;
                                }
                            }
                            else{ //No JoinPoint Exists
                                System.out.println(rootNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                                createMapJoinPoint(rootNode, opQuery, child, inputTable.getTableName());

                                return;
                            }

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

                    extractInputInfoFromTableScan(root);

                    MyTable inputTable = tableRegistry.fetchTableByAlias(tableDesc.getAlias());

                    if(inputTable.getHasPartitions()){
                        System.out.println(root.getOperator().getOperatorId() + ": InputTable: "+tableName+" has Partitions! Partitions are not supported as of now!");
                        int countPartitions = inputTable.getAllPartitions().size();

                        System.out.println(root.getOperator().getOperatorId() + " Current TableScanOperator needs access to: "+countPartitions+" partitions...");
                        System.out.println(root.getOperator().getOperatorId() + " Listing PartitionNames for easier debugging...");
                        for(MyPartition m : inputTable.getAllPartitions()){
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
                        initialiseNewQuery(null, opQuery, inputTable);

                        /*---Extract Columns from TableScan---*/
                        System.out.println(root.getOperator().getOperatorId() + ": Beginning Query Construction from root...");

                        TableScanOperator tbsOp = (TableScanOperator) root.getOperator();
                        TableScanDesc tableScanDesc = tbsOp.getConf();

                        /*---Access select needed columns---*/
                        System.out.println(root.getOperator().getOperatorId() + ": Accessing needed Columns...");
                        List<String> neededColumns = tableScanDesc.getNeededColumns();

                        /*---Access Child of TableScan(must be only 1)---*/
                        List<Operator<?>> children = tbsOp.getChildOperators();

                        if (children.size() == 1) {
                            Operator<?> child = children.get(0);
                            if ((child instanceof FilterOperator) || (child instanceof ReduceSinkOperator) || (child instanceof HashTableSinkOperator)) {
                                if(child instanceof FilterOperator)
                                    System.out.println(root.getOperator().getOperatorId() + ": TS--->FIL connection discovered! Child is a FilterOperator!");
                                else if(child instanceof ReduceSinkOperator)
                                    System.out.println(root.getOperator().getOperatorId() + ": TS--->RS connection discovered! Child is a ReduceSinkOperator!");
                                else
                                    System.out.println(root.getOperator().getOperatorId() + ": TS---->HASH connection discovered! Child is HashTableSinkOperator!");

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
                            else if(child instanceof MapJoinOperator){
                                System.out.println(root.getOperator().getOperatorId()+": Discovered TS-->MAP_JOIN connection!");

                                System.out.println(root.getOperator().getOperatorId()+": Current Query will be ending here...");

                                String updatedSchemaString = root.getOperator().getSchema().toString();

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

                                opQuery.setLocalQueryString(" select " + expression + " from " + inputTable.getTableName().toLowerCase());
                                opQuery.setExaremeQueryString(" select " + expression + " from " + inputTable.getTableName().toLowerCase());


                            /*---Finalising outputTable---*/
                                System.out.println(root.getOperator().getOperatorId()+": Finalising OutputTable...");

                                MyTable outputTable = new MyTable();
                                outputTable.setIsAFile(false);
                                outputTable.setBelongingDatabaseName(inputTable.getBelongingDataBaseName());
                                outputTable.setTableName(root.getOperatorName().toLowerCase());
                                outputTable.setHasPartitions(false);

                                List<FieldSchema> outputCols = new LinkedList<>();
                                for(TableRegEntry regEntry : tableRegistry.getEntries()){
                                    if(tableScanDesc.getAlias().equals(regEntry.getAlias())){
                                        for(ColumnTypePair pair : regEntry.getColumnTypeMap().getColumnAndTypeList()){
                                            for(String neededCol : neededColumns){
                                                if(pair.getColumnName().equals(neededCol)){
                                                    FieldSchema outputField = new FieldSchema();
                                                    outputField.setName(pair.getColumnName());
                                                    outputField.setType(pair.getColumnType());
                                                    outputCols.add(outputField);
                                                }
                                            }
                                        }
                                    }
                                }

                                outputTable.setAllCols(outputCols);
                                opQuery.setOutputTable(outputTable);

                            /*---Finalize local part of Query---*/
                                opQuery.setLocalQueryString("create table "+root.getOperator().getOperatorId().toLowerCase()+" as "+opQuery.getLocalQueryString());

                                System.out.println(root.getOperator().getOperatorId()+": Query is currently: ["+opQuery.getLocalQueryString()+"]");

                            /*---Check if OpLink is to be created---*/
                                System.out.println(root.getOperator().getOperatorId()+": Checking if OpLink can be created...");
                                checkForPossibleOpLinks(root,  opQuery, outputTable);

                            /*----Adding Finished Query to List----*/
                                System.out.println(root.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
                                allQueries.add(opQuery);

                                System.out.println(root.getOperator().getOperatorId()+": Checking if JOIN_POINT: "+child.getOperatorId()+" exists...");

                                if(joinPointList.size() > 0){
                                    boolean joinPExists = false;
                                    for(JoinPoint jP : joinPointList){
                                        if(jP.getId().equals(child.getOperatorId())){
                                            joinPExists = true;

                                            System.out.println(root.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" Exists! Removing it from List!");

                                            OperatorQuery associatedQuery = jP.getAssociatedOpQuery();
                                            String otherParent = jP.getCreatedById();
                                            String latestAncestorTableName2 = jP.getLatestAncestorTableName();
                                            OperatorNode secondFather = jP.getOtherFatherNode();
                                            String joinPhrase = jP.getJoinPhrase();

                                            joinPointList.remove(jP);

                                            associatedQuery.addInputTable(opQuery.getOutputTable());

                                            if(joinPhrase.equals("") == false) {
                                                associatedQuery.setLocalQueryString(associatedQuery.getLocalQueryString().concat(" " + joinPhrase + " " + opQuery.getOutputTable().getTableName().toLowerCase() + " on "));
                                                associatedQuery.setExaremeQueryString(associatedQuery.getExaremeQueryString().concat(" " + joinPhrase + " " + opQuery.getOutputTable().getTableName().toLowerCase() + " on "));
                                            }

                                            System.out.println(root.getOperator().getOperatorId()+": Moving to child operator...");

                                            OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                                            goToChildOperator(childNode, root, associatedQuery, updatedSchemaString, opQuery.getOutputTable().getTableName().toLowerCase(), latestAncestorTableName2, secondFather);

                                            return;
                                        }
                                    }

                                    if(joinPExists == false){
                                        System.out.println(root.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                                        createMapJoinPoint(root, opQuery, child, inputTable.getTableName());

                                        return;
                                    }
                                }
                                else{ //No JoinPoint Exists
                                    System.out.println(root.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                                    createMapJoinPoint(root, opQuery, child, inputTable.getTableName());

                                    return;
                                }

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
            if((fatherOperatorNode.getOperator() instanceof TableScanOperator) || (fatherOperatorNode.getOperator() instanceof JoinOperator) || (fatherOperatorNode.getOperator() instanceof MapJoinOperator) ){
                if(fatherOperatorNode.getOperator() instanceof TableScanOperator)
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a TableScanOperator: "+fatherOperatorNode.getOperatorName());
                else if(fatherOperatorNode.getOperator() instanceof JoinOperator)
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a JoinOperator: "+fatherOperatorNode.getOperatorName());
                else
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a MapJoinOperator: "+fatherOperatorNode.getOperatorName());

                String updatedSchemaString = currentOperatorNode.getOperator().getSchema().toString();

                if(currentOperatorNode.getOperator().getSchema() == null){
                    updatedSchemaString = latestAncestorSchema;
                }

                if(currentOperatorNode.getOperator().getColumnExprMap() == null){
                    if(currentOperatorNode.getOperator().getSchema() == null){
                        addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);
                        updatedSchemaString = latestAncestorSchema;
                    }
                    else{
                        if(fatherOperatorNode.getOperator() instanceof TableScanOperator) {
                            useSchemaToFillAliasesForTable(currentOperatorNode, fatherOperatorNode, latestAncestorTableName1);
                        }
                        else {
                            addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);
                            updatedSchemaString = latestAncestorSchema;
                        }
                    }
                }
                else{
                    updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);
                }

                if(fatherOperatorNode.getOperator() instanceof TableScanOperator) {
                    /*----Extracting predicate of FilterOp----*/
                    if ((fatherOperatorNode.getOperator().getParentOperators() == null) || (fatherOperatorNode.getOperator().getParentOperators().size() == 0)) {
                        /*---Add where statement to Query---*/
                        addWhereStatementToQuery(currentOperatorNode, currentOpQuery, latestAncestorTableName1);
                    } else {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Father TableScanOperator is not ROOT! Unsupported yet!");
                        System.exit(0);
                    }

                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a type of JoinOperator! Use ancestral schema to add Columns and transform current Schema!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Ancestral Schema: "+latestAncestorSchema);

                    /*---Add where statement to Query---*/
                    addWhereStatementToQuery2(currentOperatorNode, fatherOperatorNode, currentOpQuery, latestAncestorSchema, latestAncestorTableName1);

                }

                /*---Check Type of Child---*/
                List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
                if(children != null){
                    if(children.size() == 1){
                        Operator<?> child = children.get(0);
                        if(child instanceof SelectOperator) {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered FIL--->SEL Connection! Child is select: " + child.getOperatorId());
                            if ( (fatherOperatorNode.getOperator() instanceof JoinOperator) || ( fatherOperatorNode.getOperator() instanceof MapJoinOperator) ) {
                                if (currentOpQuery.getLocalQueryString().contains("select ") == false) {
                                    latestAncestorSchema = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, null, latestAncestorSchema, currentOpQuery, null);
                                }
                            }

                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Current Query will be ending here...");

                            if (fatherOperatorNode.getOperator() instanceof TableScanOperator){

                                /*---Finalising outputTable---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");
                                MyTable outputTable = new MyTable();
                                outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                                outputTable.setIsAFile(false);
                                List<FieldSchema> newCols = new LinkedList<>();

                                String newSchemaString = addNewPossibleAliases(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, null);

                                newSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, newSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols);


                                /*---Finalize local part of Query---*/
                                currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

                                /*---Check if OpLink is to be created---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
                                checkForPossibleOpLinks(currentOperatorNode,  currentOpQuery, outputTable);

                                /*----Adding Finished Query to List----*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
                                allQueries.add(currentOpQuery);

                                /*----Begin a new Query----*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
                                OperatorQuery opQuery = new OperatorQuery();
                                initialiseNewQuery(currentOpQuery, opQuery, outputTable);

                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + opQuery.getLocalQueryString() + "]");

                                OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                                /*---Moving to Child Operator---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Moving to Child: " + child.getOperatorId());
                                goToChildOperator(childNode, currentOperatorNode, opQuery, updatedSchemaString, child.getOperatorId().toLowerCase(), latestAncestorTableName2, null);
                            }
                            else if( (fatherOperatorNode.getOperator() instanceof JoinOperator) || ( fatherOperatorNode.getOperator() instanceof MapJoinOperator)){ //This differs from above case because the above case refers to ROOT TableScan ending

                                /*---Finalising outputTable---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");
                                MyTable outputTable = new MyTable();
                                outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                                outputTable.setIsAFile(false);
                                List<FieldSchema> newCols = new LinkedList<>();

                                updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols);

                                /*---Finalize local part of Query---*/
                                currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

                                /*---Check if OpLink is to be created---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
                                checkForPossibleOpLinks(currentOperatorNode,  currentOpQuery, outputTable);

                                /*----Adding Finished Query to List----*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
                                allQueries.add(currentOpQuery);

                                /*----Begin a new Query----*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
                                OperatorQuery opQuery = new OperatorQuery();
                                initialiseNewQuery(currentOpQuery, opQuery, currentOpQuery.getOutputTable());

                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + opQuery.getLocalQueryString() + "]");

                                OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                                /*---Moving to Child Operator---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Moving to Child: " + child.getOperatorId());
                                goToChildOperator(childNode, currentOperatorNode, opQuery, updatedSchemaString, child.getOperatorId().toLowerCase(), latestAncestorTableName2, null);

                            }
                            else {
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Unsupported type of father for ending Query!");
                                System.exit(0);
                            }


                        }
                        else if(child instanceof GroupByOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered FIL--->GBY Connection! Child is select: " + child.getOperatorId());
                            if(currentOpQuery.getLocalQueryString().contains("select ") == false) {
                                updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null);
                            }

                             /*---Moving to Child Operator---*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Moving to Child: " + child.getOperatorId());
                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);

                        }
                        else if(child instanceof ReduceSinkOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered FIL--->RS Connection! Child is select: " + child.getOperatorId());
                            if (fatherOperatorNode.getOperator() instanceof JoinOperator) {
                                if (currentOpQuery.getLocalQueryString().contains("select ") == false) {
                                    updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null);
                                }
                            }

                             /*---Moving to Child Operator---*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Moving to Child: " + child.getOperatorId());
                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, currentOperatorNode.getOperator().getSchema().toString(), latestAncestorTableName1, latestAncestorTableName2, null);

                        }
                        else if(child instanceof HashTableSinkOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered FIL--->HASH Connection! Child is select: " + child.getOperatorId());

                            if (currentOpQuery.getLocalQueryString().contains("select ") == false) {
                                updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null);
                            }

                             /*---Moving to Child Operator---*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Moving to Child: " + child.getOperatorId());
                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, currentOperatorNode.getOperator().getSchema().toString(), latestAncestorTableName1, latestAncestorTableName2, null);

                        }
                        else if(child instanceof MapJoinOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered FIL--->MAP_JOIN Connection! Child is select: " + child.getOperatorId());

                            if (currentOpQuery.getLocalQueryString().contains("select ") == false) {
                                updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null);
                            }

                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

                            /*---Finalising outputTable---*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");

                            MyTable outputTable = new MyTable();
                            outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                            outputTable.setIsAFile(false);
                            List<FieldSchema> newCols = new LinkedList<>();

                            updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, currentOperatorNode.getOperator().getSchema().toString(), currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols);

                            /*---Finalize local part of Query---*/
                            currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

                            /*---Check if OpLink is to be created---*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
                            checkForPossibleOpLinks(currentOperatorNode,  currentOpQuery, outputTable);

                            /*----Adding Finished Query to List----*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
                            allQueries.add(currentOpQuery);

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

                                        if(joinPhrase.equals("") == false) {
                                            associatedQuery.setLocalQueryString(associatedQuery.getLocalQueryString().concat(" " + joinPhrase + " " + currentOpQuery.getOutputTable().getTableName().toLowerCase() + " on "));
                                            associatedQuery.setExaremeQueryString(associatedQuery.getExaremeQueryString().concat(" " + joinPhrase + " " + currentOpQuery.getOutputTable().getTableName().toLowerCase() + " on "));
                                        }

                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                                        OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                                        goToChildOperator(childNode, currentOperatorNode, associatedQuery, currentOperatorNode.getOperator().getSchema().toString(), currentOpQuery.getOutputTable().getTableName().toLowerCase(), latestAncestorTableName2, secondFather);

                                        return;
                                    }
                                }

                                if(joinPExists == false){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                                    createMapJoinPoint(currentOperatorNode, currentOpQuery, child, latestAncestorTableName1);

                                    return;
                                }
                            }
                            else{ //No JoinPoint Exists
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                                createMapJoinPoint(currentOperatorNode, currentOpQuery, child, latestAncestorTableName1);

                                return;
                            }

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
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": TableScanOperator has null columnExprMap! Assumming ancestor's schema...");

                    if(currentOperatorNode.getOperator().getSchema() == null){
                        addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);
                    }
                    else{
                        useSchemaToFillAliasesForTable(currentOperatorNode, fatherOperatorNode, latestAncestorTableName1);
                    }

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
                            else if(child instanceof UnionOperator){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered FS-->TS-->UNION connection!");

                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

                                String updatedSchemaString = latestAncestorSchema;

                                /*---Check select statement exists---*/
                                if(currentOpQuery.getLocalQueryString().contains("select ") == false) {
                                    updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null);
                                }

                                /*---Finalising outputTable---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");

                                MyTable outputTable = new MyTable();
                                outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                                outputTable.setIsAFile(false);
                                List<FieldSchema> newCols = new LinkedList<>();

                                updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols);

                                /*---Finalize local part of Query---*/
                                currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

                                /*---Check if OpLink is to be created---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
                                checkForPossibleOpLinks(currentOperatorNode,  currentOpQuery, outputTable);

                                /*----Adding Finished Query to List----*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
                                allQueries.add(currentOpQuery);

                                //Get Union Details
                                UnionOperator unionOp = (UnionOperator) child;
                                UnionDesc unionDesc = unionOp.getConf();

                                int numOfUnionInputs = unionDesc.getNumInputs();

                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if UNION_POINT: "+child.getOperatorId()+" exists...");

                                if(unionPointList.size() > 0){
                                    boolean unionPExists = false;
                                    for(UnionPoint uP : unionPointList){
                                        if(uP.getId().equals(child.getOperatorId())){
                                            unionPExists = true;
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": UNION_POINT: "+child.getOperatorId()+" Exists!");

                                            if(numOfUnionInputs - 1 == uP.getPassesMade()){ //This is the final input branch
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": UNION_POINT: "+child.getOperatorId()+" is the final branch!");

                                                finaliseUnionPoint(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, child, latestAncestorTableName1, updatedSchemaString, uP);

                                                return;
                                            }
                                            else{ //This is not the final input branch
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": UNION_POINT: "+child.getOperatorId()+" is not the final branch...");

                                                updateUnionPoint(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, child, latestAncestorTableName1, updatedSchemaString, uP);

                                                return;
                                            }
                                        }
                                    }

                                    if(unionPExists == false){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": UNION_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                                        createUnionPoint(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, child, latestAncestorTableName1, updatedSchemaString);

                                        return;
                                    }
                                }
                                else{ //No JoinPoint Exists
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": UNION_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                                    createUnionPoint(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, child, latestAncestorTableName1, updatedSchemaString);

                                    return;
                                }

                            }
                            else if(child instanceof MapJoinOperator){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered FS-->TS-->MAP_JOIN connection!");

                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

                                String updatedSchemaString = latestAncestorSchema;

                                /*---Check select statement exists---*/
                                if(currentOpQuery.getLocalQueryString().contains("select ") == false) {
                                    updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null);
                                }

                                /*---Finalising outputTable---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");

                                MyTable outputTable = new MyTable();
                                outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                                outputTable.setIsAFile(false);
                                List<FieldSchema> newCols = new LinkedList<>();

                                updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols);

                                /*---Finalize local part of Query---*/
                                currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

                                /*---Check if OpLink is to be created---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
                                checkForPossibleOpLinks(currentOperatorNode,  currentOpQuery, outputTable);

                                /*----Adding Finished Query to List----*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
                                allQueries.add(currentOpQuery);

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

                                            if(joinPhrase.equals("") == false) {
                                                associatedQuery.setLocalQueryString(associatedQuery.getLocalQueryString().concat(" " + joinPhrase + " " + currentOpQuery.getOutputTable().getTableName().toLowerCase() + " on "));
                                                associatedQuery.setExaremeQueryString(associatedQuery.getExaremeQueryString().concat(" " + joinPhrase + " " + currentOpQuery.getOutputTable().getTableName().toLowerCase() + " on "));
                                            }

                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                                            OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                                            goToChildOperator(childNode, currentOperatorNode, associatedQuery, updatedSchemaString, currentOpQuery.getOutputTable().getTableName().toLowerCase(), latestAncestorTableName2, secondFather);

                                            return;
                                        }
                                    }

                                    if(joinPExists == false){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                                        createMapJoinPoint(currentOperatorNode, currentOpQuery, child, latestAncestorTableName1);

                                        return;
                                    }
                                }
                                else{ //No JoinPoint Exists
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                                    createMapJoinPoint(currentOperatorNode, currentOpQuery, child, latestAncestorTableName1);

                                    return;
                                }

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
            if((fatherOperatorNode.getOperator() instanceof FilterOperator) || (fatherOperatorNode.getOperator() instanceof JoinOperator) || (fatherOperatorNode.getOperator() instanceof MapJoinOperator) || (fatherOperatorNode.getOperator() instanceof ReduceSinkOperator) || (fatherOperatorNode.getOperator() instanceof TableScanOperator)){
                if(fatherOperatorNode.getOperator() instanceof FilterOperator) {
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Parent Operator is FilterOperator!");
                }
                else if(fatherOperatorNode.getOperator() instanceof JoinOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Parent Operator is JoinOperator!");
                }
                else if(fatherOperatorNode.getOperator() instanceof MapJoinOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Parent Operator is MapJoinOperator!");
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
                    updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null);
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
                    updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null);
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
                        else if(child instanceof FileSinkOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered SEL--->FS Connection! Child is : "+child.getOperatorId());
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
                groupByHasFatherSelectFilterJoinReduceSinkMapJoin(currentOperatorNode, currentOpQuery, fatherOperatorNode, otherFatherNode, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2);

            }
            else if(fatherOperatorNode.getOperator() instanceof ReduceSinkOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is ReduceSinkOperator: "+fatherOperatorNode.getOperatorName());

                groupByHasFatherSelectFilterJoinReduceSinkMapJoin(currentOperatorNode, currentOpQuery, fatherOperatorNode, otherFatherNode, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2);

            }
            else if(fatherOperatorNode.getOperator() instanceof FilterOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is FilterOperator: "+fatherOperatorNode.getOperatorName());
                groupByHasFatherSelectFilterJoinReduceSinkMapJoin(currentOperatorNode, currentOpQuery, fatherOperatorNode, otherFatherNode, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2);

            }
            else if(fatherOperatorNode.getOperator() instanceof JoinOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is JoinOperator: "+fatherOperatorNode.getOperatorName());
                groupByHasFatherSelectFilterJoinReduceSinkMapJoin(currentOperatorNode, currentOpQuery, fatherOperatorNode, otherFatherNode, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2);

            }
            else{
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported Type of Parent Operator for GroupByOperator...");
                System.exit(0);
            }
        }
        else if(currentOperatorNode.getOperator() instanceof ReduceSinkOperator){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Operator is a ReduceSinkOperator...");
            if(fatherOperatorNode.getOperator() instanceof GroupByOperator) {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Father is GroupByOperator: " + fatherOperatorNode.getOperatorName());

                reduceSinkHasFatherGroup( currentOperatorNode,  fatherOperatorNode,  otherFatherNode,  currentOpQuery, latestAncestorSchema,  latestAncestorTableName1, latestAncestorTableName2);

            }
            else if(fatherOperatorNode.getOperator() instanceof FilterOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is FilterOperator: "+fatherOperatorNode.getOperatorName());

                reduceSinkHasFatherFilter(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2);

            }
            else if(fatherOperatorNode.getOperator() instanceof FileSinkOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a FileSinkOperator: "+fatherOperatorNode.getOperatorName());

                reduceSinkHasFatherFileSink(currentOperatorNode, currentOpQuery, fatherOperatorNode, otherFatherNode, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2);
            }
            else if(fatherOperatorNode.getOperator() instanceof TableScanOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is TableScanOperator: "+fatherOperatorNode.getOperatorName());

                reduceSinkHasFatherTableScan(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2);

            }
            else if(fatherOperatorNode.getOperator() instanceof SelectOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a SelectOperator: "+fatherOperatorNode.getOperatorName());

                reduceSinkHasFatherSelect(currentOperatorNode, currentOpQuery, fatherOperatorNode, otherFatherNode, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2);

            }
            else if(fatherOperatorNode.getOperator() instanceof UnionOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a UnionOperator: "+fatherOperatorNode.getOperatorName());

                reduceSinkHasFatherUnion(currentOperatorNode, currentOpQuery, fatherOperatorNode, otherFatherNode, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2);

            }
            else{
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported Type of Parent Operator for ReduceSinkOperator...");
                System.exit(0);
            }
        }
        else if(currentOperatorNode.getOperator() instanceof HashTableSinkOperator){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Operator is a HashTableOperator...");
            if(fatherOperatorNode.getOperator() instanceof FilterOperator){

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a FilterOperator: "+fatherOperatorNode.getOperatorName());

                hashTableSinkHasFatherFilter(currentOperatorNode, currentOpQuery, fatherOperatorNode, otherFatherNode, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2);

            }
            else if(fatherOperatorNode.getOperator() instanceof TableScanOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a TableScanOperator: "+fatherOperatorNode.getOperatorName());

                hashTableSinkHasFatherTableScan(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2);
            }
            else{
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported Type of Parent Operator for HashTableSinkOperator...");
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

                joinHasFatherReduceSinks(currentOperatorNode, currentOpQuery, fatherOperatorNode, otherFatherNode, latestAncestorTableName1, latestAncestorTableName2);

            }
            else{
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported Type of Parent Operator for ReduceSinkOperator...");
                System.exit(0);
            }
        }
        else if(currentOperatorNode.getOperator() instanceof MapJoinOperator) {
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Operator is a MapJoinOperator...");
            if(otherFatherNode == null){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": otherFather is NULL! Error!");
                System.exit(0);
            }
            if(( (fatherOperatorNode.getOperator() instanceof HashTableSinkOperator) || (fatherOperatorNode.getOperator() instanceof FilterOperator) || (fatherOperatorNode.getOperator() instanceof TableScanOperator) ) && ( (otherFatherNode.getOperator() instanceof HashTableSinkOperator) || (otherFatherNode.getOperator() instanceof FilterOperator)  || (otherFatherNode.getOperator() instanceof TableScanOperator) )){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": MapJoinOperator: "+currentOperatorNode.getOperatorName()+" has parents: "+fatherOperatorNode.getOperatorName()+" , "+otherFatherNode.getOperatorName());

                mapJoinHasFatherHashFilterTableScan(currentOperatorNode, currentOpQuery, fatherOperatorNode, otherFatherNode, latestAncestorTableName1, latestAncestorTableName2);

            }
            else{
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported Type of Parent Operator for MapJoinOperator...");
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

                limitHasFatherSelectGroup(currentOperatorNode, fatherOperatorNode, currentOpQuery, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2);

            }
            else{
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported Type of Parent Operator for LimitOperator...");
                System.exit(0);
            }
        }
        else if(currentOperatorNode.getOperator() instanceof FileSinkOperator){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Operator is a FileSinkOperator...");
            if((fatherOperatorNode.getOperator() instanceof LimitOperator) || (fatherOperatorNode.getOperator() instanceof GroupByOperator) || (fatherOperatorNode.getOperator() instanceof SelectOperator) || (fatherOperatorNode.getOperator() instanceof MapJoinOperator)){
                if(fatherOperatorNode.getOperator() instanceof LimitOperator) {
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Father is a LimitOperator...");
                }
                else if(fatherOperatorNode.getOperator() instanceof GroupByOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Father is a GroupByOperator...");
                }
                else if(fatherOperatorNode.getOperator() instanceof SelectOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Father is a GroupByOperator...");
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Father is a MapJoinOperator...");
                }

                fileSinkHasFatherLimitGroupSelectMap(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2);
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
                listSinkHasFatherFileSink(currentOperatorNode, fatherOperatorNode, currentOpQuery, latestAncestorSchema);
            }
            else{
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported Type of Parent Operator for FileSinkOperator...");
                System.exit(0);
            }
        }
        else if(currentOperatorNode.getOperator() instanceof UnionOperator){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Operator is a UnionOperator...");
            if(fatherOperatorNode.getOperator() instanceof TableScanOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a TableScanSinkOperator...(but UnionOperator has more parent actually)...");
                unionHasFatherTableScan(currentOperatorNode, fatherOperatorNode, currentOpQuery, latestAncestorSchema);
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

    public void hashTableSinkHasFatherFilter(OperatorNode currentOperatorNode, OperatorQuery currentOpQuery, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, String latestAncestorSchema, String latestAncestorTableName1, String latestAncestorTableName2){

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is FilterOperator: "+fatherOperatorNode.getOperatorName());

        String updatedSchemaString = "";

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if GrandFather is Root...");
        OperatorNode grandFatherOperatorNode = exaremeGraph.getOperatorNodeByName(fatherOperatorNode.getOperator().getParentOperators().get(0).getOperatorId());

        if((grandFatherOperatorNode.getOperator().getParentOperators() == null) || (grandFatherOperatorNode.getOperator().getParentOperators().size() == 0)){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": GrandFather is root...TableScan!");

            if( (currentOperatorNode.getOperator().getSchema() == null) || currentOperatorNode.getOperator().getSchema().toString().equals("()") ){
                if(currentOperatorNode.getOperator().getColumnExprMap() != null){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": RowSchema is empty! We will build Schema from ColumnExprMap...");

                    updatedSchemaString = buildSchemaFromColumnExprMap(currentOperatorNode, grandFatherOperatorNode.getOperator().getSchema().toString(), fatherOperatorNode);

                }
                else{
                    addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);
                    updatedSchemaString = latestAncestorSchema;
                }
            }
            else {
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": TableScanOperator has null columnExprMap! Assumming ancestor's schema...");

                if(currentOperatorNode.getOperator().getColumnExprMap() == null){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": columnExprMap is empty! Assuming older Schema!");

                    updatedSchemaString = latestAncestorSchema;

                    if(currentOperatorNode.getOperator().getSchema() == null){
                        addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);
                    }
                    else{
                        useSchemaToFillAliasesForTable(currentOperatorNode, fatherOperatorNode, latestAncestorTableName1);
                    }

                }
                else {
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...");
                    updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);
                }
            }
        }
        else{
            if(currentOperatorNode.getOperator().getColumnExprMap() == null){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": ColumnExprMap is empty!");
                if((currentOperatorNode.getOperator().getSchema() == null) || (currentOperatorNode.getOperator().getSchema().toString().equals("()"))){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is () ! Assuming older schema!");
                    updatedSchemaString = latestAncestorSchema;
                    addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is not empty! BuildColumnExprMap from schema!");
                    updatedSchemaString = buildSchemaFromColumnExprMap(currentOperatorNode, fatherOperatorNode.getOperator().getSchema().toString(), fatherOperatorNode);
                }
            }
            else {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...");
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);
            }
        }

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
        if(currentOpQuery.getLocalQueryString().contains("select ") == false){
            updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, latestAncestorSchema, currentOpQuery, null);
        }

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

        /*---Finalising outputTable---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");

        //if(currentOperatorNode.getOperator().getSchema().toString().equals("()")){
        //    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is empty! Assuming older Schema!");
         //   updatedSchemaString = latestAncestorSchema;
        //}

        MyTable outputTable = new MyTable();
        outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
        outputTable.setIsAFile(false);
        List<FieldSchema> newCols = new LinkedList<>();

        updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols);

        /*---Finalize local part of Query---*/
        currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

        /*---Check if OpLink is to be created---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
        checkForPossibleOpLinks(currentOperatorNode,  currentOpQuery, outputTable);

        /*----Adding Finished Query to List----*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
        allQueries.add(currentOpQuery);


        //TODO JOIN
        /*---Moving to Child Operator---*/
        List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
        if(children != null){
            if(children.size() == 1){
                Operator<?> child = children.get(0);
                if(child instanceof MapJoinOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered HASH--->MAP_JOIN connection!");
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

                                if(joinPhrase.equals("") == false) {
                                    associatedQuery.setLocalQueryString(associatedQuery.getLocalQueryString().concat(" " + joinPhrase + " " + currentOpQuery.getOutputTable().getTableName().toLowerCase() + " on "));
                                    associatedQuery.setExaremeQueryString(associatedQuery.getExaremeQueryString().concat(" " + joinPhrase + " " + currentOpQuery.getOutputTable().getTableName().toLowerCase() + " on "));
                                }

                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                                OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                                goToChildOperator(childNode, currentOperatorNode, associatedQuery, updatedSchemaString, currentOpQuery.getOutputTable().getTableName().toLowerCase(), latestAncestorTableName2, secondFather);

                                return;
                            }
                        }

                        if(joinPExists == false){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                            createMapJoinPoint(currentOperatorNode, currentOpQuery, child, latestAncestorTableName1);

                            return;
                        }
                    }
                    else{ //No JoinPoint Exists
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                        createMapJoinPoint(currentOperatorNode, currentOpQuery, child, latestAncestorTableName1);

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

    public void reduceSinkHasFatherGroup(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, OperatorQuery currentOpQuery, String latestAncestorSchema, String latestAncestorTableName1, String latestAncestorTableName2){

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Adding new possible Aliases...");
        String updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
        if(currentOpQuery.getLocalQueryString().contains("select ") == false){
            updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, latestAncestorSchema, currentOpQuery, null);
        }

        /*---Add possible Order By Keys---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if ORDER BY exists in this RS...");
        discoverOrderByKeys(currentOperatorNode, fatherOperatorNode, currentOpQuery, latestAncestorTableName1);

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

        /*---Finalising outputTable---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");
        MyTable outputTable = new MyTable();
        outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
        outputTable.setIsAFile(false);
        List<FieldSchema> newCols = new LinkedList<>();

        if(currentOperatorNode.getOperator().getSchema().toString().equals("()")){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is empty! Assuming older Schema!");
            updatedSchemaString = latestAncestorSchema;
        }

        updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols);

        /*---Finalize local part of Query---*/
        currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

        /*---Check if OpLink is to be created---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
        checkForPossibleOpLinks(currentOperatorNode,  currentOpQuery, outputTable);

        /*----Adding Finished Query to List----*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
        allQueries.add(currentOpQuery);

        /*----Begin a new Query----*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
        OperatorQuery opQuery = new OperatorQuery();
        initialiseNewQuery(currentOpQuery, opQuery, currentOpQuery.getOutputTable());

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

    public void reduceSinkHasFatherFilter(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, OperatorQuery currentOpQuery, String latestAncestorSchema, String latestAncestorTableName1, String latestAncestorTableName2){

        String updatedSchemaString = "";

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if GrandFather is Root...");
        OperatorNode grandFatherOperatorNode = exaremeGraph.getOperatorNodeByName(fatherOperatorNode.getOperator().getParentOperators().get(0).getOperatorId());

        if((grandFatherOperatorNode.getOperator().getParentOperators() == null) || (grandFatherOperatorNode.getOperator().getParentOperators().size() == 0)){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": GrandFather is root...TableScan!");

            if( (currentOperatorNode.getOperator().getSchema() == null) || currentOperatorNode.getOperator().getSchema().toString().equals("()") ){
                if(currentOperatorNode.getOperator().getColumnExprMap() != null){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": RowSchema is empty! We will build Schema from ColumnExprMap...");

                    updatedSchemaString = buildSchemaFromColumnExprMap(currentOperatorNode, grandFatherOperatorNode.getOperator().getSchema().toString(), fatherOperatorNode);

                }
                else{
                    updatedSchemaString = latestAncestorSchema;
                    addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);
                }
            }
            else {
                if(currentOperatorNode.getOperator().getColumnExprMap() == null){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is empty! Assuming older Schema!");
                    updatedSchemaString = latestAncestorSchema;
                    addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);
                }
                else {
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...");
                    updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);
                }
            }
        }
        else{
            if(currentOperatorNode.getOperator().getColumnExprMap() == null){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": ColumnExprMap is empty!");
                if((currentOperatorNode.getOperator().getSchema() == null) || (currentOperatorNode.getOperator().getSchema().toString().equals("()"))){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is () ! Assuming older schema!");
                    updatedSchemaString = latestAncestorSchema;
                    addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is not empty! BuildColumnExprMap from schema!");
                    updatedSchemaString = buildSchemaFromColumnExprMap(currentOperatorNode, fatherOperatorNode.getOperator().getSchema().toString(), fatherOperatorNode);
                }
            }
            else {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...");
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);
            }
        }

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
        if(currentOpQuery.getLocalQueryString().contains("select ") == false){
            updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, latestAncestorSchema, currentOpQuery, null);
        }

        /*---Add possible Order By Keys---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if ORDER BY exists in this RS...");
        discoverOrderByKeys(currentOperatorNode, fatherOperatorNode, currentOpQuery, latestAncestorTableName1);

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

        /*---Finalising outputTable---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");

        //if(currentOperatorNode.getOperator().getSchema().toString().equals("()")){
        //   System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is empty! Assuming older Schema!");
        //    updatedSchemaString = latestAncestorSchema;
        //}

        MyTable outputTable = new MyTable();
        outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
        outputTable.setIsAFile(false);
        List<FieldSchema> newCols = new LinkedList<>();

        updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols);

        /*---Finalize local part of Query---*/
        currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

        /*---Check if OpLink is to be created---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
        checkForPossibleOpLinks(currentOperatorNode,  currentOpQuery, outputTable);

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

                                if(joinPhrase.equals("") == false) {
                                    associatedQuery.setLocalQueryString(associatedQuery.getLocalQueryString().concat(" " + joinPhrase + " " + currentOpQuery.getOutputTable().getTableName().toLowerCase() + " on "));
                                    associatedQuery.setExaremeQueryString(associatedQuery.getExaremeQueryString().concat(" " + joinPhrase + " " + currentOpQuery.getOutputTable().getTableName().toLowerCase() + " on "));
                                }

                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                                OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                                goToChildOperator(childNode, currentOperatorNode, associatedQuery, updatedSchemaString, currentOpQuery.getOutputTable().getTableName().toLowerCase(), latestAncestorTableName2, secondFather);

                                return;
                            }
                        }

                        if(joinPExists == false){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                            createJoinPoint(currentOperatorNode, currentOpQuery, child, latestAncestorTableName1);

                            return;
                        }
                    }
                    else{ //No JoinPoint Exists
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                        createJoinPoint(currentOperatorNode, currentOpQuery, child, latestAncestorTableName1);

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

    public void reduceSinkHasFatherTableScan(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, OperatorQuery currentOpQuery, String latestAncestorSchema, String latestAncestorTableName1, String latestAncestorTableName2){

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: reduceSinkHasFatherTableScan");

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if Father is Root...");
        if((fatherOperatorNode.getOperator().getParentOperators() == null) || (fatherOperatorNode.getOperator().getParentOperators().size() == 0)){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is root...OK!");
        }
        else{
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is not ROOT! Error!");
            System.exit(0);
        }

        String updatedSchemaString = "";

        if( (currentOperatorNode.getOperator().getSchema() == null) || currentOperatorNode.getOperator().getSchema().toString().equals("()") ){
            if(currentOperatorNode.getOperator().getColumnExprMap() != null){
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": RowSchema is empty! We will build Schema from ColumnExprMap...");

                updatedSchemaString = buildSchemaFromColumnExprMap(currentOperatorNode, latestAncestorSchema, fatherOperatorNode);

            }
            else{
                addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);
                updatedSchemaString = latestAncestorSchema;
            }
        }
        else{
            if(currentOperatorNode.getOperator().getColumnExprMap() == null){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": ColumnExprMap is empty!");

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is not empty! BuildColumnExprMap from schema!");
                updatedSchemaString = buildSchemaFromColumnExprMap(currentOperatorNode, fatherOperatorNode.getOperator().getSchema().toString(), fatherOperatorNode);

            }
            else {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...");
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);
            }
        }

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
        if(currentOpQuery.getLocalQueryString().contains("select ") == false){
            updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null);
        }

        /*---Add possible Order By Keys---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if ORDER BY exists in this RS...");
        discoverOrderByKeys(currentOperatorNode, fatherOperatorNode, currentOpQuery, latestAncestorTableName1);

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

        /*---Finalising outputTable---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");

        MyTable outputTable = new MyTable();
        outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
        outputTable.setIsAFile(false);
        List<FieldSchema> newCols = new LinkedList<>();

        updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols);

        /*---Finalize local part of Query---*/
        currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

        /*---Check if OpLink is to be created---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
        checkForPossibleOpLinks(currentOperatorNode,  currentOpQuery, outputTable);

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

                                if(joinPhrase.equals("") == false) {
                                    associatedQuery.setLocalQueryString(associatedQuery.getLocalQueryString().concat(" " + joinPhrase + " " + currentOpQuery.getOutputTable().getTableName().toLowerCase() + " on "));
                                    associatedQuery.setExaremeQueryString(associatedQuery.getExaremeQueryString().concat(" " + joinPhrase + " " + currentOpQuery.getOutputTable().getTableName().toLowerCase() + " on "));
                                }

                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                                OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                                goToChildOperator(childNode, currentOperatorNode, associatedQuery, updatedSchemaString, currentOpQuery.getOutputTable().getTableName().toLowerCase(), latestAncestorTableName2, secondFather);

                                return;
                            }
                        }

                        if(joinPExists == false){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                            createJoinPoint(currentOperatorNode, currentOpQuery, child, latestAncestorTableName1);

                            return;
                        }
                    }
                    else{ //No JoinPoint Exists
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                        createJoinPoint(currentOperatorNode, currentOpQuery, child, latestAncestorTableName1);

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

    public void hashTableSinkHasFatherTableScan(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, OperatorQuery currentOpQuery, String latestAncestorSchema, String latestAncestorTableName1, String latestAncestorTableName2){

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: hashTableSinkScan");

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if Father is Root...");
        if((fatherOperatorNode.getOperator().getParentOperators() == null) || (fatherOperatorNode.getOperator().getParentOperators().size() == 0)){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is root...OK!");
        }
        else{
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is not ROOT! Error!");
            System.exit(0);
        }

        String updatedSchemaString = "";

        if( (currentOperatorNode.getOperator().getSchema() == null) || currentOperatorNode.getOperator().getSchema().toString().equals("()") ){
            if(currentOperatorNode.getOperator().getColumnExprMap() != null){
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": RowSchema is empty! We will build Schema from ColumnExprMap...");

                updatedSchemaString = buildSchemaFromColumnExprMap(currentOperatorNode, latestAncestorSchema, fatherOperatorNode);

            }
            else{
                addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);
                updatedSchemaString = latestAncestorSchema;
            }
        }
        else{
            if(currentOperatorNode.getOperator().getColumnExprMap() == null){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": ColumnExprMap is empty!");

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is not empty! BuildColumnExprMap from schema!");
                updatedSchemaString = buildSchemaFromColumnExprMap(currentOperatorNode, fatherOperatorNode.getOperator().getSchema().toString(), fatherOperatorNode);

            }
            else {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...");
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);
            }
        }

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
        if(currentOpQuery.getLocalQueryString().contains("select ") == false){
            updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null);
        }

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

        /*---Finalising outputTable---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");

        MyTable outputTable = new MyTable();
        outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
        outputTable.setIsAFile(false);
        List<FieldSchema> newCols = new LinkedList<>();

        updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols);

        /*---Finalize local part of Query---*/
        currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

        /*---Check if OpLink is to be created---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
        checkForPossibleOpLinks(currentOperatorNode,  currentOpQuery, outputTable);

        /*----Adding Finished Query to List----*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
        allQueries.add(currentOpQuery);


        //TODO JOIN
        /*---Moving to Child Operator---*/
        List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
        if(children != null){
            if(children.size() == 1){
                Operator<?> child = children.get(0);
                if(child instanceof MapJoinOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered HASH-->JOIN connection!");
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

                                if(joinPhrase.equals("") == false) {
                                    associatedQuery.setLocalQueryString(associatedQuery.getLocalQueryString().concat(" " + joinPhrase + " " + currentOpQuery.getOutputTable().getTableName().toLowerCase() + " on "));
                                    associatedQuery.setExaremeQueryString(associatedQuery.getExaremeQueryString().concat(" " + joinPhrase + " " + currentOpQuery.getOutputTable().getTableName().toLowerCase() + " on "));
                                }

                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                                OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                                goToChildOperator(childNode, currentOperatorNode, associatedQuery, updatedSchemaString, currentOpQuery.getOutputTable().getTableName().toLowerCase(), latestAncestorTableName2, secondFather);

                                return;
                            }
                        }

                        if(joinPExists == false){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                            createMapJoinPoint(currentOperatorNode, currentOpQuery, child, latestAncestorTableName1);

                            return;
                        }
                    }
                    else{ //No MapJoinPoint Exists
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                        createMapJoinPoint(currentOperatorNode, currentOpQuery, child, latestAncestorTableName1);

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

    public void reduceSinkHasFatherFileSink(OperatorNode currentOperatorNode, OperatorQuery currentOpQuery, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, String latestAncestorSchema, String latestAncestorTableName1, String latestAncestorTableName2){

        String updatedSchemaString = "";

        if(currentOperatorNode.getOperator().getParentOperators().get(0) instanceof TableScanOperator){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": True father was a TableScanOperator and was ommited! Use ancestral schema to add Columns and transform current Schema!");
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Ancestral Schema: "+latestAncestorSchema);

            updatedSchemaString = latestAncestorSchema;

            if(currentOperatorNode.getOperator().getColumnExprMap() == null) {
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Ancestral Schema: "+latestAncestorSchema);
                useSchemaToFillAliases(currentOperatorNode, fatherOperatorNode, latestAncestorSchema);
            }
            else {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...");
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);
            }

        }
        else {

            if(currentOperatorNode.getOperator().getColumnExprMap() == null){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": ColumnExprMap is empty!");
                if((currentOperatorNode.getOperator().getSchema() == null) || (currentOperatorNode.getOperator().getSchema().toString().equals("()"))){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is () ! Assuming older schema!");
                    updatedSchemaString = latestAncestorSchema;
                    addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is not empty! BuildColumnExprMap from schema!");
                    updatedSchemaString = buildSchemaFromColumnExprMap(currentOperatorNode, fatherOperatorNode.getOperator().getSchema().toString(), fatherOperatorNode);
                }
            }
            else {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...");
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);
            }

        }

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
        if(currentOpQuery.getLocalQueryString().contains("select ") == false){
            updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null);
        }

        /*---Add possible Order By Keys---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if ORDER BY exists in this RS...");
        discoverOrderByKeys(currentOperatorNode, fatherOperatorNode, currentOpQuery, latestAncestorTableName1);

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

        /*---Finalising outputTable---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");
        MyTable outputTable = new MyTable();
        outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
        outputTable.setIsAFile(false);
        List<FieldSchema> newCols = new LinkedList<>();

        if(currentOperatorNode.getOperator().getSchema().toString().equals("()")){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is empty! Assuming older Schema! FILESINK WITH () SCHEMA NOT FIXED");
            updatedSchemaString = latestAncestorSchema;
            System.exit(0);
        }

        updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols);

        /*---Finalize local part of Query---*/
        currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

        /*---Check if OpLink is to be created---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
        checkForPossibleOpLinks(currentOperatorNode,  currentOpQuery, outputTable);

        /*----Adding Finished Query to List----*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
        allQueries.add(currentOpQuery);

        /*----Begin a new Query----*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
        OperatorQuery opQuery = new OperatorQuery();
        initialiseNewQuery(currentOpQuery, opQuery, currentOpQuery.getOutputTable());

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

    public void groupByHasFatherSelectFilterJoinReduceSinkMapJoin(OperatorNode currentOperatorNode, OperatorQuery currentOpQuery, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, String latestAncestorSchema, String latestAncestorTableName1, String latestAncestorTableName2){

        /*------------------------First begin a new group by Query-------------------------*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": We will finish the query before adding Group By...");
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current query will be ending here...");

        List<FieldSchema> selectCols2 = new LinkedList<>();
        if(currentOpQuery.getLocalQueryString().contains("select ") == false){
            addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, latestAncestorSchema, currentOpQuery, selectCols2);
        }

        /*---Finalising outputTable---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");
        MyTable outputTable = new MyTable();
        outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
        outputTable.setIsAFile(false);
        List<FieldSchema> newCols = new LinkedList<>();

        latestAncestorSchema = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, latestAncestorSchema, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols);

        /*---Finalize local part of Query---*/
        currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

        /*---Check if OpLink is to be created---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
        checkForPossibleOpLinks(currentOperatorNode,  currentOpQuery, outputTable);

        /*----Adding Finished Query to List----*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
        allQueries.add(currentOpQuery);


        /*----Begin a new Query----*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
        OperatorQuery opQuery = new OperatorQuery();
        initialiseNewQuery(currentOpQuery, opQuery, currentOpQuery.getOutputTable());

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+opQuery.getLocalQueryString()+"]");


        String updatedSchemaString = "";

        /*---------------------------Next, check if aggregators exist (this changes the approach we take)-----------------------*/
        GroupByOperator groupByOp = (GroupByOperator) currentOperatorNode.getOperator();
        GroupByDesc groupByDesc = groupByOp.getConf();

        if(groupByOp == null){
            System.out.println("GroupByDesc is null!");
            System.exit(0);
        }

        if((groupByDesc.getAggregators() != null) && (groupByDesc.getAggregators().size() > 0)){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" GROUP BY contains aggregations...");
            if(currentOperatorNode.getOperator().getColumnExprMap() != null){
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...but also fixing schema from structs...");
                String tempSchema = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);

                /*---Fix Schema containing aggregators---*/
                MyMap someMap = new MyMap(true);
                String fixedSchema = "";
                if(currentOperatorNode.getOperator().getSchema().toString().contains("struct<")) {
                    fixedSchema = extractColsFromTypeNameWithStructs(currentOperatorNode.getOperator().getSchema().toString(), someMap, currentOperatorNode.getOperator().getSchema().toString(), true);
                }
                else{
                    fixedSchema = tempSchema;
                    fixedSchema = extractColsFromTypeName(fixedSchema, someMap, fixedSchema, true);
                    for(ColumnTypePair somePair : someMap.getColumnAndTypeList()){
                        if(somePair.getColumnName().contains("_col")){
                            if(somePair.getColumnType().equals("decimal")){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" Pair: ("+somePair.getColumnName()+", "+somePair.getColumnType()+") will get a struct type instead");
                                somePair.setColumnType("struct");
                                fixedSchema = fixedSchema.replace("decimal", "struct");
                                System.out.println("Schema now: "+fixedSchema);
                            }
                            else{
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" Pair: ("+somePair.getColumnName()+", "+somePair.getColumnType()+") NOT READY TO BE CONVERTED TO STUCT");
                                System.exit(0);
                            }
                        }
                    }
                }

                /*---Add possible new aggregations---*/

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" Creating subMap with aggregations only...");
                MyMap subMap = new MyMap(true);
                for(ColumnTypePair oldPair : someMap.getColumnAndTypeList()){
                    if(oldPair.getColumnType().contains("struct")){
                        subMap.addPair(oldPair);
                    }
                }

                ArrayList<AggregationDesc> aggregationDescs = groupByDesc.getAggregators();
                if(subMap.getColumnAndTypeList().size() != aggregationDescs.size()){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" Expected aggregationsDesc to have same size as subMap! Printing subMap for debugging and exitting!");
                    subMap.printMap();
                    System.exit(0);
                }

                //Using fatherOperatorNode try to fix each aggregationExpression
                List<String> aggregations = new LinkedList<>();
                int j = 0;
                for(AggregationDesc aggDesc : aggregationDescs){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" Working on aggregation: "+aggDesc.getExprString());

                    String aggregationPhrase = aggDesc.getExprString();

                    //Get Parameters
                    List<String> parameterColNames = new LinkedList<>();
                    if(aggDesc.getParameters() == null){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" NO parameters for aggregation: "+aggDesc.toString());
                        System.exit(0);
                    }

                    if(aggDesc.getParameters().size() == 0){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" NO parameters for aggregation: "+aggDesc.toString());
                        System.exit(0);
                    }

                    for(ExprNodeDesc exprNode : aggDesc.getParameters()){
                        if(exprNode.getCols() == null){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" NO parameter cols for aggregation: "+aggDesc.toString());
                            System.exit(0);
                        }

                        if(exprNode.getCols().size() == 0){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" NO parameter cols for aggregation: "+aggDesc.toString());
                            System.exit(0);
                        }

                        for(String col : exprNode.getCols()) {
                            parameterColNames.add(col);
                        }

                    }

                    //Try to locate the real aliases of the cols through the father
                    for(String paramCol : parameterColNames){
                        System.out.println("Attempting to locate paramCol: "+paramCol+" in TableRegistry...");
                        boolean columnLocated = false;
                        for(TableRegEntry tableRegEntry : tableRegistry.getEntries()){
                            for(ColumnTypePair somePair : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()){
                                if(somePair.getColumnName().equals(paramCol)){ //Match with columnName - now look for father
                                    for(StringParameter sP : somePair.getAltAliasPairs()){
                                        if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){ //Located father
                                            System.out.println("Located realAlias: "+somePair.getColumnName());
                                            columnLocated = true;
                                            opQuery.addUsedColumn(somePair.getColumnName(), currentOperatorNode.getOperatorName().toLowerCase());
                                            break;
                                        }
                                    }
                                }
                                else{
                                    for(StringParameter sP : somePair.getAltAliasPairs()){
                                        if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){ //Located father
                                            if(sP.getValue().equals(paramCol)){
                                                System.out.println("Located realAlias: "+somePair.getColumnName());
                                                if(aggregationPhrase.contains(paramCol+" ")){
                                                    aggregationPhrase = aggregationPhrase.replace(paramCol+" ", somePair.getColumnName()+" ");
                                                }
                                                else if(aggregationPhrase.contains(paramCol+")")){

                                                    aggregationPhrase = aggregationPhrase.replace(paramCol+")", somePair.getColumnName()+")");
                                                }
                                                columnLocated = true;
                                                opQuery.addUsedColumn(somePair.getColumnName(), currentOperatorNode.getOperatorName().toLowerCase());
                                                break;
                                            }
                                        }
                                    }
                                }

                                if(columnLocated == true) break;
                            }

                            if(columnLocated == true) break;
                        }

                        if(columnLocated == false){ //Extra search
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" Perfoming search in Aggregation Map for: "+aggDesc.toString());
                            for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                                for(StringParameter sP : aggPair.getAltAliasPairs()){
                                    if( sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()) || (sP.getParemeterType().equals(currentOperatorNode.getOperatorName()))){
                                        if(sP.getValue().equals(paramCol)){
                                            String wantedAncestor = fetchlatestAncestorWithDifferentValue(aggPair.getAltAliasPairs(), paramCol, currentOperatorNode.getOperatorName(), fatherOperatorNode.getOperatorName(), null);

                                            boolean failedWithAncestor = false;
                                            for(StringParameter sP2 : aggPair.getAltAliasPairs()){
                                                if(sP2.getParemeterType().equals(wantedAncestor)){
                                                    System.out.println("Located realAlias: "+sP2.getValue()+" in aggregationMap!");
                                                    columnLocated = true;
                                                    failedWithAncestor = true;
                                                    if(aggregationPhrase.contains(paramCol+" ")){

                                                        aggregationPhrase = aggregationPhrase.replace(paramCol+" ", sP2.getValue()+" ");
                                                    }
                                                    else if(aggregationPhrase.contains(paramCol+")")){

                                                        aggregationPhrase = aggregationPhrase.replace(paramCol+")", sP2.getValue()+")");
                                                    }

                                                    opQuery.addUsedColumn(sP2.getValue(), currentOperatorNode.getOperatorName().toLowerCase());

                                                    break;
                                                }
                                            }

                                            if(failedWithAncestor == false){
                                                System.out.println("FailedWithAncestor: "+wantedAncestor+" to locate real alias of: "+paramCol +" in aggrMap!");
                                                System.exit(0);
                                            }

                                            break;


                                        }
                                    }
                                }

                                if(columnLocated == true) break;
                            }
                        }

                        if(columnLocated == false){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" Failed to find real alias for agg parameter col: "+paramCol);
                            System.exit(0);
                        }

                    }

                    boolean newAgg = true;
                    for(String ag : aggregations){
                        if(ag.equals(aggregationPhrase)){
                            newAgg = false;
                            break;
                        }
                    }

                    if(newAgg == true) {
                        aggregations.add(aggregationPhrase);

                        if(aggregations.size() == 1){
                            //Fix non aggreagations aliases
                            System.out.println("Fixing non aggreagation columns...");

                            MyMap anotherTempMap = new MyMap(true);
                            fixedSchema = extractColsFromTypeName(fixedSchema, anotherTempMap, fixedSchema, true);

                            for(ColumnTypePair tempPair : anotherTempMap.getColumnAndTypeList()){
                                boolean locatedColumn = false;
                                if(tempPair.getColumnType().equals("struct") == false){
                                    System.out.println("Working on alias: "+tempPair.getColumnName()+" - Type: "+tempPair.getColumnType());
                                    for(TableRegEntry tableRegEntry : tableRegistry.getEntries()){
                                        //System.out.println("Accessing: "+tableRegEntry.getAlias());
                                        for(ColumnTypePair realPair : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()){
                                            //System.out.println("Accessing: "+realPair.getColumnName()+" - Type: "+realPair.getColumnType());
                                            if(realPair.getColumnType().equals(tempPair.getColumnType())){
                                                if(realPair.getColumnName().equals(tempPair.getColumnName())){
                                                    //System.out.println("is real Alias...");
                                                    for(StringParameter sP : realPair.getAltAliasPairs()){ //Schema contains the latest allies and we have already called addNewPossibleALiases before
                                                        if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                                            //System.out.println("Located father1...");
                                                            if(fixedSchema.contains(tempPair.getColumnName()+":")){
                                                                System.out.println("Alias in schema: "+tempPair.getColumnName() + " becomes: "+realPair.getColumnName());
                                                                fixedSchema = fixedSchema.replace(tempPair.getColumnName()+":", realPair.getColumnName()+":");
                                                                System.out.println("Schema after replace: "+fixedSchema);
                                                            }
                                                            locatedColumn = true;
                                                            break;
                                                        }
                                                        else if((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))){
                                                            //System.out.println("Located father2...");
                                                            if(fixedSchema.contains(tempPair.getColumnName()+":")){
                                                                System.out.println("Alias in schema: "+tempPair.getColumnName() + " becomes: "+realPair.getColumnName());
                                                                fixedSchema = fixedSchema.replace(tempPair.getColumnName()+":", realPair.getColumnName()+":");
                                                                System.out.println("Schema after replace: "+fixedSchema);
                                                            }
                                                            locatedColumn = true;
                                                            break;
                                                        }
                                                    }

                                                    if(locatedColumn) break;
                                                }
                                                else{
                                                    for(StringParameter sP : realPair.getAltAliasPairs()){ //Schema contains the latest allies and we have already called addNewPossibleALiases before
                                                        if(sP.getParemeterType().equals(currentOperatorNode.getOperatorName())){
                                                            //System.out.println("Will compare: "+tempPair.getColumnName() + " with: "+sP.getValue() + " on currentNode: "+currentOperatorNode.getOperatorName());
                                                            if(sP.getValue().equals(tempPair.getColumnName())) {
                                                                if (fixedSchema.contains(tempPair.getColumnName()+":")) {
                                                                    System.out.println("Alias in schema: "+tempPair.getColumnName() + " becomes: "+realPair.getColumnName());
                                                                    fixedSchema = fixedSchema.replace(tempPair.getColumnName()+":", realPair.getColumnName()+":");
                                                                    System.out.println("Schema after replace: "+fixedSchema);
                                                                    locatedColumn = true;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }

                                            if(locatedColumn) break;
                                        }

                                        if(locatedColumn) break;
                                    }

                                    if(locatedColumn == false){ //Might be constant value locate it in Constants Map
                                        boolean foundToBeConstant = false;
                                        System.out.println("Will search in ommitedConstantsMap...");
                                        for(ColumnTypePair constPair : ommitedConstantsMap.getColumnAndTypeList()){
                                            if(tempPair.getColumnType().equals(constPair.getColumnType())){
                                                for(StringParameter sP : constPair.getAltAliasPairs()){
                                                    if(sP.getParemeterType().equals(currentOperatorNode.getOperatorName())){
                                                        if(sP.getValue().equals(tempPair.getColumnName())){
                                                            locatedColumn = true;
                                                            System.out.println("Alias: "+tempPair.getColumnName() + " is a Constant value with Real Alias: "+constPair);
                                                            foundToBeConstant = true;
                                                            break;
                                                        }
                                                    }
                                                }

                                                if(foundToBeConstant) break;
                                            }
                                        }

                                        if(foundToBeConstant == false){
                                            System.out.println("Alias: "+tempPair.getColumnName() + " is NEITHER COLUMN NOR CONSTANT...MIGHT BE AGGR...CHECK IT!!");
                                            System.exit(0);
                                        }
                                    }

                                }

                            }

                        }

                        /*-----------------ATTEMPT TO PLACE NEW AGGREAGATION IN AGGRMAP-------*/
                        if(aggregationsMap.getColumnAndTypeList().size() == 0){

                            aggregationsMap.addPair(new ColumnTypePair(aggregationPhrase, "struct")); //WARNING: nothing will happen IF ALREADY EXISTS IN MAP
                            aggregationsMap.getColumnAndTypeList().get(0).addAltAlias(currentOperatorNode.getOperatorName(), subMap.getColumnAndTypeList().get(j).getColumnName());
                            //if(fixedSchema.contains(subMap.getColumnAndTypeList().get(j).getColumnName())){
                            //    fixedSchema = fixedSchema.replace(subMap.getColumnAndTypeList().get(j).getColumnName() , aggregationPhrase);
                            //}
                        }
                        else{
                            boolean phraseExists = false;
                            //int l = 0;
                            for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                                if(aggPair.getColumnName().equals(aggregationPhrase)){
                                    phraseExists = true;
                                    boolean altAliasExists = false;
                                    for(StringParameter sP : aggPair.getAltAliasPairs()){
                                        if(sP.getParemeterType().equals(currentOperatorNode)){
                                            altAliasExists = true;
                                            if(sP.getValue().equals(subMap.getColumnAndTypeList().get(j).getColumnName()) == false){
                                                sP.setValue(subMap.getColumnAndTypeList().get(j).getColumnName());
                                                //if(fixedSchema.contains(subMap.getColumnAndTypeList().get(j).getColumnName())){
                                                //    fixedSchema = fixedSchema.replace(subMap.getColumnAndTypeList().get(j).getColumnName() , aggregationPhrase);
                                                //}
                                            }
                                        }
                                    }

                                    if(altAliasExists == false){
                                        aggPair.addAltAlias(currentOperatorNode.getOperatorName(), subMap.getColumnAndTypeList().get(j).getColumnName());
                                        //if(fixedSchema.contains(subMap.getColumnAndTypeList().get(j).getColumnName())){
                                        //    fixedSchema = fixedSchema.replace(subMap.getColumnAndTypeList().get(j).getColumnName() , aggregationPhrase);
                                        //}
                                    }

                                }

                                //l++;
                            }

                            if(phraseExists == false){
                                //l = 0;
                                aggregationsMap.addPair(new ColumnTypePair(aggregationPhrase, "struct")); //WARNING: nothing will happen IF ALREADY EXISTS IN MAP
                                for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                                    if(aggPair.getColumnName().equals(aggregationPhrase)){
                                        aggPair.addAltAlias(currentOperatorNode.getOperatorName(), subMap.getColumnAndTypeList().get(j).getColumnName());
                                        //if(fixedSchema.contains(subMap.getColumnAndTypeList().get(j).getColumnName())){
                                        //    fixedSchema = fixedSchema.replace(subMap.getColumnAndTypeList().get(j).getColumnName() , aggregationPhrase);
                                       // }
                                    }

                                    //l++;
                                }
                            }
                        }

                    }

                    j++;
                }

                updatedSchemaString = fixedSchema;

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" Printing AggrMap for debugging: ");
                aggregationsMap.printMap();

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" Schema finally is: "+updatedSchemaString);

            }
            else{
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" Currently unable to handle any case involving null currentColumnExprMap and aggregations...Sorry!");
                System.exit(0);
            }

            //TODO ADD SELECT COLUMNS AND BEGIN NEW QUERY
        }
        else {
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" Simple GROUP BY with no aggregations...");
            if (currentOperatorNode.getOperator().getColumnExprMap() == null) {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": ColumnExprMap is empty!");
                if ((currentOperatorNode.getOperator().getSchema() == null) || (currentOperatorNode.getOperator().getSchema().toString().equals("()"))) {
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": RowSchema is () ! Assuming older schema!");
                    updatedSchemaString = latestAncestorSchema;
                    addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);
                } else {
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": RowSchema is not empty! BuildColumnExprMap from schema!");
                    updatedSchemaString = buildSchemaFromColumnExprMap(currentOperatorNode, fatherOperatorNode.getOperator().getSchema().toString(), fatherOperatorNode);
                }
            } else {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...");
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);
            }
        }

        //Add select columns based on updatedSchemaString TODO: USE OUTPUT COLS INSTEAD
        List<FieldSchema> selectCols = new LinkedList<>();
        if(opQuery.getLocalQueryString().contains("select ") == false){
            addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, opQuery, selectCols);
        }

        /*---Discover Group By Keys----*/
        List<FieldSchema> groupByKeys = new LinkedList<>();
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Now Discovering Group By Keys...");
        discoverGroupByKeys(currentOperatorNode, fatherOperatorNode, opQuery, latestAncestorTableName1, groupByDesc, groupByKeys);

        List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
        if(children != null){
            if(children.size() == 1){
                Operator<?> child = children.get(0);
                if(child instanceof ReduceSinkOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered GBY--->RS connection!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                    /*---Moving to child---*/
                    goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, opQuery, updatedSchemaString, currentOperatorNode.getOperatorName().toLowerCase(), latestAncestorTableName2, null);
                }
                else if(child instanceof FileSinkOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered GBY--->FS connection!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                    /*---Moving to child---*/
                    goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, opQuery, updatedSchemaString, currentOperatorNode.getOperatorName().toLowerCase(), latestAncestorTableName2, null);
                }
                else if(child instanceof SelectOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered GBY--->SEL connection!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                    /*---Moving to child---*/
                    goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, opQuery, updatedSchemaString, currentOperatorNode.getOperatorName().toLowerCase(), latestAncestorTableName2, null);
                }
                else if(child instanceof LimitOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered GBY--->LIM connection!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                    /*---Moving to child---*/
                    goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, opQuery, updatedSchemaString, currentOperatorNode.getOperatorName().toLowerCase(), latestAncestorTableName2, null);
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

    public void reduceSinkHasFatherSelect(OperatorNode currentOperatorNode, OperatorQuery currentOpQuery, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, String latestAncestorSchema, String latestAncestorTableName1, String latestAncestorTableName2){

        String updatedSchemaString = "";

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: reduceSinkHasFatherSelect");

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if GrandFather is Root...");
        OperatorNode grandFatherOperatorNode = exaremeGraph.getOperatorNodeByName(fatherOperatorNode.getOperator().getParentOperators().get(0).getOperatorId());

        if((grandFatherOperatorNode.getOperator().getParentOperators() == null) || (grandFatherOperatorNode.getOperator().getParentOperators().size() == 0)){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": GrandFather is root...TableScan!");

            if( (currentOperatorNode.getOperator().getSchema() == null) || currentOperatorNode.getOperator().getSchema().toString().equals("()") ){
                if(currentOperatorNode.getOperator().getColumnExprMap() != null){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": RowSchema is empty! We will build Schema from ColumnExprMap...");

                    updatedSchemaString = buildSchemaFromColumnExprMap(currentOperatorNode, grandFatherOperatorNode.getOperator().getSchema().toString(), fatherOperatorNode);

                }
                else{
                    addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);
                    updatedSchemaString = latestAncestorSchema;
                }
            }
            else {
                if(currentOperatorNode.getOperator().getColumnExprMap() == null){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": ColumnExprMap is empty! Assuming older Schema!");
                    updatedSchemaString = latestAncestorSchema;
                    addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);
                }
                else {
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...");
                    updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);
                }
            }
        }
        else{
            if(currentOperatorNode.getOperator().getColumnExprMap() == null){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": ColumnExprMap is empty!");
                if((currentOperatorNode.getOperator().getSchema() == null) || (currentOperatorNode.getOperator().getSchema().toString().equals("()"))){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is () ! Assuming older schema!");
                    updatedSchemaString = latestAncestorSchema;
                    addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is not empty! BuildColumnExprMap from schema!");
                    updatedSchemaString = buildSchemaFromColumnExprMap(currentOperatorNode, fatherOperatorNode.getOperator().getSchema().toString(), fatherOperatorNode);
                }
            }
            else {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...");
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);
            }
        }

        /*---Add possible Order By Keys---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if ORDER BY exists in this RS...");
        discoverOrderByKeys(currentOperatorNode, fatherOperatorNode, currentOpQuery, latestAncestorTableName1);

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

        /*---Finalising outputTable---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");
        MyTable outputTable = new MyTable();
        outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
        outputTable.setIsAFile(false);
        List<FieldSchema> newCols = new LinkedList<>();

        //if(currentOperatorNode.getOperator().getSchema().toString().equals("()")){
        //    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is empty! Assuming older Schema!");
        //    updatedSchemaString = latestAncestorSchema;
        //}

        updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols);

        /*---Finalize local part of Query---*/
        currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

        /*---Check if OpLink is to be created---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
        checkForPossibleOpLinks(currentOperatorNode,  currentOpQuery, outputTable);

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
                    initialiseNewQuery(currentOpQuery, opQuery, currentOpQuery.getOutputTable());

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

    public void reduceSinkHasFatherUnion(OperatorNode currentOperatorNode, OperatorQuery currentOpQuery, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, String latestAncestorSchema, String latestAncestorTableName1, String latestAncestorTableName2){

        String updatedSchemaString = "";

        if( (currentOperatorNode.getOperator().getSchema() == null) || currentOperatorNode.getOperator().getSchema().toString().equals("()") ){
            if(currentOperatorNode.getOperator().getColumnExprMap() != null){
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": RowSchema is empty! We will build Schema from ColumnExprMap...");

                updatedSchemaString = buildSchemaFromColumnExprMap(currentOperatorNode, fatherOperatorNode.getOperator().getSchema().toString(), fatherOperatorNode);

            }
            else{
                addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);
                updatedSchemaString = latestAncestorSchema;
            }
        }
        else{
            if(currentOperatorNode.getOperator().getColumnExprMap() == null){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": ColumnExprMap is empty!");

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is not empty! BuildColumnExprMap from schema!");
                updatedSchemaString = buildSchemaFromColumnExprMap(currentOperatorNode, fatherOperatorNode.getOperator().getSchema().toString(), fatherOperatorNode);

            }
            else {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...");
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);
            }
        }

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
        if(currentOpQuery.getLocalQueryString().contains("select ") == false){
            updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, latestAncestorSchema, currentOpQuery, null);
        }

        /*---Add possible Order By Keys---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if ORDER BY exists in this RS...");
        discoverOrderByKeys(currentOperatorNode, fatherOperatorNode, currentOpQuery, latestAncestorTableName1);

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

        /*---Finalising outputTable---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");
        MyTable outputTable = new MyTable();
        outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
        outputTable.setIsAFile(false);
        List<FieldSchema> newCols = new LinkedList<>();

        //if(currentOperatorNode.getOperator().getSchema().toString().equals("()")){
        //    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": RowSchema is empty! Assuming older Schema!");
        //    updatedSchemaString = latestAncestorSchema;
        //}

        updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols);

        /*---Finalize local part of Query---*/
        currentOpQuery.setLocalQueryString("create table "+currentOperatorNode.getOperator().getOperatorId().toLowerCase()+" as "+currentOpQuery.getLocalQueryString());

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

        /*---Check if OpLink is to be created---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if OpLink can be created...");
        checkForPossibleOpLinks(currentOperatorNode,  currentOpQuery, outputTable);

        /*----Adding Finished Query to List----*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
        allQueries.add(currentOpQuery);

        /*----Begin a new Query----*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
        OperatorQuery opQuery = new OperatorQuery();
        initialiseNewQuery(currentOpQuery, opQuery, currentOpQuery.getOutputTable());

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

    public void joinHasFatherReduceSinks(OperatorNode currentOperatorNode, OperatorQuery currentOpQuery, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, String latestAncestorTableName1, String latestAncestorTableName2){

        String updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);

        //TODO ADD KEY SEARCH AND OUTPUT SELECT COLUMNS
        List<String> joinColumns = new LinkedList<>();

        handleJoinKeys(currentOperatorNode, currentOpQuery, fatherOperatorNode, otherFatherNode, joinColumns);

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Now we will use keys to form the join expression...");

        //Add Join col1 = col2 part of expression
        if(joinColumns.size() > 0) {
            currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString().concat(" " + joinColumns.get(0) + " = " + joinColumns.get(1) + " "));
            currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString().concat(" " + joinColumns.get(0) + " = " + joinColumns.get(1) + " "));

            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + currentOpQuery.getLocalQueryString() + "]");
        }
        else{
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN COLS ARE NULL, (NOT NORMAL JOIN)");
        }

        /*---Moving to Child Operator---*/
        List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
        if(children != null) {
            if (children.size() == 1) {
                Operator<?> child = children.get(0);
                if (child instanceof SelectOperator) {
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered JOIN--->SELECT connection!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Current Query will be ending here with Select statement addition...");

                    /*---Add select statement to Query---*/
                    updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode,updatedSchemaString, currentOpQuery, null);

                    /*---Finalising outputTable---*/
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Finalising OutputTable...");
                    MyTable outputTable = new MyTable();
                    outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                    outputTable.setIsAFile(false);
                    List<FieldSchema> newCols = new LinkedList<>();

                    updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols);

                    /*---Finalize local part of Query---*/
                    currentOpQuery.setLocalQueryString("create table " + currentOperatorNode.getOperator().getOperatorId().toLowerCase() + " as " + currentOpQuery.getLocalQueryString());

                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + currentOpQuery.getLocalQueryString() + "]");

                    /*---Check if OpLink is to be created---*/
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Checking if OpLink can be created...");
                    checkForPossibleOpLinks(currentOperatorNode,  currentOpQuery, outputTable);

                    /*----Adding Finished Query to List----*/
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Adding OperatorQuery to QueryList...");
                    allQueries.add(currentOpQuery);

                    /*----Begin a new Query----*/
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
                    OperatorQuery opQuery = new OperatorQuery();
                    initialiseNewQuery(currentOpQuery, opQuery, currentOpQuery.getOutputTable());

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

    public void mapJoinHasFatherHashFilterTableScan(OperatorNode currentOperatorNode, OperatorQuery currentOpQuery, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, String latestAncestorTableName1, String latestAncestorTableName2){

        String updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode);

        //TODO ADD KEY SEARCH AND OUTPUT SELECT COLUMNS
        List<String> joinColumns = new LinkedList<>();

        handleMapJoinKeys(currentOperatorNode, currentOpQuery, fatherOperatorNode, otherFatherNode, joinColumns);

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Now we will use keys to form the join expression...");

        if(joinColumns.size() > 0) {
            //Add Join col1 = col2 part of expression
            currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString().concat(" " + joinColumns.get(0) + " = " + joinColumns.get(1) + " "));
            currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString().concat(" " + joinColumns.get(0) + " = " + joinColumns.get(1) + " "));

            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + currentOpQuery.getLocalQueryString() + "]");
        }
        else{
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN HAS NULL Columns! Not normal MapJoin!");
        }

        /*---Moving to Child Operator---*/
        List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
        if(children != null) {
            if (children.size() == 1) {
                Operator<?> child = children.get(0);
                if (child instanceof FileSinkOperator) {
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered MAPJOIN--->FS connection!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Moving to child operator...");

                    goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);

                }
                else if(child instanceof FilterOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered MAPJOIN--->FIL connection!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Moving to child operator...");

                    goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);

                }
                else if(child instanceof SelectOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered MAPJOIN--->SEL connection!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Moving to child operator...");

                    goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);

                }
                else {
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported type of Child for MapJoin...");
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

    public void unionHasFatherTableScan(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorQuery currentOpQuery, String schema){

        System.out.println(currentOperatorNode.getOperator() + ": Since this is a Union Operator we will retrieve its parents and their nodes...");

        List<Operator<?>> parentOperators = currentOperatorNode.getOperator().getParentOperators();

        String updatedSchemaString = schema;

        if(currentOperatorNode.getOperator().getColumnExprMap() != null) {
            for (Operator<?> parent : parentOperators) {
                OperatorNode parentNode = exaremeGraph.getOperatorNodeByName(parent.getOperatorId());
                System.out.println(currentOperatorNode.getOperator() + ": Will call addNewPossibleAliases with parent: " + parentNode.getOperatorName());
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, parentNode, null);
            }
        }
        else{
            for (Operator<?> parent : parentOperators) {
                OperatorNode parentNode = exaremeGraph.getOperatorNodeByName(parent.getOperatorId());
                System.out.println(currentOperatorNode.getOperator() + ": Will call addNewPossibleAliases with parent: " + parentNode.getOperatorName());
                useSchemaToFillAliases(currentOperatorNode, parentNode, schema);
            }
        }
        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Current Query will be ending here...");

        /*---Add select statement to Query---*/
        //updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, schema, currentOpQuery, null);

        /*---Finalising outputTable---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Finalising OutputTable...");
        MyTable outputTable = new MyTable();
        outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
        outputTable.setIsAFile(false);
        List<FieldSchema> newCols = new LinkedList<>();

        updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, null, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols);

        /*---Finalize local part of Query---*/
        currentOpQuery.setLocalQueryString("create table " + currentOperatorNode.getOperator().getOperatorId().toLowerCase() + " as " + currentOpQuery.getLocalQueryString());

        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + currentOpQuery.getLocalQueryString() + "]");

        /*---Check if OpLink is to be created---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Checking if OpLink can be created...");
        checkForPossibleOpLinks(currentOperatorNode,  currentOpQuery, outputTable);

        /*----Adding Finished Query to List----*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Adding OperatorQuery to QueryList...");
        allQueries.add(currentOpQuery);

        /*----Begin a new Query----*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
        OperatorQuery opQuery = new OperatorQuery();
        initialiseNewQuery(currentOpQuery, opQuery, currentOpQuery.getOutputTable());

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+opQuery.getLocalQueryString()+"]");

        /*---Moving to Child Operator---*/
        List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
        if(children != null) {
            if (children.size() == 1) {
                Operator<?> child = children.get(0);
                if (child instanceof ReduceSinkOperator) {
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered UNION--->RS connection!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Moving to child operator...");

                    goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, opQuery, updatedSchemaString, currentOperatorNode.getOperatorName().toLowerCase(), null, null);

                }
                else {
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported type of Child for Union...");
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

    /*
        Strictly from older schema
     */

    public String buildSchemaFromColumnExprMap(OperatorNode currentOperatorNode, String olderSchema, OperatorNode fatherOperatorNode){

        System.out.println(currentOperatorNode.getOperatorName() + ": Currently calling buildSchemaFromColumnExprMap...");

        if(fatherOperatorNode != null) {
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+ ": First copying all altAliases of FatherOperatorNode1...");
            addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);
        }

        MyMap someMap = new MyMap(true);

        String pastSchema = "";
        if(olderSchema.contains("struct<")){
            pastSchema = extractColsFromTypeNameWithStructs(olderSchema, someMap, olderSchema, false);
        }
        else{
            pastSchema = extractColsFromTypeName(olderSchema, someMap, olderSchema, false);
        }

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

        String updatedSchemaString = "(";
        int i = 0;
        for(Map.Entry<String, ExprNodeDesc> entry : currentOperatorNode.getOperator().getColumnExprMap().entrySet()){
            String newColName = entry.getKey();
            String pastName = entry.getValue().getCols().get(0);

            boolean matchFound = false;
            String nameForSchema = "";
            String typeForSchema = "";

            for(ColumnTypePair oldPair : someMap.getColumnAndTypeList()){ //Search all the parent operator Map
                if(oldPair.getColumnName().equals(pastName)){
                    for(TableRegEntry regEntry : tableRegistry.getEntries()){
                        for(ColumnTypePair mapPair : regEntry.getColumnTypeMap().getColumnAndTypeList()){
                            if(mapPair.getColumnType().equals(oldPair.getColumnType())){
                                List<StringParameter> altAliases = mapPair.getAltAliasPairs();
                                for(StringParameter sP : altAliases){
                                    if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                        if(sP.getValue().equals(oldPair.getColumnName())){
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": NewColName: "+newColName+" - was added in map as altAlias for: "+mapPair.getColumnName());
                                            matchFound = true;
                                            nameForSchema = mapPair.getColumnName();
                                            typeForSchema = mapPair.getColumnType();
                                            mapPair.modifyAltAlias(currentOperatorNode.getOperatorName(), oldPair.getColumnName(), newColName);
                                            break;
                                        }
                                    }

                                }

                                if(matchFound == true) {
                                    break;
                                }
                            }
                        }

                        if(matchFound == true) break;
                    }

                    if(matchFound == true) break;
                }

            }

            if(matchFound == false){
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": NewColName: "+newColName+" has no match!");
                //System.exit(0);
            }

            if(i == currentOperatorNode.getOperator().getColumnExprMap().entrySet().size() - 1){
                updatedSchemaString = updatedSchemaString.concat(nameForSchema + ": " + typeForSchema + ")");
            }
            else{
                updatedSchemaString = updatedSchemaString.concat(nameForSchema + ": " + typeForSchema + ",");
            }

            i++;
        }

        return updatedSchemaString;

    }

    public void limitHasFatherSelectGroup(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorQuery currentOpQuery, String latestAncestorSchema, String latestAncestorTableName1, String latestAncestorTableName2){

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: limitHasFatherSelectGroup");

        if(currentOperatorNode.getOperator().getColumnExprMap() == null){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Assume the schema of FatherOperator(Select)...");

            addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);

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

    public void createJoinPoint(OperatorNode currentOperatorNode, OperatorQuery currentOpQuery, Operator<?> child, String latestAncestorTableName1){

        System.out.println(currentOperatorNode.getOperatorName() + ": Currently calling createJoinPoint...");

        /*----Begin a new Query----*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
        OperatorQuery opQuery = new OperatorQuery();
        initialiseNewQuery(currentOpQuery, opQuery, currentOpQuery.getOutputTable());

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");
        JoinOperator joinOp = (JoinOperator) child;
        JoinDesc joinDesc = joinOp.getConf();

        JoinCondDesc[] joinCondDescs = joinDesc.getConds();
        if(joinCondDescs.length > 1){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": More than 1 JoinCondDesc! Error!");
            System.exit(0);
        }

        String joinPhrase = "";

        boolean nullValue = false;

        for(Map.Entry<Byte, String> entry : joinDesc.getKeysString().entrySet()){
            if(entry.getValue() == null){
                nullValue = true;
                break;
            }
        }

        if(nullValue == false) {
            if (joinCondDescs[0].getType() == 0) {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Type=0 - JoinString: " + joinCondDescs[0].getJoinCondString());
                joinPhrase = " inner join ";
            } else if (joinCondDescs[0].getType() == 1) {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Type=1 - JoinString: " + joinCondDescs[0].getJoinCondString());
                joinPhrase = " left join ";
            } else if (joinCondDescs[0].getType() == 2) {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Type=2 - JoinString: " + joinCondDescs[0].getJoinCondString());
                joinPhrase = " right join ";
            } else if (joinCondDescs[0].getType() == 3) {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Type=3 - JoinString: " + joinCondDescs[0].getJoinCondString());
                joinPhrase = " full outer join ";
            } else {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": UNSUPPORTED JOIN TYPE! JoinString: " + joinCondDescs[0].getJoinCondString());
                System.exit(0);
                //joinPhrase = " full outer join ";
            }
        }

        JoinPoint joinP = new JoinPoint(child.getOperatorId(), currentOperatorNode.getOperatorName(), opQuery, latestAncestorTableName1, currentOperatorNode, joinPhrase);

        joinPointList.add(joinP);

    }

    public void createMapJoinPoint(OperatorNode currentOperatorNode, OperatorQuery currentOpQuery, Operator<?> child, String latestAncestorTableName1){

        System.out.println(currentOperatorNode.getOperatorName() + ": Currently calling createMapJoinPoint...");

        /*----Begin a new Query----*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
        OperatorQuery opQuery = new OperatorQuery();
        initialiseNewQuery(currentOpQuery, opQuery, currentOpQuery.getOutputTable());

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");
        MapJoinOperator joinOp = (MapJoinOperator) child;
        MapJoinDesc joinDesc = joinOp.getConf();

        JoinCondDesc[] joinCondDescs = joinDesc.getConds();
        if(joinCondDescs.length > 1){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": More than 1 JoinCondDesc! Error!");
            System.exit(0);
        }

        boolean nullValue = false;

        for(Map.Entry<Byte, String> entry : joinDesc.getKeysString().entrySet()){
            if(entry.getValue() == null){
                nullValue = true;
                break;
            }
        }

        String joinPhrase = "";

        if(nullValue == false) {
            if (joinCondDescs[0].getType() == 0) {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Type=0 - JoinString: " + joinCondDescs[0].getJoinCondString());
                joinPhrase = " inner join ";
            } else if (joinCondDescs[0].getType() == 1) {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Type=1 - JoinString: " + joinCondDescs[0].getJoinCondString());
                joinPhrase = " left join ";
            } else if (joinCondDescs[0].getType() == 2) {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Type=2 - JoinString: " + joinCondDescs[0].getJoinCondString());
                joinPhrase = " right join ";
            } else if (joinCondDescs[0].getType() == 3) {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Type=3 - JoinString: " + joinCondDescs[0].getJoinCondString());
                joinPhrase = " full outer join ";
            } else {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": UNSUPPORTED JOIN TYPE! JoinString: " + joinCondDescs[0].getJoinCondString());
                System.exit(0);
                //joinPhrase = " full outer join ";
            }
        }

        JoinPoint joinP = new JoinPoint(child.getOperatorId(), currentOperatorNode.getOperatorName(), opQuery, latestAncestorTableName1, currentOperatorNode, joinPhrase);

        joinPointList.add(joinP);

    }

    public void createUnionPoint(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, OperatorQuery currentOpQuery, Operator<?> child, String latestAncestorTableName1, String latestSchema){

        System.out.println(currentOperatorNode.getOperatorName() + ": Currently calling createUnionPoint...");

        /*----Begin a new Query----*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
        OperatorQuery opQuery = new OperatorQuery();
        initialiseNewQuery(currentOpQuery, opQuery, currentOpQuery.getOutputTable());

        List<FieldSchema> usedColumns = new LinkedList<>();

        /*---Check for Select statement and add used cols---*/
        if(opQuery.getLocalQueryString().contains("select ") == false){
            String updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode,latestSchema, opQuery, usedColumns);
        }
        else{
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": While preparing for UnionPoint select statement exists...error!");
            System.exit(0);
        }

        String unionPhrase = " union all "; //UNION DISTINCT, OR UNION INFO IS NOT AVAILABLE AT THIS POINT

        opQuery.setLocalQueryString(opQuery.getLocalQueryString() + " ");
        opQuery.setExaremeQueryString(opQuery.getExaremeQueryString() + " ");

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+opQuery.getLocalQueryString()+"]");
        UnionOperator unionOp = (UnionOperator) child;
        UnionDesc unionDesc = unionOp.getConf();

        int numOfInputs = unionDesc.getNumInputs();

        UnionPoint unionP = new UnionPoint(child.getOperatorId(), currentOperatorNode.getOperatorName(), opQuery, latestAncestorTableName1, currentOperatorNode, unionPhrase, numOfInputs);

        unionP.addInputTableAndUsedCols(currentOpQuery.getOutputTable().getTableName().toLowerCase(), usedColumns);

        unionPointList.add(unionP);

    }

    public void finaliseUnionPoint(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, OperatorQuery currentOpQuery, Operator<?> child, String latestAncestorTableName1, String latestSchema, UnionPoint unionPoint){

        System.out.println(currentOperatorNode.getOperatorName() + ": Currently calling updateUnionPoint...");

        /*----Begin a new Query----*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
        OperatorQuery opQuery = new OperatorQuery();
        initialiseNewQuery(currentOpQuery, opQuery, currentOpQuery.getOutputTable());

        List<FieldSchema> usedColumns = new LinkedList<>();

        /*---Check for Select statement and add used cols---*/
        if(opQuery.getLocalQueryString().contains("select ") == false){
            String updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode,latestSchema, opQuery, usedColumns);
        }
        else{
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": While preparing for UnionPoint select statement exists...error!");
            System.exit(0);
        }

        String unionPhrase = " union all "; //UNION DISTINCT, OR UNION INFO IS NOT AVAILABLE AT THIS POINT

        //Continue where we left off
        OperatorQuery associatedOpQuery = unionPoint.getAssociatedQuery();

        unionPoint.addInputTableAndUsedCols(currentOpQuery.getOutputTable().getTableName().toLowerCase(), usedColumns);

        //Bring over new used columns
        for(FieldSchema f : usedColumns){
            associatedOpQuery.addUsedColumn(f.getName(), currentOpQuery.getOutputTable().getTableName().toLowerCase());
        }

        associatedOpQuery.addInputTable(currentOpQuery.getOutputTable());
        associatedOpQuery.setLocalQueryString(associatedOpQuery.getLocalQueryString() + " " + unionPhrase + " " + opQuery.getLocalQueryString() + " ");
        associatedOpQuery.setExaremeQueryString(associatedOpQuery.getExaremeQueryString() + " " + unionPhrase + " " + opQuery.getLocalQueryString() + " ");

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+associatedOpQuery.getLocalQueryString()+"]");

        //Finalise union point
        unionPoint.setAssociatedQuery(associatedOpQuery);

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": UNION_POINT: "+child.getOperatorId()+" Exists! Removing it from List!");

        unionPointList.remove(unionPoint);

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

        OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

        goToChildOperator(childNode, currentOperatorNode, associatedOpQuery, latestSchema, currentOpQuery.getOutputTable().getTableName().toLowerCase(), null, null);

        return;

    }

    public void updateUnionPoint(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, OperatorQuery currentOpQuery, Operator<?> child, String latestAncestorTableName1, String latestSchema, UnionPoint unionPoint){

        System.out.println(currentOperatorNode.getOperatorName() + ": Currently calling updateUnionPoint...");

        /*----Begin a new Query----*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
        OperatorQuery opQuery = new OperatorQuery();
        initialiseNewQuery(currentOpQuery, opQuery, currentOpQuery.getOutputTable());

        List<FieldSchema> usedColumns = new LinkedList<>();

        /*---Check for Select statement and add used cols---*/
        if(opQuery.getLocalQueryString().contains("select ") == false){
            String updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode,latestSchema, opQuery, usedColumns);
        }
        else{
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": While preparing for UnionPoint select statement exists...error!");
            System.exit(0);
        }

        String unionPhrase = " union all "; //UNION DISTINCT, OR UNION INFO IS NOT AVAILABLE AT THIS POINT

        //Continue where we left off
        OperatorQuery associatedOpQuery = unionPoint.getAssociatedQuery();

        unionPoint.addInputTableAndUsedCols(currentOpQuery.getOutputTable().getTableName().toLowerCase(), usedColumns);

        //Bring over new used columns
        for(FieldSchema f : usedColumns){
            associatedOpQuery.addUsedColumn(f.getName(), currentOpQuery.getOutputTable().getTableName().toLowerCase());
        }

        associatedOpQuery.addInputTable(currentOpQuery.getOutputTable());
        associatedOpQuery.setLocalQueryString(associatedOpQuery.getLocalQueryString() + " " + unionPhrase + " " + opQuery.getLocalQueryString() + " ");
        associatedOpQuery.setExaremeQueryString(associatedOpQuery.getExaremeQueryString() + " " + unionPhrase + " " + opQuery.getLocalQueryString() + " ");

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+associatedOpQuery.getLocalQueryString()+"]");

        //Save changes
        unionPoint.setAssociatedQuery(associatedOpQuery);

    }

    public void fileSinkHasFatherLimitGroupSelectMap(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, OperatorQuery currentOpQuery, String latestAncestorSchema, String latestAncestorTableName1, String latestAncestorTableName2){

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: fileSinkHasFatherLimitGroupSelect");
        String updatedSchemaString = "";
        updatedSchemaString = latestAncestorSchema;

        if(currentOperatorNode.getOperator().getColumnExprMap() == null){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": FileSink has null columnExprMap! Assumming ancestor's schema...");

            if(currentOperatorNode.getOperator().getSchema() != null){

                if(currentOperatorNode.getOperator().getSchema().toString().equals(fatherOperatorNode.getOperator().getSchema().toString())){
                    addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);
                }
                else {
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": We will use the currentSchema however to fill new Aliases for children...");
                    useSchemaToFillAliases(currentOperatorNode, fatherOperatorNode, latestAncestorSchema);
                }
            }

            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
            if(currentOpQuery.getLocalQueryString().contains("select ") == false){
                updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode,latestAncestorSchema, currentOpQuery, null);
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

                        goToChildOperator(childNode, currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);
                    }
                    else if(child instanceof TableScanOperator){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered FS--->TS connection!");
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current query will be ending here!");

                        /*---Finalising outputTable---*/
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Finalising OutputTable...");
                        MyTable outputTable = new MyTable();
                        outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                        outputTable.setIsAFile(false);
                        List<FieldSchema> newCols = new LinkedList<>();

                        //System.out.println("before finalise..");

                        updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperatorName().toLowerCase(), outputTable, newCols);

                        /*---Finalize local part of Query---*/
                        currentOpQuery.setLocalQueryString("create table " + currentOperatorNode.getOperator().getOperatorId().toLowerCase() + " as " + currentOpQuery.getLocalQueryString());

                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + currentOpQuery.getLocalQueryString() + "]");

                        /*---Check if OpLink is to be created---*/
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Checking if OpLink can be created...");
                        checkForPossibleOpLinks(currentOperatorNode,  currentOpQuery, outputTable);

                        /*----Adding Finished Query to List----*/
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Adding OperatorQuery to QueryList...");
                        allQueries.add(currentOpQuery);

                        /*----Begin a new Query----*/
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Beginning a New Query...");
                        OperatorQuery opQuery = new OperatorQuery();
                        initialiseNewQuery(currentOpQuery, opQuery, currentOpQuery.getOutputTable());

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
                    List<FieldSchema> newCols = new LinkedList<>();

                    updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols);

                    /*---Finalize local part of Query---*/
                    currentOpQuery.setLocalQueryString("create table " + currentOperatorNode.getOperator().getOperatorId().toLowerCase() + " as " + currentOpQuery.getLocalQueryString());

                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + currentOpQuery.getLocalQueryString() + "]");

                    /*---Check if OpLink is to be created---*/
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Checking if OpLink can be created...");
                    checkForPossibleOpLinks(currentOperatorNode,  currentOpQuery, outputTable);

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

    public void listSinkHasFatherFileSink(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorQuery currentOpQuery, String latestAncestorSchema){

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: listSinkHasFatherFileSink");

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Check if ColumnExprMap is NULL...");
        if(currentOperatorNode.getOperator().getColumnExprMap() == null) {

            addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);

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
                List<FieldSchema> newCols = new LinkedList<>();

                latestAncestorSchema = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, null, currentOpQuery, latestAncestorSchema, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols);

                /*---Finalize local part of Query---*/
                currentOpQuery.setLocalQueryString("create table " + currentOperatorNode.getOperator().getOperatorId().toLowerCase() + " as " + currentOpQuery.getLocalQueryString());

                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + currentOpQuery.getLocalQueryString() + "]");

                /*---Check if OpLink is to be created---*/
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Checking if OpLink can be created...");
                checkForPossibleOpLinks(currentOperatorNode,  currentOpQuery, outputTable);

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

    public String addSelectStatementToQuery(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, String latestAncestorSchema, OperatorQuery currentOpQuery, List<FieldSchema> usedCols){

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: addSelectStatementToQuery...");

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query does not contain SELECT statement (needs to be added)...");
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Constructing SELECT statement columns...");
        MyMap aMap = new MyMap(false);

        String updatedSchemaString = extractColsFromTypeName(latestAncestorSchema, aMap, latestAncestorSchema, true);

        /*---Find real possible alias thourgh extra map for _col columns----*/
        for(ColumnTypePair pair : aMap.getColumnAndTypeList()){
            if(pair.getColumnName().contains("_col")){
                if(pair.getColumnType().equals("struct")){ //If it is an aggregation column
                    boolean structFound = false;
                    for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){ //Get an aggreagationPair from the List
                        for(StringParameter sP : aggPair.getAltAliasPairs()){ //Get its alt aliases
                            if(sP.getParemeterType().equals(currentOperatorNode.getOperatorName())){ //Locate the currentNode in the List

                                if(sP.getValue().equals(pair.getColumnName())) { //Locate the value as an altAlias in the List

                                    structFound = true;
                                    if(pair.getColumnName().equals(aggPair.getAltAliasPairs().get(0).getValue())){ //If the altAlias is equal to the first altAlias in the List then we need the aggregation expression
                                        pair.setColumnName(aggPair.getColumnName() + " as " + pair.getColumnName());
                                    }
                                    else{ //The latest altAlias is different from the first - This means that we need the altAlias of the father to refer to
                                        boolean aliasLocated = false;
                                        String wantedAncestor = "";
                                        if(otherFatherNode != null)
                                            wantedAncestor = fetchlatestAncestorWithDifferentValue(aggPair.getAltAliasPairs(), pair.getColumnName(), currentOperatorNode.getOperatorName(), fatherOperatorNode.getOperatorName(), otherFatherNode.getOperatorName());
                                        else
                                            wantedAncestor = fetchlatestAncestorWithDifferentValue(aggPair.getAltAliasPairs(), pair.getColumnName(), currentOperatorNode.getOperatorName(), fatherOperatorNode.getOperatorName(), null);

                                        for(StringParameter sP2 : aggPair.getAltAliasPairs()){
                                            if(sP2.getParemeterType().equals(wantedAncestor)){
                                                pair.setColumnName(sP2.getValue());
                                                aliasLocated = true;
                                                break;
                                            }
                                        }

                                        //Attempt to add used column if not existing
                                        if(pair.getColumnName().contains(" as ") == false) {
                                            currentOpQuery.addUsedColumn(pair.getColumnName(), wantedAncestor);

                                            if (usedCols != null) {
                                                FieldSchema f = new FieldSchema();
                                                f.setName(pair.getColumnName());
                                                f.setType(pair.getColumnType());
                                                usedCols.add(f);
                                            }
                                        }

                                        if(aliasLocated == false){
                                            System.out.println("Attempted to locate alternate aggr alias for: "+pair.getColumnName()+" but failed... - LatestAncestor Checked: "+wantedAncestor);
                                            System.exit(0);
                                        }
                                    }

                                }

                            }
                        }
                    }
                }
                else{

                    boolean foundToBeConstant = false;
                    for (ColumnTypePair constPair : ommitedConstantsMap.getColumnAndTypeList()) {
                            if (pair.getColumnType().equals(constPair.getColumnType())) {
                                for (StringParameter sP : constPair.getAltAliasPairs()) {
                                    if ((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()))) {
                                        if (sP.getValue().equals(pair.getColumnName())) {
                                            System.out.println("Alias: " + pair.getColumnName() + " is a Constant value and will be ommited...");
                                            pair.setColumnName("__ommited_by_author_of_code__1213");
                                            foundToBeConstant = true;
                                            break;
                                        }
                                    }
                                    else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                        if (sP.getValue().equals(pair.getColumnName())) {
                                            System.out.println("Alias: " + pair.getColumnName() + " is a Constant value and will be ommited...");
                                            pair.setColumnName("__ommited_by_author_of_code__1213");
                                            foundToBeConstant = true;
                                            break;
                                        }
                                    }
                                    else if ((sP.getParemeterType().equals(currentOperatorNode.getOperatorName()))) {
                                        if (sP.getValue().equals(pair.getColumnName())) {
                                            System.out.println("Alias: " + pair.getColumnName() + " is a Constant value and will be ommited...");
                                            pair.setColumnName("__ommited_by_author_of_code__1213");
                                            foundToBeConstant = true;
                                            break;
                                        }
                                    }
                                }

                                if (foundToBeConstant) break;
                            }
                        }

                }
            }
        }



        String selectString = "";
        List<ColumnTypePair> pairs = aMap.getColumnAndTypeList();
        for(int i = 0; i < pairs.size(); i++){
            if(pairs.get(i).getColumnName().equals("__ommited_by_author_of_code__1213")) continue;
            if(i == pairs.size() - 1){
                selectString = selectString + " " + pairs.get(i).getColumnName()+ " ";
            }
            else{
                selectString = selectString + " " + pairs.get(i).getColumnName() + ",";
            }

        }

        currentOpQuery.setLocalQueryString("select"+selectString+currentOpQuery.getLocalQueryString());
        currentOpQuery.setExaremeQueryString("select"+selectString+currentOpQuery.getExaremeQueryString());

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

        return updatedSchemaString;

    }

    public void handleJoinKeys(OperatorNode currentOperatorNode, OperatorQuery currentOpQuery, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, List<String> joinColumns){

            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: handleJoinKeys...");

            JoinOperator joinOp = (JoinOperator) currentOperatorNode.getOperator();
            JoinDesc joinDesc = joinOp.getConf();

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing keys of MapJoin...");
        Map<Byte, String> keys = joinDesc.getKeysString();

        boolean nullValues = false;

        if (keys != null) {
            //TODO: HANDLE COLS WITH SAME NAME ON DIFFERENT TABLES

            for (Map.Entry<Byte, String> entry : keys.entrySet()) {
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing keys of MapJoin...");
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Key: " + entry.getKey() + " : Value: " + entry.getValue());

                if(entry.getValue() == null){
                    nullValues = true;
                    break;
                }

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

                //Located Value
                boolean valueLocated = false;
                String realValueName = "";

                for(TableRegEntry regEntry : tableRegistry.getEntries()){
                    for(ColumnTypePair p : regEntry.getColumnTypeMap().getColumnAndTypeList()){
                        if(columnType.equals(p.getColumnType())){
                            if(p.getColumnName().equals(columnName)){
                                List<StringParameter> altAliases = p.getAltAliasPairs();
                                for(StringParameter sP : altAliases){
                                    if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                        valueLocated = true;
                                        realValueName = columnName;
                                        joinColumns.add(realValueName);

                                        //Add JOIN USED Column
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + fatherOperatorNode);
                                        currentOpQuery.addUsedColumn(p.getColumnName(), fatherOperatorNode.getOperatorName());
                                        break;
                                    }
                                    else if(sP.getParemeterType().equals(otherFatherNode.getOperatorName())){
                                        valueLocated = true;
                                        realValueName = columnName;
                                        joinColumns.add(realValueName);

                                        //Add JOIN USED Column
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + otherFatherNode);
                                        currentOpQuery.addUsedColumn(p.getColumnName(), otherFatherNode.getOperatorName());
                                        break;
                                    }
                                }

                                if(valueLocated == true) break;

                            }
                            else{
                                List<StringParameter> altAliases = p.getAltAliasPairs();
                                for(StringParameter sP : altAliases){
                                    if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){ //Father1 Exists as AltAlias for this Column
                                        if(sP.getValue().equals(columnName)){ //Father holds the value of this key
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Located match through: " + fatherOperatorNode.getOperatorName());
                                            realValueName = p.getColumnName();
                                            valueLocated = true;
                                            joinColumns.add(realValueName);

                                            //Add JOIN USED Column
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + fatherOperatorNode);
                                            currentOpQuery.addUsedColumn(p.getColumnName(), fatherOperatorNode.getOperatorName());

                                            break;
                                        }
                                    }
                                    else if(sP.getParemeterType().equals(otherFatherNode.getOperatorName())){
                                        if(sP.getValue().equals(columnName)){ //Father holds the value of this key
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Located match through: " + otherFatherNode.getOperatorName());
                                            realValueName = p.getColumnName();
                                            valueLocated = true;
                                            joinColumns.add(realValueName);

                                            //Add JOIN USED Column
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + otherFatherNode);
                                            currentOpQuery.addUsedColumn(p.getColumnName(), otherFatherNode.getOperatorName());

                                            break;
                                        }
                                    }
                                }

                                if(valueLocated == true) break;
                            }
                        }
                    }

                    if(valueLocated == true) break;
                }

                if(valueLocated == false){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Failed to locate the real alias of: "+columnName);
                    System.exit(0);
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real alias of: "+columnName+" is: "+realValueName);
                }

            }

            //Same alias for the 2 keys
            if(joinColumns.size() > 0){
                if(joinColumns.get(0).equals(joinColumns.get(1))){
                    List<String> joinColumnsFixed = new LinkedList<>();
                    joinColumnsFixed.add(fatherOperatorNode.getOperatorName() + "." + joinColumns.get(0));
                    joinColumnsFixed.add(otherFatherNode.getOperatorName() + "." + joinColumns.get(1));

                    joinColumns = joinColumnsFixed;

                }
            }

        }
        else{
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Join Keys are null!");
            System.exit(0);
        }

    }

    public void handleMapJoinKeys(OperatorNode currentOperatorNode, OperatorQuery currentOpQuery, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, List<String> joinColumns){

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: handleMapJoinKeys...");

        MapJoinOperator joinOp = (MapJoinOperator) currentOperatorNode.getOperator();
        MapJoinDesc joinDesc = joinOp.getConf();

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing keys of MapJoin...");
        Map<Byte, String> keys = joinDesc.getKeysString();

        boolean nullValues = false;

        if (keys != null) {
            //TODO: HANDLE COLS WITH SAME NAME ON DIFFERENT TABLES

            for (Map.Entry<Byte, String> entry : keys.entrySet()) {
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing keys of MapJoin...");
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Key: " + entry.getKey() + " : Value: " + entry.getValue());

                if(entry.getValue() == null){
                    nullValues = true;
                    break;
                }

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

                //Located Value
                boolean valueLocated = false;
                String realValueName = "";

                for(TableRegEntry regEntry : tableRegistry.getEntries()){
                    for(ColumnTypePair p : regEntry.getColumnTypeMap().getColumnAndTypeList()){
                        if(columnType.equals(p.getColumnType())){
                            if(p.getColumnName().equals(columnName)){
                                List<StringParameter> altAliases = p.getAltAliasPairs();
                                for(StringParameter sP : altAliases){
                                    if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                        valueLocated = true;
                                        realValueName = columnName;
                                        joinColumns.add(realValueName);

                                        //Add JOIN USED Column
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + fatherOperatorNode);
                                        currentOpQuery.addUsedColumn(p.getColumnName(), fatherOperatorNode.getOperatorName());
                                        break;
                                    }
                                    else if(sP.getParemeterType().equals(otherFatherNode.getOperatorName())){
                                        valueLocated = true;
                                        realValueName = columnName;
                                        joinColumns.add(realValueName);

                                        //Add JOIN USED Column
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + otherFatherNode);
                                        currentOpQuery.addUsedColumn(p.getColumnName(), otherFatherNode.getOperatorName());
                                        break;
                                    }
                                }

                                if(valueLocated == true) break;

                            }
                            else{
                                List<StringParameter> altAliases = p.getAltAliasPairs();
                                for(StringParameter sP : altAliases){
                                    if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){ //Father1 Exists as AltAlias for this Column
                                        if(sP.getValue().equals(columnName)){ //Father holds the value of this key
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Located match through: " + fatherOperatorNode.getOperatorName());
                                            realValueName = p.getColumnName();
                                            valueLocated = true;
                                            joinColumns.add(realValueName);

                                            //Add JOIN USED Column
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + fatherOperatorNode);
                                            currentOpQuery.addUsedColumn(p.getColumnName(), fatherOperatorNode.getOperatorName());

                                            break;
                                        }
                                    }
                                    else if(sP.getParemeterType().equals(otherFatherNode.getOperatorName())){
                                        if(sP.getValue().equals(columnName)){ //Father holds the value of this key
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Located match through: " + otherFatherNode.getOperatorName());
                                            realValueName = p.getColumnName();
                                            valueLocated = true;
                                            joinColumns.add(realValueName);

                                            //Add JOIN USED Column
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + otherFatherNode);
                                            currentOpQuery.addUsedColumn(p.getColumnName(), otherFatherNode.getOperatorName());

                                            break;
                                        }
                                    }
                                }

                                if(valueLocated == true) break;
                            }
                        }
                    }

                    if(valueLocated == true) break;
                }

                if(valueLocated == false){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Failed to locate the real alias of: "+columnName);
                    System.exit(0);
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real alias of: "+columnName+" is: "+realValueName);
                }

            }

            //Same alias for the 2 keys
            if(joinColumns.size() > 0){
                if(joinColumns.get(0).equals(joinColumns.get(1))){
                    List<String> joinColumnsFixed = new LinkedList<>();
                    joinColumnsFixed.add(fatherOperatorNode.getOperatorName() + "." + joinColumns.get(0));
                    joinColumnsFixed.add(otherFatherNode.getOperatorName() + "." + joinColumns.get(1));

                    joinColumns = joinColumnsFixed;

                }
            }

        }
        else{
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Join Keys are null!");
            System.exit(0);
        }

    }

    public void checkForPossibleOpLinks(OperatorNode currentOperatorNode, OperatorQuery currentOpQuery, MyTable outputTable){

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: checkForPossibleOpLinks...");

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

    }

    public void discoverOrderByKeys(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorQuery currentOpQuery, String latestAncestorTableName1){

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: discoverOrderByKeys...");

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
                            MyMap keysMap = new MyMap(false);
                            String[] partsPairs = reduceSinkDesc.getKeyColString().split(",");

                            /*---Check key validity---*/
                            List<String> validParts = new LinkedList<>();

                            for(String partPair : partsPairs){

                                boolean isValid = true;

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
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Parts.length must be 3! Error! parts.length="+parts.length+" - Key: "+parts+" is not valid!");
                                    isValid = false;
                                    continue;
                                }

                                if(type.equals("string")){
                                    if(currentColName.contains("\'")){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Key: "+currentColName+" is not valid and will be ommited...");
                                        isValid = false;
                                    }
                                }

                                if(isValid == true){
                                    validParts.add(partPair);
                                }

                            }

                            if(validParts.size() != exprCols.size()){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": PartsPairs length and exprCols size ARE NOT equal! Error! partsPair.length="+partsPairs.length);
                                System.exit(0);
                            }

                            for(String partPair : validParts){ //Add all keys to a map
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

                                //System.out.println("Currently searching for key: ("+keyPair.getColumnName()+","+keyPair.getColumnType()+")\n");

                                for(TableRegEntry tableRegEntry : tableRegistry.getEntries()){
                                    for(ColumnTypePair cP : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()){
                                        if(cP.getColumnType().equals(keyPair.getColumnType())) {
                                            if(cP.getColumnName().equals(keyPair.getColumnName())) { //Column has original name
                                                //System.out.println("Direct match with: ("+cP.getColumnName()+","+cP.getColumnType()+")...check for father now...");
                                                List<StringParameter> altAliases = cP.getAltAliasPairs();
                                                for(StringParameter sP : altAliases){
                                                    if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){ //Now just check that father is also inline(if not then wrong table)
                                                        aliasFound = true;
                                                        realAliasOfKey = cP.getColumnName();
                                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+"is: "+realAliasOfKey);
                                                        break;
                                                    }
                                                }

                                                if(aliasFound == true) break;
                                            }
                                            else{
                                                List<StringParameter> altAliases = cP.getAltAliasPairs();
                                                //System.out.println("Will check the altAliases of: ("+cP.getColumnName()+","+cP.getColumnType()+")....");
                                                for(StringParameter sP : altAliases){
                                                    //System.out.println("Currently Examining Pair: ("+sP.getParemeterType()+","+sP.getValue()+")....(Searching for sP.getParemeterType() == equals(currentOperatorNode) )...");
                                                    if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                                        //System.out.println("CurrentOperatorNode located...(will check for KEY.".concat(keyPair.getColumnName()));
                                                        if(keyPair.getColumnName().equals(sP.getValue()) || keyPair.getColumnName().equals("KEY."+sP.getValue()) ){
                                                            aliasFound = true;
                                                            realAliasOfKey = cP.getColumnName();
                                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+"is: "+realAliasOfKey);
                                                            break;
                                                        }
                                                    }

                                                }

                                                if(aliasFound == true) break;
                                            }
                                        }
                                    }

                                    if(aliasFound == true) break;
                                }

                                boolean isConst = false;
                                if(aliasFound == false){ //Search in consts map
                                    for(ColumnTypePair constPair : ommitedConstantsMap.getColumnAndTypeList()){
                                        if(constPair.getColumnType().equals(keyPair.getColumnType())) {
                                            for (StringParameter altAlias : constPair.getAltAliasPairs()) {
                                                if (altAlias.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                                    if (altAlias.getValue().equals(keyPair.getColumnName())){
                                                        aliasFound = true;
                                                        isConst = true;
                                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+"is a Const value and will be ommited... ");
                                                        break;
                                                    }
                                                }
                                            }

                                            if(aliasFound) break;
                                        }
                                    }
                                }


                                if(aliasFound == false){
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+" was never found! Error!");
                                    System.exit(0);
                                }

                                if(isConst == false) {
                                    if(i == 0){
                                        orderByString = orderByString.concat(" " + realAliasOfKey + " " + order + " ");
                                    }
                                    else{
                                        orderByString = orderByString.concat(", " + realAliasOfKey + " " + order + " ");
                                    }

                                    //Add used column order by
                                    currentOpQuery.addUsedColumn(realAliasOfKey, latestAncestorTableName1);
                                }
                            }

                            //Fix string if ',' is contained in the last
                            orderByString = orderByString.trim();
                            if(orderByString.toCharArray()[orderByString.length() - 1] == ','){
                                char[] someCharArray = new char[orderByString.length() - 1];
                                char[] oldCharArray = orderByString.toCharArray();
                                for(int j = 0; j < someCharArray.length; j++){
                                    someCharArray[j] = oldCharArray[j];
                                }

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

    }

    public void discoverGroupByKeys(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorQuery currentOpQuery, String latestAncestorTableName1, GroupByDesc groupByDesc, List<FieldSchema> groupKeys){

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: discoverGroupByKeys...");

        List<String> groupByKeys = new LinkedList<>();
        MyMap changeMap = new MyMap(false);
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

                                if( (col == null) || (col.equals("NULL")) ){
                                    continue;
                                }
                                else {
                                    boolean fg = false;
                                    if (groupByKeys.size() == 0) groupByKeys.add(col);
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
                            }
                        } else {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"\t\tCols: NULL");
                        }
                    }
                }

                MyMap keysMap = new MyMap(false);
                String[] partsPairs = groupByDesc.getKeyString().split(",");

                List<String> validParts = new LinkedList<>();

                /*---Check key validity---*/
                for(String partPair : partsPairs){

                    boolean isValid = true;

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
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Parts.length must be 3! Error! parts.length="+parts.length+" - Key: "+parts+" is not valid!");
                        isValid = false;
                        continue;
                    }

                    if(type.equals("string")){
                        if(currentColName.contains("\'")){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Key: "+currentColName+" is not valid and will be ommited...");
                            isValid = false;
                        }
                    }

                    if(isValid == true){
                        validParts.add(partPair);
                    }

                }

                if(validParts.size() != groupByKeys.size()){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": PartsPairs length and groupByKeys size ARE NOT equal! Error! partsPair.length="+partsPairs.length);
                    System.exit(0);
                }

                for(String partPair : validParts){ //Add all keys to a map
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

                String expression = "";

                //Use the map to locate the real aliases and form the Group by clause
                for(int i = 0; i < keysMap.getColumnAndTypeList().size(); i++){
                    ColumnTypePair keyPair = keysMap.getColumnAndTypeList().get(i);

                    String realAliasOfKey = "";
                    boolean aliasFound = false;
                    String fatherName = fatherOperatorNode.getOperatorName();

                    for(TableRegEntry tableRegEntry : tableRegistry.getEntries()){
                        for(ColumnTypePair cP : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()){
                            if(cP.getColumnType().equals(keyPair.getColumnType())) {
                                if(cP.getColumnName().equals(keyPair.getColumnName())) { //Column has original name now check if father is also inline
                                    List<StringParameter> altAliases = cP.getAltAliasPairs();
                                    for(StringParameter sP : altAliases){
                                        if( sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()) ){
                                            aliasFound = true;
                                            realAliasOfKey = cP.getColumnName();
                                            FieldSchema savedColumn = new FieldSchema();
                                            savedColumn.setName(realAliasOfKey);
                                            savedColumn.setType(keyPair.getColumnType());
                                            groupKeys.add(savedColumn);
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+"is: "+realAliasOfKey);
                                            break;
                                        }
                                    }

                                    if(aliasFound == true) break;
                                }
                                else{
                                    List<StringParameter> altAliases = cP.getAltAliasPairs();
                                    for(StringParameter sP : altAliases){ //Group by keys usually consist of FatherOperatorNode column aliases
                                        if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                            if(keyPair.getColumnName().equals(sP.getValue()) || keyPair.getColumnName().equals("KEY."+sP.getValue()) ){
                                                aliasFound = true;
                                                realAliasOfKey = cP.getColumnName();
                                                FieldSchema savedColumn = new FieldSchema();
                                                savedColumn.setName(realAliasOfKey);
                                                savedColumn.setType(keyPair.getColumnType());
                                                groupKeys.add(savedColumn);
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+"is: "+realAliasOfKey);
                                                break;
                                            }
                                        }

                                    }

                                    if(aliasFound == true) break;
                                }
                            }
                        }

                        if(aliasFound == true) break;
                    }


                    boolean isConst = false;
                    if(aliasFound == false){ //Search in consts map
                        for(ColumnTypePair constPair : ommitedConstantsMap.getColumnAndTypeList()){
                            if(constPair.getColumnType().equals(keyPair.getColumnType())) {
                                for (StringParameter altAlias : constPair.getAltAliasPairs()) {
                                    if (altAlias.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                        if (altAlias.getValue().equals(keyPair.getColumnName())){
                                            aliasFound = true;
                                            isConst = true;
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+"is a Const value and will be ommited... ");
                                            break;
                                        }
                                    }
                                }

                                if(aliasFound) break;
                            }
                        }
                    }

                    if(aliasFound == false){
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+" was never found! Error!");
                        System.exit(0);
                    }

                    if(isConst == false) {
                        if(i == 0){
                            expression = expression.concat(" " + realAliasOfKey + " ");
                        }
                        else{
                            expression = expression.concat(", " + realAliasOfKey + " ");
                        }

                        //Add used column group by
                        currentOpQuery.addUsedColumn(realAliasOfKey, latestAncestorTableName1);
                    }

                }

                //Fix string if ',' is contained in the last
                expression = expression.trim();
                if(expression.toCharArray()[expression.length() - 1] == ','){
                    char[] someCharArray = new char[expression.length() - 1];
                    char[] oldCharArray = expression.toCharArray();
                    for(int j = 0; j < someCharArray.length; j++){
                        someCharArray[j] = oldCharArray[j];
                    }

                }

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real GroupBy Keys: "+expression);

                currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString() + " group by "+expression);
                currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString() + " group by "+expression);

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");
            }
        }

    }

    public void initialiseNewQuery(OperatorQuery currentOpQuery, OperatorQuery newQuery, MyTable inputTable){

        System.out.println("Accessing method: initialiseNewQuery...");

        newQuery.setDataBasePath(currentDatabasePath);
        newQuery.setLocalQueryString("");
        newQuery.setExaremeQueryString("");
        newQuery.addInputTable(inputTable);
        newQuery.setAssignedContainer("c0");
        newQuery.setLocalQueryString(" from "+inputTable.getTableName().toLowerCase());
        newQuery.setExaremeQueryString(" from "+inputTable.getTableName().toLowerCase());

    }

    public void addWhereStatementToQuery(OperatorNode currentOperatorNode, OperatorQuery currentOpQuery, String latestAncestorTableName1){

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: addWhereStatementToQuery...");

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

    }

    public void addWhereStatementToQuery2(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorQuery currentOpQuery, String latestAncestorSchema, String latestAncestorTableName1){

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: addWhereStatementToQuery2...");

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+ ": First copying all altAliases of FatherOperatorNode1...");
        addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+ ": Forming ancestorMap...");
        MyMap ancestorMap = new MyMap(true);
        latestAncestorSchema = extractColsFromTypeName(latestAncestorSchema, ancestorMap, latestAncestorSchema, true);

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+ ": Forming descendentMap...");
        MyMap descendentMap = new MyMap(true);
        String descendentSchema = "";

        if(currentOperatorNode.getOperator().getSchema().toString().contains("struct<")){
            descendentSchema = extractColsFromTypeNameWithStructs(currentOperatorNode.getOperator().getSchema().toString(), descendentMap, currentOperatorNode.getOperator().getSchema().toString(), true);
        }
        else{
            descendentSchema = extractColsFromTypeName(currentOperatorNode.getOperator().getSchema().toString(), descendentMap, currentOperatorNode.getOperator().getSchema().toString(), true);
        }

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

            boolean matchFound = false;

            if(ancestorPair.getColumnType().equals(descendentPair.getColumnType())){
                for(TableRegEntry tableRegEntry : tableRegistry.getEntries()){
                    for(ColumnTypePair p : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()){
                        if(p.getColumnType().equals(ancestorPair.getColumnType())){
                            if(p.getColumnName().equals(ancestorPair.getColumnName())){ //Located now check that father is in line
                                List<StringParameter> altAliases = p.getAltAliasPairs();
                                for(StringParameter sP : altAliases){
                                    if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){

                                            matchFound = true;
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Match found! - Pair: "+ancestorPair.getColumnName()+" - "+descendentPair.getColumnName());
                                            if(predCols != null){
                                                if(predCols.size() > 0){
                                                    for(String c : predCols){
                                                        if(c.equals(descendentPair.getColumnName())){
                                                            if(predicateString != null){
                                                                if(predicateString.contains(c)){
                                                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": This column also exists in the predicate! Replacing!");
                                                                    predicateString = predicateString.replace(c+" ", ancestorPair.getColumnName()+" ");
                                                                }
                                                            }

                                                            //Add used column
                                                            currentOpQuery.addUsedColumn(ancestorPair.getColumnName(), latestAncestorTableName1);
                                                        }
                                                    }
                                                }
                                            }

                                            p.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName());

                                            break;



                                    }

                                }
                            }


                            if(matchFound == true){
                                break;
                            }

                        }
                    }

                    if(matchFound == true) break;
                }

                if(matchFound == false){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Failed to find match for : "+descendentPair.getColumnName());
                    System.exit(0);
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

    /*
        Locates specific table Alias and father operator
        and adds possible new Aliases

        Works on the assumption that column expr map does not exist
     */

    public void useSchemaToFillAliasesForTable(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, String tableName){

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+ ": First copying all altAliases of FatherOperatorNode1...");
        addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);

        //System.out.println("useSchemaToFillAliasesForTable: TableRegistry before was...");
        //printTableRegistry();

        boolean aliasFound = false;
        for(TableRegEntry entry : tableRegistry.getEntries()){

            if(entry.getAlias().equals(tableName)){ //Located specific Alias...
                aliasFound = true;
                MyMap tempMap = new MyMap(false);
                String schemaString = extractColsFromTypeName(currentOperatorNode.getOperator().getSchema().toString(), tempMap, currentOperatorNode.getOperator().getSchema().toString(), true);

                for(ColumnTypePair newPair : tempMap.getColumnAndTypeList()) { //Get a new pair
                    boolean matchFound = false;
                    String opName = "";
                    String newAlias = "";

                    for (ColumnTypePair pair : entry.getColumnTypeMap().getColumnAndTypeList()) {
                        if(pair.getColumnType().equals(newPair.getColumnType())) { //Column with the same type
                            List<StringParameter> altAliases = pair.getAltAliasPairs(); //Get altAliases
                            for (StringParameter sP : altAliases) {
                                if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) { //Located father
                                    if(sP.getValue().equals(newPair.getColumnName())) {
                                        opName = currentOperatorNode.getOperatorName();
                                        newAlias = newPair.getColumnName();
                                        System.out.println("useSchemaToFillAliasesForTable: Located new Alias for: " + pair.getColumnName() + " through FatherOperatorNode: " + fatherOperatorNode.getOperatorName() + " - AliasValue= " + newPair.getColumnName());
                                        matchFound = true;

                                        pair.modifyAltAlias(opName, sP.getValue(), newAlias);

                                        break;
                                    }
                                }

                            }

                            if(matchFound == true) {
                                break;
                            }
                        }

                    }

                    if(matchFound == false) {
                        boolean foundToBeConstant = false;
                        for (ColumnTypePair constPair : ommitedConstantsMap.getColumnAndTypeList()) {
                            if (newPair.getColumnType().equals(constPair.getColumnType())) {
                                for (StringParameter sP : constPair.getAltAliasPairs()) {
                                    if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                        if (sP.getValue().equals(newPair.getColumnName())) {
                                            System.out.println("Alias: " + newPair.getColumnName() + " is a Constant value and will be ommited...");
                                            foundToBeConstant = true;
                                            matchFound = true;
                                            break;
                                        }
                                    }
                                }

                                if (foundToBeConstant) break;
                            }
                        }
                    }

                    if(matchFound == true) break;

                    if(matchFound == false){
                        System.out.println("useSchemaToFillAliasesForTable: Failed to find altAlias for: "+newPair.getColumnName());
                        System.exit(0);
                    }

                }

                break;
            }

        }

        if(aliasFound == false){
            System.out.println("useSchemaToFillAliasesForTable: Table with alias: "+aliasFound+" was never found...");
        }

        System.out.println("useSchemaToFillAliasesForTable: TableRegistry now is...");
        printTableRegistry();

    }

    public String useSchemaToFillAliases(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, String latestAncestorSchema){

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+ ": First copying all altAliases of FatherOperatorNode1...");
        addAliasesBasedOnFather(currentOperatorNode, fatherOperatorNode);

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: useSchemaToFillAliases...");

        MyMap ancestorMap = new MyMap(true);
        latestAncestorSchema = extractColsFromTypeName(latestAncestorSchema, ancestorMap, latestAncestorSchema, true);

        MyMap descendentMap = new MyMap(true);
        String descendentSchema = "";
        if(currentOperatorNode.getOperator().getSchema().toString().contains("struct<")){
            descendentSchema = extractColsFromTypeNameWithStructs(currentOperatorNode.getOperator().getSchema().toString(), descendentMap, currentOperatorNode.getOperator().getSchema().toString(), true);

        }
        else{
            descendentSchema = extractColsFromTypeName(currentOperatorNode.getOperator().getSchema().toString(), descendentMap, currentOperatorNode.getOperator().getSchema().toString(), true);
        }

        if(ancestorMap.getColumnAndTypeList().size() != descendentMap.getColumnAndTypeList().size()){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Maps do not have equal size!");
            System.exit(0);
        }

        for(int i = 0; i < ancestorMap.getColumnAndTypeList().size(); i++){
            ColumnTypePair ancestorPair = ancestorMap.getColumnAndTypeList().get(i);
            ColumnTypePair descendentPair = descendentMap.getColumnAndTypeList().get(i);

            boolean matchFound = false;

            if(ancestorPair.getColumnType().equals(descendentPair.getColumnType())){
                for(TableRegEntry entry : tableRegistry.getEntries()){
                    for(ColumnTypePair p : entry.getColumnTypeMap().getColumnAndTypeList()){
                        if(p.getColumnType().equals(ancestorPair.getColumnType())) {
                            if(p.getColumnName().equals(ancestorPair.getColumnName())){ //Located, now check if Father is inline
                                List<StringParameter> altAliases = p.getAltAliasPairs();
                                for(StringParameter sP : altAliases){
                                    if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Match found for Alias: "+descendentPair.getColumnName()+" - Real Name is: "+p.getColumnName()+" - through Father: "+fatherOperatorNode.getOperatorName());
                                        matchFound = true;
                                        p.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName());
                                        break;
                                    }
                                }

                                if(matchFound == true){
                                    break;
                                }
                            }
                        }

                    }

                    if(matchFound == true) break;

                }

                if(matchFound == false){
                    String aliasValue = "";
                    if(descendentPair.getColumnType().equals("struct")){
                        System.out.println("Will check in aggrMap for match...");
                        for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                            for(StringParameter sP : aggPair.getAltAliasPairs()){
                                if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                    aliasValue = sP.getValue();
                                    if(sP.getValue().equals(ancestorPair.getColumnName())){
                                        System.out.println("Located value in altAliases of: "+aggPair.getColumnName()+" now find the right alias for it...");
                                        matchFound = true;
                                        if(sP.getValue().equals(descendentPair.getColumnName())){ //Father has the same value as descendent
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Match found for Alias: "+descendentPair.getColumnName()+" - Real Name is: "+sP.getValue()+" - through Father: "+fatherOperatorNode.getOperatorName());
                                            matchFound = true;
                                            aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName());
                                            break;
                                        }
                                        else{
                                            if (descendentPair.getColumnName().contains("KEY.") || (descendentPair.getColumnName().contains("VALUE."))) {
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Match found for Alias: "+descendentPair.getColumnName()+" - Real Name is: "+sP.getValue()+" - through Father: "+fatherOperatorNode.getOperatorName());
                                                matchFound = true;
                                                aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName());
                                                break;
                                            }
                                        }
                                    }

                                    if(matchFound) break;
                                }
                            }

                            if(matchFound) break;
                        }
                    }

                }

            }
            else{
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Possible match Pair does not have same Type - Pair: "+ancestorPair.getColumnName()+" - "+descendentPair.getColumnName());
                System.exit(0);
            }

            if(matchFound == false){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Failed to find match for: "+descendentPair.getColumnName());
                System.exit(0);
            }


        }

        return descendentSchema;

    }

    public String fetchlatestAncestorWithDifferentValue(List<StringParameter> altAliases, String value, String currentNode, String fatherNode, String otherFatherNode){

        //Check if current value is the first
        if(altAliases.get(0).getValue().equals(value)) return currentNode;

        //First check if parent has different value
        for(StringParameter sP : altAliases){
            if(sP.getParemeterType().equals(fatherNode)){
                if(sP.getValue().equals(value) == false){
                    return fatherNode;
                }
            }
        }

        if(otherFatherNode != null){
            for(StringParameter sP : altAliases){
                if(sP.getParemeterType().equals(otherFatherNode)){
                    if(sP.getValue().equals(value) == false){
                        return otherFatherNode;
                    }
                }
            }

            //Now find the latest ancestor
            String currentAncestor = otherFatherNode;
            String currentValue = value;
            String previousFather = "";
            String wantedValue = "";
            boolean failed = true;

            do {

                for (int j = 0; j < altAliases.size(); j++) {
                    if (j < altAliases.size() - 1) {
                        if (altAliases.get(j + 1).getParemeterType().equals(currentAncestor)) {
                            previousFather = altAliases.get(j).getParemeterType();
                            wantedValue = altAliases.get(j).getValue();
                            break;
                        }
                    }
                }

                if (wantedValue.equals(currentValue) == false) {
                    failed = false;
                    break;
                }
                else {
                    currentAncestor = previousFather;
                }

            } while (true);

            if(failed == false) return previousFather;

        }

        //Now find the latest ancestor
        String currentAncestor = fatherNode;
        String currentValue = value;
        String previousFather = "";
        String wantedValue = "";

        do {

            for (int j = 0; j < altAliases.size(); j++) {
                if (j < altAliases.size() - 1) {
                    if (altAliases.get(j + 1).getParemeterType().equals(currentAncestor)) {
                        previousFather = altAliases.get(j).getParemeterType();
                        wantedValue = altAliases.get(j).getValue();
                        break;
                    }
                }
            }

            if (wantedValue.equals(currentValue) == false) break;
            else {
                currentAncestor = previousFather;
            }

        } while (true);

        return previousFather;

    }

    public String finaliseOutputTable(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, OperatorQuery currentOpQuery, String stringToExtractFrom, String outputTableName, MyTable outputTable, List<FieldSchema> newCols){

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: finaliseOutputTable...");

        MyMap someMap = new MyMap(false);

        if(stringToExtractFrom.contains("struct<")){
            stringToExtractFrom = extractColsFromTypeNameWithStructs(stringToExtractFrom, someMap, stringToExtractFrom, true);
        }
        else {
            stringToExtractFrom = extractColsFromTypeName(stringToExtractFrom, someMap, stringToExtractFrom, true);
        }

        /*---Find real possible alias thourgh extra map for _col columns----*/
        for(ColumnTypePair pair : someMap.getColumnAndTypeList()){ //For Every pair
            if(pair.getColumnName().contains("_col")){ //If it contains _col (aka Unknown column)
                if(pair.getColumnType().equals("struct")){ //If it is an aggregation column
                    boolean structFound = false;
                    for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){ //Get an aggreagationPair from the List
                        for(StringParameter sP : aggPair.getAltAliasPairs()){ //Get its alt aliases
                            if(sP.getParemeterType().equals(currentOperatorNode.getOperatorName())){ //Locate the currentNode in the List

                                if(sP.getValue().equals(pair.getColumnName())) { //Locate the value as an altAlias in the List

                                    structFound = true;
                                    if(pair.getColumnName().equals(aggPair.getAltAliasPairs().get(0).getValue())){ //If the altAlias is equal to the first altAlias in the List then we need the aggregation expression
                                        pair.setColumnName(aggPair.getAltAliasPairs().get(0).getValue()); //Just keep the same value - TODO: CHECK THIS BETTER (DIFFERENT FROM SELECT)
                                    }
                                    else{ //The latest altAlias is different from the first - This means that we need the altAlias of the father to refer to
                                        boolean aliasLocated = false;
                                        String wantedAncestor = "";
                                        if(otherFatherNode != null)
                                            wantedAncestor = fetchlatestAncestorWithDifferentValue(aggPair.getAltAliasPairs(), pair.getColumnName(), currentOperatorNode.getOperatorName(), fatherOperatorNode.getOperatorName(), otherFatherNode.getOperatorName());
                                        else
                                            wantedAncestor = fetchlatestAncestorWithDifferentValue(aggPair.getAltAliasPairs(), pair.getColumnName(), currentOperatorNode.getOperatorName(), fatherOperatorNode.getOperatorName(), null);

                                        for(StringParameter sP2 : aggPair.getAltAliasPairs()){
                                            if(sP2.getParemeterType().equals(wantedAncestor)){
                                                    if(stringToExtractFrom.contains(","+pair.getColumnName()+":")){
                                                        stringToExtractFrom = stringToExtractFrom.replace(","+pair.getColumnName()+":", ","+sP2.getValue()+":");
                                                    }
                                                    else if(stringToExtractFrom.contains("("+pair.getColumnName())){
                                                        stringToExtractFrom = stringToExtractFrom.replace("("+pair.getColumnName()+":", "("+sP2.getValue()+":");
                                                    }
                                                pair.setColumnName(sP2.getValue());
                                                aliasLocated = true;
                                                break;
                                            }
                                        }

                                        if(aliasLocated == false){
                                            System.out.println("Attempted to locate alternate aggr alias for: "+pair.getColumnName()+" but failed... - LatestAncestor Checked: "+wantedAncestor);
                                            System.exit(0);
                                        }
                                    }

                                }

                            }
                        }
                    }
                }
                else{ //Handle unknown constant column

                    boolean foundToBeConstant = false;
                    for (ColumnTypePair constPair : ommitedConstantsMap.getColumnAndTypeList()) {
                        if (pair.getColumnType().equals(constPair.getColumnType())) {
                            for (StringParameter sP : constPair.getAltAliasPairs()) {
                                if ((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()))) {
                                    if (sP.getValue().equals(pair.getColumnName())) {
                                        System.out.println("Alias: " + pair.getColumnName() + " is a Constant value and will be ommited...");
                                        pair.setColumnName("__ommited_by_author_of_code__1213");
                                        foundToBeConstant = true;
                                        break;
                                    }
                                }
                                else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                    if (sP.getValue().equals(pair.getColumnName())) {
                                        System.out.println("Alias: " + pair.getColumnName() + " is a Constant value and will be ommited...");
                                        pair.setColumnName("__ommited_by_author_of_code__1213");
                                        foundToBeConstant = true;
                                        break;
                                    }
                                }
                                else if ((sP.getParemeterType().equals(currentOperatorNode.getOperatorName()))) {
                                    if (sP.getValue().equals(pair.getColumnName())) {
                                        System.out.println("Alias: " + pair.getColumnName() + " is a Constant value and will be ommited...");
                                        pair.setColumnName("__ommited_by_author_of_code__1213");
                                        foundToBeConstant = true;
                                        break;
                                    }
                                }
                            }

                            if (foundToBeConstant) break;
                        }
                    }

                }
            }
        }

        System.out.println("Final aliases that will be used: ");
        someMap.printMap();

        String selectString = "";
        List<ColumnTypePair> pairs = someMap.getColumnAndTypeList();
        for(int i = 0; i < pairs.size(); i++){
            if(pairs.get(i).getColumnName().equals("__ommited_by_author_of_code__1213")) continue;

            if(pairs.get(i).getColumnName().contains("BLOCK__OFFSET__INSIDE__FILE")){
                continue;
            }
            else if(pairs.get(i).getColumnName().contains("INPUT__FILE__NAME")){
                continue;
            }
            else if(pairs.get(i).getColumnName().contains("ROW__ID")){
                continue;
            }
            else if(pairs.get(i).getColumnName().contains("rowid")){
                continue;
            }
            else if(pairs.get(i).getColumnName().contains("bucketid")){
                continue;
            }
            else if(pairs.get(i).getColumnName().contains("__ommited_by_author_of_code__1213")){
                continue;
            }

            FieldSchema f = new FieldSchema();
            f.setName(pairs.get(i).getColumnName());
            f.setType(pairs.get(i).getColumnType());
            newCols.add(f);


        }

        outputTable.setTableName(outputTableName);
        outputTable.setAllCols(newCols);
        outputTable.setHasPartitions(false);

        currentOpQuery.setOutputTable(outputTable);
        currentOpQuery.setExaremeOutputTableName("R_"+outputTable.getTableName().toLowerCase()+"_0");

        return stringToExtractFrom;

    }

    /* Used to create an Exareme Table Definition either
    from a Hive Table or a Hive Partition
     */

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
                    definition = definition.concat("DECIMAL(10,5)");
                }
                else if(type.contains("float")){
                    definition = definition.concat("FLOAT");
                }
                else if(type.contains("char")){
                    definition = definition.concat("TEXT");
                }
                else if(type.contains("varchar")){
                    definition = definition.concat("TEXT");
                }
                else if(type.contains("date")){
                    definition = definition.concat("DATE");
                }
                else if(type.contains("double")){
                    definition = definition.concat("REAL");
                }
                else if(type.contains("double precision")){
                    definition = definition.concat("REAL");
                }
                else if(type.contains("bigint")){
                    definition = definition.concat("BIGINT");
                }
                else if(type.contains("smallint")){
                    definition = definition.concat("SMALLINT");
                }
                else if(type.contains("tinyint")){
                    definition = definition.concat("TINYINT");
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
                    definition = definition.concat("DECIMAL(10,5),");
                }
                else if(type.contains("float")){
                    definition = definition.concat("FLOAT,");
                }
                else if(type.contains("char")){
                    definition = definition.concat("TEXT,");
                }
                else if(type.contains("varchar")){
                    definition = definition.concat("TEXT,");
                }
                else if(type.contains("date")){
                    definition = definition.concat("DATE,");
                }
                else if(type.contains("double")){
                    definition = definition.concat("REAL,");
                }
                else if(type.contains("double precision")){
                    definition = definition.concat("REAL,");
                }
                else if(type.contains("bigint")){
                    definition = definition.concat("BIGINT,");
                }
                else if(type.contains("smallint")){
                    definition = definition.concat("SMALLINT,");
                }
                else if(type.contains("tinyint")){
                    definition = definition.concat("TINYINT,");
                }
                else{
                    System.out.println("exaremeTableDefinition: Unsupported Hive Type! Type: "+type);
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
                    definition = definition.concat("DECIMAL(10,5)");
                }
                else if(type.contains("char")){
                    definition = definition.concat("TEXT");
                }
                else if(type.contains("varchar")){
                    definition = definition.concat("TEXT");
                }
                else if(type.contains("date")){
                    definition = definition.concat("DATE");
                }
                else if(type.contains("float")){
                    definition = definition.concat("FLOAT,");
                }
                else if(type.contains("double")){
                    definition = definition.concat("REAL");
                }
                else if(type.contains("double precision")){
                    definition = definition.concat("REAL");
                }
                else if(type.contains("bigint")){
                    definition = definition.concat("BIGINT");
                }
                else if(type.contains("smallint")){
                    definition = definition.concat("SMALLINT");
                }
                else if(type.contains("tinyint")){
                    definition = definition.concat("TINYINT");
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
                else if(type.contains("varchar")){
                    definition = definition.concat("TEXT,");
                }
                else if(type.contains("date")){
                    definition = definition.concat("DATE,");
                }
                else if(type.contains("float")){
                    definition = definition.concat("FLOAT,");
                }
                else if(type.contains("double")){
                    definition = definition.concat("REAL,");
                }
                else if(type.contains("double precision")){
                    definition = definition.concat("REAL,");
                }
                else if(type.contains("bigint")){
                    definition = definition.concat("BIGINT,");
                }
                else if(type.contains("smallint")){
                    definition = definition.concat("SMALLINT,");
                }
                else if(type.contains("tinyint")){
                    definition = definition.concat("TINYINT,");
                }
                else{
                    System.out.println("exaremeTableDefinition: Unsupported Hive Type! Type: "+type);
                    System.exit(0);
                }
            }
        }

        definition = "create table "+somePartition.getBelogingTableName().toLowerCase()+" ("+definition+" )";

        return definition;
    }


    /* Used by createCartesianCombinations
     */

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

    /*
        Used to create CartesianCombination of Input Partitions
     */

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

    /*
        The below method takes care of translating OperatorQueries to AdpDBSelectOperators
        essentially being the last step before printing the Exareme Plan
     */

    public List<AdpDBSelectOperator> translateToExaremeOps(){

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
