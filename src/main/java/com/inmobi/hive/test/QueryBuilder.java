package com.inmobi.hive.test;

import madgik.exareme.common.app.engine.AdpDBOperatorType;
import madgik.exareme.common.app.engine.AdpDBSelectOperator;
import madgik.exareme.common.app.engine.scheduler.elasticTree.system.data.Table;
import madgik.exareme.common.schema.Select;
import madgik.exareme.common.schema.TableView;
import madgik.exareme.common.schema.expression.Comments;
import madgik.exareme.common.schema.expression.DataPattern;
import madgik.exareme.common.schema.expression.SQLSelect;
import madgik.exareme.utils.embedded.process.MadisProcess;
import madgik.exareme.utils.embedded.process.QueryResultStream;
import org.apache.commons.httpclient.URI;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
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
    LinkedHashMap<String, MyMap> operatorCastMap; //Keeps track of CAST() for every column that has been ever casted
    List<OperatorQuery> allQueries;
    List<MyTable> inputTables;
    List<MyTable> outputTables;
    List<OpLink> opLinksList;
    String currentDatabasePath;
    String madisPath = "";
    FileSystem hadoopFS; //Needed for convert operations

    boolean outputTableIsFile = false;
    String createTableName = "";

    List<JoinPoint> joinPointList = new LinkedList<>();
    List<UnionPoint> unionPointList = new LinkedList<>();

    public QueryBuilder(ExaremeGraph graph, List<MyTable> inputT, List<MyPartition> inputP, List<MyTable> outputT, List<MyPartition> outputP, String databasePath, FileSystem fs, String madis){
        exaremeGraph = graph;
        allQueries = new LinkedList<>();
        aggregationsMap = new MyMap(false);
        ommitedConstantsMap = new MyMap(false);
        tableRegistry = new TableRegistry();
        operatorCastMap = new LinkedHashMap<>();
        inputTables = inputT;
        outputTables = outputT;
        opLinksList = new LinkedList<>();
        currentDatabasePath = databasePath;
        hadoopFS = fs;
        madisPath = madis;

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
                outputTableIsFile = true;
            }
            else{
                System.out.println("Output Table: " + outputTable.getTableName());
                outputTableIsFile = false;
                createTableName = outputTable.getTableName().toLowerCase();
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

    public void addNewCastPair(String operatorName, String realAlias, String realType, String castType, String currentOpAlias, String castColAlias, String castExpr){

        if(operatorCastMap.isEmpty()){
            MyMap operatorMap = new MyMap(true);
            ColumnTypePair pair = new ColumnTypePair(realAlias, realType);
            pair.addAltAlias(operatorName, currentOpAlias, false);
            pair.getAltAliasPairs().get(0).setExtraValue(castColAlias);
            pair.getAltAliasPairs().get(0).setCastExpr(castExpr);
            pair.addCastType(castType);
            operatorMap.addPair(pair);
            operatorCastMap.put(operatorName, operatorMap);
        }
        else{
            for(Map.Entry<String, MyMap> entry : operatorCastMap.entrySet()){
                if(entry.getKey().equals(operatorName)){
                    MyMap existingMap = entry.getValue();
                    for(ColumnTypePair existingPair : existingMap.getColumnAndTypeList()){
                        if(existingPair.getColumnName().equals(realAlias)){
                            if(existingPair.getColumnType().equals(realType)){
                                existingPair.addCastType(castType);
                                //existingPair.getAltAliasPairs().get(0).setExtraValue(castColAlias);
                                //existingPair.getAltAliasPairs().get(0).setCastExpr(castExpr);
                                existingPair.getAltAliasPairs().get(0).setParemeterType(operatorName);
                                existingPair.getAltAliasPairs().get(0).setValue(currentOpAlias);
                                return;
                            }
                        }
                    }
                    ColumnTypePair pair = new ColumnTypePair(realAlias, realType);
                    pair.addAltAlias(operatorName, currentOpAlias, false);
                    pair.getAltAliasPairs().get(0).setExtraValue(castColAlias);
                    pair.getAltAliasPairs().get(0).setCastExpr(castExpr);
                    pair.addCastType(castType);
                    entry.getValue().addPair(pair);
                    return;
                }
            }

            MyMap operatorMap = new MyMap(true);
            ColumnTypePair pair = new ColumnTypePair(realAlias, realType);
            pair.addAltAlias(operatorName, currentOpAlias, false);
            pair.getAltAliasPairs().get(0).setExtraValue(castColAlias);
            pair.getAltAliasPairs().get(0).setCastExpr(castExpr);
            pair.addCastType(castType);
            operatorMap.addPair(pair);
            operatorCastMap.put(operatorName, operatorMap);
        }

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

    public void fixColTypePairContainingCast(String commaPart, List<String> colAndType){ // , CAST( _c1 AS decimal) (type: decimal)

        char[] schemaToCharArray = commaPart.toCharArray();

        String comboToLocate = "CAST(";
        char[] comboToArray = comboToLocate.toCharArray();
        int neededCorrectChars = comboToLocate.length();

        int j = 0;
        boolean gatherMode = false;
        List<Character> castPhrase = new LinkedList<>();
        for(int i = 0; i < schemaToCharArray.length; i++){
            if(gatherMode == false) {
                if (schemaToCharArray[i] == comboToArray[j]) { //Located a correct char
                    j++;
                } else {
                    j = 0;
                }

                if (j == neededCorrectChars) { //Completely located decimal(
                    gatherMode = true;
                }
            }
            else{
                if(schemaToCharArray[i] == ')'){
                    gatherMode = false;
                    j = 0;
                }
                castPhrase.add(new Character(schemaToCharArray[i]));
            }

        }

        char[] insidePhraseArray = new char[castPhrase.size()];

        int k = 0;
        for(Character c : castPhrase){
            insidePhraseArray[k] = castPhrase.get(k);
            k++;
        }

        String insidePhrase = new String(insidePhraseArray);

        String fullCastPhrase = "CAST(" + insidePhrase;

        colAndType.add(fullCastPhrase);

        System.out.println("fixColTypePairContainingCast: CAST PHRASE: "+fullCastPhrase);
        if(commaPart.contains(fullCastPhrase)) {
            String typePart = commaPart.replace(fullCastPhrase, "");
            typePart = typePart.trim();
            typePart = typePart.replace("(type: ", "");
            typePart = typePart.replace(")", "");
            System.out.println("fixColTypePairContainingCast: CAST TYPE: "+typePart);
            colAndType.add(typePart);
        }
        else{
            System.out.println("fixColTypePairContainingCast: Phrase does not contain: "+fullCastPhrase);
        }

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
                    schemaToCharArray[i] = '~';
                }
            }
            else{
                if(schemaToCharArray[i] == ')'){
                    replaceMode = false;
                    j = 0;
                }
                schemaToCharArray[i] = '~';
            }

        }

        String updatedString = new String(schemaToCharArray);

        updatedString = updatedString.replace("~", "");


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

    public String addNewPossibleAliases(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorNode fatherOperatorNode2, OperatorQuery currentOpQuery){

        System.out.println(currentOperatorNode.getOperatorName()+": Accessing method: addNewPossibleAliases...");

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

        int currentEntryValue = 0;
        List<String> bannedColumnList = new LinkedList<>();

        for(Map.Entry<String, ExprNodeDesc> entry : currentOperatorNode.getOperator().getColumnExprMap().entrySet()) {
            if (entry.getKey().equals("ROW__ID") || entry.getKey().equals("BLOCK__OFFSET__INSIDE__FILE") || entry.getKey().equals("INPUT__FILE__NAME"))
                continue;
            ExprNodeDesc oldValue = entry.getValue();
            if (oldValue == null) {
                continue;
            }
            if ((oldValue.getName().contains("Const ") || oldValue.toString().contains("Const "))) {
                String constType = "";
                if (oldValue.toString().contains(" int ")) {
                    constType = "int";
                } else if (oldValue.toString().contains(" float ")) {
                    constType = "float";
                } else if (oldValue.toString().contains(" decimal ")) {
                    constType = oldValue.toString().split(" ")[1];
                } else if (oldValue.toString().contains(" string ")) {
                    constType = "string";
                } else if (oldValue.toString().contains("char")) {
                    constType = "char";
                } else if (oldValue.toString().contains("varchar")) {
                    constType = "varchar";
                } else if (oldValue.toString().contains("date")) {
                    constType = "date";
                } else if (oldValue.toString().contains("double")) {
                    constType = "double";
                } else if (oldValue.toString().contains("double precision")) {
                    constType = "double precision";
                } else if (oldValue.toString().contains("bigint")) {
                    constType = "bigint";
                } else if (oldValue.toString().contains("smallint")) {
                    constType = "smallint";
                } else if (oldValue.toString().contains("tinyint")) {
                    constType = "tinyint";
                } else {
                    System.out.println("Unsupported Const type for : " + oldValue.toString());
                    System.exit(0);
                }

                boolean ommitAlias = false;
                for (TableRegEntry tableRegEntry : tableRegistry.getEntries()) {
                    for (ColumnTypePair pair2 : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()) {
                        if (pair2.getColumnType().equals(constType)) {
                            if (pair2.getColumnName().equals(entry.getKey())) {
                                System.out.println("Constant Value: " + pair2.getColumnName() + " also exists in tableRegistry! Omitting!");
                                ommitAlias = true;
                                break;
                            }
                        }
                    }
                    if (ommitAlias) break;
                }

                if (ommitAlias == false) {
                    ColumnTypePair theNewPair = new ColumnTypePair(oldValue.toString(), constType);
                    theNewPair.addAltAlias(currentOperatorNode.getOperatorName(), entry.getKey(), false);
                    checkIfConstantMapBreaksRegistry(constType, currentOperatorNode.getOperatorName(), entry.getKey());
                    ommitedConstantsMap.addPair(theNewPair);
                }


                continue;
            }

            String oldColumnName = "";

            boolean genericUDFBridge = false;
            boolean castToDecimal = false;

            if (entry.getValue().toString().contains("GenericUDFBridge(")) {
                System.out.println("Entry.getValue(): "+entry.getValue()+" contains GenericUDFBridge...");
                oldColumnName = entry.getValue().toString();
                oldColumnName = oldColumnName.replace("GenericUDFBridge(", "");
                oldColumnName = oldColumnName.replace(")", "");
                oldColumnName = oldColumnName.replace("Column[", "");
                oldColumnName = oldColumnName.replace("]", "");
                System.out.println("Entry.getValue() now is : "+oldColumnName);
                genericUDFBridge = true;
            }
            else if(entry.getValue().toString().contains("GenericUDFToDecimal(")){
                System.out.println("Entry.getValue(): "+entry.getValue()+" contains GenericUDFToDecimal...");
                oldColumnName = entry.getValue().toString();
                oldColumnName = oldColumnName.replace("GenericUDFToDecimal(", "");
                oldColumnName = oldColumnName.replace(")", "");
                oldColumnName = oldColumnName.replace("Column[", "");
                oldColumnName = oldColumnName.replace("]", "");
                System.out.println("Entry.getValue() now is : "+oldColumnName);
                genericUDFBridge = true;
                castToDecimal = true;
            }
            else{
                oldColumnName = oldValue.getCols().get(0);
            }

            List<TableRegEntry> tableEntries = tableRegistry.getEntries();

            boolean matchFound = false;

            boolean existsAgainInValues = false;
            int multipleEntryValues = 0;
            for(Map.Entry<String, ExprNodeDesc> entry2 : currentOperatorNode.getOperator().getColumnExprMap().entrySet()){
                if(entry2 != null){
                    if(entry2.getValue().toString().equals(entry.getValue().toString())){
                        if(entry2.getKey().equals(entry.getKey()) == false){
                            if(entry2.getValue().toString().contains("reducesinkkey")){
                                existsAgainInValues = true;
                                multipleEntryValues++;
                            }
                        }
                    }
                }
            }

            if(multipleEntryValues > 1){
                System.out.println("Can't support more than 2 same entry values...");
                System.exit(0);
            }

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
                                        if(cP.getColumnType().equals(tempPair.getColumnType()) || tempPair.getColumnType().equals(cP.getLatestAltCastType())){
                                            for(StringParameter sP : cP.getAltAliasPairs()){
                                                if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                                    if(sP.getValue().equals(entry.getKey())){
                                                        locatedInSchema = true;
                                                        if(tempPair.getColumnType().equals(cP.getLatestAltCastType())){
                                                            targetType = tempPair.getColumnType();
                                                        }
                                                        else{
                                                            targetType = cP.getColumnType();
                                                        }
                                                        foundWithOlderName = true;
                                                        break;
                                                    }
                                                    else if(sP.getValue().equals(oldColumnName)){
                                                        locatedInSchema = true;
                                                        if(tempPair.getColumnType().equals(cP.getLatestAltCastType())){
                                                            targetType = tempPair.getColumnType();
                                                        }
                                                        else{
                                                            targetType = cP.getColumnType();
                                                        }
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
                                        if(cP.getColumnType().equals(tempPair.getColumnType()) || cP.getLatestAltCastType().equals(tempPair.getColumnType())){
                                            for(StringParameter sP : cP.getAltAliasPairs()){
                                                if(sP.getParemeterType().equals(fatherOperatorNode2.getOperatorName())){
                                                    if(sP.getValue().equals(entry.getKey())){
                                                        locatedInSchema = true;
                                                        if(tempPair.getColumnType().equals(cP.getLatestAltCastType())){
                                                            targetType = tempPair.getColumnType();
                                                        }
                                                        else{
                                                            targetType = cP.getColumnType();
                                                        }
                                                        foundWithOlderName = true;
                                                        break;
                                                    }
                                                    else if(sP.getValue().equals(oldColumnName)){
                                                        locatedInSchema = true;
                                                        if(tempPair.getColumnType().equals(cP.getLatestAltCastType())){
                                                            targetType = tempPair.getColumnType();
                                                        }
                                                        else{
                                                            targetType = cP.getColumnType();
                                                        }
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
                        if(entry.getKey().equals(oldColumnName)){
                            System.out.println("Does not add anything new and does not exist in schema...continue");
                            continue;
                        }
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

            for(TableRegEntry regEntry : tableEntries){ //Run through all Input Tables of Table Registry //TODO: HANDLE CASE OF JOIN BETWEEN TWO SAME TABLES WITH SAME COLUMN TO JOIN

                List<ColumnTypePair> columnTypePairs = regEntry.getColumnTypeMap().getColumnAndTypeList();

                for(ColumnTypePair pair : columnTypePairs){ //Run through the column Map of each table
                    List<StringParameter> altAliases = pair.getAltAliasPairs(); //Get the alt Aliases of a column

                    if(hasMoreThan1Match == true){
                        if(pair.getColumnType().equals(targetType) == false){
                            if(pair.hasLatestAltCastType(targetType) == false){
                                continue;
                            }
                        }
                    }

                    if(genericUDFBridge == false) {
                        if (allowTypeComparisons == true) {
                            if (pair.getColumnType().equals(possibleType) == false) {
                                if (pair.hasLatestAltCastType(possibleType) == false) {
                                    continue;
                                }
                            }
                        }
                    }

                    if(existsAgainInValues) {
                        if (bannedColumnList.size() > 0) {
                            boolean banned = false;
                            for (String s : bannedColumnList) {
                                if (s.equals(pair.getColumnName())) {
                                    banned = true;
                                    break;
                                }
                            }

                            if(banned) continue;
                        }
                    }

                    boolean checkForCast = false;
                    if(pair.hasLatestAltCastType(possibleType)){
                        if(pair.getColumnType().equals(possibleType) == false){
                            checkForCast = true;
                        }
                    }

                    if(altAliases.size() == 0){
                        System.out.println("addNewPossibleAlias: AltAlias must always be > 0");
                        System.exit(0);
                    }

                    for(StringParameter sP : altAliases){ //Try to find father through the altAliases
                        if((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) ){ //Father1 Located
                            if(sP.getValue().equals(oldColumnName)){ //Column names match

                                String properAliasName = pair.getColumnName();

                                boolean locatedOutOfRegistry = false;
                                if(checkForCast && (genericUDFBridge == false)){
                                    System.out.println("addNewPossibleAlias: Since we have cast type let's check out of registry...");
                                    for(Map.Entry<String, MyMap> castEntry : operatorCastMap.entrySet()){ //Check for cast in cast map
                                        if(castEntry.getKey().equals(fatherOperatorNode.getOperatorName())) {
                                            for (ColumnTypePair castPair : castEntry.getValue().getColumnAndTypeList()) {
                                                if(castPair.getColumnName().equals(pair.getColumnName())){
                                                    if(castPair.getColumnType().equals(pair.getColumnType())){
                                                        if(castPair.getLatestAltCastType().equals(pair.getLatestAltCastType())){
                                                            if(castPair.getAltAliasPairs().get(0).getValue().equals(oldColumnName)){
                                                                System.out.println("addNewPossibleAlias: Located corresponding cast column: "+castPair.getAltAliasPairs().get(0).getExtraValue());
                                                                String actualCastName = castPair.getAltAliasPairs().get(0).getExtraValue();
                                                                boolean parameterLocated = false;
                                                                System.out.println("addNewPossibleAlias: Take it one step further! Looking if contained in aggregations...");
                                                                for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                                                                    for(String pValue : aggPair.getParameterValues()){
                                                                        if(pValue.equals(actualCastName)){
                                                                            for(StringParameter altAliasAggr : aggPair.getAltAliasPairs()){
                                                                                if(altAliasAggr.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                                                                    if(altAliasAggr.getValue().equals(oldColumnName)){
                                                                                        System.out.println("addNewPossibleAlias: After all oldColumnName: "+oldColumnName+" is an aggregation! ");
                                                                                        parameterLocated = true;
                                                                                        actualCastName = fetchlatestRequiredAncestorOfAggr(currentOperatorNode, currentOpQuery, aggPair.getColumnName(), aggPair.getAltAliasPairs(), oldColumnName);
                                                                                        properAliasName = actualCastName;
                                                                                        matchFound = true;
                                                                                        locatedOutOfRegistry = true;
                                                                                        aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), oldColumnName, entry.getKey(), true);

                                                                                        if (tempMap.getColumnAndTypeList().size() > 1) {
                                                                                            if (schemaString.contains("," + entry.getKey() + ":")) { //Update schema
                                                                                                schemaString = schemaString.replace("," + entry.getKey() + ":", "," + properAliasName + ":");
                                                                                                System.out.println("Schema becomes: " + schemaString);
                                                                                            } else {
                                                                                                if (schemaString.contains("(" + entry.getKey() + ":")) { //Update schema
                                                                                                    schemaString = schemaString.replace("(" + entry.getKey() + ":", "(" + properAliasName + ":");
                                                                                                    System.out.println("Schema becomes: " + schemaString);
                                                                                                }
                                                                                            }
                                                                                        } else {
                                                                                            if (schemaString.contains("(" + entry.getKey() + ":")) { //Update schema
                                                                                                schemaString = schemaString.replace("(" + entry.getKey() + ":", "(" + properAliasName + ":");
                                                                                                System.out.println("Schema becomes: " + schemaString);
                                                                                            } else {
                                                                                                if (schemaString.contains("(" + entry.getKey() + ":")) { //Update schema
                                                                                                    schemaString = schemaString.replace("(" + entry.getKey() + ":", "(" + properAliasName + ":");
                                                                                                    System.out.println("Schema becomes: " + schemaString);
                                                                                                }
                                                                                            }
                                                                                        }

                                                                                        break;
                                                                                    }
                                                                                }
                                                                            }

                                                                            if(parameterLocated) break;
                                                                        }
                                                                    }

                                                                    if(parameterLocated) break;
                                                                }

                                                                if(parameterLocated == false){
                                                                    properAliasName = actualCastName;
                                                                    matchFound = true;
                                                                    locatedOutOfRegistry = true;
                                                                    addNewCastPair(currentOperatorNode.getOperatorName(), pair.getColumnName(), pair.getColumnType(), pair.getLatestAltCastType(), entry.getKey(), "casteddecimal_"+currentOperatorNode.getOperatorName()+"_"+pair.getColumnName(), "cast( "+pair.getColumnName() + " as decimal(10,5) )");
                                                                }

                                                                if(matchFound) break;

                                                            }
                                                        }
                                                    }
                                                }
                                            }

                                            if(matchFound) break;
                                        }
                                    }
                                }

                                if(locatedOutOfRegistry == false) {
                                    matchFound = true;
                                    if (genericUDFBridge) {
                                        if (possibleType != "") {
                                            pair.addCastType(possibleType);
                                            if (castToDecimal) {
                                                addNewCastPair(currentOperatorNode.getOperatorName(), properAliasName, pair.getColumnType(), possibleType, entry.getKey(), "casteddecimal_" + currentOperatorNode.getOperatorName() + "_" + pair.getColumnName(), "cast( " + properAliasName + " as decimal(10,5) )");
                                                System.out.println("OldColumnName: " + oldColumnName + " - New ColumnName: " + entry.getKey() + " with Real Alias: " + pair.getColumnName() + " has castExpr: " + "cast( " + pair.getColumnName() + " as decimal(10,5) )");
                                            }
                                        }
                                    }

                                    if (tempMap.getColumnAndTypeList().size() > 1) {
                                        if (schemaString.contains("," + entry.getKey() + ":")) { //Update schema
                                            schemaString = schemaString.replace("," + entry.getKey() + ":", "," + pair.getColumnName() + ":");
                                            System.out.println("Schema becomes: " + schemaString);
                                        } else {
                                            if (schemaString.contains("(" + entry.getKey() + ":")) { //Update schema
                                                schemaString = schemaString.replace("(" + entry.getKey() + ":", "(" + pair.getColumnName() + ":");
                                                System.out.println("Schema becomes: " + schemaString);
                                            }
                                        }
                                    } else {
                                        if (schemaString.contains("(" + entry.getKey() + ":")) { //Update schema
                                            schemaString = schemaString.replace("(" + entry.getKey() + ":", "(" + pair.getColumnName() + ":");
                                            System.out.println("Schema becomes: " + schemaString);
                                        } else {
                                            if (schemaString.contains("(" + entry.getKey() + ":")) { //Update schema
                                                schemaString = schemaString.replace("(" + entry.getKey() + ":", "(" + pair.getColumnName() + ":");
                                                System.out.println("Schema becomes: " + schemaString);
                                            }
                                        }
                                    }
                                    if (existsAgainInValues) {
                                        if (currentEntryValue == 0) {
                                            pair.modifyAltAlias(currentOperatorNode.getOperatorName(), oldColumnName, entry.getKey(), false); //Modify and bring new alias
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": SPECIAL CASE: Key: " + oldColumnName + " is mapped to many Entries so the we will have two modifications for this Node: " + currentOperatorNode.getOperator());
                                            currentEntryValue++;
                                            bannedColumnList.add(pair.getColumnName());
                                        } else {
                                            currentEntryValue++;
                                            pair.modifyAltAlias(currentOperatorNode.getOperatorName(), oldColumnName, entry.getKey(), false); //Modify and bring new alias
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": (old Alias)= " + oldColumnName + " matched with (new Alias)=" + entry.getKey() + " for Operator= " + currentOperatorNode.getOperatorName() + " through fatherNode= " + fatherOperatorNode.getOperatorName());
                                        }
                                    } else {
                                        pair.modifyAltAlias(currentOperatorNode.getOperatorName(), oldColumnName, entry.getKey(), false); //Modify and bring new alias
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": (old Alias)= " + oldColumnName + " matched with (new Alias)=" + entry.getKey() + " for Operator= " + currentOperatorNode.getOperatorName() + " through fatherNode= " + fatherOperatorNode.getOperatorName());
                                    }
                                }

                                break;
                            }
                        }
                        else if((fatherOperatorNode2 != null) && (sP.getParemeterType().equals(fatherOperatorNode2.getOperatorName())) ) {
                            if(sP.getValue().equals(oldColumnName)){ //Column names match

                                String properAliasName = pair.getColumnName();

                                boolean locatedOutOfRegistry = false;
                                if(checkForCast && (genericUDFBridge == false)){
                                    System.out.println("addNewPossibleAlias: Since we have cast type let's check out of registry...");
                                    for(Map.Entry<String, MyMap> castEntry : operatorCastMap.entrySet()){ //Check for cast in cast map
                                        if(castEntry.getKey().equals(fatherOperatorNode2.getOperatorName())) {
                                            for (ColumnTypePair castPair : castEntry.getValue().getColumnAndTypeList()) {
                                                if(castPair.getColumnName().equals(pair.getColumnName())){
                                                    if(castPair.getColumnType().equals(pair.getColumnType())){
                                                        if(castPair.getLatestAltCastType().equals(pair.getLatestAltCastType())){
                                                            if(castPair.getAltAliasPairs().get(0).getValue().equals(oldColumnName)){
                                                                System.out.println("addNewPossibleAlias: Located corresponding cast column: "+castPair.getAltAliasPairs().get(0).getExtraValue());
                                                                String actualCastName = castPair.getAltAliasPairs().get(0).getExtraValue();
                                                                boolean parameterLocated = false;
                                                                System.out.println("addNewPossibleAlias: Take it one step further! Looking if contained in aggregations...");
                                                                for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                                                                    for(String pValue : aggPair.getParameterValues()){
                                                                        if(pValue.equals(actualCastName)){
                                                                            for(StringParameter altAliasAggr : aggPair.getAltAliasPairs()){
                                                                                if(altAliasAggr.getParemeterType().equals(fatherOperatorNode2.getOperatorName())){
                                                                                    if(altAliasAggr.getValue().equals(oldColumnName)){
                                                                                        System.out.println("addNewPossibleAlias: After all oldColumnName: "+oldColumnName+" is an aggregation! ");
                                                                                        parameterLocated = true;
                                                                                        actualCastName = fetchlatestRequiredAncestorOfAggr(currentOperatorNode, currentOpQuery, aggPair.getColumnName(), aggPair.getAltAliasPairs(), oldColumnName);
                                                                                        matchFound = true;
                                                                                        properAliasName = actualCastName;
                                                                                        locatedOutOfRegistry = true;
                                                                                        aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), oldColumnName, entry.getKey(), true);

                                                                                        if (tempMap.getColumnAndTypeList().size() > 1) {
                                                                                            if (schemaString.contains("," + entry.getKey() + ":")) { //Update schema
                                                                                                schemaString = schemaString.replace("," + entry.getKey() + ":", "," + properAliasName + ":");
                                                                                                System.out.println("Schema becomes: " + schemaString);
                                                                                            } else {
                                                                                                if (schemaString.contains("(" + entry.getKey() + ":")) { //Update schema
                                                                                                    schemaString = schemaString.replace("(" + entry.getKey() + ":", "(" + properAliasName + ":");
                                                                                                    System.out.println("Schema becomes: " + schemaString);
                                                                                                }
                                                                                            }
                                                                                        } else {
                                                                                            if (schemaString.contains("(" + entry.getKey() + ":")) { //Update schema
                                                                                                schemaString = schemaString.replace("(" + entry.getKey() + ":", "(" + properAliasName + ":");
                                                                                                System.out.println("Schema becomes: " + schemaString);
                                                                                            } else {
                                                                                                if (schemaString.contains("(" + entry.getKey() + ":")) { //Update schema
                                                                                                    schemaString = schemaString.replace("(" + entry.getKey() + ":", "(" + properAliasName + ":");
                                                                                                    System.out.println("Schema becomes: " + schemaString);
                                                                                                }
                                                                                            }
                                                                                        }

                                                                                        break;
                                                                                    }
                                                                                }
                                                                            }

                                                                            if(parameterLocated) break;
                                                                        }
                                                                    }

                                                                    if(parameterLocated) break;
                                                                }

                                                                if(parameterLocated == false){
                                                                    properAliasName = actualCastName;
                                                                    matchFound = true;
                                                                    locatedOutOfRegistry = true;
                                                                    addNewCastPair(currentOperatorNode.getOperatorName(), pair.getColumnName(), pair.getColumnType(), pair.getLatestAltCastType(), entry.getKey(), "casteddecimal_"+currentOperatorNode.getOperatorName()+"_"+pair.getColumnName(), "cast( "+pair.getColumnName() + " as decimal(10,5) )");
                                                                }

                                                                if(matchFound) break;

                                                            }
                                                        }
                                                    }
                                                }
                                            }

                                            if(matchFound) break;
                                        }
                                    }
                                }

                                if(locatedOutOfRegistry == false) {
                                    matchFound = true;
                                    if (genericUDFBridge) {
                                        if (possibleType != "") {
                                            pair.addCastType(possibleType);
                                            if (castToDecimal) {
                                                addNewCastPair(currentOperatorNode.getOperatorName(), pair.getColumnName(), pair.getColumnType(), possibleType, entry.getKey(), "casteddecimal_" + currentOperatorNode.getOperatorName() + "_" + pair.getColumnName(), "cast( " + pair.getColumnName() + " as decimal(10,5) )");
                                                System.out.println("OldColumnName: " + oldColumnName + " - New ColumnName: " + entry.getKey() + " with Real Alias: " + pair.getColumnName() + " has castExpr: " + "cast( " + pair.getColumnName() + " as decimal(10,5) )");
                                            }
                                        }
                                    }
                                    if (tempMap.getColumnAndTypeList().size() > 1) {
                                        if (schemaString.contains("," + entry.getKey() + ":")) { //Update schema
                                            schemaString = schemaString.replace("," + entry.getKey() + ":", "," + pair.getColumnName() + ":");
                                            System.out.println("Schema becomes: " + schemaString);
                                        } else {
                                            if (schemaString.contains("(" + entry.getKey() + ":")) { //Update schema
                                                schemaString = schemaString.replace("(" + entry.getKey() + ":", "(" + pair.getColumnName() + ":");
                                                System.out.println("Schema becomes: " + schemaString);
                                            }
                                        }
                                    } else {
                                        if (schemaString.contains("(" + entry.getKey() + ":")) { //Update schema
                                            schemaString = schemaString.replace("(" + entry.getKey() + ":", "(" + pair.getColumnName() + ":");
                                            System.out.println("Schema becomes: " + schemaString);
                                        } else {
                                            if (schemaString.contains("(" + entry.getKey() + ":")) { //Update schema
                                                schemaString = schemaString.replace("(" + entry.getKey() + ":", "(" + pair.getColumnName() + ":");
                                                System.out.println("Schema becomes: " + schemaString);
                                            }
                                        }
                                    }
                                    if (existsAgainInValues) {
                                        if (currentEntryValue == 0) {
                                            pair.modifyAltAlias(currentOperatorNode.getOperatorName(), oldColumnName, entry.getKey(), false); //Modify and bring new alias
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": SPECIAL CASE: Key: " + oldColumnName + " is mapped to many Entries so the we will have two modifications for this Node: " + currentOperatorNode.getOperator());
                                            currentEntryValue++;
                                            bannedColumnList.add(pair.getColumnName());
                                        } else {
                                            currentEntryValue++;
                                            pair.modifyAltAlias(currentOperatorNode.getOperatorName(), oldColumnName, entry.getKey(), false); //Modify and bring new alias
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": (old Alias)= " + oldColumnName + " matched with (new Alias)=" + entry.getKey() + " for Operator= " + currentOperatorNode.getOperatorName() + " through fatherNode= " + fatherOperatorNode2.getOperatorName());
                                        }
                                    } else {
                                        pair.modifyAltAlias(currentOperatorNode.getOperatorName(), oldColumnName, entry.getKey(), false); //Modify and bring new alias
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": (old Alias)= " + oldColumnName + " matched with (new Alias)=" + entry.getKey() + " for Operator= " + currentOperatorNode.getOperatorName() + " through fatherNode= " + fatherOperatorNode2.getOperatorName());
                                    }
                                }

                                break;
                            }
                        }
                    }

                    if(existsAgainInValues){
                        if(matchFound){
                            break;
                        }
                    }

                }

                if(existsAgainInValues){
                    if(matchFound){
                        if(currentEntryValue == 2) {
                            existsAgainInValues = false;
                            currentEntryValue = 0;
                            bannedColumnList.remove(0);
                        }
                        break;
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
                                constPair.modifyAltAlias(currentOperatorNode.getOperatorName(), oldColumnName, entry.getKey(), false); //Modify and bring new alias
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
                                constPair.modifyAltAlias(currentOperatorNode.getOperatorName(), oldColumnName, entry.getKey(), false); //Modify and bring new alias
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
                                if(genericUDFBridge){
                                    if(possibleType != ""){
                                        aggPair.addCastType(possibleType);
                                    }
                                }
                                matchFound = true;
                                aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), altAlias.getValue(), entry.getKey(), true);
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": (old Alias)= "+oldColumnName+" matched with (new Alias)="+entry.getKey()+" for Operator= "+currentOperatorNode.getOperatorName()+" in aggregationMap!");
                                for(ColumnTypePair somePair : tempMap.getColumnAndTypeList()){
                                    if(entry.getKey().equals(somePair.getColumnName())){
                                        if(somePair.getColumnType().equals("struct") == false){
                                            aggPair.addCastType(somePair.getColumnType());
                                            break;
                                        }
                                    }
                                }
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

    public void printCastMap(){

        System.out.println("\n-----------CAST MAP----------");
        if(operatorCastMap.size() > 0){
            for(Map.Entry<String, MyMap> entry : operatorCastMap.entrySet()){
                System.out.println("Operator: "+entry.getKey());
                entry.getValue().printMap();
            }
        }
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
                    pair.addAltAlias(currentOperatorNode.getOperatorName(), wantedAlias, false);
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
                pair.addAltAlias(currentOperatorNode.getOperatorName(), wantedAlias, true);
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
                pair.addAltAlias(currentOperatorNode.getOperatorName(), wantedAlias, false);
            }

        }

        //printCastMap();
        //The same for CastMap
        for(Map.Entry<String, MyMap> entry : operatorCastMap.entrySet()){
            if(entry.getKey().equals(fatherOperatorNode.getOperatorName())){
                System.out.println("addNewAliasesBasedOnFather: Father exists in operatorCastMap...");
                String newEntryName = currentOperatorNode.getOperatorName();
                MyMap newMap = new MyMap(true);
                for(ColumnTypePair oldPair : entry.getValue().getColumnAndTypeList()){
                    ColumnTypePair newPair = new ColumnTypePair(oldPair.getColumnName(), oldPair.getColumnType());
                    newPair.addCastType(oldPair.getLatestAltCastType());
                    newPair.addAltAlias(currentOperatorNode.getOperatorName(), oldPair.getAltAliasPairs().get(0).getValue(), true);
                    newPair.getAltAliasPairs().get(0).setCastExpr(oldPair.getAltAliasPairs().get(0).getCastExpr());
                    newPair.getAltAliasPairs().get(0).setExtraValue(oldPair.getAltAliasPairs().get(0).getExtraValue());
                    newMap.addPair(newPair);
                }
                boolean alreadyExists = false;
                for(Map.Entry<String, MyMap> entry2 : operatorCastMap.entrySet()){
                    if(entry2.getKey().equals(currentOperatorNode.getOperatorName())){
                        alreadyExists = true;
                        boolean mapChanged = false;
                        System.out.println("Entry: "+entry2.getKey()+" already exists...add only possible new aliases...");
                        List<ColumnTypePair> additionsToMake = new LinkedList<>();
                        for(ColumnTypePair cP : entry2.getValue().getColumnAndTypeList()){
                            boolean colExists = false;

                            for(ColumnTypePair newPair : newMap.getColumnAndTypeList()){
                                if(newPair.getColumnName().equals(cP.getColumnName())){
                                    if(newPair.getColumnType().equals(cP.getColumnType())){
                                        if(newPair.getAltAliasPairs().get(0).getExtraValue().equals(cP.getAltAliasPairs().get(0).getExtraValue()) == false){
                                            additionsToMake.add(newPair);
                                            colExists = true;
                                        }
                                        else{
                                            colExists = true;
                                        }
                                    }
                                }

                                if(colExists == false){
                                    additionsToMake.add(newPair);
                                }

                            }
                        }

                        if(additionsToMake.size() > 0){
                            for(ColumnTypePair nP : additionsToMake){
                                entry2.getValue().addPair(nP);
                            }

                            break;
                        }


                    }
                }
                if(alreadyExists == false) {
                    //System.out.println("blooooo");
                    operatorCastMap.put(newEntryName, newMap);
                    break;
                    //System.out.println("bloooao");
                }
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

                            String neededColsSchema = "";

                            System.out.println(rootNode.getOperator().getOperatorId() + ": Adding needed columns as select Columns if possible...");
                            if (neededColumns.size() > 0) {
                                List<FieldSchema> neededFields = new LinkedList<>();
                                MyMap tempMap = new MyMap(false);
                                String tempSchema = extractColsFromTypeName(rootNode.getOperator().getSchema().toString(), tempMap, rootNode.getOperator().getSchema().toString(), false);
                                String expression = "";
                                neededColsSchema = "(";
                                int i = 0;
                                for (i = 0; i < neededColumns.size(); i++) {
                                    if (i == neededColumns.size() - 1) {
                                        expression = expression.concat(" " + neededColumns.get(i));
                                        String type = "";
                                        for(ColumnTypePair tempPair : tempMap.getColumnAndTypeList()){
                                            if(tempPair.getColumnName().equals(neededColumns.get(i))){
                                                type = tempPair.getColumnType();
                                            }
                                        }
                                        neededColsSchema = neededColsSchema + neededColumns.get(i) + ": " + type + ")";

                                    } else {
                                        expression = expression.concat(" " + neededColumns.get(i) + ",");
                                        String type = "";
                                        for(ColumnTypePair tempPair : tempMap.getColumnAndTypeList()){
                                            if(tempPair.getColumnName().equals(neededColumns.get(i))){
                                                type = tempPair.getColumnType();
                                            }
                                        }
                                        neededColsSchema = neededColsSchema + neededColumns.get(i) + ": " + type + ",";
                                    }
                                }

                                /*---Locate new USED columns---*/
                                for (String n : neededColumns) {
                                    opQuery.addUsedColumn(n, tableScanDesc.getAlias());
                                    System.out.println(rootNode.getOperatorName()+": USED Column Addition: ("+n+" , "+tableScanDesc.getAlias()+")");
                                }

                                String extraAlias = "";
                                if(tableRegistry.getEntries().get(0).getAlias().equals(tableRegistry.getEntries().get(0).getAssociatedTable().getTableName()) == false){
                                    extraAlias = tableRegistry.getEntries().get(0).getAlias();
                                }

                                opQuery.setLocalQueryString(" select " + expression + " from " + inputTables.get(0).getTableName().toLowerCase() + " " + extraAlias + " ");
                                opQuery.setExaremeQueryString(" select " + expression + " from " + inputTables.get(0).getTableName().toLowerCase() + " " + extraAlias + " ");

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
                            goToChildOperator(targetChildNode, rootNode, opQuery, neededColsSchema, opQuery.getInputTables().get(0).getTableName().toLowerCase(), null, null);

                            System.out.println(rootNode.getOperator().getOperatorId() + ": Returned from Child...");
                        }
                        else if(child instanceof SelectOperator) {

                            System.out.println(rootNode.getOperator().getOperatorId() + ": TS--->SEL connection discovered! Child is a SelectOperator!");

                            String neededColsSchema = "";
                            System.out.println(rootNode.getOperator().getOperatorId() + ": Adding needed columns as select Columns if possible...");
                            if (neededColumns.size() > 0) {
                                List<FieldSchema> neededFields = new LinkedList<>();
                                MyMap tempMap = new MyMap(false);
                                String tempSchema = extractColsFromTypeName(rootNode.getOperator().getSchema().toString(), tempMap, rootNode.getOperator().getSchema().toString(), false);
                                String expression = "";
                                neededColsSchema = "(";
                                int i = 0;
                                for (i = 0; i < neededColumns.size(); i++) {
                                    if (i == neededColumns.size() - 1) {
                                        expression = expression.concat(" " + neededColumns.get(i));
                                        String type = "";
                                        for(ColumnTypePair tempPair : tempMap.getColumnAndTypeList()){
                                            if(tempPair.getColumnName().equals(neededColumns.get(i))){
                                                type = tempPair.getColumnType();
                                            }
                                        }
                                        neededColsSchema = neededColsSchema + neededColumns.get(i) + ": " + type + ")";

                                    } else {
                                        expression = expression.concat(" " + neededColumns.get(i) + ",");
                                        String type = "";
                                        for(ColumnTypePair tempPair : tempMap.getColumnAndTypeList()){
                                            if(tempPair.getColumnName().equals(neededColumns.get(i))){
                                                type = tempPair.getColumnType();
                                            }
                                        }
                                        neededColsSchema = neededColsSchema + neededColumns.get(i) + ": " + type + ",";
                                    }
                                }

                                /*---Locate new USED columns---*/
                                for (String n : neededColumns) {
                                    opQuery.addUsedColumn(n, tableScanDesc.getAlias());
                                    System.out.println(rootNode.getOperatorName()+": USED Column Addition: ("+n+" , "+tableScanDesc.getAlias()+")");
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
                            goToChildOperator(targetChildNode, rootNode, opQuery, neededColsSchema, opQuery.getInputTables().get(0).getTableName().toLowerCase(), null, null);

                            System.out.println(rootNode.getOperator().getOperatorId() + ": Returned from Child...");
                        }
                        else if(child instanceof MapJoinOperator){
                            System.out.println(rootNode.getOperator().getOperatorId()+": Discovered TS-->MAP_JOIN connection!");

                            System.out.println(rootNode.getOperator().getOperatorId()+": Current Query will be ending here...");

                            String updatedSchemaString = rootNode.getOperator().getSchema().toString();

                            if(neededColumns.size() == 0){
                                System.out.println("TS BEFORE MAPJOIN HAS NEEDED COLUMNS == 0");
                                System.exit(0);
                            }
                            List<FieldSchema> neededFields = new LinkedList<>();
                            MyMap tempMap = new MyMap(false);
                            String tempSchema = extractColsFromTypeName(rootNode.getOperator().getSchema().toString(), tempMap, rootNode.getOperator().getSchema().toString(), false);
                            String expression = "";
                            updatedSchemaString = "(";
                            int i = 0;
                            for (i = 0; i < neededColumns.size(); i++) {
                                if (i == neededColumns.size() - 1) {
                                    expression = expression.concat(" " + neededColumns.get(i));
                                    String type = "";
                                    for(ColumnTypePair tempPair : tempMap.getColumnAndTypeList()){
                                        if(tempPair.getColumnName().equals(neededColumns.get(i))){
                                            type = tempPair.getColumnType();
                                        }
                                    }
                                    updatedSchemaString = updatedSchemaString + neededColumns.get(i) + ": " + type + ")";

                                } else {
                                    expression = expression.concat(" " + neededColumns.get(i) + ",");
                                    String type = "";
                                    for(ColumnTypePair tempPair : tempMap.getColumnAndTypeList()){
                                        if(tempPair.getColumnName().equals(neededColumns.get(i))){
                                            type = tempPair.getColumnType();
                                        }
                                    }
                                    updatedSchemaString = updatedSchemaString + neededColumns.get(i) + ": " + type + ",";
                                }
                            }

                            /*---Locate new USED columns---*/
                            for (String n : neededColumns) {
                                opQuery.addUsedColumn(n, tableScanDesc.getAlias());
                                System.out.println(rootNode.getOperatorName()+": USED Column Addition: ("+n+" , "+tableScanDesc.getAlias()+")");
                            }

                            String extraAlias = "";
                            if(tableRegistry.getEntries().get(0).getAlias().equals(tableRegistry.getEntries().get(0).getAssociatedTable().getTableName()) == false){
                                extraAlias = tableRegistry.getEntries().get(0).getAlias();
                            }

                            opQuery.setLocalQueryString(" select " + expression + " from " + inputTables.get(0).getTableName().toLowerCase() + " " + extraAlias + " ");
                            opQuery.setExaremeQueryString(" select " + expression + " from " + inputTables.get(0).getTableName().toLowerCase() + " " + extraAlias + " ");

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

                                        goToChildOperator(childNode, rootNode, associatedQuery, updatedSchemaString, rootNode.getOperatorName().toLowerCase(), latestAncestorTableName2, secondFather);

                                        return;
                                    }
                                }

                                if(joinPExists == false){
                                    System.out.println(rootNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                                    createMapJoinPoint(rootNode, opQuery, child, rootNode.getOperatorName().toLowerCase(), null);

                                    return;
                                }
                            }
                            else{ //No JoinPoint Exists
                                System.out.println(rootNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                                createMapJoinPoint(rootNode, opQuery, child, rootNode.getOperatorName().toLowerCase(), null);

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
                                String neededColsSchema = "";
                                System.out.println(root.getOperator().getOperatorId() + ": Adding needed columns as select Columns if possible...");
                                if (neededColumns.size() > 0) {
                                    List<FieldSchema> neededFields = new LinkedList<>();
                                    MyMap tempMap = new MyMap(false);
                                    String tempSchema = extractColsFromTypeName(root.getOperator().getSchema().toString(), tempMap, root.getOperator().getSchema().toString(), false);
                                    String expression = "";
                                    neededColsSchema = "(";
                                    int i = 0;
                                    for (i = 0; i < neededColumns.size(); i++) {
                                        if (i == neededColumns.size() - 1) {
                                            expression = expression.concat(" " + neededColumns.get(i));
                                            String type = "";
                                            for(ColumnTypePair tempPair : tempMap.getColumnAndTypeList()){
                                                if(tempPair.getColumnName().equals(neededColumns.get(i))){
                                                    type = tempPair.getColumnType();
                                                }
                                            }
                                            neededColsSchema = neededColsSchema + neededColumns.get(i) + ": " + type + ")";

                                        } else {
                                            expression = expression.concat(" " + neededColumns.get(i) + ",");
                                            String type = "";
                                            for(ColumnTypePair tempPair : tempMap.getColumnAndTypeList()){
                                                if(tempPair.getColumnName().equals(neededColumns.get(i))){
                                                    type = tempPair.getColumnType();
                                                }
                                            }
                                            neededColsSchema = neededColsSchema + neededColumns.get(i) + ": " + type + ",";
                                        }
                                    }

                                    /*---Locate new USED columns---*/
                                    for (String n : neededColumns) {
                                        opQuery.addUsedColumn(n, inputTable.getTableName());
                                        System.out.println(root.getOperatorName()+": USED Column Addition: ("+n+" , "+inputTable.getTableName()+")");
                                    }

                                    String extraAlias = "";
                                    if(tableDesc.getAlias() != null){
                                        if(tableDesc.getAlias().equals(inputTable.getTableName()) == false){
                                            extraAlias = tableDesc.getAlias();
                                        }
                                    }

                                    opQuery.setLocalQueryString(" select " + expression + " from " + inputTable.getTableName().toLowerCase() + " " + extraAlias + " ");
                                    opQuery.setExaremeQueryString(" select " + expression + " from " + inputTable.getTableName().toLowerCase() + " " + extraAlias + " ");


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
                                goToChildOperator(targetChildNode, root, opQuery, neededColsSchema, inputTable.getTableName().toLowerCase(), null, null);

                                System.out.println(root.getOperator().getOperatorId() + ": Returned from Child...");
                            }
                            else if(child instanceof SelectOperator) {

                                System.out.println(root.getOperator().getOperatorId() + ": TS--->SEL connection discovered! Child is a SelectOperator!");

                                String neededColsSchema = "";
                                System.out.println(root.getOperator().getOperatorId() + ": Adding needed columns as select Columns if possible...");
                                if (neededColumns.size() > 0) {
                                    List<FieldSchema> neededFields = new LinkedList<>();
                                    MyMap tempMap = new MyMap(false);
                                    String tempSchema = extractColsFromTypeName(root.getOperator().getSchema().toString(), tempMap, root.getOperator().getSchema().toString(), false);
                                    String expression = "";
                                    neededColsSchema = "(";
                                    int i = 0;
                                    for (i = 0; i < neededColumns.size(); i++) {
                                        if (i == neededColumns.size() - 1) {
                                            expression = expression.concat(" " + neededColumns.get(i));
                                            String type = "";
                                            for(ColumnTypePair tempPair : tempMap.getColumnAndTypeList()){
                                                if(tempPair.getColumnName().equals(neededColumns.get(i))){
                                                    type = tempPair.getColumnType();
                                                }
                                            }
                                            neededColsSchema = neededColsSchema + neededColumns.get(i) + ": " + type + ")";

                                        } else {
                                            expression = expression.concat(" " + neededColumns.get(i) + ",");
                                            String type = "";
                                            for(ColumnTypePair tempPair : tempMap.getColumnAndTypeList()){
                                                if(tempPair.getColumnName().equals(neededColumns.get(i))){
                                                    type = tempPair.getColumnType();
                                                }
                                            }
                                            neededColsSchema = neededColsSchema + neededColumns.get(i) + ": " + type + ",";
                                        }
                                    }

                                /*---Locate new USED columns---*/
                                    for (String n : neededColumns) {
                                        opQuery.addUsedColumn(n, tableScanDesc.getAlias());
                                        System.out.println(root.getOperatorName()+": USED Column Addition: ("+n+" , "+tableScanDesc.getAlias()+")");
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
                                goToChildOperator(targetChildNode, root, opQuery, neededColsSchema, opQuery.getInputTables().get(0).getTableName().toLowerCase(), null, null);

                                System.out.println(root.getOperator().getOperatorId() + ": Returned from Child...");
                            }
                            else if(child instanceof MapJoinOperator){
                                System.out.println(root.getOperator().getOperatorId()+": Discovered TS-->MAP_JOIN connection!");

                                System.out.println(root.getOperator().getOperatorId()+": Current Query will be ending here...");

                                String updatedSchemaString = root.getOperator().getSchema().toString();

                                if(neededColumns.size() == 0){
                                    System.out.println("TS--->MAPJOIN NEEDED COLS SIZE == 0");
                                    System.exit(0);
                                }
                                String neededColsSchema = "";
                                List<FieldSchema> neededFields = new LinkedList<>();
                                MyMap tempMap = new MyMap(false);
                                String tempSchema = extractColsFromTypeName(root.getOperator().getSchema().toString(), tempMap, root.getOperator().getSchema().toString(), false);
                                String expression = "";
                                neededColsSchema = "(";
                                int i = 0;
                                for (i = 0; i < neededColumns.size(); i++) {
                                    if (i == neededColumns.size() - 1) {
                                        expression = expression.concat(" " + neededColumns.get(i));
                                        String type = "";
                                        for(ColumnTypePair tempPair : tempMap.getColumnAndTypeList()){
                                            if(tempPair.getColumnName().equals(neededColumns.get(i))){
                                                type = tempPair.getColumnType();
                                            }
                                        }
                                        neededColsSchema = neededColsSchema + neededColumns.get(i) + ": " + type + ")";

                                    } else {
                                        expression = expression.concat(" " + neededColumns.get(i) + ",");
                                        String type = "";
                                        for(ColumnTypePair tempPair : tempMap.getColumnAndTypeList()){
                                            if(tempPair.getColumnName().equals(neededColumns.get(i))){
                                                type = tempPair.getColumnType();
                                            }
                                        }
                                        neededColsSchema = neededColsSchema + neededColumns.get(i) + ": " + type + ",";
                                    }
                                }

                            /*---Locate new USED columns---*/
                                for (String n : neededColumns) {
                                    opQuery.addUsedColumn(n, tableScanDesc.getAlias());
                                    System.out.println(root.getOperatorName()+": USED Column Addition: ("+n+" , "+tableScanDesc.getAlias()+")");
                                }

                                String extraAlias = "";
                                if(tableDesc.getAlias() != null){
                                    if(tableDesc.getAlias().equals(inputTable.getTableName()) == false){
                                        extraAlias = tableDesc.getAlias();
                                    }
                                }

                                opQuery.setLocalQueryString(" select " + expression + " from " + inputTable.getTableName().toLowerCase() + " " + extraAlias + " ");
                                opQuery.setExaremeQueryString(" select " + expression + " from " + inputTable.getTableName().toLowerCase() + " " + extraAlias + " ");

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

                                            goToChildOperator(childNode, root, associatedQuery, neededColsSchema, root.getOperator().getOperatorId().toLowerCase(), latestAncestorTableName2, secondFather);

                                            return;
                                        }
                                    }

                                    if(joinPExists == false){
                                        System.out.println(root.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                                        createMapJoinPoint(root, opQuery, child, root.getOperatorName().toLowerCase(), null);

                                        return;
                                    }
                                }
                                else{ //No JoinPoint Exists
                                    System.out.println(root.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");
                                    createMapJoinPoint(root, opQuery, child, root.getOperatorName().toLowerCase(), null);

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
            if((fatherOperatorNode.getOperator() instanceof TableScanOperator) || (fatherOperatorNode.getOperator() instanceof JoinOperator) || (fatherOperatorNode.getOperator() instanceof MapJoinOperator) || (fatherOperatorNode.getOperator() instanceof SelectOperator) || (fatherOperatorNode.getOperator() instanceof GroupByOperator) ){
                if(fatherOperatorNode.getOperator() instanceof TableScanOperator)
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a TableScanOperator: "+fatherOperatorNode.getOperatorName());
                else if(fatherOperatorNode.getOperator() instanceof JoinOperator)
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a JoinOperator: "+fatherOperatorNode.getOperatorName());
                else if(fatherOperatorNode.getOperator() instanceof SelectOperator)
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a SelectOperator: "+fatherOperatorNode.getOperatorName());
                else if(fatherOperatorNode.getOperator() instanceof GroupByOperator)
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a GroupByOperator: "+fatherOperatorNode.getOperatorName());
                else
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a MapJoinOperator: "+fatherOperatorNode.getOperatorName());

                String updatedSchemaString = currentOperatorNode.getOperator().getSchema().toString();
                if(currentOpQuery.getLocalQueryString().contains("select")) {
                    if(fatherOperatorNode.getOperator() instanceof TableScanOperator)
                        updatedSchemaString = latestAncestorSchema;
                    else
                        updatedSchemaString = currentOperatorNode.getOperator().getSchema().toString();
                }
                else{
                        updatedSchemaString = latestAncestorSchema;
                    }

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
                    updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);
                }

                if(fatherOperatorNode.getOperator() instanceof TableScanOperator) {
                    /*----Extracting predicate of FilterOp----*/
                    if ((fatherOperatorNode.getOperator().getParentOperators() == null) || (fatherOperatorNode.getOperator().getParentOperators().size() == 0)) {
                        /*---Add where statement to Query---*/
                        addWhereStatementToQuery(currentOperatorNode, currentOpQuery, latestAncestorTableName1, latestAncestorTableName2);
                    } else {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Father TableScanOperator is not ROOT! Unsupported yet!");
                        System.exit(0);
                    }

                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a not a root TableScan! Use ancestral schema to add Columns and transform current Schema!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Ancestral Schema: "+latestAncestorSchema);

                    /*---Add where statement to Query---*/
                    addWhereStatementToQuery2(currentOperatorNode, fatherOperatorNode, currentOpQuery, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2);

                }

                /*---Check Type of Child---*/
                List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
                if(children != null){
                    if(children.size() == 1){
                        Operator<?> child = children.get(0);
                        if(child instanceof SelectOperator) {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered FIL--->SEL Connection! Child is select: " + child.getOperatorId());
                            if ( (fatherOperatorNode.getOperator() instanceof JoinOperator) || ( fatherOperatorNode.getOperator() instanceof MapJoinOperator) || (fatherOperatorNode.getOperator() instanceof GroupByOperator)) {
                                if (currentOpQuery.getLocalQueryString().contains("select ") == false) {
                                    latestAncestorSchema = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, null, latestAncestorSchema, currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);
                                }
                            }

                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Current Query will be ending here...");

                            if ( (fatherOperatorNode.getOperator() instanceof TableScanOperator) || ( fatherOperatorNode.getOperator() instanceof FilterOperator) ){

                                /*---Finalising outputTable---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");
                                MyTable outputTable = new MyTable();
                                outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                                outputTable.setIsAFile(false);
                                List<FieldSchema> newCols = new LinkedList<>();

                                String newSchemaString = addNewPossibleAliases(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, null, currentOpQuery);

                                newSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, newSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestAncestorTableName1, latestAncestorTableName2, null);


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
                                goToChildOperator(childNode, currentOperatorNode, opQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), null, null);
                            }
                            else if( (fatherOperatorNode.getOperator() instanceof JoinOperator) || ( fatherOperatorNode.getOperator() instanceof MapJoinOperator) || ( fatherOperatorNode.getOperator() instanceof GroupByOperator)){ //This differs from above case because the above case refers to ROOT TableScan ending

                                /*---Finalising outputTable---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");
                                MyTable outputTable = new MyTable();
                                outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                                outputTable.setIsAFile(false);
                                List<FieldSchema> newCols = new LinkedList<>();

                                updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestAncestorTableName1, latestAncestorTableName2, null);

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
                                goToChildOperator(childNode, currentOperatorNode, opQuery, updatedSchemaString, currentOperatorNode.getOperatorName().toLowerCase(), latestAncestorTableName2, null);

                            }
                            else {
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Unsupported type of father for ending Query!");
                                System.exit(0);
                            }


                        }
                        else if(child instanceof GroupByOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered FIL--->GBY Connection! Child is select: " + child.getOperatorId());
                            if(currentOpQuery.getLocalQueryString().contains("select ") == false) {
                                updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);
                            }

                             /*---Moving to Child Operator---*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Moving to Child: " + child.getOperatorId());
                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);

                        }
                        else if(child instanceof ReduceSinkOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered FIL--->RS Connection! Child is select: " + child.getOperatorId());
                            if (fatherOperatorNode.getOperator() instanceof JoinOperator) {
                                if (currentOpQuery.getLocalQueryString().contains("select ") == false) {
                                    updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);
                                }
                            }

                             /*---Moving to Child Operator---*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Moving to Child: " + child.getOperatorId());
                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);

                        }
                        else if(child instanceof HashTableSinkOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered FIL--->HASH Connection! Child is select: " + child.getOperatorId());

                            if (currentOpQuery.getLocalQueryString().contains("select ") == false) {
                                updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);
                            }

                             /*---Moving to Child Operator---*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Moving to Child: " + child.getOperatorId());
                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);

                        }
                        else if(child instanceof FileSinkOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered FIL--->FS Connection! Child is : "+child.getOperatorId());
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child...");
                            OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                            /*---Move to Child---*/
                            goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);
                        }
                        else if(child instanceof MapJoinOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered FIL--->MAP_JOIN Connection! Child is select: " + child.getOperatorId());

                            if (currentOpQuery.getLocalQueryString().contains("select ") == false) {
                                updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);
                            }

                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

                            /*---Finalising outputTable---*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");

                            MyTable outputTable = new MyTable();
                            outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                            outputTable.setIsAFile(false);
                            List<FieldSchema> newCols = new LinkedList<>();

                            updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestAncestorTableName1, latestAncestorTableName2, null);

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

                                    createMapJoinPoint(currentOperatorNode, currentOpQuery, child, currentOpQuery.getOutputTable().getTableName().toLowerCase(), latestAncestorTableName2);

                                    return;
                                }
                            }
                            else{ //No JoinPoint Exists
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                                createMapJoinPoint(currentOperatorNode, currentOpQuery, child, currentOpQuery.getOutputTable().getTableName().toLowerCase(), latestAncestorTableName2);

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
                                    updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);
                                }

                                /*---Finalising outputTable---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");

                                MyTable outputTable = new MyTable();
                                outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                                outputTable.setIsAFile(false);
                                List<FieldSchema> newCols = new LinkedList<>();

                                updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestAncestorTableName1, latestAncestorTableName2, null);

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
                                    updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);
                                }

                                /*---Finalising outputTable---*/
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");

                                MyTable outputTable = new MyTable();
                                outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                                outputTable.setIsAFile(false);
                                List<FieldSchema> newCols = new LinkedList<>();

                                updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols ,latestAncestorTableName1, latestAncestorTableName2, null);

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

                                        createMapJoinPoint(currentOperatorNode, currentOpQuery, child, currentOpQuery.getOutputTable().getTableName().toLowerCase(), latestAncestorTableName2);

                                        return;
                                    }
                                }
                                else{ //No JoinPoint Exists
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                                    createMapJoinPoint(currentOperatorNode, currentOpQuery, child, currentOpQuery.getOutputTable().getTableName().toLowerCase(), latestAncestorTableName2);

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
                String updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);

                if(currentOpQuery.getLocalQueryString().contains("select ") == false) {
                    updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);
                }

                List<FieldSchema> selectCols = addPurgeSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);

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
                        else if(child instanceof UnionOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered SEL-->UNION connection!");

                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

                            /*---Finalising outputTable---*/
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");

                            MyTable outputTable = new MyTable();
                            outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                            outputTable.setIsAFile(false);
                            List<FieldSchema> newCols = new LinkedList<>();

                            updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestAncestorTableName1, latestAncestorTableName2, null);

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
                        else if(child instanceof FilterOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered SEL--->FIL connection!");
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Moving to child operator...");
                            OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

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
                String updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);

                if(currentOpQuery.getLocalQueryString().contains("select ") == false) {
                    updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);
                }

                List<FieldSchema> selectCols = addPurgeSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);

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
                        else if(child instanceof GroupByOperator){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered SEL--->GBY Connection! Child is : "+child.getOperatorId());
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
            else if(fatherOperatorNode.getOperator() instanceof UnionOperator){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is UnionOperator: "+fatherOperatorNode.getOperatorName());
                groupByHasFatherUnion(currentOperatorNode, currentOpQuery, fatherOperatorNode, otherFatherNode, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2);

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
            if(( (fatherOperatorNode.getOperator() instanceof HashTableSinkOperator) || (fatherOperatorNode.getOperator() instanceof FilterOperator) || (fatherOperatorNode.getOperator() instanceof MapJoinOperator) | (fatherOperatorNode.getOperator() instanceof TableScanOperator) || (fatherOperatorNode.getOperator() instanceof UnionOperator) ) && ( (otherFatherNode.getOperator() instanceof HashTableSinkOperator) || (otherFatherNode.getOperator() instanceof FilterOperator)  || (otherFatherNode.getOperator() instanceof TableScanOperator) || (otherFatherNode.getOperator() instanceof TableScanOperator) || (otherFatherNode.getOperator() instanceof MapJoinOperator) )){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": MapJoinOperator: "+currentOperatorNode.getOperatorName()+" has parents: "+fatherOperatorNode.getOperatorName()+" , "+otherFatherNode.getOperatorName());

                mapJoinHasFatherHashFilterTableScanUnion(currentOperatorNode, currentOpQuery, fatherOperatorNode, otherFatherNode, latestAncestorTableName1, latestAncestorTableName2);

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
            if((fatherOperatorNode.getOperator() instanceof LimitOperator) || (fatherOperatorNode.getOperator() instanceof GroupByOperator) || (fatherOperatorNode.getOperator() instanceof SelectOperator) || (fatherOperatorNode.getOperator() instanceof MapJoinOperator)  || (fatherOperatorNode.getOperator() instanceof JoinOperator) || (fatherOperatorNode.getOperator() instanceof FilterOperator)){
                if(fatherOperatorNode.getOperator() instanceof LimitOperator) {
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Father is a LimitOperator...");
                }
                else if(fatherOperatorNode.getOperator() instanceof GroupByOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Father is a GroupByOperator...");
                }
                else if(fatherOperatorNode.getOperator() instanceof SelectOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Father is a GroupByOperator...");
                }
                else if(fatherOperatorNode.getOperator() instanceof FilterOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Father is a FilterOperator...");
                }
                else if(fatherOperatorNode.getOperator() instanceof JoinOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Father is a JoinOperator...");
                }
                else{
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Father is a MapJoinOperator...");
                }

                fileSinkHasFatherLimitGroupSelectMapFilterJoin(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2);
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
                listSinkHasFatherFileSink(currentOperatorNode, fatherOperatorNode, currentOpQuery, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2);
            }
            else{
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported Type of Parent Operator for FileSinkOperator...");
                System.exit(0);
            }
        }
        else if(currentOperatorNode.getOperator() instanceof UnionOperator){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Operator is a UnionOperator...");
            if( (fatherOperatorNode.getOperator() instanceof TableScanOperator) || (fatherOperatorNode.getOperator() instanceof SelectOperator)){
                if(fatherOperatorNode.getOperator() instanceof TableScanOperator)
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a TableScanSinkOperator...(but UnionOperator has more parent actually)...");
                else if(fatherOperatorNode.getOperator() instanceof SelectOperator)
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Father is a SelectOperator...(but UnionOperator has more parent actually)...");

                unionHasFatherTableScan(currentOperatorNode, fatherOperatorNode, currentOpQuery, latestAncestorSchema, latestAncestorTableName1, latestAncestorTableName2);
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
                    updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);
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
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);
            }
        }

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
        if(currentOpQuery.getLocalQueryString().contains("select ") == false){
            updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, latestAncestorSchema, currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);
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

        updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestAncestorTableName1, latestAncestorTableName2, null);

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

                            createMapJoinPoint(currentOperatorNode, currentOpQuery, child, currentOpQuery.getOutputTable().getTableName().toLowerCase(), null);

                            return;
                        }
                    }
                    else{ //No JoinPoint Exists
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                        createMapJoinPoint(currentOperatorNode, currentOpQuery, child, currentOpQuery.getOutputTable().getTableName().toLowerCase(), null);

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
        String updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
        if(currentOpQuery.getLocalQueryString().contains("select ") == false){
            updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, latestAncestorSchema, currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);
        }

        /*---Add possible Order By Keys---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if ORDER BY exists in this RS...");
        List<FieldSchema> orderByFields = new LinkedList<>();
        discoverOrderByKeys(currentOperatorNode, fatherOperatorNode, currentOpQuery, latestAncestorTableName1, latestAncestorTableName2, orderByFields);

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

        updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestAncestorTableName1, latestAncestorTableName2, orderByFields);

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
                    updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);
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
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);
            }
        }

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
        if(currentOpQuery.getLocalQueryString().contains("select ") == false){
            updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, latestAncestorSchema, currentOpQuery, null , latestAncestorTableName1, latestAncestorTableName2);
        }

        /*---Add possible Order By Keys---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if ORDER BY exists in this RS...");
        List<FieldSchema> orderByFields = new LinkedList<>();
        discoverOrderByKeys(currentOperatorNode, fatherOperatorNode, currentOpQuery, latestAncestorTableName1, latestAncestorTableName2, orderByFields);

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

        updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestAncestorTableName1, latestAncestorTableName2, orderByFields);

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

                            createJoinPoint(currentOperatorNode, currentOpQuery, child, currentOpQuery.getOutputTable().getTableName().toLowerCase());

                            return;
                        }
                    }
                    else{ //No JoinPoint Exists
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                        createJoinPoint(currentOperatorNode, currentOpQuery, child, currentOpQuery.getOutputTable().getTableName().toLowerCase());

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
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);
            }
        }

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
        if(currentOpQuery.getLocalQueryString().contains("select ") == false){
            updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);
        }

        /*---Add possible Order By Keys---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if ORDER BY exists in this RS...");
        List<FieldSchema> orderByFields = new LinkedList<>();
        discoverOrderByKeys(currentOperatorNode, fatherOperatorNode, currentOpQuery, latestAncestorTableName1, latestAncestorTableName2, orderByFields);

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

        /*---Finalising outputTable---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");

        MyTable outputTable = new MyTable();
        outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
        outputTable.setIsAFile(false);
        List<FieldSchema> newCols = new LinkedList<>();

        updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestAncestorTableName1, latestAncestorTableName2, orderByFields);

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

                            createJoinPoint(currentOperatorNode, currentOpQuery, child, currentOpQuery.getOutputTable().getTableName().toLowerCase());

                            return;
                        }
                    }
                    else{ //No JoinPoint Exists
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                        createJoinPoint(currentOperatorNode, currentOpQuery, child, currentOpQuery.getOutputTable().getTableName().toLowerCase());

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
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);
            }
        }

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
        if(currentOpQuery.getLocalQueryString().contains("select ") == false){
            updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);
        }

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

        /*---Finalising outputTable---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");

        MyTable outputTable = new MyTable();
        outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
        outputTable.setIsAFile(false);
        List<FieldSchema> newCols = new LinkedList<>();

        updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestAncestorTableName1, latestAncestorTableName2, null);

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

                            createMapJoinPoint(currentOperatorNode, currentOpQuery, child, currentOpQuery.getOutputTable().getTableName().toLowerCase(), null);

                            return;
                        }
                    }
                    else{ //No MapJoinPoint Exists
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                        createMapJoinPoint(currentOperatorNode, currentOpQuery, child, currentOpQuery.getOutputTable().getTableName().toLowerCase(), null);

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
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);
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
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);
            }

        }

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
        if(currentOpQuery.getLocalQueryString().contains("select ") == false){
            updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);
        }

        /*---Add possible Order By Keys---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if ORDER BY exists in this RS...");
        List<FieldSchema> orderByFields = new LinkedList<>();
        discoverOrderByKeys(currentOperatorNode, fatherOperatorNode, currentOpQuery, latestAncestorTableName1, latestAncestorTableName2, orderByFields);

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

        updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestAncestorTableName1, latestAncestorTableName2, orderByFields);

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

                    goToChildOperator(childNode, currentOperatorNode, opQuery, updatedSchemaString, currentOpQuery.getOutputTable().getTableName().toLowerCase(), latestAncestorTableName2, null);
                }
                else if(child instanceof SelectOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered RS--->SEL connection!");
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

    public void groupByHasFatherSelectFilterJoinReduceSinkMapJoin(OperatorNode currentOperatorNode, OperatorQuery currentOpQuery, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, String latestAncestorSchema, String latestAncestorTableName1, String latestAncestorTableName2) {

        OperatorQuery opQuery = new OperatorQuery();

        /*------------------------First begin a new group by Query-------------------------*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": We will finish the query before adding Group By...");
        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Current query will be ending here...");

        List<FieldSchema> selectCols2 = new LinkedList<>();
        if (currentOpQuery.getLocalQueryString().contains("select ") == false) {
            addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, latestAncestorSchema, currentOpQuery, selectCols2, latestAncestorTableName1, latestAncestorTableName2);
        }

        /*---Finalising outputTable---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Finalising OutputTable...");
        MyTable outputTable = new MyTable();
        outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
        outputTable.setIsAFile(false);
        List<FieldSchema> newCols = new LinkedList<>();

        latestAncestorSchema = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, latestAncestorSchema, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestAncestorTableName1, latestAncestorTableName2, null);

        /*---Finalize local part of Query---*/
        currentOpQuery.setLocalQueryString("create table " + currentOperatorNode.getOperator().getOperatorId().toLowerCase() + " as " + currentOpQuery.getLocalQueryString());

        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + currentOpQuery.getLocalQueryString() + "]");

        /*---Check if OpLink is to be created---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Checking if OpLink can be created...");
        checkForPossibleOpLinks(currentOperatorNode, currentOpQuery, outputTable);

        /*----Adding Finished Query to List----*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Adding OperatorQuery to QueryList...");
        allQueries.add(currentOpQuery);


        /*----Begin a new Query----*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Beginning a New Query...");
        opQuery = new OperatorQuery();
        initialiseNewQuery(currentOpQuery, opQuery, currentOpQuery.getOutputTable());

        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + opQuery.getLocalQueryString() + "]");

        String updatedSchemaString = "";

        /*---------------------------Next, check if aggregators exist (this changes the approach we take)-----------------------*/
        GroupByOperator groupByOp = (GroupByOperator) currentOperatorNode.getOperator();
        GroupByDesc groupByDesc = groupByOp.getConf();

        if(groupByOp == null){
            System.out.println("GroupByDesc is null!");
            System.exit(0);
        }

        if((groupByDesc.getAggregators() != null) && (groupByDesc.getAggregators().size() > 0)){

            boolean newAggregators = false;
            int countNewAggregators = 0;
            for(AggregationDesc aggDesc : groupByDesc.getAggregators()){
                if(aggDesc.getMode().name().equals("PARTIAL1")){
                    countNewAggregators++;
                }
            }

            if(countNewAggregators == groupByDesc.getAggregators().size()){
                newAggregators = true;
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"We have discovered NEW aggregators!!");
            }
            //else if(countNewAggregators == 0){
            //    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"All aggegators have existed before! Locate them!");
            //}
            //else{
            //    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Not all aggregators are PARTIAL1 MODE...ERROR! Unsupported!");
            //    System.exit(0);
            //}

            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" GROUP BY contains aggregations...");
            if(currentOperatorNode.getOperator().getColumnExprMap() != null){
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...but also fixing schema from structs...");
                String tempSchema = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);

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
                                for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                                    for(StringParameter sP : aggPair.getAltAliasPairs()){
                                        if(sP.getParemeterType().equals(currentOperatorNode.getOperatorName())){
                                            if(sP.getValue().equals(somePair.getColumnName())){
                                                aggPair.addCastType("decimal");
                                            }
                                        }
                                    }
                                }
                                System.out.println("Schema now: "+fixedSchema);
                            }
                            else if(somePair.getColumnType().equals("double")){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" Pair: ("+somePair.getColumnName()+", "+somePair.getColumnType()+") will get a struct type instead");
                                for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                                    for(StringParameter sP : aggPair.getAltAliasPairs()){
                                        if(sP.getParemeterType().equals(currentOperatorNode.getOperatorName())){
                                            if(sP.getValue().equals(somePair.getColumnName())){
                                                aggPair.addCastType("double");
                                                somePair.setColumnType("struct");
                                                fixedSchema = fixedSchema.replace("double", "struct");
                                            }
                                        }
                                    }
                                }
                                System.out.println("Schema now: "+fixedSchema);
                            }
                            else{
                                //Might be a const value
                                boolean isConst = false;
                                for(ColumnTypePair constPair : ommitedConstantsMap.getColumnAndTypeList()){
                                    if(constPair.getColumnType().equals(somePair.getColumnType())){
                                        for(StringParameter sP : constPair.getAltAliasPairs()){
                                            if(sP.getParemeterType().equals(currentOperatorNode.getOperatorName())){
                                                if(sP.getValue().equals(somePair.getColumnName())){
                                                    isConst = true;
                                                    break;
                                                }
                                            }
                                        }

                                        if(isConst) break;
                                    }
                                }

                                if(isConst == false) {
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " Pair: (" + somePair.getColumnName() + ", " + somePair.getColumnType() + ") NOT READY TO BE CONVERTED TO STUCT");
                                    System.exit(0);
                                }
                                else{
                                    somePair.setColumnName("ommited_constant_123_from_panos#");
                                }
                            }
                        }
                    }
                }

                /*---Add possible new aggregations---*/

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" Creating subMap with aggregations only...");
                MyMap subMap = new MyMap(true);
                for(ColumnTypePair oldPair : someMap.getColumnAndTypeList()){
                    if(oldPair.getColumnName().contains("_col")) {
                        if (oldPair.getColumnType().contains("struct")) {
                            subMap.addPair(oldPair);
                        }
                        else{
                            if (oldPair.getColumnType().contains("decimal")) {
                                subMap.addPair(oldPair);
                                if(fixedSchema.contains(","+oldPair.getColumnName()+": "+oldPair.getColumnType()+",")) {
                                    fixedSchema = fixedSchema.replace("," + oldPair.getColumnName() + ": " + oldPair.getColumnType() + ",", "," + oldPair.getColumnName() + ": " + "struct,");
                                }
                                else if(fixedSchema.contains("("+oldPair.getColumnName()+": "+oldPair.getColumnType()+",")){
                                    fixedSchema = fixedSchema.replace("(" + oldPair.getColumnName() + ": " + oldPair.getColumnType() + ",", "(" + oldPair.getColumnName() + ": " + "struct,");
                                }
                                else if(fixedSchema.contains("("+oldPair.getColumnName()+": "+oldPair.getColumnType()+")")){
                                    fixedSchema = fixedSchema.replace("(" + oldPair.getColumnName() + ": " + oldPair.getColumnType() + ")", "(" + oldPair.getColumnName() + ": " + "struct)");
                                }
                                else if(fixedSchema.contains(","+oldPair.getColumnName()+": "+oldPair.getColumnType()+")")){
                                    fixedSchema = fixedSchema.replace("," + oldPair.getColumnName() + ": " + oldPair.getColumnType() + ")", "," + oldPair.getColumnName() + ": " + "struct)");
                                }
                            }
                            else if(oldPair.getColumnType().contains("double")){
                                subMap.addPair(oldPair);
                                if(fixedSchema.contains(","+oldPair.getColumnName()+": "+oldPair.getColumnType()+",")) {
                                    fixedSchema = fixedSchema.replace("," + oldPair.getColumnName() + ": " + oldPair.getColumnType() + ",", "," + oldPair.getColumnName() + ": " + "struct,");
                                }
                                else if(fixedSchema.contains("("+oldPair.getColumnName()+": "+oldPair.getColumnType()+",")){
                                    fixedSchema = fixedSchema.replace("(" + oldPair.getColumnName() + ": " + oldPair.getColumnType() + ",", "(" + oldPair.getColumnName() + ": " + "struct,");
                                }
                                else if(fixedSchema.contains("("+oldPair.getColumnName()+": "+oldPair.getColumnType()+")")){
                                    fixedSchema = fixedSchema.replace("(" + oldPair.getColumnName() + ": " + oldPair.getColumnType() + ")", "(" + oldPair.getColumnName() + ": " + "struct)");
                                }
                                else if(fixedSchema.contains(","+oldPair.getColumnName()+": "+oldPair.getColumnType()+")")){
                                    fixedSchema = fixedSchema.replace("," + oldPair.getColumnName() + ": " + oldPair.getColumnType() + ")", "," + oldPair.getColumnName() + ": " + "struct)");
                                }
                            }
                        }
                    }
                }

                ArrayList<AggregationDesc> aggregationDescs = groupByDesc.getAggregators();
                if(subMap.getColumnAndTypeList().size() != aggregationDescs.size()){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" Expected aggregationsDesc to have same size as subMap! Printing subMap for debugging and exitting!");
                    subMap.printMap();
                    System.exit(0);
                }

                List<String> aggregations = new LinkedList<>();
                int j = 0;
                for(AggregationDesc aggDesc : aggregationDescs){ //Get an aggregation
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " Working on aggregation: " + aggDesc.getExprString());

                    String aggregationPhrase = aggDesc.getExprString();

                    if(aggDesc.getMode().name().equals("PARTIAL1") || (aggDesc.getMode().name().equals("COMPLETE"))){ //This will be a new aggregator (Partial means that we will encouter it later - and complete means that it appears once now only

                        //Get Parameters
                        List<String> parameterColNames = new LinkedList<>();
                        if (aggDesc.getParameters() == null) {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " NO parameters for aggregation: " + aggDesc.toString());
                            System.exit(0);
                        }

                        if (aggDesc.getParameters().size() == 0) {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " NO parameters for aggregation: " + aggDesc.toString());
                            System.exit(0);
                        }

                        for (ExprNodeDesc exprNode : aggDesc.getParameters()) {
                            if (exprNode.getCols() == null) {
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " NO parameter cols for aggregation: " + aggDesc.toString());
                                System.exit(0);
                            }

                            if (exprNode.getCols().size() == 0) {
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " NO parameter cols for aggregation: " + aggDesc.toString());
                                System.exit(0);
                            }

                            for (String col : exprNode.getCols()) {
                                parameterColNames.add(col);
                            }

                        }

                        List<String> paramRealNames = new LinkedList<>();
                        List<String> paramRealTypes = new LinkedList<>();

                        //Try to locate the real aliases of the cols through the father
                        for (String paramCol : parameterColNames) {
                            System.out.println("Attempting to locate paramCol: " + paramCol + " in TableRegistry...");
                            boolean columnLocated = false;
                            for (TableRegEntry tableRegEntry : tableRegistry.getEntries()) {
                                String extraAlias = "";
                                if (tableRegEntry.getAlias().equals(tableRegEntry.getAssociatedTable().getTableName()) == false) {
                                    extraAlias = tableRegEntry.getAlias() + ".";
                                }
                                for (ColumnTypePair somePair : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()) {

                                    if (somePair.getColumnName().equals(paramCol)) { //Match with columnName - now look for father
                                        for (StringParameter sP : somePair.getAltAliasPairs()) {
                                            if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) { //Located father

                                                String trueName = extraAlias + somePair.getColumnName();
                                                String trueType = somePair.getColumnType();

                                                boolean hasCastAlias = false;
                                                for(Map.Entry<String, MyMap> castEntry : operatorCastMap.entrySet()){
                                                    if(castEntry.getKey().equals(fatherOperatorNode.getOperatorName())){
                                                        for(ColumnTypePair castPair : castEntry.getValue().getColumnAndTypeList()){
                                                            if(castPair.getColumnName().equals(somePair.getColumnName())){
                                                                if(castPair.getAltAliasPairs().get(0).getValue().equals(paramCol)){
                                                                    trueName = castPair.getAltAliasPairs().get(0).getExtraValue();
                                                                    trueType = castPair.getColumnType();
                                                                    hasCastAlias = true;
                                                                    break;
                                                                }
                                                            }
                                                        }

                                                        if(hasCastAlias) break;
                                                    }
                                                }

                                                System.out.println("Located realAlias: " + trueName);

                                                columnLocated = true;
                                                paramRealNames.add(trueName);
                                                paramRealTypes.add(trueType);
                                                opQuery.addUsedColumn(trueName, currentOperatorNode.getOperatorName().toLowerCase());
                                                System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+trueName+" , "+currentOperatorNode.getOperatorName().toLowerCase()+")");
                                                break;
                                            }
                                        }
                                    } else {
                                        for (StringParameter sP : somePair.getAltAliasPairs()) {
                                            if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) { //Located father
                                                if (sP.getValue().equals(paramCol)) {
                                                    String trueName = extraAlias + somePair.getColumnName();
                                                    String trueType = somePair.getColumnType();

                                                    boolean hasCastAlias = false;
                                                    for(Map.Entry<String, MyMap> castEntry : operatorCastMap.entrySet()){
                                                        if(castEntry.getKey().equals(fatherOperatorNode.getOperatorName())){
                                                            for(ColumnTypePair castPair : castEntry.getValue().getColumnAndTypeList()){
                                                                if(castPair.getColumnName().equals(somePair.getColumnName())){
                                                                    if(castPair.getAltAliasPairs().get(0).getValue().equals(paramCol)){
                                                                        trueName = castPair.getAltAliasPairs().get(0).getExtraValue();
                                                                        trueType = castPair.getColumnType();
                                                                        hasCastAlias = true;
                                                                        break;
                                                                    }
                                                                }
                                                            }

                                                            if(hasCastAlias) break;
                                                        }
                                                    }

                                                    System.out.println("Located realAlias: " + trueName);

                                                    if (aggregationPhrase.contains(paramCol + " ")) {
                                                        aggregationPhrase = aggregationPhrase.replace(paramCol + " ", trueName + " ");
                                                    } else if (aggregationPhrase.contains(paramCol + ")")) {

                                                        aggregationPhrase = aggregationPhrase.replace(paramCol + ")", trueName + ")");
                                                    }

                                                    columnLocated = true;
                                                    paramRealNames.add(trueName);
                                                    paramRealTypes.add(trueType);
                                                    opQuery.addUsedColumn(trueName, currentOperatorNode.getOperatorName().toLowerCase());
                                                    System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+trueName+" , "+currentOperatorNode.getOperatorName().toLowerCase()+")");
                                                    break;

                                                }
                                            }
                                        }
                                    }

                                    if (columnLocated == true) break;
                                }

                                if (columnLocated == true) break;
                            }

                            if (columnLocated == false) { //Extra search
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " Perfoming search in Aggregation Map for: " + aggDesc.toString());
                                for (ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()) {
                                    for (StringParameter sP : aggPair.getAltAliasPairs()) {
                                        if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()) || (sP.getParemeterType().equals(currentOperatorNode.getOperatorName()))) {
                                            if (sP.getValue().equals(paramCol)) {

                                                String properAliasName = "";
                                                properAliasName = fetchlatestRequiredAncestorOfAggr(currentOperatorNode, currentOpQuery, aggPair.getColumnName(), aggPair.getAltAliasPairs(), paramCol);

                                                System.out.println("Real Alias for: " + paramCol + " is: " + properAliasName + " in aggregationMap!");

                                                if (aggregationPhrase.contains(paramCol + " ")) {

                                                    aggregationPhrase = aggregationPhrase.replace(paramCol + " ", properAliasName + " ");
                                                } else if (aggregationPhrase.contains(paramCol + ")")) {

                                                    aggregationPhrase = aggregationPhrase.replace(paramCol + ")", properAliasName + ")");
                                                }

                                                columnLocated = true;

                                                paramRealNames.add(properAliasName);
                                                paramRealTypes.add("struct");

                                                opQuery.addUsedColumn(properAliasName, currentOperatorNode.getOperatorName().toLowerCase());
                                                System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+properAliasName+" , "+currentOperatorNode.getOperatorName().toLowerCase()+")");

                                                break;


                                            }
                                        }
                                    }

                                    if (columnLocated == true) break;
                                }
                            }

                            if (columnLocated == false) {
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " Failed to find real alias for agg parameter col: " + paramCol);
                                System.exit(0);
                            }

                        }

                        boolean newAgg = true;
                        for (String ag : aggregations) {
                            if (ag.equals(aggregationPhrase)) {
                                newAgg = false;
                                break;
                            }
                        }

                        if (newAgg == true) {
                            aggregations.add(aggregationPhrase);

                            if (aggregations.size() == 1) {
                                //Fix non aggreagations aliases
                                System.out.println("Fixing non aggreagation columns...");

                                MyMap anotherTempMap = new MyMap(true);
                                fixedSchema = extractColsFromTypeName(fixedSchema, anotherTempMap, fixedSchema, true);

                                for (ColumnTypePair tempPair : anotherTempMap.getColumnAndTypeList()) {
                                    boolean locatedColumn = false;
                                    if (tempPair.getColumnType().equals("struct") == false) {
                                        System.out.println("Working on alias: " + tempPair.getColumnName() + " - Type: " + tempPair.getColumnType());
                                        for (TableRegEntry tableRegEntry : tableRegistry.getEntries()) {
                                            //System.out.println("Accessing: "+tableRegEntry.getAlias());
                                            for (ColumnTypePair realPair : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()) {
                                                //System.out.println("Accessing: "+realPair.getColumnName()+" - Type: "+realPair.getColumnType());
                                                if (realPair.getColumnType().equals(tempPair.getColumnType())) {
                                                    if (realPair.getColumnName().equals(tempPair.getColumnName())) {
                                                        //System.out.println("is real Alias...");
                                                        for (StringParameter sP : realPair.getAltAliasPairs()) { //Schema contains the latest allies and we have already called addNewPossibleALiases before
                                                            if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                                                //System.out.println("Located father1...");
                                                                if (fixedSchema.contains(tempPair.getColumnName() + ":")) {
                                                                    System.out.println("Alias in schema: " + tempPair.getColumnName() + " becomes: " + realPair.getColumnName());
                                                                    fixedSchema = fixedSchema.replace(tempPair.getColumnName() + ":", realPair.getColumnName() + ":");
                                                                    System.out.println("Schema after replace: " + fixedSchema);
                                                                }
                                                                locatedColumn = true;
                                                                break;
                                                            } else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                                                //System.out.println("Located father2...");
                                                                if (fixedSchema.contains(tempPair.getColumnName() + ":")) {
                                                                    System.out.println("Alias in schema: " + tempPair.getColumnName() + " becomes: " + realPair.getColumnName());
                                                                    fixedSchema = fixedSchema.replace(tempPair.getColumnName() + ":", realPair.getColumnName() + ":");
                                                                    System.out.println("Schema after replace: " + fixedSchema);
                                                                }
                                                                locatedColumn = true;
                                                                break;
                                                            }
                                                        }

                                                        if (locatedColumn) break;
                                                    } else {
                                                        for (StringParameter sP : realPair.getAltAliasPairs()) { //Schema contains the latest allies and we have already called addNewPossibleALiases before
                                                            if (sP.getParemeterType().equals(currentOperatorNode.getOperatorName())) {
                                                                //System.out.println("Will compare: "+tempPair.getColumnName() + " with: "+sP.getValue() + " on currentNode: "+currentOperatorNode.getOperatorName());
                                                                if (sP.getValue().equals(tempPair.getColumnName())) {
                                                                    if (fixedSchema.contains(tempPair.getColumnName() + ":")) {
                                                                        System.out.println("Alias in schema: " + tempPair.getColumnName() + " becomes: " + realPair.getColumnName());
                                                                        fixedSchema = fixedSchema.replace(tempPair.getColumnName() + ":", realPair.getColumnName() + ":");
                                                                        System.out.println("Schema after replace: " + fixedSchema);
                                                                        locatedColumn = true;
                                                                        break;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }

                                                if (locatedColumn) break;
                                            }

                                            if (locatedColumn) break;
                                        }

                                        if (locatedColumn == false) { //Might be constant value locate it in Constants Map
                                            boolean foundToBeConstant = false;
                                            System.out.println("Will search in ommitedConstantsMap...");
                                            for (ColumnTypePair constPair : ommitedConstantsMap.getColumnAndTypeList()) {
                                                if (tempPair.getColumnType().equals(constPair.getColumnType())) {
                                                    for (StringParameter sP : constPair.getAltAliasPairs()) {
                                                        if (sP.getParemeterType().equals(currentOperatorNode.getOperatorName())) {
                                                            if (sP.getValue().equals(tempPair.getColumnName())) {
                                                                locatedColumn = true;
                                                                System.out.println("Alias: " + tempPair.getColumnName() + " is a Constant value with Real Alias: " + constPair);
                                                                foundToBeConstant = true;
                                                                break;
                                                            }
                                                        }
                                                    }

                                                    if (foundToBeConstant) break;
                                                }
                                            }

                                            if (foundToBeConstant == false) {
                                                System.out.println("Alias: " + tempPair.getColumnName() + " is NEITHER COLUMN NOR CONSTANT...MIGHT BE AGGR...CHECK IT!!");
                                                System.exit(0);
                                            }
                                        }

                                    }

                                }

                            }

                            /*-----------------ATTEMPT TO PLACE NEW AGGREAGATION IN AGGRMAP-------*/
                            if (aggregationsMap.getColumnAndTypeList().size() == 0) {

                                aggregationsMap.addPair(new ColumnTypePair(aggregationPhrase, "struct")); //WARNING: nothing will happen IF ALREADY EXISTS IN MAP
                                aggregationsMap.getColumnAndTypeList().get(0).addAltAlias(currentOperatorNode.getOperatorName(), subMap.getColumnAndTypeList().get(j).getColumnName(), true);

                                aggregationsMap.getColumnAndTypeList().get(0).addCastType(subMap.getColumnAndTypeList().get(j).getColumnType());

                                aggregationsMap.getColumnAndTypeList().get(0).setParameterValueAndType(paramRealNames, paramRealTypes);
                                if (subMap.getColumnAndTypeList().get(j).getColumnType().equals("struct") == false) {
                                    aggregationsMap.getColumnAndTypeList().get(0).addCastType(subMap.getColumnAndTypeList().get(j).getColumnType());
                                }
                                //if(fixedSchema.contains(subMap.getColumnAndTypeList().get(j).getColumnName())){
                                //    fixedSchema = fixedSchema.replace(subMap.getColumnAndTypeList().get(j).getColumnName() , aggregationPhrase);
                                //}
                            } else {
                                boolean phraseExists = false;
                                //int l = 0;
                                for (ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()) {
                                    if (aggPair.getColumnName().equals(aggregationPhrase)) {
                                        phraseExists = true;
                                        boolean altAliasExists = false;
                                        for (StringParameter sP : aggPair.getAltAliasPairs()) {
                                            if (sP.getParemeterType().equals(currentOperatorNode)) {
                                                altAliasExists = true;
                                                if (sP.getValue().equals(subMap.getColumnAndTypeList().get(j).getColumnName()) == false) {
                                                    sP.setValue(subMap.getColumnAndTypeList().get(j).getColumnName());
                                                    if (subMap.getColumnAndTypeList().get(j).getColumnType().equals("struct") == false) {
                                                        aggPair.addCastType(subMap.getColumnAndTypeList().get(j).getColumnType());
                                                    }
                                                    //if(fixedSchema.contains(subMap.getColumnAndTypeList().get(j).getColumnName())){
                                                    //    fixedSchema = fixedSchema.replace(subMap.getColumnAndTypeList().get(j).getColumnName() , aggregationPhrase);
                                                    //}
                                                }
                                            }
                                        }

                                        if (altAliasExists == false) {
                                            aggPair.addAltAlias(currentOperatorNode.getOperatorName(), subMap.getColumnAndTypeList().get(j).getColumnName(), true);
                                            //if(fixedSchema.contains(subMap.getColumnAndTypeList().get(j).getColumnName())){
                                            //    fixedSchema = fixedSchema.replace(subMap.getColumnAndTypeList().get(j).getColumnName() , aggregationPhrase);
                                            //}
                                            if (subMap.getColumnAndTypeList().get(j).getColumnType().equals("struct") == false) {
                                                aggPair.addCastType(subMap.getColumnAndTypeList().get(j).getColumnType());
                                            }
                                        }

                                    }

                                    //l++;
                                }

                                if (phraseExists == false) {
                                    //l = 0;
                                    aggregationsMap.addPair(new ColumnTypePair(aggregationPhrase, "struct")); //WARNING: nothing will happen IF ALREADY EXISTS IN MAP
                                    for (ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()) {
                                        if (aggPair.getColumnName().equals(aggregationPhrase)) {
                                            aggPair.addAltAlias(currentOperatorNode.getOperatorName(), subMap.getColumnAndTypeList().get(j).getColumnName(), true);
                                            if (subMap.getColumnAndTypeList().get(j).getColumnType().equals("struct") == false) {
                                                aggPair.addCastType(subMap.getColumnAndTypeList().get(j).getColumnType());
                                            }
                                            aggPair.setParameterValueAndType(paramRealNames, paramRealTypes);
                                            //if(fixedSchema.contains(subMap.getColumnAndTypeList().get(j).getColumnName())){
                                            //    fixedSchema = fixedSchema.replace(subMap.getColumnAndTypeList().get(j).getColumnName() , aggregationPhrase);
                                            // }
                                        }

                                        //l++;
                                    }
                                }
                            }

                        }

                    }
                    else{ //In any other MODE an aggregation has appeared before

                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " Has MODE: " + aggDesc.getMode().name() + " assumed to already exist!");
                        //Get Parameters
                        List<String> parameterColNames = new LinkedList<>();
                        if (aggDesc.getParameters() == null) {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " NO parameters for aggregation: " + aggDesc.toString());
                            System.exit(0);
                        }

                        if (aggDesc.getParameters().size() == 0) {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " NO parameters for aggregation: " + aggDesc.toString());
                            System.exit(0);
                        }

                        for (ExprNodeDesc exprNode : aggDesc.getParameters()) {
                            if (exprNode.getCols() == null) {
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " NO parameter cols for aggregation: " + aggDesc.toString());
                                System.exit(0);
                            }

                            if (exprNode.getCols().size() == 0) {
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " NO parameter cols for aggregation: " + aggDesc.toString());
                                System.exit(0);
                            }

                            for (String col : exprNode.getCols()) {
                                parameterColNames.add(col);
                            }

                        }

                        if(parameterColNames.size() > 1){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " More than 1 Parameter for aggregation: "+aggregationPhrase);
                            System.exit(0);
                        }

                        String parameter = parameterColNames.get(0);

                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " Searching on the assumption that: "+parameter+" is an older aggregation...");
                        boolean located = false;

                        for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                            //System.out.println("COMPARING: "+aggPair.getColumnName()+" with: "+subMap.getColumnAndTypeList().get(j).getColumnName());
                            //char[] tempArray1 = aggPair.getColumnName().toCharArray();
                            //char[] tempArray2 = subMap.getColumnAndTypeList().get(j).getColumnName().toCharArray();
                            //if((tempArray1[0] == tempArray2[0]) && (tempArray1[1] == tempArray2[1]) && (tempArray1[2] == tempArray2[2])) { //First 3 chars are equal
                            if( (aggPair.getColumnName().contains("avg(") && (aggregationPhrase.contains("avg("))) || ( (aggPair.getColumnName().contains("sum(") && (aggregationPhrase.contains("sum(")) )) ){
                                for (StringParameter sP : aggPair.getAltAliasPairs()) {
                                    if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                        if (sP.getValue().equals(parameter)) {
                                            aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), parameter, subMap.getColumnAndTypeList().get(j).getColumnName(), true);
                                            aggPair.addCastType(subMap.getColumnAndTypeList().get(j).getColumnType());
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Parameter: " + parameter + " was matched with aggPair: " + aggPair.getColumnName());
                                            located = true;
                                            break;
                                        }
                                    }
                                }
                            }

                            if(located) break;
                        }

                        if(located == false){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Aggreagation: "+aggregationPhrase+" failed to match with older aggregation!");
                            System.exit(0);
                        }

                    }

                    j++;

                }

                updatedSchemaString = fixedSchema;

                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " Printing AggrMap for debugging: ");
                aggregationsMap.printMap();

                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " Schema finally is: " + updatedSchemaString);


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
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);
            }
        }

        //Add select columns based on updatedSchemaString TODO: USE OUTPUT COLS INSTEAD
        List<FieldSchema> selectCols = new LinkedList<>();
        if(opQuery.getLocalQueryString().contains("select ") == false){
            addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, opQuery, selectCols, currentOperatorNode.getOperatorName(), null);
        }

        /*---Discover Group By Keys----*/
        List<FieldSchema> groupByKeys = new LinkedList<>();
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Now Discovering Group By Keys...");
        discoverGroupByKeys(currentOperatorNode, fatherOperatorNode, opQuery, currentOperatorNode.getOperatorName(), null, groupByDesc, groupByKeys);

        List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
        if(children != null){
            if(children.size() == 1){
                Operator<?> child = children.get(0);
                if(child instanceof ReduceSinkOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered GBY--->RS connection!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                    /*---Moving to child---*/
                    goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, opQuery, updatedSchemaString, currentOperatorNode.getOperatorName().toLowerCase(), null, null);
                }
                else if(child instanceof FileSinkOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered GBY--->FS connection!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                    /*---Moving to child---*/
                    goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, opQuery, updatedSchemaString, currentOperatorNode.getOperatorName().toLowerCase(), null, null);
                }
                else if(child instanceof SelectOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered GBY--->SEL connection!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                    /*---Moving to child---*/
                    goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, opQuery, updatedSchemaString, currentOperatorNode.getOperatorName().toLowerCase(), null, null);
                }
                else if(child instanceof FilterOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered GBY--->FIL connection!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                    /*---Moving to child---*/
                    goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, opQuery, updatedSchemaString, currentOperatorNode.getOperatorName().toLowerCase(), null, null);
                }
                else if(child instanceof LimitOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered GBY--->LIM connection!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                    /*---Moving to child---*/
                    goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, opQuery, updatedSchemaString, currentOperatorNode.getOperatorName().toLowerCase(), null, null);
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

    public void groupByHasFatherUnion(OperatorNode currentOperatorNode, OperatorQuery currentOpQuery, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, String latestAncestorSchema, String latestAncestorTableName1, String latestAncestorTableName2) {

        String updatedSchemaString = "";

        /*---------------------------Check if aggregators exist (this changes the approach we take)-----------------------*/
        GroupByOperator groupByOp = (GroupByOperator) currentOperatorNode.getOperator();
        GroupByDesc groupByDesc = groupByOp.getConf();

        if(groupByOp == null){
            System.out.println("GroupByDesc is null!");
            System.exit(0);
        }

        if((groupByDesc.getAggregators() != null) && (groupByDesc.getAggregators().size() > 0)){

            boolean newAggregators = false;
            int countNewAggregators = 0;
            for(AggregationDesc aggDesc : groupByDesc.getAggregators()){
                if(aggDesc.getMode().name().equals("PARTIAL1")){
                    countNewAggregators++;
                }
            }

            if(countNewAggregators == groupByDesc.getAggregators().size()){
                newAggregators = true;
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"We have discovered NEW aggregators!!");
            }
            //else if(countNewAggregators == 0){
            //    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"All aggegators have existed before! Locate them!");
            //}
            //else{
            //    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Not all aggregators are PARTIAL1 MODE...ERROR! Unsupported!");
            //    System.exit(0);
            //}

            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" GROUP BY contains aggregations...");
            if(currentOperatorNode.getOperator().getColumnExprMap() != null){
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding new possible Aliases...but also fixing schema from structs...");
                String tempSchema = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);

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
                                for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                                    for(StringParameter sP : aggPair.getAltAliasPairs()){
                                        if(sP.getParemeterType().equals(currentOperatorNode.getOperatorName())){
                                            if(sP.getValue().equals(somePair.getColumnName())){
                                                aggPair.addCastType("decimal");
                                            }
                                        }
                                    }
                                }
                                System.out.println("Schema now: "+fixedSchema);
                            }
                            else if(somePair.getColumnType().equals("double")){
                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" Pair: ("+somePair.getColumnName()+", "+somePair.getColumnType()+") will get a struct type instead");
                                for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                                    for(StringParameter sP : aggPair.getAltAliasPairs()){
                                        if(sP.getParemeterType().equals(currentOperatorNode.getOperatorName())){
                                            if(sP.getValue().equals(somePair.getColumnName())){
                                                aggPair.addCastType("double");
                                                somePair.setColumnType("struct");
                                                fixedSchema = fixedSchema.replace("double", "struct");
                                            }
                                        }
                                    }
                                }
                                System.out.println("Schema now: "+fixedSchema);
                            }
                            else{
                                //Might be a const value
                                boolean isConst = false;
                                for(ColumnTypePair constPair : ommitedConstantsMap.getColumnAndTypeList()){
                                    if(constPair.getColumnType().equals(somePair.getColumnType())){
                                        for(StringParameter sP : constPair.getAltAliasPairs()){
                                            if(sP.getParemeterType().equals(currentOperatorNode.getOperatorName())){
                                                if(sP.getValue().equals(somePair.getColumnName())){
                                                    isConst = true;
                                                    break;
                                                }
                                            }
                                        }

                                        if(isConst) break;
                                    }
                                }

                                if(isConst == false) {
                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " Pair: (" + somePair.getColumnName() + ", " + somePair.getColumnType() + ") NOT READY TO BE CONVERTED TO STUCT");
                                    System.exit(0);
                                }
                                else{
                                    somePair.setColumnName("ommited_constant_123_from_panos#");
                                }
                            }
                        }
                    }
                }

                /*---Add possible new aggregations---*/

                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" Creating subMap with aggregations only...");
                MyMap subMap = new MyMap(true);
                for(ColumnTypePair oldPair : someMap.getColumnAndTypeList()){
                    if(oldPair.getColumnName().contains("_col")) {
                        if (oldPair.getColumnType().contains("struct")) {
                            subMap.addPair(oldPair);
                        }
                        else{
                            if (oldPair.getColumnType().contains("decimal")) {
                                subMap.addPair(oldPair);
                                if(fixedSchema.contains(","+oldPair.getColumnName()+": "+oldPair.getColumnType()+",")) {
                                    fixedSchema = fixedSchema.replace("," + oldPair.getColumnName() + ": " + oldPair.getColumnType() + ",", "," + oldPair.getColumnName() + ": " + "struct,");
                                }
                                else if(fixedSchema.contains("("+oldPair.getColumnName()+": "+oldPair.getColumnType()+",")){
                                    fixedSchema = fixedSchema.replace("(" + oldPair.getColumnName() + ": " + oldPair.getColumnType() + ",", "(" + oldPair.getColumnName() + ": " + "struct,");
                                }
                                else if(fixedSchema.contains("("+oldPair.getColumnName()+": "+oldPair.getColumnType()+")")){
                                    fixedSchema = fixedSchema.replace("(" + oldPair.getColumnName() + ": " + oldPair.getColumnType() + ")", "(" + oldPair.getColumnName() + ": " + "struct)");
                                }
                                else if(fixedSchema.contains(","+oldPair.getColumnName()+": "+oldPair.getColumnType()+")")){
                                    fixedSchema = fixedSchema.replace("," + oldPair.getColumnName() + ": " + oldPair.getColumnType() + ")", "," + oldPair.getColumnName() + ": " + "struct)");
                                }
                            }
                            else if(oldPair.getColumnType().contains("double")){
                                subMap.addPair(oldPair);
                                if(fixedSchema.contains(","+oldPair.getColumnName()+": "+oldPair.getColumnType()+",")) {
                                    fixedSchema = fixedSchema.replace("," + oldPair.getColumnName() + ": " + oldPair.getColumnType() + ",", "," + oldPair.getColumnName() + ": " + "struct,");
                                }
                                else if(fixedSchema.contains("("+oldPair.getColumnName()+": "+oldPair.getColumnType()+",")){
                                    fixedSchema = fixedSchema.replace("(" + oldPair.getColumnName() + ": " + oldPair.getColumnType() + ",", "(" + oldPair.getColumnName() + ": " + "struct,");
                                }
                                else if(fixedSchema.contains("("+oldPair.getColumnName()+": "+oldPair.getColumnType()+")")){
                                    fixedSchema = fixedSchema.replace("(" + oldPair.getColumnName() + ": " + oldPair.getColumnType() + ")", "(" + oldPair.getColumnName() + ": " + "struct)");
                                }
                                else if(fixedSchema.contains(","+oldPair.getColumnName()+": "+oldPair.getColumnType()+")")){
                                    fixedSchema = fixedSchema.replace("," + oldPair.getColumnName() + ": " + oldPair.getColumnType() + ")", "," + oldPair.getColumnName() + ": " + "struct)");
                                }
                            }
                        }
                    }
                }

                ArrayList<AggregationDesc> aggregationDescs = groupByDesc.getAggregators();
                if(subMap.getColumnAndTypeList().size() != aggregationDescs.size()){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+" Expected aggregationsDesc to have same size as subMap! Printing subMap for debugging and exitting!");
                    subMap.printMap();
                    System.exit(0);
                }

                List<String> aggregations = new LinkedList<>();
                int j = 0;
                for(AggregationDesc aggDesc : aggregationDescs){ //Get an aggregation
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " Working on aggregation: " + aggDesc.getExprString());

                    String aggregationPhrase = aggDesc.getExprString();

                    if(aggDesc.getMode().name().equals("PARTIAL1") || (aggDesc.getMode().name().equals("COMPLETE"))){ //This will be a new aggregator (Partial means that we will encouter it later - and complete means that it appears once now only

                        //Get Parameters
                        List<String> parameterColNames = new LinkedList<>();
                        if (aggDesc.getParameters() == null) {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " NO parameters for aggregation: " + aggDesc.toString());
                            System.exit(0);
                        }

                        if (aggDesc.getParameters().size() == 0) {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " NO parameters for aggregation: " + aggDesc.toString());
                            System.exit(0);
                        }

                        for (ExprNodeDesc exprNode : aggDesc.getParameters()) {
                            if (exprNode.getCols() == null) {
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " NO parameter cols for aggregation: " + aggDesc.toString());
                                System.exit(0);
                            }

                            if (exprNode.getCols().size() == 0) {
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " NO parameter cols for aggregation: " + aggDesc.toString());
                                System.exit(0);
                            }

                            for (String col : exprNode.getCols()) {
                                parameterColNames.add(col);
                            }

                        }

                        List<String> paramRealNames = new LinkedList<>();
                        List<String> paramRealTypes = new LinkedList<>();

                        //Try to locate the real aliases of the cols through the father
                        for (String paramCol : parameterColNames) {
                            System.out.println("Attempting to locate paramCol: " + paramCol + " in TableRegistry...");
                            boolean columnLocated = false;
                            for (TableRegEntry tableRegEntry : tableRegistry.getEntries()) {
                                String extraAlias = "";
                                if (tableRegEntry.getAlias().equals(tableRegEntry.getAssociatedTable().getTableName()) == false) {
                                    extraAlias = tableRegEntry.getAlias() + ".";
                                }
                                for (ColumnTypePair somePair : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()) {

                                    if (somePair.getColumnName().equals(paramCol)) { //Match with columnName - now look for father
                                        for (StringParameter sP : somePair.getAltAliasPairs()) {
                                            if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) { //Located father
                                                String trueName = extraAlias + somePair.getColumnName();
                                                String trueType = somePair.getColumnType();

                                                boolean hasCastAlias = false;
                                                for(Map.Entry<String, MyMap> castEntry : operatorCastMap.entrySet()){
                                                    if(castEntry.getKey().equals(fatherOperatorNode.getOperatorName())){
                                                        for(ColumnTypePair castPair : castEntry.getValue().getColumnAndTypeList()){
                                                            if(castPair.getColumnName().equals(somePair.getColumnName())){
                                                                if(castPair.getAltAliasPairs().get(0).getValue().equals(paramCol)){
                                                                    trueName = castPair.getAltAliasPairs().get(0).getExtraValue();
                                                                    trueType = castPair.getColumnType();
                                                                    hasCastAlias = true;
                                                                    break;
                                                                }
                                                            }
                                                        }

                                                        if(hasCastAlias) break;
                                                    }
                                                }

                                                System.out.println("Located realAlias: " + trueName);

                                                columnLocated = true;
                                                paramRealNames.add(trueName);
                                                paramRealTypes.add(trueType);
                                                currentOpQuery.addUsedColumn(trueName, currentOperatorNode.getOperatorName().toLowerCase());
                                                System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+trueName+" , "+currentOperatorNode.getOperatorName().toLowerCase()+")");
                                                break;
                                            }
                                        }
                                    } else {
                                        for (StringParameter sP : somePair.getAltAliasPairs()) {
                                            if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) { //Located father
                                                if (sP.getValue().equals(paramCol)) {
                                                    String trueName = extraAlias + somePair.getColumnName();
                                                    String trueType = somePair.getColumnType();

                                                    boolean hasCastAlias = false;
                                                    for(Map.Entry<String, MyMap> castEntry : operatorCastMap.entrySet()){
                                                        if(castEntry.getKey().equals(fatherOperatorNode.getOperatorName())){
                                                            for(ColumnTypePair castPair : castEntry.getValue().getColumnAndTypeList()){
                                                                if(castPair.getColumnName().equals(somePair.getColumnName())){
                                                                    if(castPair.getAltAliasPairs().get(0).getValue().equals(paramCol)){
                                                                        trueName = castPair.getAltAliasPairs().get(0).getExtraValue();
                                                                        trueType = castPair.getColumnType();
                                                                        hasCastAlias = true;
                                                                        break;
                                                                    }
                                                                }
                                                            }

                                                            if(hasCastAlias) break;
                                                        }
                                                    }

                                                    System.out.println("Located realAlias: " + trueName);

                                                    if (aggregationPhrase.contains(paramCol + " ")) {
                                                        aggregationPhrase = aggregationPhrase.replace(paramCol + " ", trueName + " ");
                                                    } else if (aggregationPhrase.contains(paramCol + ")")) {

                                                        aggregationPhrase = aggregationPhrase.replace(paramCol + ")", trueName + ")");
                                                    }

                                                    columnLocated = true;
                                                    paramRealNames.add(trueName);
                                                    paramRealTypes.add(trueType);
                                                    currentOpQuery.addUsedColumn(trueName, currentOperatorNode.getOperatorName().toLowerCase());
                                                    System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+trueName+" , "+currentOperatorNode.getOperatorName().toLowerCase()+")");
                                                    break;
                                                }
                                            }
                                        }
                                    }

                                    if (columnLocated == true) break;
                                }

                                if (columnLocated == true) break;
                            }

                            if (columnLocated == false) { //Extra search
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " Perfoming search in Aggregation Map for: " + aggDesc.toString());
                                for (ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()) {
                                    for (StringParameter sP : aggPair.getAltAliasPairs()) {
                                        if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()) || (sP.getParemeterType().equals(currentOperatorNode.getOperatorName()))) {
                                            if (sP.getValue().equals(paramCol)) {

                                                String properAliasName = "";
                                                properAliasName = fetchlatestRequiredAncestorOfAggr(currentOperatorNode, currentOpQuery, aggPair.getColumnName(), aggPair.getAltAliasPairs(), paramCol);

                                                System.out.println("Real Alias for: " + paramCol + " is: " + properAliasName + " in aggregationMap!");

                                                if (aggregationPhrase.contains(paramCol + " ")) {

                                                    aggregationPhrase = aggregationPhrase.replace(paramCol + " ", properAliasName + " ");
                                                } else if (aggregationPhrase.contains(paramCol + ")")) {

                                                    aggregationPhrase = aggregationPhrase.replace(paramCol + ")", properAliasName + ")");
                                                }

                                                columnLocated = true;

                                                paramRealNames.add(properAliasName);
                                                paramRealTypes.add("struct");

                                                currentOpQuery.addUsedColumn(properAliasName, currentOperatorNode.getOperatorName().toLowerCase());
                                                System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+properAliasName+" , "+currentOperatorNode.getOperatorName().toLowerCase()+")");

                                                break;


                                            }
                                        }
                                    }

                                    if (columnLocated == true) break;
                                }
                            }

                            if (columnLocated == false) {
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " Failed to find real alias for agg parameter col: " + paramCol);
                                System.exit(0);
                            }

                        }

                        boolean newAgg = true;
                        for (String ag : aggregations) {
                            if (ag.equals(aggregationPhrase)) {
                                newAgg = false;
                                break;
                            }
                        }

                        if (newAgg == true) {
                            aggregations.add(aggregationPhrase);

                            if (aggregations.size() == 1) {
                                //Fix non aggreagations aliases
                                System.out.println("Fixing non aggreagation columns...");

                                MyMap anotherTempMap = new MyMap(true);
                                fixedSchema = extractColsFromTypeName(fixedSchema, anotherTempMap, fixedSchema, true);

                                for (ColumnTypePair tempPair : anotherTempMap.getColumnAndTypeList()) {
                                    boolean locatedColumn = false;
                                    if (tempPair.getColumnType().equals("struct") == false) {
                                        System.out.println("Working on alias: " + tempPair.getColumnName() + " - Type: " + tempPair.getColumnType());
                                        for (TableRegEntry tableRegEntry : tableRegistry.getEntries()) {
                                            //System.out.println("Accessing: "+tableRegEntry.getAlias());
                                            for (ColumnTypePair realPair : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()) {
                                                //System.out.println("Accessing: "+realPair.getColumnName()+" - Type: "+realPair.getColumnType());
                                                if (realPair.getColumnType().equals(tempPair.getColumnType())) {
                                                    if (realPair.getColumnName().equals(tempPair.getColumnName())) {
                                                        //System.out.println("is real Alias...");
                                                        for (StringParameter sP : realPair.getAltAliasPairs()) { //Schema contains the latest allies and we have already called addNewPossibleALiases before
                                                            if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                                                //System.out.println("Located father1...");
                                                                if (fixedSchema.contains(tempPair.getColumnName() + ":")) {
                                                                    System.out.println("Alias in schema: " + tempPair.getColumnName() + " becomes: " + realPair.getColumnName());
                                                                    fixedSchema = fixedSchema.replace(tempPair.getColumnName() + ":", realPair.getColumnName() + ":");
                                                                    System.out.println("Schema after replace: " + fixedSchema);
                                                                }
                                                                locatedColumn = true;
                                                                break;
                                                            } else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                                                //System.out.println("Located father2...");
                                                                if (fixedSchema.contains(tempPair.getColumnName() + ":")) {
                                                                    System.out.println("Alias in schema: " + tempPair.getColumnName() + " becomes: " + realPair.getColumnName());
                                                                    fixedSchema = fixedSchema.replace(tempPair.getColumnName() + ":", realPair.getColumnName() + ":");
                                                                    System.out.println("Schema after replace: " + fixedSchema);
                                                                }
                                                                locatedColumn = true;
                                                                break;
                                                            }
                                                        }

                                                        if (locatedColumn) break;
                                                    } else {
                                                        for (StringParameter sP : realPair.getAltAliasPairs()) { //Schema contains the latest allies and we have already called addNewPossibleALiases before
                                                            if (sP.getParemeterType().equals(currentOperatorNode.getOperatorName())) {
                                                                //System.out.println("Will compare: "+tempPair.getColumnName() + " with: "+sP.getValue() + " on currentNode: "+currentOperatorNode.getOperatorName());
                                                                if (sP.getValue().equals(tempPair.getColumnName())) {
                                                                    if (fixedSchema.contains(tempPair.getColumnName() + ":")) {
                                                                        System.out.println("Alias in schema: " + tempPair.getColumnName() + " becomes: " + realPair.getColumnName());
                                                                        fixedSchema = fixedSchema.replace(tempPair.getColumnName() + ":", realPair.getColumnName() + ":");
                                                                        System.out.println("Schema after replace: " + fixedSchema);
                                                                        locatedColumn = true;
                                                                        break;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }

                                                if (locatedColumn) break;
                                            }

                                            if (locatedColumn) break;
                                        }

                                        if (locatedColumn == false) { //Might be constant value locate it in Constants Map
                                            boolean foundToBeConstant = false;
                                            System.out.println("Will search in ommitedConstantsMap...");
                                            for (ColumnTypePair constPair : ommitedConstantsMap.getColumnAndTypeList()) {
                                                if (tempPair.getColumnType().equals(constPair.getColumnType())) {
                                                    for (StringParameter sP : constPair.getAltAliasPairs()) {
                                                        if (sP.getParemeterType().equals(currentOperatorNode.getOperatorName())) {
                                                            if (sP.getValue().equals(tempPair.getColumnName())) {
                                                                locatedColumn = true;
                                                                System.out.println("Alias: " + tempPair.getColumnName() + " is a Constant value with Real Alias: " + constPair);
                                                                foundToBeConstant = true;
                                                                break;
                                                            }
                                                        }
                                                    }

                                                    if (foundToBeConstant) break;
                                                }
                                            }

                                            if (foundToBeConstant == false) {
                                                System.out.println("Alias: " + tempPair.getColumnName() + " is NEITHER COLUMN NOR CONSTANT...MIGHT BE AGGR...CHECK IT!!");
                                                System.exit(0);
                                            }
                                        }

                                    }

                                }

                            }

                            /*-----------------ATTEMPT TO PLACE NEW AGGREAGATION IN AGGRMAP-------*/
                            if (aggregationsMap.getColumnAndTypeList().size() == 0) {

                                aggregationsMap.addPair(new ColumnTypePair(aggregationPhrase, "struct")); //WARNING: nothing will happen IF ALREADY EXISTS IN MAP
                                aggregationsMap.getColumnAndTypeList().get(0).addAltAlias(currentOperatorNode.getOperatorName(), subMap.getColumnAndTypeList().get(j).getColumnName(), true);

                                aggregationsMap.getColumnAndTypeList().get(0).addCastType(subMap.getColumnAndTypeList().get(j).getColumnType());

                                aggregationsMap.getColumnAndTypeList().get(0).setParameterValueAndType(paramRealNames, paramRealTypes);
                                if (subMap.getColumnAndTypeList().get(j).getColumnType().equals("struct") == false) {
                                    aggregationsMap.getColumnAndTypeList().get(0).addCastType(subMap.getColumnAndTypeList().get(j).getColumnType());
                                }
                                //if(fixedSchema.contains(subMap.getColumnAndTypeList().get(j).getColumnName())){
                                //    fixedSchema = fixedSchema.replace(subMap.getColumnAndTypeList().get(j).getColumnName() , aggregationPhrase);
                                //}
                            } else {
                                boolean phraseExists = false;
                                //int l = 0;
                                for (ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()) {
                                    if (aggPair.getColumnName().equals(aggregationPhrase)) {
                                        phraseExists = true;
                                        boolean altAliasExists = false;
                                        for (StringParameter sP : aggPair.getAltAliasPairs()) {
                                            if (sP.getParemeterType().equals(currentOperatorNode)) {
                                                altAliasExists = true;
                                                if (sP.getValue().equals(subMap.getColumnAndTypeList().get(j).getColumnName()) == false) {
                                                    sP.setValue(subMap.getColumnAndTypeList().get(j).getColumnName());
                                                    if (subMap.getColumnAndTypeList().get(j).getColumnType().equals("struct") == false) {
                                                        aggPair.addCastType(subMap.getColumnAndTypeList().get(j).getColumnType());
                                                    }
                                                    //if(fixedSchema.contains(subMap.getColumnAndTypeList().get(j).getColumnName())){
                                                    //    fixedSchema = fixedSchema.replace(subMap.getColumnAndTypeList().get(j).getColumnName() , aggregationPhrase);
                                                    //}
                                                }
                                            }
                                        }

                                        if (altAliasExists == false) {
                                            aggPair.addAltAlias(currentOperatorNode.getOperatorName(), subMap.getColumnAndTypeList().get(j).getColumnName(), true);
                                            //if(fixedSchema.contains(subMap.getColumnAndTypeList().get(j).getColumnName())){
                                            //    fixedSchema = fixedSchema.replace(subMap.getColumnAndTypeList().get(j).getColumnName() , aggregationPhrase);
                                            //}
                                            if (subMap.getColumnAndTypeList().get(j).getColumnType().equals("struct") == false) {
                                                aggPair.addCastType(subMap.getColumnAndTypeList().get(j).getColumnType());
                                            }
                                        }

                                    }

                                    //l++;
                                }

                                if (phraseExists == false) {
                                    //l = 0;
                                    aggregationsMap.addPair(new ColumnTypePair(aggregationPhrase, "struct")); //WARNING: nothing will happen IF ALREADY EXISTS IN MAP
                                    for (ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()) {
                                        if (aggPair.getColumnName().equals(aggregationPhrase)) {
                                            aggPair.addAltAlias(currentOperatorNode.getOperatorName(), subMap.getColumnAndTypeList().get(j).getColumnName(), true);
                                            if (subMap.getColumnAndTypeList().get(j).getColumnType().equals("struct") == false) {
                                                aggPair.addCastType(subMap.getColumnAndTypeList().get(j).getColumnType());
                                            }
                                            aggPair.setParameterValueAndType(paramRealNames, paramRealTypes);
                                            //if(fixedSchema.contains(subMap.getColumnAndTypeList().get(j).getColumnName())){
                                            //    fixedSchema = fixedSchema.replace(subMap.getColumnAndTypeList().get(j).getColumnName() , aggregationPhrase);
                                            // }
                                        }

                                        //l++;
                                    }
                                }
                            }

                        }

                    }
                    else{ //In any other MODE an aggregation has appeared before

                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " Has MODE: " + aggDesc.getMode().name() + " assumed to already exist!");
                        //Get Parameters
                        List<String> parameterColNames = new LinkedList<>();
                        if (aggDesc.getParameters() == null) {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " NO parameters for aggregation: " + aggDesc.toString());
                            System.exit(0);
                        }

                        if (aggDesc.getParameters().size() == 0) {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " NO parameters for aggregation: " + aggDesc.toString());
                            System.exit(0);
                        }

                        for (ExprNodeDesc exprNode : aggDesc.getParameters()) {
                            if (exprNode.getCols() == null) {
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " NO parameter cols for aggregation: " + aggDesc.toString());
                                System.exit(0);
                            }

                            if (exprNode.getCols().size() == 0) {
                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " NO parameter cols for aggregation: " + aggDesc.toString());
                                System.exit(0);
                            }

                            for (String col : exprNode.getCols()) {
                                parameterColNames.add(col);
                            }

                        }

                        if(parameterColNames.size() > 1){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " More than 1 Parameter for aggregation: "+aggregationPhrase);
                            System.exit(0);
                        }

                        String parameter = parameterColNames.get(0);

                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " Searching on the assumption that: "+parameter+" is an older aggregation...");
                        boolean located = false;

                        for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                            //System.out.println("COMPARING: "+aggPair.getColumnName()+" with: "+subMap.getColumnAndTypeList().get(j).getColumnName());
                            //char[] tempArray1 = aggPair.getColumnName().toCharArray();
                            //char[] tempArray2 = subMap.getColumnAndTypeList().get(j).getColumnName().toCharArray();
                            //if((tempArray1[0] == tempArray2[0]) && (tempArray1[1] == tempArray2[1]) && (tempArray1[2] == tempArray2[2])) { //First 3 chars are equal
                            if( (aggPair.getColumnName().contains("avg(") && (aggregationPhrase.contains("avg("))) || ( (aggPair.getColumnName().contains("sum(") && (aggregationPhrase.contains("sum(")) )) ){
                                for (StringParameter sP : aggPair.getAltAliasPairs()) {
                                    if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                        if (sP.getValue().equals(parameter)) {
                                            aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), parameter, subMap.getColumnAndTypeList().get(j).getColumnName(), true);
                                            aggPair.addCastType(subMap.getColumnAndTypeList().get(j).getColumnType());
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Parameter: " + parameter + " was matched with aggPair: " + aggPair.getColumnName());
                                            located = true;
                                            break;
                                        }
                                    }
                                }
                            }

                            if(located) break;
                        }

                        if(located == false){
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Aggreagation: "+aggregationPhrase+" failed to match with older aggregation!");
                            System.exit(0);
                        }

                    }

                    j++;

                }

                updatedSchemaString = fixedSchema;

                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " Printing AggrMap for debugging: ");
                aggregationsMap.printMap();

                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + " Schema finally is: " + updatedSchemaString);


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
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);
            }
        }

        //Add select columns based on updatedSchemaString TODO: USE OUTPUT COLS INSTEAD
        List<FieldSchema> selectCols = new LinkedList<>();
        if(currentOpQuery.getLocalQueryString().contains("select ") == false){
            addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, selectCols, currentOperatorNode.getOperatorName(), null);
        }

        /*---Discover Group By Keys----*/
        List<FieldSchema> groupByKeys = new LinkedList<>();
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": "+"Now Discovering Group By Keys...");
        discoverGroupByKeys(currentOperatorNode, fatherOperatorNode, currentOpQuery, latestAncestorTableName1, latestAncestorTableName2, groupByDesc, groupByKeys);

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
                else if(child instanceof SelectOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered GBY--->SEL connection!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                    /*---Moving to child---*/
                    goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);
                }
                else if(child instanceof FilterOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered GBY--->FIL connection!");
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
                    updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);
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
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);
            }
        }

        /*---Add possible Order By Keys---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if ORDER BY exists in this RS...");
        List<FieldSchema> orderByFields = new LinkedList<>();
        discoverOrderByKeys(currentOperatorNode, fatherOperatorNode, currentOpQuery, latestAncestorTableName1, latestAncestorTableName2, orderByFields);

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

        updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestAncestorTableName1, latestAncestorTableName2, orderByFields);

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

                    goToChildOperator(childNode, currentOperatorNode, opQuery, updatedSchemaString, currentOpQuery.getOutputTable().getTableName().toLowerCase(), null, null);

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
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);
            }
        }

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
        if(currentOpQuery.getLocalQueryString().contains("select ") == false){
            updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, latestAncestorSchema, currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);
        }

        /*---Add possible Order By Keys---*/
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if ORDER BY exists in this RS...");
        List<FieldSchema> orderByFields = new LinkedList<>();
        discoverOrderByKeys(currentOperatorNode, fatherOperatorNode, currentOpQuery, latestAncestorTableName1, latestAncestorTableName2, orderByFields);

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

        updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestAncestorTableName1, latestAncestorTableName2, orderByFields);

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

                    goToChildOperator(childNode, currentOperatorNode, opQuery, updatedSchemaString, currentOpQuery.getOutputTable().getTableName().toLowerCase(), null, null);
                }
                else if(child instanceof SelectOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Discovered RS--->SEL connection!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Moving to child operator...");

                    OperatorNode childNode = exaremeGraph.getOperatorNodeByName(child.getOperatorId());

                    goToChildOperator(childNode, currentOperatorNode, opQuery, updatedSchemaString, currentOpQuery.getOutputTable().getTableName().toLowerCase(), null, null);
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

        String updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);

        //TODO ADD KEY SEARCH AND OUTPUT SELECT COLUMNS
        List<String> joinColumns = new LinkedList<>();

        handleJoinKeys(currentOperatorNode, currentOpQuery, fatherOperatorNode, otherFatherNode, joinColumns);

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Now we will use keys to form the join expression...");

        //Add Join col1 = col2 part of expression
        if(joinColumns.size() > 0) {
            if(joinColumns.size() == 2){
                currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString().concat(" " + joinColumns.get(0) + " = " + joinColumns.get(1) + " "));
                currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString().concat(" " + joinColumns.get(0) + " = " + joinColumns.get(1) + " "));
            }
            else{
                String joinString = " ";
                for(int j = 0; j < joinColumns.size() / 2; j++){
                    if(j == 0){
                        joinString = joinString + joinColumns.get(j) + " = " + joinColumns.get(j + joinColumns.size() / 2) + " ";
                    }
                    else{
                        joinString = joinString + " and " + joinColumns.get(j) + " = " + joinColumns.get(j + joinColumns.size() / 2) + " ";
                    }
                }
                currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString().concat(" " + joinString + " "));
                currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString().concat(" " + joinString + " "));
            }

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
                    updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode,updatedSchemaString, currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);

                    /*---Finalising outputTable---*/
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Finalising OutputTable...");
                    MyTable outputTable = new MyTable();
                    outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                    outputTable.setIsAFile(false);
                    List<FieldSchema> newCols = new LinkedList<>();

                    updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestAncestorTableName1, latestAncestorTableName2, null);

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

                    goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, opQuery, updatedSchemaString, currentOperatorNode.getOperatorName(), null, null);


                }
                else if(child instanceof FilterOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered JOIN--->FIL connection!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Moving to child operator...");

                    goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, currentOpQuery, updatedSchemaString, latestAncestorTableName1, latestAncestorTableName2, null);

                }
                else if (child instanceof FileSinkOperator) {
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered JOIN--->FS connection!");
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

    public void mapJoinHasFatherHashFilterTableScanUnion(OperatorNode currentOperatorNode, OperatorQuery currentOpQuery, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, String latestAncestorTableName1, String latestAncestorTableName2){

        String updatedSchemaString = addNewPossibleAliases(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery);

        //TODO ADD KEY SEARCH AND OUTPUT SELECT COLUMNS
        List<String> joinColumns = new LinkedList<>();

        handleMapJoinKeys(currentOperatorNode, currentOpQuery, fatherOperatorNode, otherFatherNode, joinColumns);

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Now we will use keys to form the join expression...");

        if(joinColumns.size() > 0) {
            //Add Join col1 = col2 part of expression
            if(joinColumns.size() == 2){
                currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString().concat(" " + joinColumns.get(0) + " = " + joinColumns.get(1) + " "));
                currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString().concat(" " + joinColumns.get(0) + " = " + joinColumns.get(1) + " "));
            }
            else{
                String joinString = " ";
                for(int j = 0; j < joinColumns.size() / 2; j++){
                    if(j == 0){
                        joinString = joinString + joinColumns.get(j) + " = " + joinColumns.get(j + joinColumns.size() / 2) + " ";
                    }
                    else{
                        joinString = joinString + " and " + joinColumns.get(j) + " = " + joinColumns.get(j + joinColumns.size() / 2) + " ";
                    }
                }
                currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString().concat(" " + joinString + " "));
                currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString().concat(" " + joinString + " "));
            }
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
                else if(child instanceof MapJoinOperator){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered MAP_JOIN--->MAP_JOIN Connection! Child is select: " + child.getOperatorId());

                    if (currentOpQuery.getLocalQueryString().contains("select ") == false) {
                        updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode, updatedSchemaString, currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);
                    }

                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current Query will be ending here...");

                    /*---Finalising outputTable---*/
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Finalising OutputTable...");

                    MyTable outputTable = new MyTable();
                    outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                    outputTable.setIsAFile(false);
                    List<FieldSchema> newCols = new LinkedList<>();

                    updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestAncestorTableName1, latestAncestorTableName2, null);

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

                            createMapJoinPoint(currentOperatorNode, currentOpQuery, child, currentOpQuery.getOutputTable().getTableName().toLowerCase(), latestAncestorTableName2);

                            return;
                        }
                    }
                    else{ //No JoinPoint Exists
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                        createMapJoinPoint(currentOperatorNode, currentOpQuery, child, currentOpQuery.getOutputTable().getTableName().toLowerCase(), latestAncestorTableName2);

                        return;
                    }

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

    public void unionHasFatherTableScan(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorQuery currentOpQuery, String schema, String latestTable1, String latestTable2){

        System.out.println(currentOperatorNode.getOperator() + ": Since this is a Union Operator we will retrieve its parents and their nodes...");

        List<Operator<?>> parentOperators = currentOperatorNode.getOperator().getParentOperators();

        System.out.println(currentOperatorNode.getOperator() + ": Parents: "+parentOperators.toString());

        String updatedSchemaString = schema;

        if(currentOperatorNode.getOperator().getColumnExprMap() != null) {
            for (Operator<?> parent : parentOperators) {
                OperatorNode parentNode = exaremeGraph.getOperatorNodeByName(parent.getOperatorId());
                System.out.println(currentOperatorNode.getOperator() + ": Will call addNewPossibleAliases with parent: " + parentNode.getOperatorName());
                updatedSchemaString = addNewPossibleAliases(currentOperatorNode, parentNode, null, currentOpQuery);
            }
        }
        else{
            for (Operator<?> parent : parentOperators) {
                OperatorNode parentNode = exaremeGraph.getOperatorNodeByName(parent.getOperatorId());
                System.out.println(currentOperatorNode.getOperator() + ": Will call useSchemaToFillAliases with parent: " + parentNode.getOperatorName());
                useSchemaToFillAliases(currentOperatorNode, parentNode, schema);
            }
        }
        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Current Query will be ending here...");

        /*---Add select statement to Query---*/
        //updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, schema, currentOpQuery, null);

        /*---Moving to Child Operator---*/
        List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
        if(children != null) {
            if (children.size() == 1) {
                Operator<?> child = children.get(0);
                if (child instanceof ReduceSinkOperator) {

                    /*---Finalising outputTable---*/
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Finalising OutputTable...");
                    MyTable outputTable = new MyTable();
                    outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                    outputTable.setIsAFile(false);
                    List<FieldSchema> newCols = new LinkedList<>();

                    updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, null, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestTable1, latestTable2, null);

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

                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered UNION--->RS connection!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Moving to child operator...");

                    goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, opQuery, updatedSchemaString, currentOperatorNode.getOperatorName().toLowerCase(), null, null);

                }
                else if(child instanceof GroupByOperator){

                            /*---Finalising outputTable---*/
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Finalising OutputTable...");
                    MyTable outputTable = new MyTable();
                    outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                    outputTable.setIsAFile(false);
                    List<FieldSchema> newCols = new LinkedList<>();

                    updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, null, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestTable1, latestTable2, null);

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

                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered UNION--->GBY connection!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Moving to child operator...");

                    goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, opQuery, updatedSchemaString, latestTable1, latestTable2, null);

                }
                else if(child instanceof MapJoinOperator){

                    /*---Finalising outputTable---*/
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Finalising OutputTable...");
                    MyTable outputTable = new MyTable();
                    outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                    outputTable.setIsAFile(false);
                    List<FieldSchema> newCols = new LinkedList<>();

                    updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, null, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestTable1, latestTable2, null);

                    /*---Finalize local part of Query---*/
                    currentOpQuery.setLocalQueryString("create table " + currentOperatorNode.getOperator().getOperatorId().toLowerCase() + " as " + currentOpQuery.getLocalQueryString());

                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Query is currently: [" + currentOpQuery.getLocalQueryString() + "]");

                    /*---Check if OpLink is to be created---*/
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Checking if OpLink can be created...");
                    checkForPossibleOpLinks(currentOperatorNode,  currentOpQuery, outputTable);

                    /*----Adding Finished Query to List----*/
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Adding OperatorQuery to QueryList...");
                    allQueries.add(currentOpQuery);

                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Discovered UNION--->MAP_JOIN Connection! Child is select: " + child.getOperatorId());

                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if JOIN_POINT: "+child.getOperatorId()+" exists...");

                    if(joinPointList.size() > 0){
                        boolean joinPExists = false;
                        for(JoinPoint jP : joinPointList){
                            if(jP.getId().equals(child.getOperatorId())){
                                joinPExists = true;

                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" Exists! Removing it from List!");

                                OperatorQuery associatedQuery = jP.getAssociatedOpQuery();
                                String otherParent = jP.getCreatedById();
                                String latestAncestorTableName2 = jP.getLatestAncestorTableName();
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

                            createMapJoinPoint(currentOperatorNode, currentOpQuery, child, currentOperatorNode.getOperatorName().toLowerCase(), null);

                            return;
                        }
                    }
                    else{ //No JoinPoint Exists
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": JOIN_POINT: "+child.getOperatorId()+" does not exist! We will create it and return!");

                        createMapJoinPoint(currentOperatorNode, currentOpQuery, child, currentOperatorNode.getOperatorName().toLowerCase(), null);

                        return;
                    }

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
                                            mapPair.modifyAltAlias(currentOperatorNode.getOperatorName(), oldPair.getColumnName(), newColName, false);
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

    public void createMapJoinPoint(OperatorNode currentOperatorNode, OperatorQuery currentOpQuery, Operator<?> child, String latestAncestorTableName1, String latestAncestorTableName2){

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
            String updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode,latestSchema, opQuery, usedColumns, latestAncestorTableName1, null);
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
            String updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode,latestSchema, opQuery, usedColumns, latestAncestorTableName1, null);
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
            System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+f.getName()+" , "+currentOpQuery.getOutputTable().getTableName().toLowerCase()+")");
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
            String updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode,latestSchema, opQuery, usedColumns, latestAncestorTableName1, null);
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
            System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+f.getName()+" , "+currentOpQuery.getOutputTable().getTableName().toLowerCase()+")");
        }

        associatedOpQuery.addInputTable(currentOpQuery.getOutputTable());
        associatedOpQuery.setLocalQueryString(associatedOpQuery.getLocalQueryString() + " " + unionPhrase + " " + opQuery.getLocalQueryString() + " ");
        associatedOpQuery.setExaremeQueryString(associatedOpQuery.getExaremeQueryString() + " " + unionPhrase + " " + opQuery.getLocalQueryString() + " ");

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+associatedOpQuery.getLocalQueryString()+"]");

        //Save changes
        unionPoint.setAssociatedQuery(associatedOpQuery);

    }

    public void fileSinkHasFatherLimitGroupSelectMapFilterJoin(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, OperatorQuery currentOpQuery, String latestAncestorSchema, String latestAncestorTableName1, String latestAncestorTableName2){

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: fileSinkHasFatherLimitGroupSelectJoin");
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

            List<Operator<?>> children = currentOperatorNode.getOperator().getChildOperators();
            if(children != null){
                if((children.size() == 1) || (children.size() == 2)){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
                    if(currentOpQuery.getLocalQueryString().contains("select ") == false){
                        updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode,latestAncestorSchema, currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);
                    }
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

                        updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperatorName().toLowerCase(), outputTable, newCols, latestAncestorTableName1, latestAncestorTableName2, null);

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
                            System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+f.getName()+" , "+currentOpQuery.getOutputTable().getTableName().toLowerCase()+")");
                        }

                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+opQuery.getLocalQueryString()+"]");

                        goToChildOperator(exaremeGraph.getOperatorNodeByName(child.getOperatorId()), currentOperatorNode, opQuery, updatedSchemaString, currentOpQuery.getOutputTable().getTableName().toLowerCase(), null, null);

                    }
                    else{
                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Unsupported type of Child for FS!");
                        System.exit(0);
                    }
                }
                else if(children.size() == 0){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Reached LEAF FS!!! We did it!");
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Current query will be ending here!");

                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Checking if current Query contains Select statement...");
                    if(currentOpQuery.getLocalQueryString().contains("select ") == false){
                        updatedSchemaString = addSelectStatementToQuery(currentOperatorNode, fatherOperatorNode, otherFatherNode,currentOperatorNode.getOperator().getSchema().toString(), currentOpQuery, null, latestAncestorTableName1, latestAncestorTableName2);
                    }
                    else{
                        updatedSchemaString = currentOperatorNode.getOperator().getSchema().toString();
                    }

                    /*---Finalising outputTable---*/
                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Finalising OutputTable...");
                    MyTable outputTable = new MyTable();
                    outputTable.setBelongingDatabaseName(currentOpQuery.getInputTables().get(0).getBelongingDataBaseName());
                    outputTable.setIsAFile(false);
                    List<FieldSchema> newCols = new LinkedList<>();

                    updatedSchemaString = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, otherFatherNode, currentOpQuery, updatedSchemaString, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestAncestorTableName1, latestAncestorTableName2, null);

                    if(outputTableIsFile){
                        outputTable.setTableName(createTableName);
                    }
                    /*---Finalize local part of Query---*/
                    currentOpQuery.setLocalQueryString("create table " + createTableName + " as " + currentOpQuery.getLocalQueryString());

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

    public void listSinkHasFatherFileSink(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorQuery currentOpQuery, String latestAncestorSchema, String latestTable1, String latestTable2){

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

                latestAncestorSchema = finaliseOutputTable(currentOperatorNode, fatherOperatorNode, null, currentOpQuery, latestAncestorSchema, currentOperatorNode.getOperator().getOperatorId().toLowerCase(), outputTable, newCols, latestTable1, latestTable2, null);

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

    public String extractOperatorNameFromAggrAlias(String alias){

        String[] parts = alias.split("_");

        return (parts[1]+"_"+parts[2]);

    }

    public List<FieldSchema> addPurgeSelectStatementToQuery(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, String latestAncestorSchema, OperatorQuery currentOpQuery, List<FieldSchema> usedCols, String latestTable1, String latestTable2) {

        System.out.println("LATESTTABLE1: "+latestTable1+" - LATESTTABLE2: "+latestTable2);

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: addPurgeSelectStatementToQuery...");

        List<FieldSchema> newCols = new LinkedList<>();

        System.out.println("addPurgeSelectStatementToQuery: We must create a new SELECT statement for this OperatorQuery and replace the old one!");
        if(currentOpQuery.getLocalQueryString().contains(" from ")){
            String[] parts = currentOpQuery.getLocalQueryString().split(" from ");
            if(parts.length > 2){
                System.out.println("addPurgeSelectStatementToQuery: MORE THAN 2 SELECT STATEMENTS MIGHT EXIST! UNSUPPORTED!");
                System.exit(0);
            }
            String afterFromPhrase = parts[1];
            afterFromPhrase = " from " + afterFromPhrase;

            if( (currentOperatorNode.getOperator() instanceof SelectOperator) == false){
                System.out.println("addPurgeSelectStatementToQuery: CurrentNode is not SelectOperator! Error!");
                System.exit(0);
            }

            SelectDesc selectDesc = ((SelectOperator) currentOperatorNode.getOperator()).getConf();
            String originalSelectString = selectDesc.getColListString();

            System.out.println("addPurgeSelectStatementToQuery: Will use: "+originalSelectString);

            if(originalSelectString.contains(" struct")){
                System.out.println("addPurgeSelectStatementToQuery: Select cols contain struct");
                originalSelectString = fixSchemaContainingStructs(originalSelectString);
            }
            else if(originalSelectString.contains(" decimal")){
                System.out.println("addPurgeSelectStatementToQuery: Select cols contain decimal");
                originalSelectString = fixSchemaContainingDecimals(originalSelectString);
            }

            List<String> newSelectPhrases = new LinkedList<>();
            if(originalSelectString.contains(", ")){
                String[] commaParts = originalSelectString.split(",");
                System.out.println("addPurgeSelectStatementToQuery: Located: "+commaParts.length+" cols...");
                for(String commaPart : commaParts){
                    FieldSchema f = new FieldSchema();
                    boolean ready = false;
                    commaPart = commaPart.trim();
                    if(commaPart.contains("'")){
                        System.out.println("addPurgeSelectStatementToQuery: CommaPart: "+commaPart+" contains CONST...Skipping...");
                        continue;
                    }
                    if(commaPart.contains("CAST(") && (commaPart.contains("avg(") == false)){ //Cast phrase
                        char[] castCommaPart = commaPart.toCharArray();
                        List<String> colAndType = new LinkedList<>();
                        fixColTypePairContainingCast(commaPart, colAndType);

                        boolean replaceParam = false;
                        List<String> fixColAndType = new LinkedList<>();
                        for(TableRegEntry tableRegEntry : tableRegistry.getEntries()){
                            for(ColumnTypePair cP : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()){
                                for(StringParameter sP : cP.getAltAliasPairs()){
                                    if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                        if(colAndType.get(0).contains(" "+sP.getValue())){ //Located parameter
                                            //Find in column Map
                                            for(Map.Entry<String, MyMap> mapEntry : operatorCastMap.entrySet()){ //Locate cast in cast map
                                                if(mapEntry.getKey().equals(currentOperatorNode.getOperatorName())){ //Located operator that performs cast
                                                    for(ColumnTypePair c : mapEntry.getValue().getColumnAndTypeList()){
                                                        if(c.getColumnName().equals(cP.getColumnName())){ //Located parameter col in its real alias
                                                            if(c.getLatestAltCastType().equals(colAndType.get(1))) { //Type is also correct
                                                                System.out.println("addPurgeSelectStatementToQuery: Located cast in castMap!");
                                                                currentOpQuery.addUsedColumn(cP.getColumnName(), latestTable1);
                                                                currentOpQuery.addUsedColumn(cP.getColumnName(), latestTable2);
                                                                System.out.println("addPurgeSelectStatementToQuery: Replacing parameter: " + sP.getValue() + " with: " + cP.getColumnName());
                                                                f.setName(c.getAltAliasPairs().get(0).getExtraValue());
                                                                String tempCol = colAndType.get(0).replace(" " + sP.getValue(), cP.getColumnName());
                                                                tempCol = tempCol + " as " + c.getAltAliasPairs().get(0).getExtraValue();
                                                                fixColAndType.add(tempCol);
                                                                fixColAndType.add(colAndType.get(1));
                                                                f.setType(fixColAndType.get(1));
                                                                newCols.add(f);
                                                                newSelectPhrases.add(tempCol);
                                                                replaceParam = true;
                                                                ready = true;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                                if(replaceParam) break;
                            }

                            if(replaceParam) break;
                        }

                        if(replaceParam == false){
                            System.out.println("addPurgeSelectStatementToQuery: Searching in aggrMap for parameter match... ");
                            for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                                for(StringParameter sP : aggPair.getAltAliasPairs()){
                                    if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                        if(colAndType.get(0).contains(" "+sP.getValue())){
                                            String properAlias = fetchlatestRequiredAncestorOfAggr(currentOperatorNode, currentOpQuery, aggPair.getColumnName(), aggPair.getAltAliasPairs(), sP.getValue());
                                            //Find in column Map
                                            for(Map.Entry<String, MyMap> mapEntry : operatorCastMap.entrySet()){ //Locate cast in cast map
                                                if(mapEntry.getKey().equals(currentOperatorNode.getOperatorName())){ //Located operator that performs cast
                                                    for(ColumnTypePair c : mapEntry.getValue().getColumnAndTypeList()){
                                                        if(c.getColumnName().equals(properAlias)){ //Located parameter col in its real alias
                                                            if(c.getLatestAltCastType().equals(colAndType.get(1))) { //Type is also correct
                                                                System.out.println("addPurgeSelectStatementToQuery: Located cast in castMap!");
                                                                currentOpQuery.addUsedColumn(properAlias, latestTable1);
                                                                currentOpQuery.addUsedColumn(properAlias, latestTable2);
                                                                System.out.println("addPurgeSelectStatementToQuery: Replacing parameter: " + sP.getValue() + " with: " + properAlias);
                                                                f.setName(c.getAltAliasPairs().get(0).getExtraValue());
                                                                String tempCol = colAndType.get(0).replace(" " + sP.getValue(), properAlias);
                                                                tempCol = tempCol + " as " + c.getAltAliasPairs().get(0).getExtraValue();
                                                                fixColAndType.add(tempCol);
                                                                fixColAndType.add(colAndType.get(1));
                                                                f.setType(fixColAndType.get(1));
                                                                newCols.add(f);
                                                                newSelectPhrases.add(tempCol);
                                                                replaceParam = true;
                                                                ready = true;
                                                                break;
                                                            }
                                                        }
                                                    }

                                                    if(ready) break;
                                                }
                                            }

                                            if(ready) break;
                                        }
                                    }

                                }

                                if(replaceParam) break;
                            }

                        }

                        if(replaceParam == false){
                            System.out.println("addPurgeSelectStatementToQuery: Failed to replace parameter of CAST() phrase!");
                            System.exit(0);
                        }
                    }
                    else{ //Normal Col
                        String colName = "";
                        String colType = "";
                        commaPart = commaPart.trim();
                        commaPart = commaPart.replace(" (type: ", " ");
                        String[] tempParts = commaPart.split(" ");
                        colName = tempParts[0];
                        colType = tempParts[1].replace(")", "");
                        System.out.println("addPurgeSelectStatementToQuery: Extracted pair: ("+colName+", "+colType+")");

                        boolean containsUDF = false;
                        String previousType = "";
                        if (colName.contains("UDFToLong(")) {
                            colName = clearColumnFromUDFToLong(colName);
                            colName = colName.replace("(", "");
                            colName = colName.replace(")", "");
                            previousType = "int";
                            containsUDF = true;
                        }
                        else if(colName.contains("upper(")){
                            colName = clearColumnFromUDFUpper(colName);
                            colName = colName.replace("(", "");
                            colName = colName.replace(")", "");
                            previousType = "string";
                            containsUDF = true;
                        }else if(colName.contains("lower(")){
                            colName = clearColumnFromUDFLower(colName);
                            colName = colName.replace("(", "");
                            colName = colName.replace(")", "");
                            previousType = "lower";
                            containsUDF = true;
                        }
                        else if(colName.contains("UDFToDouble(")){
                            colName = clearColumnFromUDFToDouble(colName);
                            colName = colName.replace("(", "");
                            colName = colName.replace(")", "");
                            previousType = "float";
                            containsUDF = true;
                        }

                        for(TableRegEntry tableRegEntry : tableRegistry.getEntries()){
                            for(ColumnTypePair p : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()){
                                if(p.getColumnType().equals(colType) || p.hasLatestAltCastType(colType)){
                                    if(p.getColumnName().equals(colName)){
                                        for(StringParameter sP : p.getAltAliasPairs()){
                                            if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                                currentOpQuery.addUsedColumn(p.getColumnName(), latestTable1);
                                                currentOpQuery.addUsedColumn(p.getColumnName(), latestTable2);
                                                f.setName(p.getColumnName());
                                                f.setType(p.getColumnType());
                                                newCols.add(f);
                                                newSelectPhrases.add(p.getColumnName());
                                                System.out.println("addPurgeSelectStatementToQuery: Discovered new REAL ALIAS FOR Select Column: "+colName+" alias is: ("+f.getName()+", "+f.getType()+") ");
                                                ready = true;
                                                break;
                                            }
                                        }
                                    }
                                    else{
                                        for(StringParameter sP : p.getAltAliasPairs()){
                                            if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                                if(sP.getValue().equals(colName)){
                                                    currentOpQuery.addUsedColumn(p.getColumnName(), latestTable1);
                                                    currentOpQuery.addUsedColumn(p.getColumnName(), latestTable2);
                                                    f.setName(p.getColumnName());
                                                    f.setType(p.getColumnType());
                                                    newCols.add(f);
                                                    newSelectPhrases.add(p.getColumnName());
                                                    System.out.println("addPurgeSelectStatementToQuery: Discovered new REAL ALIAS FOR Select Column: "+colName+" alias is: ("+f.getName()+", "+f.getType()+") ");
                                                    ready = true;
                                                    break;
                                                }
                                            }
                                        }
                                    }

                                    if(ready) break;
                                }
                            }

                            if(ready) break;
                        }

                        if(ready == false){
                            System.out.println("addPurgeSelectStatementToQuery: Checking if col: "+colName+" is aggregation...");
                            for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                                if( (aggPair.getColumnType().equals(colType)) || (aggPair.getLatestAltCastType().equals(colType))){
                                    for(StringParameter sP : aggPair.getAltAliasPairs()){
                                        if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                            if(sP.getValue().equals(colName)){
                                                String properAliasName = fetchlatestRequiredAncestorOfAggr(currentOperatorNode, currentOpQuery, aggPair.getColumnName(), aggPair.getAltAliasPairs(), colName);
                                                currentOpQuery.addUsedColumn(properAliasName, latestTable1);
                                                currentOpQuery.addUsedColumn(properAliasName, latestTable2);
                                                f.setName(properAliasName);
                                                f.setType(colType);
                                                newCols.add(f);
                                                newSelectPhrases.add(properAliasName);
                                                System.out.println("addPurgeSelectStatementToQuery: Discovered new REAL ALIAS FOR Select Column: "+colName+" alias is: ("+properAliasName+", "+f.getType()+") ");
                                                ready = true;
                                                break;
                                            }
                                        }
                                    }

                                    if(ready) break;
                                }
                            }
                        }

                        if(ready == false){
                            System.out.println("addPurgeSelectStatementToQuery: Column: "+colName+" never found a match...");
                            System.exit(0);
                        }
                    }
                }
            }
            else{ //Only 1 column
                FieldSchema f = new FieldSchema();
                boolean ready = false;
                originalSelectString = originalSelectString.trim();
                if(originalSelectString.contains("CAST(") && (originalSelectString.contains("avg(") == false)){ //Cast phrase
                    char[] castCommaPart = originalSelectString.toCharArray();
                    List<String> colAndType = new LinkedList<>();
                    fixColTypePairContainingCast(originalSelectString, colAndType);

                    boolean replaceParam = false;
                    List<String> fixColAndType = new LinkedList<>();
                    for(TableRegEntry tableRegEntry : tableRegistry.getEntries()){
                        for(ColumnTypePair cP : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()){
                            for(StringParameter sP : cP.getAltAliasPairs()){
                                if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                    if(colAndType.get(0).contains(" "+sP.getValue())){ //Located parameter
                                        //Find in column Map
                                        for(Map.Entry<String, MyMap> mapEntry : operatorCastMap.entrySet()){ //Locate cast in cast map
                                            if(mapEntry.getKey().equals(currentOperatorNode.getOperatorName())){ //Located operator that performs cast
                                                for(ColumnTypePair c : mapEntry.getValue().getColumnAndTypeList()){
                                                    if(c.getColumnName().equals(cP.getColumnName())){ //Located parameter col in its real alias
                                                        if(c.getLatestAltCastType().equals(colAndType.get(1))) { //Type is also correct
                                                            System.out.println("addPurgeSelectStatementToQuery: Located cast in castMap!");
                                                            currentOpQuery.addUsedColumn(cP.getColumnName(), latestTable1);
                                                            currentOpQuery.addUsedColumn(cP.getColumnName(), latestTable2);
                                                            System.out.println("addPurgeSelectStatementToQuery: Replacing parameter: " + sP.getValue() + " with: " + cP.getColumnName());
                                                            f.setName(c.getAltAliasPairs().get(0).getExtraValue());
                                                            String tempCol = colAndType.get(0).replace(" " + sP.getValue(), cP.getColumnName());
                                                            tempCol = tempCol + " as " + c.getAltAliasPairs().get(0).getExtraValue();
                                                            fixColAndType.add(tempCol);
                                                            fixColAndType.add(colAndType.get(1));
                                                            f.setType(fixColAndType.get(1));
                                                            newCols.add(f);
                                                            newSelectPhrases.add(tempCol);
                                                            replaceParam = true;
                                                            ready = true;
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            if(replaceParam) break;
                        }

                        if(replaceParam) break;
                    }

                    if(replaceParam == false){
                        System.out.println("addPurgeSelectStatementToQuery: Searching in aggrMap for parameter match... ");
                        for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                            for(StringParameter sP : aggPair.getAltAliasPairs()){
                                if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                    if(colAndType.get(0).contains(" "+sP.getValue())){
                                        String properAlias = fetchlatestRequiredAncestorOfAggr(currentOperatorNode, currentOpQuery, aggPair.getColumnName(), aggPair.getAltAliasPairs(), sP.getValue());
                                        //Find in column Map
                                        for(Map.Entry<String, MyMap> mapEntry : operatorCastMap.entrySet()){ //Locate cast in cast map
                                            if(mapEntry.getKey().equals(currentOperatorNode.getOperatorName())){ //Located operator that performs cast
                                                for(ColumnTypePair c : mapEntry.getValue().getColumnAndTypeList()){
                                                    if(c.getColumnName().equals(properAlias)){ //Located parameter col in its real alias
                                                        if(c.getLatestAltCastType().equals(colAndType.get(1))) { //Type is also correct
                                                            System.out.println("addPurgeSelectStatementToQuery: Located cast in castMap!");
                                                            currentOpQuery.addUsedColumn(properAlias, latestTable1);
                                                            currentOpQuery.addUsedColumn(properAlias, latestTable2);
                                                            System.out.println("addPurgeSelectStatementToQuery: Replacing parameter: " + sP.getValue() + " with: " + properAlias);
                                                            f.setName(c.getAltAliasPairs().get(0).getExtraValue());
                                                            String tempCol = colAndType.get(0).replace(" " + sP.getValue(), properAlias);
                                                            tempCol = tempCol + " as " + c.getAltAliasPairs().get(0).getExtraValue();
                                                            fixColAndType.add(tempCol);
                                                            fixColAndType.add(colAndType.get(1));
                                                            f.setType(fixColAndType.get(1));
                                                            newCols.add(f);
                                                            newSelectPhrases.add(tempCol);
                                                            replaceParam = true;
                                                            ready = true;
                                                            break;
                                                        }
                                                    }
                                                }

                                                if(ready) break;
                                            }
                                        }

                                        if(ready) break;
                                    }
                                }

                            }

                            if(replaceParam) break;
                        }

                    }

                    if(replaceParam == false){
                        System.out.println("addPurgeSelectStatementToQuery: Failed to replace parameter of CAST() phrase!");
                        System.exit(0);
                    }
                }
                else{ //Normal Col
                    String colName = "";
                    String colType = "";
                    originalSelectString = originalSelectString.trim();
                    originalSelectString = originalSelectString.replace(" (type: ", " ");
                    String[] tempParts = originalSelectString.split(" ");
                    colName = tempParts[0];
                    colType = tempParts[1].replace(")", "");
                    System.out.println("addPurgeSelectStatementToQuery: Extracted pair: ("+colName+", "+colType+")");

                    boolean containsUDF = false;
                    String previousType = "";
                    if (colName.contains("UDFToLong(")) {
                        colName = clearColumnFromUDFToLong(colName);
                        colName = colName.replace("(", "");
                        colName = colName.replace(")", "");
                        previousType = "int";
                        containsUDF = true;
                    }
                    else if(colName.contains("upper(")){
                        colName = clearColumnFromUDFUpper(colName);
                        colName = colName.replace("(", "");
                        colName = colName.replace(")", "");
                        previousType = "string";
                        containsUDF = true;
                    }else if(colName.contains("lower(")){
                        colName = clearColumnFromUDFLower(colName);
                        colName = colName.replace("(", "");
                        colName = colName.replace(")", "");
                        previousType = "lower";
                        containsUDF = true;
                    }
                    else if(colName.contains("UDFToDouble(")){
                        colName = clearColumnFromUDFToDouble(colName);
                        colName = colName.replace("(", "");
                        colName = colName.replace(")", "");
                        previousType = "float";
                        containsUDF = true;
                    }

                    for(TableRegEntry tableRegEntry : tableRegistry.getEntries()){
                        for(ColumnTypePair p : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()){
                            if(p.getColumnType().equals(colType) || p.hasLatestAltCastType(colType)){
                                if(p.getColumnName().equals(colName)){
                                    for(StringParameter sP : p.getAltAliasPairs()){
                                        if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                            currentOpQuery.addUsedColumn(p.getColumnName(), latestTable1);
                                            currentOpQuery.addUsedColumn(p.getColumnName(), latestTable2);
                                            f.setName(p.getColumnName());
                                            f.setType(p.getColumnType());
                                            newCols.add(f);
                                            newSelectPhrases.add(p.getColumnName());
                                            System.out.println("addPurgeSelectStatementToQuery: Discovered new REAL ALIAS FOR Select Column: "+colName+" alias is: ("+f.getName()+", "+f.getType()+") ");
                                            ready = true;
                                            break;
                                        }
                                    }
                                }
                                else{
                                    for(StringParameter sP : p.getAltAliasPairs()){
                                        if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                            if(sP.getValue().equals(colName)){
                                                currentOpQuery.addUsedColumn(p.getColumnName(), latestTable1);
                                                currentOpQuery.addUsedColumn(p.getColumnName(), latestTable2);
                                                f.setName(p.getColumnName());
                                                f.setType(p.getColumnType());
                                                newCols.add(f);
                                                newSelectPhrases.add(p.getColumnName());
                                                System.out.println("addPurgeSelectStatementToQuery: Discovered new REAL ALIAS FOR Select Column: "+colName+" alias is: ("+f.getName()+", "+f.getType()+") ");
                                                ready = true;
                                                break;
                                            }
                                        }
                                    }
                                }

                                if(ready) break;
                            }
                        }

                        if(ready) break;
                    }

                    if(ready == false){
                        System.out.println("addPurgeSelectStatementToQuery: Checking if col: "+colName+" is aggregation...");
                        for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                            if( (aggPair.getColumnType().equals(colType)) || (aggPair.getLatestAltCastType().equals(colType))){
                                for(StringParameter sP : aggPair.getAltAliasPairs()){
                                    if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                        if(sP.getValue().equals(colName)){
                                            String properAliasName = fetchlatestRequiredAncestorOfAggr(currentOperatorNode, currentOpQuery, aggPair.getColumnName(), aggPair.getAltAliasPairs(), colName);
                                            currentOpQuery.addUsedColumn(properAliasName, latestTable1);
                                            currentOpQuery.addUsedColumn(properAliasName, latestTable2);
                                            f.setName(properAliasName);
                                            f.setType(colType);
                                            newCols.add(f);
                                            newSelectPhrases.add(properAliasName);
                                            System.out.println("addPurgeSelectStatementToQuery: Discovered new REAL ALIAS FOR Select Column: "+colName+" alias is: ("+properAliasName+", "+f.getType()+") ");
                                            ready = true;
                                            break;
                                        }
                                    }
                                }

                                if(ready) break;
                            }
                        }
                    }

                    if(ready == false){
                        System.out.println("addPurgeSelectStatementToQuery: Column: "+colName+" never found a match...");
                        System.exit(0);
                    }
                }

            }


            String selectString = "";
            for(int i = 0; i < newSelectPhrases.size(); i++){
                if(i == newSelectPhrases.size() - 1){
                    selectString = selectString + " " + newSelectPhrases.get(i)+ " ";
                }
                else{
                    selectString = selectString + " " + newSelectPhrases.get(i) + ",";
                }

            }

            currentOpQuery.setLocalQueryString(" select"+selectString+afterFromPhrase);
            currentOpQuery.setExaremeQueryString(" select"+selectString+afterFromPhrase);

            System.out.println("addAnyExtraColsToOutputTable: Fixed Query: "+currentOpQuery.getLocalQueryString());

        }
        else{
            System.out.println("addAnyExtraColsToOutputTable: NO FROM statement in Query!");
            System.exit(0);
        }


        return newCols;

    }

        public String addSelectStatementToQuery(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, String latestAncestorSchema, OperatorQuery currentOpQuery, List<FieldSchema> usedCols, String latestTable1, String latestTable2){

        System.out.println("LATESTTABLE1: "+latestTable1+" - LATESTTABLE2: "+latestTable2);

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: addSelectStatementToQuery...");

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query does not contain SELECT statement (needs to be added)...");
        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Constructing SELECT statement columns...");
        MyMap aMap = new MyMap(false);

        String updatedSchemaString = extractColsFromTypeName(latestAncestorSchema, aMap, latestAncestorSchema, true);

        /*---Find real possible alias thourgh extra map for _col columns----*/
        for(ColumnTypePair pair : aMap.getColumnAndTypeList()){

            if(pair.getColumnName().contains("_col")){
                boolean foundAMatch = false;
                if(pair.getColumnType().equals("struct")){ //If it is an aggregation column
                    boolean structFound = false;
                    for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){ //Get an aggreagationPair from the List
                        for(StringParameter sP : aggPair.getAltAliasPairs()){ //Get its alt aliases
                            if(sP.getParemeterType().equals(currentOperatorNode.getOperatorName())){ //Locate the currentNode in the List

                                if(sP.getValue().equals(pair.getColumnName())) { //Locate the value as an altAlias in the List

                                    structFound = true;
                                    if(aggPair.getAltAliasPairs().size() == 1){ //If the altAlias is equal to the first altAlias in the List then we need the aggregation expression
                                        pair.setColumnName(aggPair.getColumnName() + " as " + aggPair.getAltAliasPairs().get(0).getExtraValue());
                                        foundAMatch = true;
                                    }
                                    else{ //The latest altAlias is different from the first - This means that we need the altAlias of the father to refer to
                                        String properAliasName = "";
                                        properAliasName = fetchlatestRequiredAncestorOfAggr(currentOperatorNode, currentOpQuery, aggPair.getColumnName(), aggPair.getAltAliasPairs(), pair.getColumnName());

                                        System.out.println("Real Alias for: "+pair.getColumnName()+" is: "+properAliasName);

                                        //if(properAliasName.equals(sP.getExtraValue()) == false){
                                        //    System.out.println("Value: "+properAliasName+" will get new Alias: "+sP.getExtraValue());
                                        //    properAliasName = properAliasName + " as " + sP.getExtraValue();

                                        //    String opName = extractOperatorNameFromAggrAlias(properAliasName);

                                        //    currentOpQuery.addUsedColumn(pair.getColumnName(), opName);

                                        //    if (usedCols != null) {
                                        //        FieldSchema f = new FieldSchema();
                                        //        f.setName(pair.getColumnName());
                                        //        f.setType(pair.getColumnType());
                                        //        usedCols.add(f);
                                        //    }

                                        //}
                                        pair.setColumnName(properAliasName);

                                        foundAMatch = true;
                                        //Attempt to add used column if not existing
                                        if(pair.getColumnName().contains(" as ") == false) {
                                            String opName = extractOperatorNameFromAggrAlias(properAliasName);

                                            currentOpQuery.addUsedColumn(pair.getColumnName(), opName);
                                            System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+pair.getColumnName()+" , "+opName+")");

                                            if (usedCols != null) {
                                                FieldSchema f = new FieldSchema();
                                                f.setName(pair.getColumnName());
                                                f.setType(pair.getColumnType());
                                                usedCols.add(f);
                                            }
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
                                            foundAMatch = true;
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

                        if(foundAMatch == false){ //Extra search in aggrMap
                            for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){ //Get an aggreagationPair from the List
                                for(StringParameter sP : aggPair.getAltAliasPairs()){ //Get its alt aliases
                                    if(sP.getParemeterType().equals(currentOperatorNode.getOperatorName())){ //Locate the currentNode in the List

                                        if(sP.getValue().equals(pair.getColumnName())) { //Locate the value as an altAlias in the List

                                            if(aggPair.getAltAliasPairs().size() == 1){ //If the altAlias is equal to the first altAlias in the List then we need the aggregation expression
                                                pair.setColumnName(aggPair.getColumnName() + " as " + aggPair.getAltAliasPairs().get(0).getExtraValue());
                                                foundAMatch = true;
                                            }
                                            else{ //The latest altAlias is different from the first - This means that we need the altAlias of the father to refer to
                                                String properAliasName = "";
                                                properAliasName = fetchlatestRequiredAncestorOfAggr(currentOperatorNode, currentOpQuery, aggPair.getColumnName(), aggPair.getAltAliasPairs(), pair.getColumnName());

                                                System.out.println("Real Alias for: "+pair.getColumnName()+" is: "+properAliasName);

                                                //if(properAliasName.equals(sP.getExtraValue()) == false){
                                                 //   System.out.println("Value: "+properAliasName+" will get new Alias: "+sP.getExtraValue());
                                                //    properAliasName = properAliasName + " as " + sP.getExtraValue();

                                                //    String opName = extractOperatorNameFromAggrAlias(properAliasName);

                                                //    currentOpQuery.addUsedColumn(pair.getColumnName(), opName);

                                                //    if (usedCols != null) {
                                                //        FieldSchema f = new FieldSchema();
                                                //        f.setName(pair.getColumnName());
                                                //        f.setType(pair.getColumnType());
                                                //        usedCols.add(f);
                                                //    }

                                                //}
                                                pair.setColumnName(properAliasName);

                                                foundAMatch = true;

                                                //Attempt to add used column if not existing
                                                if(pair.getColumnName().contains(" as ") == false) {
                                                    String opName = extractOperatorNameFromAggrAlias(properAliasName);

                                                    currentOpQuery.addUsedColumn(pair.getColumnName(), opName);
                                                    System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+pair.getColumnName()+" , "+opName+")");

                                                    if (usedCols != null) {
                                                        FieldSchema f = new FieldSchema();
                                                        f.setName(pair.getColumnName());
                                                        f.setType(pair.getColumnType());
                                                        usedCols.add(f);
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
            else{
                boolean checkColumnIsValid = false;

                for(TableRegEntry tableRegEntry : tableRegistry.getEntries()){
                    for(ColumnTypePair cP : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()){
                        if(cP.getColumnType().equals(pair.getColumnType()) || (cP.getLatestAltCastType().equals(pair.getColumnType()))){
                            if(cP.getColumnName().equals(pair.getColumnName())){
                                //Check if currentOperator CAST this column
                                //for()
                                String extraAlias = "";
                                if(tableRegEntry.getAlias().equals(tableRegEntry.getAssociatedTable().getTableName()) == false){
                                   extraAlias = tableRegEntry.getAlias() + ".";
                                }
                                pair.setColumnName(extraAlias+pair.getColumnName());

                                if(cP.getLatestAltCastType().equals(pair.getColumnType())){
                                    if(pair.getColumnType().equals(cP.getColumnType()) == false) {
                                        System.out.println("addSelectStatementToQuery: Equal to due alt Cast type...Look in castMap...");
                                        boolean foundCast = false;
                                        for (Map.Entry<String, MyMap> entry : operatorCastMap.entrySet()) {
                                            if (entry.getKey().equals(currentOperatorNode.getOperatorName())) {
                                                for (ColumnTypePair entryCol : entry.getValue().getColumnAndTypeList()) {
                                                    if (entryCol.getColumnName().equals(cP.getColumnName())) {
                                                        pair.setColumnName(entryCol.getAltAliasPairs().get(0).getExtraValue());
                                                        System.out.println("addSelectStatementToQuery: FIX - " + cP.getColumnName() + " becomes: " + entryCol.getAltAliasPairs().get(0).getExtraValue());
                                                        foundCast = true;
                                                        break;
                                                    }
                                                }

                                                if(foundCast) break;
                                            }
                                        }
                                    }
                                }

                                for(StringParameter altAlias : cP.getAltAliasPairs()){
                                    if(otherFatherNode != null){
                                        if(altAlias.getParemeterType().equals(otherFatherNode.getOperatorName())){
                                            currentOpQuery.addUsedColumn(pair.getColumnName(), latestTable1);
                                            currentOpQuery.addUsedColumn(pair.getColumnName(), latestTable2);
                                        }
                                    }
                                    else if(fatherOperatorNode != null){
                                        if(altAlias.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                            currentOpQuery.addUsedColumn(pair.getColumnName(), latestTable1);
                                            currentOpQuery.addUsedColumn(pair.getColumnName(), latestTable2);
                                        }
                                    }
                                }

                                if (usedCols != null) {
                                    FieldSchema f = new FieldSchema();
                                    f.setName(pair.getColumnName());
                                    f.setType(pair.getColumnType());
                                    usedCols.add(f);
                                }

                                checkColumnIsValid = true;
                                break;
                            }
                            else{
                                for(StringParameter sP : cP.getAltAliasPairs()){
                                    if(sP.getParemeterType().equals(currentOperatorNode.getOperatorName())){
                                        if(sP.getValue().equals(pair.getColumnName())){
                                            String extraAlias = "";
                                            if(tableRegEntry.getAlias().equals(tableRegEntry.getAssociatedTable().getTableName()) == false){
                                                extraAlias = tableRegEntry.getAlias() + ".";
                                            }
                                            pair.setColumnName(extraAlias+pair.getColumnName());

                                            if(cP.getLatestAltCastType().equals(pair.getColumnType())){
                                                if(pair.getColumnType().equals(cP.getColumnType()) == false) {
                                                    for (Map.Entry<String, MyMap> entry : operatorCastMap.entrySet()) {
                                                        if (entry.getKey().equals(currentOperatorNode.getOperatorName())) {
                                                            for (ColumnTypePair entryCol : entry.getValue().getColumnAndTypeList()) {
                                                                if (entryCol.getColumnName().equals(cP.getColumnName())) {
                                                                    pair.setColumnName(entryCol.getAltAliasPairs().get(0).getExtraValue());
                                                                    System.out.println("addSelectStatementToQuery: FIX - " + cP.getColumnName() + " becomes: " + entryCol.getAltAliasPairs().get(0).getExtraValue());
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }

                                            for(StringParameter altAlias : cP.getAltAliasPairs()){
                                                if(otherFatherNode != null){
                                                    if(altAlias.getParemeterType().equals(otherFatherNode.getOperatorName())){
                                                        currentOpQuery.addUsedColumn(pair.getColumnName(), latestTable1);
                                                        currentOpQuery.addUsedColumn(pair.getColumnName(), latestTable2);
                                                    }
                                                }
                                                else if(fatherOperatorNode != null){
                                                    if(altAlias.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                                        currentOpQuery.addUsedColumn(pair.getColumnName(), latestTable1);
                                                        currentOpQuery.addUsedColumn(pair.getColumnName(), latestTable2);
                                                        //System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+pair.getColumnName()+" , "+latestTable1+")");
                                                    }
                                                }
                                            }

                                            if (usedCols != null) {
                                                FieldSchema f = new FieldSchema();
                                                f.setName(pair.getColumnName());
                                                f.setType(pair.getColumnType());
                                                usedCols.add(f);
                                            }

                                            checkColumnIsValid = true;
                                            break;
                                        }
                                    }
                                }

                                if(checkColumnIsValid) break;
                            }
                        }
                    }

                    if(checkColumnIsValid) break;
                }

                if(checkColumnIsValid == false){
                    //Check in aggregation map then
                    for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){ //Get an aggreagationPair from the List
                        for(StringParameter sP : aggPair.getAltAliasPairs()){ //Get its alt aliases
                            if(sP.getParemeterType().equals(currentOperatorNode.getOperatorName())){ //Locate the currentNode in the List

                                if(sP.getValue().equals(pair.getColumnName())) { //Locate the value as an altAlias in the List

                                    if(aggPair.getAltAliasPairs().size() == 1){ //If the altAlias is equal to the first altAlias in the List then we need the aggregation expression
                                        pair.setColumnName(aggPair.getColumnName() + " as " + aggPair.getAltAliasPairs().get(0).getExtraValue());
                                        checkColumnIsValid = true;
                                    }
                                    else{ //The latest altAlias is different from the first - This means that we need the altAlias of the father to refer to
                                        String properAliasName = "";
                                        properAliasName = fetchlatestRequiredAncestorOfAggr(currentOperatorNode, currentOpQuery, aggPair.getColumnName(), aggPair.getAltAliasPairs(), pair.getColumnName());

                                        System.out.println("Real Alias for: "+pair.getColumnName()+" is: "+properAliasName);

                                        //if(properAliasName.equals(sP.getExtraValue()) == false){
                                        //    System.out.println("Value: "+properAliasName+" will get new Alias: "+sP.getExtraValue());
                                        //    properAliasName = properAliasName + " as " + sP.getExtraValue();

                                        //    String opName = extractOperatorNameFromAggrAlias(properAliasName);

                                        //    currentOpQuery.addUsedColumn(pair.getColumnName(), opName);

                                        //    if (usedCols != null) {
                                        //        FieldSchema f = new FieldSchema();
                                        //        f.setName(pair.getColumnName());
                                        //        f.setType(pair.getColumnType());
                                        //        usedCols.add(f);
                                        //    }

                                        //}

                                        pair.setColumnName(properAliasName);

                                        checkColumnIsValid = true;
                                        //Attempt to add used column if not existing
                                        if(pair.getColumnName().contains(" as ") == false) {
                                            String opName = extractOperatorNameFromAggrAlias(properAliasName);

                                            currentOpQuery.addUsedColumn(pair.getColumnName(), opName);
                                            System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+pair.getColumnName()+" , "+opName+")");

                                            if (usedCols != null) {
                                                FieldSchema f = new FieldSchema();
                                                f.setName(pair.getColumnName());
                                                f.setType(pair.getColumnType());
                                                usedCols.add(f);
                                            }
                                        }

                                    }

                                }

                            }
                        }
                    }

                }

                if(checkColumnIsValid == false){
                    System.out.println("Alias: "+pair.getColumnName()+" is not valid!");
                    System.exit(0);
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

        currentOpQuery.setLocalQueryString(" select"+selectString+currentOpQuery.getLocalQueryString());
        currentOpQuery.setExaremeQueryString(" select"+selectString+currentOpQuery.getExaremeQueryString());

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Query is currently: ["+currentOpQuery.getLocalQueryString()+"]");

        return updatedSchemaString;

    }

    //TODO FIND BUG THAT MAKES DIFFERENT FROM MAPJOIN KEYS
    public void handleJoinKeys(OperatorNode currentOperatorNode, OperatorQuery currentOpQuery, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, List<String> joinColumns){

            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: handleJoinKeys...");

            JoinOperator joinOp = (JoinOperator) currentOperatorNode.getOperator();
            JoinDesc joinDesc = joinOp.getConf();

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing keys of Join...");
        Map<Byte, String> keys = joinDesc.getKeysString();

        boolean nullValues = false;

        if (keys != null) {
            //TODO: HANDLE COLS WITH SAME NAME ON DIFFERENT TABLES

            for (Map.Entry<Byte, String> entry : keys.entrySet()) {
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Accessing keys of MapJoin...");
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Key: " + entry.getKey() + " : Value: " + entry.getValue());

                if (entry.getValue() == null) {
                    nullValues = true;
                    break;
                }

                String fixedValueString = entry.getValue();

                if (entry.getValue().contains(", ")) { //MultiKey Join
                                                       //Join Keys will form an List with the all the keys of the first table first and of the second following
                    String multiKeysString = entry.getValue();

                    String[] nameTypePairs = multiKeysString.split(",");

                    for(String nameTypePair : nameTypePairs){

                        nameTypePair = nameTypePair.trim();

                        boolean containsUDF = false;

                        if (nameTypePair.contains("UDFToLong(")) {
                            nameTypePair = clearColumnFromUDFToLong(nameTypePair);
                            containsUDF = true;
                        }
                        else if(nameTypePair.contains("upper(")){
                            nameTypePair = clearColumnFromUDFUpper(nameTypePair);
                            containsUDF = true;
                        }else if(nameTypePair.contains("lower(")){
                            nameTypePair = clearColumnFromUDFLower(nameTypePair);
                            containsUDF = true;
                        }
                        else if(fixedValueString.contains("UDFToDouble(")){
                            nameTypePair = clearColumnFromUDFToDouble(nameTypePair);
                            containsUDF = true;
                        }


                        String[] parts = nameTypePair.split(" ");
                        if (parts.length != 3) {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Reading value info...Failed! Parts size: " + parts.length);
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Parts: " + parts.toString());
                            System.exit(0);
                        }
                        String columnName = parts[0];

                        if(containsUDF){
                            if(columnName.contains("(")) columnName = columnName.replace("(" , "");
                            if(columnName.contains(")")) columnName = columnName.replace(")", "");
                        }

                        String columnType = "";
                        if (parts[2].contains(")")) {
                            columnType = parts[2].replace(")", "");

                            if (containsUDF) {
                                for (TableRegEntry tableReg : tableRegistry.getEntries()) {
                                    for (ColumnTypePair cP : tableReg.getColumnTypeMap().getColumnAndTypeList()) {
                                        if (columnType.equals("bigint")) {
                                            if (cP.getColumnType().equals("int")) {
                                                if (cP.getColumnName().equals(columnName)) {
                                                    for (StringParameter sP : cP.getAltAliasPairs()) {

                                                        if ((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()))) {
                                                            cP.addCastType(columnType);
                                                        } else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                                            cP.addCastType(columnType);
                                                        }

                                                    }
                                                } else {

                                                    for (StringParameter sP : cP.getAltAliasPairs()) {

                                                        if (sP.getValue().equals(columnName)) {

                                                            if ((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()))) {
                                                                cP.addCastType(columnType);
                                                            } else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                                                cP.addCastType(columnType);
                                                            }

                                                        }

                                                    }

                                                }
                                            }

                                            columnType = "int";
                                        }
                                        else if (columnType.equals("double")) {
                                            if (cP.getColumnType().equals("float")) {
                                                if (cP.getColumnName().equals(columnName)) {
                                                    for (StringParameter sP : cP.getAltAliasPairs()) {

                                                        if ((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()))) {
                                                            cP.addCastType(columnType);
                                                        } else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                                            cP.addCastType(columnType);
                                                        }

                                                    }
                                                } else {

                                                    for (StringParameter sP : cP.getAltAliasPairs()) {

                                                        if (sP.getValue().equals(columnName)) {

                                                            if ((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()))) {
                                                                cP.addCastType(columnType);
                                                            } else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                                                cP.addCastType(columnType);
                                                            }

                                                        }

                                                    }

                                                }
                                            }

                                            columnType = "float";

                                        }

                                    }
                                }

                            }

                        } else {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": ColumnType does not contain )");
                            System.exit(0);
                        }

                        //Located Value
                        boolean valueLocated = false;
                        String realValueName = "";

                        for (TableRegEntry regEntry : tableRegistry.getEntries()) {
                            for (ColumnTypePair p : regEntry.getColumnTypeMap().getColumnAndTypeList()) {
                                if (columnType.equals(p.getColumnType())) {
                                    if (p.getColumnName().equals(columnName)) {
                                        List<StringParameter> altAliases = p.getAltAliasPairs();
                                        for (StringParameter sP : altAliases) {
                                            if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                                valueLocated = true;
                                                realValueName = columnName;
                                                joinColumns.add(realValueName);

                                                //Add JOIN USED Column
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + fatherOperatorNode);
                                                currentOpQuery.addUsedColumn(p.getColumnName(), fatherOperatorNode.getOperatorName());
                                                System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+p.getColumnName()+" , "+fatherOperatorNode.getOperatorName()+")");

                                                break;
                                            } else if (sP.getParemeterType().equals(otherFatherNode.getOperatorName())) {
                                                valueLocated = true;
                                                realValueName = columnName;
                                                joinColumns.add(realValueName);

                                                //Add JOIN USED Column
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + otherFatherNode);
                                                currentOpQuery.addUsedColumn(p.getColumnName(), otherFatherNode.getOperatorName());
                                                System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+p.getColumnName()+" , "+otherFatherNode.getOperatorName()+")");
                                                break;
                                            }
                                        }

                                        if (valueLocated == true) break;

                                    } else {
                                        List<StringParameter> altAliases = p.getAltAliasPairs();
                                        for (StringParameter sP : altAliases) {
                                            if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) { //Father1 Exists as AltAlias for this Column
                                                if (sP.getValue().equals(columnName)) { //Father holds the value of this key
                                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Located match through: " + fatherOperatorNode.getOperatorName());
                                                    realValueName = p.getColumnName();
                                                    valueLocated = true;
                                                    joinColumns.add(realValueName);

                                                    //Add JOIN USED Column
                                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + fatherOperatorNode);
                                                    currentOpQuery.addUsedColumn(p.getColumnName(), fatherOperatorNode.getOperatorName());
                                                    System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+p.getColumnName()+" , "+fatherOperatorNode.getOperatorName()+")");

                                                    break;
                                                }
                                            } else if (sP.getParemeterType().equals(otherFatherNode.getOperatorName())) {
                                                if (sP.getValue().equals(columnName)) { //Father holds the value of this key
                                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Located match through: " + otherFatherNode.getOperatorName());
                                                    realValueName = p.getColumnName();
                                                    valueLocated = true;
                                                    joinColumns.add(realValueName);

                                                    //Add JOIN USED Column
                                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + otherFatherNode);
                                                    currentOpQuery.addUsedColumn(p.getColumnName(), otherFatherNode.getOperatorName());
                                                    System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+p.getColumnName()+" , "+otherFatherNode.getOperatorName()+")");

                                                    break;
                                                }
                                            }
                                        }

                                        if (valueLocated == true) break;
                                    }
                                }
                            }

                            if (valueLocated == true) break;
                        }

                        if (valueLocated == false) {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Failed to locate the real alias of: " + columnName);
                            System.exit(0);
                        } else {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Real alias of: " + columnName + " is: " + realValueName);
                        }

                    }


                } else { //Singe Key Join

                    boolean containsUDF = false;

                    if (fixedValueString.contains("UDFToLong(")) {
                        fixedValueString = clearColumnFromUDFToLong(fixedValueString);
                        containsUDF = true;
                    }
                    else if(fixedValueString.contains("upper(")){
                        fixedValueString = clearColumnFromUDFUpper(fixedValueString);
                        containsUDF = true;
                    }else if(fixedValueString.contains("lower(")){
                        fixedValueString = clearColumnFromUDFLower(fixedValueString);
                        containsUDF = true;
                    }
                    else if(fixedValueString.contains("UDFToDouble(")){
                        fixedValueString = clearColumnFromUDFToDouble(fixedValueString);
                        containsUDF = true;
                    }

                    String[] parts = fixedValueString.split(" ");
                    if (parts.length != 3) {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Reading value info...Failed! Parts size: " + parts.length);
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Parts: " + parts);
                        System.exit(0);
                    }
                    String columnName = parts[0];
                    String columnType = "";

                    if(containsUDF){
                        if(columnName.contains("(")) columnName = columnName.replace("(" , "");
                        if(columnName.contains(")")) columnName = columnName.replace(")", "");
                    }

                    if (parts[2].contains(")")) {
                        columnType = parts[2].replace(")", "");

                        if (entry.getValue().contains("UDFToLong(")) {
                            for (TableRegEntry tableReg : tableRegistry.getEntries()) {
                                for (ColumnTypePair cP : tableReg.getColumnTypeMap().getColumnAndTypeList()) {
                                    if (columnType.equals("bigint")) {
                                        if (cP.getColumnType().equals("int")) {
                                            if (cP.getColumnName().equals(columnName)) {
                                                for (StringParameter sP : cP.getAltAliasPairs()) {

                                                    if ((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()))) {
                                                        cP.addCastType(columnType);
                                                    } else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                                        cP.addCastType(columnType);
                                                    }

                                                }
                                            } else {

                                                for (StringParameter sP : cP.getAltAliasPairs()) {

                                                    if (sP.getValue().equals(columnName)) {

                                                        if ((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()))) {
                                                            cP.addCastType(columnType);
                                                        } else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                                            cP.addCastType(columnType);
                                                        }

                                                    }

                                                }

                                            }
                                        }

                                    }

                                }
                            }

                            columnType = "int";

                        }
                        else if (entry.getValue().contains("UDFToDouble(")) {
                            for (TableRegEntry tableReg : tableRegistry.getEntries()) {
                                for (ColumnTypePair cP : tableReg.getColumnTypeMap().getColumnAndTypeList()) {
                                    if (columnType.equals("double")) {
                                        if (cP.getColumnType().equals("float")) {
                                            if (cP.getColumnName().equals(columnName)) {
                                                for (StringParameter sP : cP.getAltAliasPairs()) {

                                                    if ((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()))) {
                                                        cP.addCastType(columnType);
                                                    } else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                                        cP.addCastType(columnType);
                                                    }

                                                }
                                            } else {

                                                for (StringParameter sP : cP.getAltAliasPairs()) {

                                                    if (sP.getValue().equals(columnName)) {

                                                        if ((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()))) {
                                                            cP.addCastType(columnType);
                                                        } else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                                            cP.addCastType(columnType);
                                                        }

                                                    }

                                                }

                                            }
                                        }

                                    }

                                }
                            }

                            columnType = "float";

                        }

                    } else {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": ColumnType does not contain )");
                        System.exit(0);
                    }

                    //Located Value
                    boolean valueLocated = false;
                    String realValueName = "";

                    for (TableRegEntry regEntry : tableRegistry.getEntries()) {
                        for (ColumnTypePair p : regEntry.getColumnTypeMap().getColumnAndTypeList()) {
                            if (columnType.equals(p.getColumnType())) {
                                if (p.getColumnName().equals(columnName)) {
                                    List<StringParameter> altAliases = p.getAltAliasPairs();
                                    for (StringParameter sP : altAliases) {
                                        if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                            valueLocated = true;
                                            realValueName = columnName;
                                            joinColumns.add(realValueName);

                                            //Add JOIN USED Column
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + fatherOperatorNode);
                                            currentOpQuery.addUsedColumn(p.getColumnName(), fatherOperatorNode.getOperatorName());
                                            System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+p.getColumnName()+" , "+ fatherOperatorNode.getOperatorName()+")");
                                            break;
                                        } else if (sP.getParemeterType().equals(otherFatherNode.getOperatorName())) {
                                            valueLocated = true;
                                            realValueName = columnName;
                                            joinColumns.add(realValueName);

                                            //Add JOIN USED Column
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + otherFatherNode);
                                            currentOpQuery.addUsedColumn(p.getColumnName(), otherFatherNode.getOperatorName());
                                            System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+p.getColumnName()+" , "+otherFatherNode.getOperatorName()+")");
                                            break;
                                        }
                                    }

                                    if (valueLocated == true) break;

                                } else {
                                    List<StringParameter> altAliases = p.getAltAliasPairs();
                                    for (StringParameter sP : altAliases) {
                                        if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) { //Father1 Exists as AltAlias for this Column
                                            if (sP.getValue().equals(columnName)) { //Father holds the value of this key
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Located match through: " + fatherOperatorNode.getOperatorName());
                                                realValueName = p.getColumnName();
                                                valueLocated = true;
                                                joinColumns.add(realValueName);

                                                //Add JOIN USED Column
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + fatherOperatorNode);
                                                currentOpQuery.addUsedColumn(p.getColumnName(), fatherOperatorNode.getOperatorName());
                                                System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+p.getColumnName()+" , "+fatherOperatorNode.getOperatorName()+")");

                                                break;
                                            }
                                        } else if (sP.getParemeterType().equals(otherFatherNode.getOperatorName())) {
                                            if (sP.getValue().equals(columnName)) { //Father holds the value of this key
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Located match through: " + otherFatherNode.getOperatorName());
                                                realValueName = p.getColumnName();
                                                valueLocated = true;
                                                joinColumns.add(realValueName);

                                                //Add JOIN USED Column
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + otherFatherNode);
                                                currentOpQuery.addUsedColumn(p.getColumnName(), otherFatherNode.getOperatorName());
                                                System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+p.getColumnName()+" , "+otherFatherNode.getOperatorName()+")");

                                                break;
                                            }
                                        }
                                    }

                                    if (valueLocated == true) break;
                                }
                            }
                        }

                        if (valueLocated == true) break;
                    }

                    if (valueLocated == false) {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Failed to locate the real alias of: " + columnName);
                        System.exit(0);
                    } else {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Real alias of: " + columnName + " is: " + realValueName);
                    }

                }

            }

            //Same alias for the 2 keys
            if(joinColumns.size() > 0){
                if(joinColumns.size() == 2) {
                    if (joinColumns.get(0).equals(joinColumns.get(1))) {
                        List<String> joinColumnsFixed = new LinkedList<>();
                        joinColumnsFixed.add(fatherOperatorNode.getOperatorName() + "." + joinColumns.get(0));
                        joinColumnsFixed.add(otherFatherNode.getOperatorName() + "." + joinColumns.get(1));

                        joinColumns = joinColumnsFixed;

                    }
                }
                else{ //Multiple join keys that might have same column name but belong to different tables
                    if(joinColumns.size() % 2 == 1){
                        System.out.println("handleJoinKeys: ODD NUMBER OF JOIN KEYS! ERROR! JoinColumns: "+joinColumns.toString());
                        System.exit(0);
                    }
                    else{
                        List<String> joinColumnsFixed = new LinkedList<>();
                        List<String> halfJoinColumns1 = new LinkedList<>();
                        List<String> halfJoinColumns2 = new LinkedList<>();
                        for(int j = 0; j < joinColumns.size() / 2; j++){
                            if(joinColumns.get(j).equals(joinColumns.get(j + (joinColumns.size() / 2) ))){
                                halfJoinColumns1.add(fatherOperatorNode.getOperatorName() + "." + joinColumns.get(j));
                                halfJoinColumns2.add(otherFatherNode.getOperatorName() + "." + joinColumns.get(j));
                            }
                            else{
                                halfJoinColumns1.add(joinColumns.get(j));
                                halfJoinColumns2.add(joinColumns.get(j + (joinColumns.size() / 2) ));
                            }
                        }

                        if(halfJoinColumns1.size() != halfJoinColumns2.size()){
                            System.out.println("handleJoinKeys: halfJoinLists are not equal in size! LIST1: "+halfJoinColumns1.toString() + " - LIST2: "+halfJoinColumns2);
                            System.exit(0);
                        }

                        for(int j = 0; j < halfJoinColumns1.size(); j++){
                            joinColumnsFixed.add(halfJoinColumns1.get(j));
                        }

                        for(int j = 0; j < halfJoinColumns2.size(); j++){
                            joinColumnsFixed.add(halfJoinColumns2.get(j));
                        }

                        joinColumns = joinColumnsFixed;
                    }
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
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Accessing keys of MapJoin...");
                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Key: " + entry.getKey() + " : Value: " + entry.getValue());

                if (entry.getValue() == null) {
                    nullValues = true;
                    break;
                }

                String fixedValueString = entry.getValue();

                if (entry.getValue().contains(", ")) { //MultiKey Join
                    //Join Keys will form an List with the all the keys of the first table first and of the second following
                    String multiKeysString = entry.getValue();

                    String[] nameTypePairs = multiKeysString.split(",");

                    for(String nameTypePair : nameTypePairs){

                        nameTypePair = nameTypePair.trim();

                        boolean containsUDF = false;

                        if (nameTypePair.contains("UDFToLong(")) {
                            nameTypePair = clearColumnFromUDFToLong(nameTypePair);
                            containsUDF = true;
                        }
                        else if(nameTypePair.contains("upper(")){
                            nameTypePair = clearColumnFromUDFUpper(nameTypePair);
                            containsUDF = true;
                        }else if(nameTypePair.contains("lower(")){
                            nameTypePair = clearColumnFromUDFLower(nameTypePair);
                            containsUDF = true;
                        }
                        else if(nameTypePair.contains("UDFToDouble(")){
                            nameTypePair = clearColumnFromUDFToDouble(nameTypePair);
                            containsUDF = true;
                        }


                        String[] parts = nameTypePair.split(" ");
                        if (parts.length != 3) {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Reading value info...Failed! Parts size: " + parts.length);
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Parts: " + parts.toString());
                            System.exit(0);
                        }
                        String columnName = parts[0];

                        if(containsUDF){
                            if(columnName.contains("(")) columnName = columnName.replace("(" , "");
                            if(columnName.contains(")")) columnName = columnName.replace(")", "");
                        }

                        String columnType = "";
                        if (parts[2].contains(")")) {
                            columnType = parts[2].replace(")", "");

                            if (containsUDF) {
                                for (TableRegEntry tableReg : tableRegistry.getEntries()) {
                                    for (ColumnTypePair cP : tableReg.getColumnTypeMap().getColumnAndTypeList()) {
                                        if (columnType.equals("bigint")) {
                                            if (cP.getColumnType().equals("int")) {
                                                if (cP.getColumnName().equals(columnName)) {
                                                    for (StringParameter sP : cP.getAltAliasPairs()) {

                                                        if ((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()))) {
                                                            cP.addCastType(columnType);
                                                        } else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                                            cP.addCastType(columnType);
                                                        }

                                                    }
                                                } else {

                                                    for (StringParameter sP : cP.getAltAliasPairs()) {

                                                        if (sP.getValue().equals(columnName)) {

                                                            if ((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()))) {
                                                                cP.addCastType(columnType);
                                                            } else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                                                cP.addCastType(columnType);
                                                            }

                                                        }

                                                    }

                                                }
                                            }

                                            columnType = "int";
                                        }
                                        else if (columnType.equals("double")) {
                                            if (cP.getColumnType().equals("float")) {
                                                if (cP.getColumnName().equals(columnName)) {
                                                    for (StringParameter sP : cP.getAltAliasPairs()) {

                                                        if ((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()))) {
                                                            cP.addCastType(columnType);
                                                        } else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                                            cP.addCastType(columnType);
                                                        }

                                                    }
                                                } else {

                                                    for (StringParameter sP : cP.getAltAliasPairs()) {

                                                        if (sP.getValue().equals(columnName)) {

                                                            if ((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()))) {
                                                                cP.addCastType(columnType);
                                                            } else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                                                cP.addCastType(columnType);
                                                            }

                                                        }

                                                    }

                                                }
                                            }

                                            columnType = "float";

                                        }

                                    }
                                }

                            }

                        } else {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": ColumnType does not contain )");
                            System.exit(0);
                        }

                        //Located Value
                        boolean valueLocated = false;
                        String realValueName = "";

                        for (TableRegEntry regEntry : tableRegistry.getEntries()) {
                            for (ColumnTypePair p : regEntry.getColumnTypeMap().getColumnAndTypeList()) {
                                if (columnType.equals(p.getColumnType())) {
                                    if (p.getColumnName().equals(columnName)) {
                                        List<StringParameter> altAliases = p.getAltAliasPairs();
                                        for (StringParameter sP : altAliases) {
                                            if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                                valueLocated = true;
                                                realValueName = columnName;
                                                joinColumns.add(realValueName);

                                                //Add JOIN USED Column
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + fatherOperatorNode);
                                                currentOpQuery.addUsedColumn(p.getColumnName(), fatherOperatorNode.getOperatorName());
                                                System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+p.getColumnName()+" , "+fatherOperatorNode.getOperatorName()+")");
                                                break;
                                            } else if (sP.getParemeterType().equals(otherFatherNode.getOperatorName())) {
                                                valueLocated = true;
                                                realValueName = columnName;
                                                joinColumns.add(realValueName);

                                                //Add JOIN USED Column
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + otherFatherNode);
                                                currentOpQuery.addUsedColumn(p.getColumnName(), otherFatherNode.getOperatorName());
                                                System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+p.getColumnName()+" , "+otherFatherNode.getOperatorName()+")");
                                                break;
                                            }
                                        }

                                        if (valueLocated == true) break;

                                    } else {
                                        List<StringParameter> altAliases = p.getAltAliasPairs();
                                        for (StringParameter sP : altAliases) {
                                            if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) { //Father1 Exists as AltAlias for this Column
                                                if (sP.getValue().equals(columnName)) { //Father holds the value of this key
                                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Located match through: " + fatherOperatorNode.getOperatorName());
                                                    realValueName = p.getColumnName();
                                                    valueLocated = true;
                                                    joinColumns.add(realValueName);

                                                    //Add JOIN USED Column
                                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + fatherOperatorNode);
                                                    currentOpQuery.addUsedColumn(p.getColumnName(), fatherOperatorNode.getOperatorName());
                                                    System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+p.getColumnName()+" , "+fatherOperatorNode.getOperatorName()+")");

                                                    break;
                                                }
                                            } else if (sP.getParemeterType().equals(otherFatherNode.getOperatorName())) {
                                                if (sP.getValue().equals(columnName)) { //Father holds the value of this key
                                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Located match through: " + otherFatherNode.getOperatorName());
                                                    realValueName = p.getColumnName();
                                                    valueLocated = true;
                                                    joinColumns.add(realValueName);

                                                    //Add JOIN USED Column
                                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + otherFatherNode);
                                                    currentOpQuery.addUsedColumn(p.getColumnName(), otherFatherNode.getOperatorName());
                                                    System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+p.getColumnName()+" , "+otherFatherNode.getOperatorName()+")");

                                                    break;
                                                }
                                            }
                                        }

                                        if (valueLocated == true) break;
                                    }
                                }
                            }

                            if (valueLocated == true) break;
                        }

                        if (valueLocated == false) {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Failed to locate the real alias of: " + columnName);
                            System.exit(0);
                        } else {
                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Real alias of: " + columnName + " is: " + realValueName);
                        }

                    }


                } else { //Singe Key Join

                    boolean containsUDF = false;

                    if (fixedValueString.contains("UDFToLong(")) {
                        fixedValueString = clearColumnFromUDFToLong(fixedValueString);
                        containsUDF = true;
                    }
                    else if(fixedValueString.contains("upper(")){
                        fixedValueString = clearColumnFromUDFUpper(fixedValueString);
                        containsUDF = true;
                    }else if(fixedValueString.contains("lower(")){
                        fixedValueString = clearColumnFromUDFLower(fixedValueString);
                        containsUDF = true;
                    }
                    else if(fixedValueString.contains("UDFToDouble(")){
                        fixedValueString = clearColumnFromUDFToDouble(fixedValueString);
                        containsUDF = true;
                    }

                    String[] parts = fixedValueString.split(" ");
                    if (parts.length != 3) {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Reading value info...Failed! Parts size: " + parts.length);
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Parts: " + parts);
                        System.exit(0);
                    }
                    String columnName = parts[0];
                    String columnType = "";

                    if(containsUDF){
                        if(columnName.contains("(")) columnName = columnName.replace("(" , "");
                        if(columnName.contains(")")) columnName = columnName.replace(")", "");
                    }

                    if (parts[2].contains(")")) {
                        columnType = parts[2].replace(")", "");

                        if (entry.getValue().contains("UDFToLong(")) {
                            for (TableRegEntry tableReg : tableRegistry.getEntries()) {
                                for (ColumnTypePair cP : tableReg.getColumnTypeMap().getColumnAndTypeList()) {
                                    if (columnType.equals("bigint")) {
                                        if (cP.getColumnType().equals("int")) {
                                            if (cP.getColumnName().equals(columnName)) {
                                                for (StringParameter sP : cP.getAltAliasPairs()) {

                                                    if ((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()))) {
                                                        cP.addCastType(columnType);
                                                    } else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                                        cP.addCastType(columnType);
                                                    }

                                                }
                                            } else {

                                                for (StringParameter sP : cP.getAltAliasPairs()) {

                                                    if (sP.getValue().equals(columnName)) {

                                                        if ((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()))) {
                                                            cP.addCastType(columnType);
                                                        } else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                                            cP.addCastType(columnType);
                                                        }

                                                    }

                                                }

                                            }
                                        }

                                    }

                                }
                            }

                            columnType = "int";

                        }
                        else if (entry.getValue().contains("UDFToDouble(")) {
                            for (TableRegEntry tableReg : tableRegistry.getEntries()) {
                                for (ColumnTypePair cP : tableReg.getColumnTypeMap().getColumnAndTypeList()) {
                                    if (columnType.equals("double")) {
                                        if (cP.getColumnType().equals("float")) {
                                            if (cP.getColumnName().equals(columnName)) {
                                                for (StringParameter sP : cP.getAltAliasPairs()) {

                                                    if ((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()))) {
                                                        cP.addCastType(columnType);
                                                    } else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                                        cP.addCastType(columnType);
                                                    }

                                                }
                                            } else {

                                                for (StringParameter sP : cP.getAltAliasPairs()) {

                                                    if (sP.getValue().equals(columnName)) {

                                                        if ((fatherOperatorNode != null) && (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName()))) {
                                                            cP.addCastType(columnType);
                                                        } else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                                            cP.addCastType(columnType);
                                                        }

                                                    }

                                                }

                                            }
                                        }

                                    }

                                }
                            }

                            columnType = "float";

                        }

                    } else {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": ColumnType does not contain )");
                        System.exit(0);
                    }

                    //Located Value
                    boolean valueLocated = false;
                    String realValueName = "";

                    for (TableRegEntry regEntry : tableRegistry.getEntries()) {
                        for (ColumnTypePair p : regEntry.getColumnTypeMap().getColumnAndTypeList()) {
                            if (columnType.equals(p.getColumnType())) {
                                if (p.getColumnName().equals(columnName)) {
                                    List<StringParameter> altAliases = p.getAltAliasPairs();
                                    for (StringParameter sP : altAliases) {
                                        if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                            valueLocated = true;
                                            realValueName = columnName;
                                            joinColumns.add(realValueName);

                                            //Add JOIN USED Column
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + fatherOperatorNode);
                                            currentOpQuery.addUsedColumn(p.getColumnName(), fatherOperatorNode.getOperatorName());
                                            System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+p.getColumnName()+" , "+fatherOperatorNode.getOperatorName()+")");
                                            break;
                                        } else if (sP.getParemeterType().equals(otherFatherNode.getOperatorName())) {
                                            valueLocated = true;
                                            realValueName = columnName;
                                            joinColumns.add(realValueName);

                                            //Add JOIN USED Column
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + otherFatherNode);
                                            currentOpQuery.addUsedColumn(p.getColumnName(), otherFatherNode.getOperatorName());
                                            System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+p.getColumnName()+" , "+otherFatherNode.getOperatorName()+")");
                                            break;
                                        }
                                    }

                                    if (valueLocated == true) break;

                                } else {
                                    List<StringParameter> altAliases = p.getAltAliasPairs();
                                    for (StringParameter sP : altAliases) {
                                        if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) { //Father1 Exists as AltAlias for this Column
                                            if (sP.getValue().equals(columnName)) { //Father holds the value of this key
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Located match through: " + fatherOperatorNode.getOperatorName());
                                                realValueName = p.getColumnName();
                                                valueLocated = true;
                                                joinColumns.add(realValueName);

                                                //Add JOIN USED Column
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + fatherOperatorNode);
                                                currentOpQuery.addUsedColumn(p.getColumnName(), fatherOperatorNode.getOperatorName());
                                                System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+p.getColumnName()+" , "+fatherOperatorNode.getOperatorName()+")");

                                                break;
                                            }
                                        } else if (sP.getParemeterType().equals(otherFatherNode.getOperatorName())) {
                                            if (sP.getValue().equals(columnName)) { //Father holds the value of this key
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Located match through: " + otherFatherNode.getOperatorName());
                                                realValueName = p.getColumnName();
                                                valueLocated = true;
                                                joinColumns.add(realValueName);

                                                //Add JOIN USED Column
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Adding used column for JoinQuery - Alias: " + p.getColumnName() + " - TableName: " + otherFatherNode);
                                                currentOpQuery.addUsedColumn(p.getColumnName(), otherFatherNode.getOperatorName());
                                                System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+p.getColumnName()+" , "+otherFatherNode.getOperatorName()+")");

                                                break;
                                            }
                                        }
                                    }

                                    if (valueLocated == true) break;
                                }
                            }
                        }

                        if (valueLocated == true) break;
                    }

                    if (valueLocated == false) {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Failed to locate the real alias of: " + columnName);
                        System.exit(0);
                    } else {
                        System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Real alias of: " + columnName + " is: " + realValueName);
                    }

                }

            }

            //Same alias for the 2 keys
            if(joinColumns.size() > 0){
                if(joinColumns.size() == 2) {
                    if (joinColumns.get(0).equals(joinColumns.get(1))) {
                        List<String> joinColumnsFixed = new LinkedList<>();
                        joinColumnsFixed.add(fatherOperatorNode.getOperatorName() + "." + joinColumns.get(0));
                        joinColumnsFixed.add(otherFatherNode.getOperatorName() + "." + joinColumns.get(1));

                        joinColumns = joinColumnsFixed;

                    }
                }
                else{ //Multiple join keys that might have same column name but belong to different tables
                    if(joinColumns.size() % 2 == 1){
                        System.out.println("handleJoinKeys: ODD NUMBER OF JOIN KEYS! ERROR! JoinColumns: "+joinColumns.toString());
                        System.exit(0);
                    }
                    else{
                        List<String> joinColumnsFixed = new LinkedList<>();
                        List<String> halfJoinColumns1 = new LinkedList<>();
                        List<String> halfJoinColumns2 = new LinkedList<>();
                        for(int j = 0; j < joinColumns.size() / 2; j++){
                            if(joinColumns.get(j).equals(joinColumns.get(j + (joinColumns.size() / 2) ))){
                                halfJoinColumns1.add(fatherOperatorNode.getOperatorName() + "." + joinColumns.get(j));
                                halfJoinColumns2.add(otherFatherNode.getOperatorName() + "." + joinColumns.get(j));
                            }
                            else{
                                halfJoinColumns1.add(joinColumns.get(j));
                                halfJoinColumns2.add(joinColumns.get(j + (joinColumns.size() / 2) ));
                            }
                        }

                        if(halfJoinColumns1.size() != halfJoinColumns2.size()){
                            System.out.println("handleJoinKeys: halfJoinLists are not equal in size! LIST1: "+halfJoinColumns1.toString() + " - LIST2: "+halfJoinColumns2);
                            System.exit(0);
                        }

                        for(int j = 0; j < halfJoinColumns1.size(); j++){
                            joinColumnsFixed.add(halfJoinColumns1.get(j));
                        }

                        for(int j = 0; j < halfJoinColumns2.size(); j++){
                            joinColumnsFixed.add(halfJoinColumns2.get(j));
                        }

                        joinColumns = joinColumnsFixed;
                    }
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

    public void discoverOrderByKeys(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorQuery currentOpQuery, String latestAncestorTableName1, String latestAncestorTableName2, List<FieldSchema> orderByKeysAsFields){

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
                                        if(cP.getColumnType().equals(keyPair.getColumnType()) || cP.hasLatestAltCastType(keyPair.getColumnType())) {
                                            if(cP.getColumnName().equals(keyPair.getColumnName())) { //Column has original name
                                                //System.out.println("Direct match with: ("+cP.getColumnName()+","+cP.getColumnType()+")...check for father now...");
                                                List<StringParameter> altAliases = cP.getAltAliasPairs();
                                                for(StringParameter sP : altAliases){
                                                    if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){ //Now just check that father is also inline(if not then wrong table)
                                                        aliasFound = true;
                                                        realAliasOfKey = cP.getColumnName();
                                                        FieldSchema orderByField = new FieldSchema();
                                                        orderByField.setName(realAliasOfKey);
                                                        orderByField.setType(keyPair.getColumnType());
                                                        orderByKeysAsFields.add(orderByField);
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
                                                            FieldSchema orderByField = new FieldSchema();
                                                            orderByField.setName(realAliasOfKey);
                                                            orderByField.setType(keyPair.getColumnType());
                                                            orderByKeysAsFields.add(orderByField);
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

                                boolean isAggr = false;
                                String validAggrAlias = "";
                                if(aliasFound == false){ //Search in aggr
                                    for(ColumnTypePair aggrPair : aggregationsMap.getColumnAndTypeList()){
                                        if(aggrPair.getColumnType().equals(keyPair.getColumnType()) || aggrPair.getLatestAltCastType().equals(keyPair.getColumnType())) {
                                            for (StringParameter altAlias : aggrPair.getAltAliasPairs()) {
                                                if (altAlias.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                                    if (altAlias.getValue().equals(keyPair.getColumnName())){
                                                        aliasFound = true;
                                                        isAggr = true;
                                                        validAggrAlias = fetchlatestRequiredAncestorOfAggr(currentOperatorNode, currentOpQuery, aggrPair.getColumnName(), aggrPair.getAltAliasPairs(), keyPair.getColumnName());
                                                        realAliasOfKey = validAggrAlias;
                                                        FieldSchema orderByField = new FieldSchema();
                                                        orderByField.setName(realAliasOfKey);
                                                        orderByField.setType(keyPair.getColumnType());
                                                        orderByKeysAsFields.add(orderByField);
                                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+"is a Const value and will be ommited... ");
                                                        break;
                                                    }
                                                }
                                            }

                                            if(isAggr) break;
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
                                    currentOpQuery.addUsedColumn(realAliasOfKey, latestAncestorTableName2);
                                    //System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+realAliasOfKey+" , "+latestAncestorTableName1+")");
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

    public void discoverGroupByKeys(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorQuery currentOpQuery, String latestAncestorTableName1, String latestAncestorTableName2, GroupByDesc groupByDesc, List<FieldSchema> groupKeys){

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
                            if(cP.getColumnType().equals(keyPair.getColumnType()) || cP.hasLatestAltCastType(keyPair.getColumnType())) {
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

                    boolean isAggr = false;
                    String validAggrAlias = "";
                    if(aliasFound == false){ //Search in aggr
                        for(ColumnTypePair aggrPair : aggregationsMap.getColumnAndTypeList()){
                            if(aggrPair.getColumnType().equals(keyPair.getColumnType()) || aggrPair.getLatestAltCastType().equals(keyPair.getColumnType())) {
                                for (StringParameter altAlias : aggrPair.getAltAliasPairs()) {
                                    if (altAlias.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                        if (altAlias.getValue().equals(keyPair.getColumnName())){
                                            aliasFound = true;
                                            isAggr = true;
                                            validAggrAlias = fetchlatestRequiredAncestorOfAggr(currentOperatorNode, currentOpQuery, aggrPair.getColumnName(), aggrPair.getAltAliasPairs(), keyPair.getColumnName());
                                            realAliasOfKey = validAggrAlias;
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Real Alias of Key: "+keyPair.getColumnName()+"is a Const value and will be ommited... ");
                                            break;
                                        }
                                    }
                                }

                                if(isAggr) break;
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
                        currentOpQuery.addUsedColumn(realAliasOfKey, latestAncestorTableName2);
                        //System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+realAliasOfKey+" , "+latestAncestorTableName1+")");
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

    public void addWhereStatementToQuery(OperatorNode currentOperatorNode, OperatorQuery currentOpQuery, String latestAncestorTableName1, String latestAncestorTableName2){

        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Accessing method: addWhereStatementToQuery...");

        FilterOperator filtOp = (FilterOperator) currentOperatorNode.getOperator();
        FilterDesc filterDesc = filtOp.getConf();

        /*---Locate new USED columns---*/
        List<String> predCols = filterDesc.getPredicate().getCols();
        if (predCols != null) {
            for (String p : predCols) {
                currentOpQuery.addUsedColumn(p, latestAncestorTableName1);
                currentOpQuery.addUsedColumn(p, latestAncestorTableName2);
                //System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+p+" , "+latestAncestorTableName1+")");
            }
        }

        ExprNodeDesc predicate = filterDesc.getPredicate();
        if (predicate != null) {
            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": " + "Extracting columns of predicate...");
            List<String> filterColumns = predicate.getCols();

            String predicateString = predicate.getExprString();

            predicateString = clearColumnFromUDFToLong(predicateString);

            if(currentOpQuery.getLocalQueryString().contains(" group by ")){
                currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString().concat(" having " + predicateString + " "));
                currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString().concat(" having " + predicateString + " "));
            }
            else {
                currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString().concat(" where " + predicateString + " "));
                currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString().concat(" where " + predicateString + " "));
            }

        } else {
            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Predicate is NULL");
            System.exit(0);
        }

    }

    public void addWhereStatementToQuery2(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorQuery currentOpQuery, String latestAncestorSchema, String latestAncestorTableName1, String latestAncestorTableName2){

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
            if(descendentSchema.contains("decimal(")){
                descendentSchema = fixSchemaContainingDecimals(descendentSchema);
            }
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

        predicateString = clearColumnFromUDFToLong(predicateString);
        predicateString = clearColumnFromUDFToDouble(predicateString);
        predicateString = clearColumnFromUDFUpper(predicateString);
        predicateString = clearColumnFromUDFLower(predicateString);

        for(int i = 0; i < ancestorMap.getColumnAndTypeList().size(); i++){
            ColumnTypePair ancestorPair = ancestorMap.getColumnAndTypeList().get(i);
            ColumnTypePair descendentPair = descendentMap.getColumnAndTypeList().get(i);

            boolean matchFound = false;

            //Fetch possible latest type for ancestorPair
            String altType = "";
            for(TableRegEntry tableRegEntry : tableRegistry.getEntries()) {
                for (ColumnTypePair p : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()) {
                    if (p.getColumnType().equals(ancestorPair.getColumnType()) || p.hasLatestAltCastType(ancestorPair.getColumnType())) {
                        if(p.getColumnName().equals(ancestorPair.getColumnName())) {
                            List<StringParameter> altAliases = p.getAltAliasPairs();
                            for (StringParameter sP : altAliases) {
                                if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                    altType = p.getLatestAltCastType();
                                }
                            }
                        }
                    }
                }
            }

            if(ancestorPair.getColumnType().equals(descendentPair.getColumnType()) || (altType.equals(descendentPair.getColumnType()))){

                for(TableRegEntry tableRegEntry : tableRegistry.getEntries()){
                    for(ColumnTypePair p : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()){
                        if(p.getColumnType().equals(ancestorPair.getColumnType()) || p.hasLatestAltCastType(ancestorPair.getColumnType()) ){
                            if(p.getColumnName().equals(ancestorPair.getColumnName())){ //Located now check that father is in line
                                List<StringParameter> altAliases = p.getAltAliasPairs();
                                for(StringParameter sP : altAliases){
                                    if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){

                                        String extraAlias = "";
                                        if(tableRegEntry.getAlias().equals(tableRegEntry.getAssociatedTable().getTableName()) == false){
                                            extraAlias = tableRegEntry.getAlias() + ".";
                                        }

                                            matchFound = true;
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Match found! - Pair: "+extraAlias+ancestorPair.getColumnName()+" - "+descendentPair.getColumnName());
                                            if(predCols != null){
                                                if(predCols.size() > 0){
                                                    for(String c : predCols){
                                                        if(c.equals(descendentPair.getColumnName())){
                                                            if(predicateString != null){
                                                                if(predicateString.contains(c)){
                                                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": This column also exists in the predicate! Replacing!");
                                                                    if(predicateString.contains(c+" ")){
                                                                        predicateString = predicateString.replace(c+" ", extraAlias+ancestorPair.getColumnName()+" ");
                                                                    }
                                                                    else if(predicateString.contains(c+",")){
                                                                        predicateString = predicateString.replace(c+",", extraAlias+ancestorPair.getColumnName()+",");
                                                                    }
                                                                    else if(predicateString.contains(c+")")){
                                                                        predicateString = predicateString.replace(c+")", extraAlias+ancestorPair.getColumnName()+")");
                                                                    }
                                                                    else{
                                                                        System.out.println("Failed to replace: "+c+" with alias: "+extraAlias+ancestorPair.getColumnName()+" in predicate string!");
                                                                        System.exit(0);
                                                                    }
                                                                    c = c.replace(c, extraAlias+ancestorPair.getColumnName());
                                                                }
                                                            }

                                                            //Add used column
                                                            currentOpQuery.addUsedColumn(extraAlias+ancestorPair.getColumnName(), latestAncestorTableName1);
                                                            currentOpQuery.addUsedColumn(extraAlias+ancestorPair.getColumnName(), latestAncestorTableName2);
                                                            //System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+p.getColumnName()+" , "+latestAncestorTableName1+")");
                                                        }
                                                    }
                                                }
                                            }

                                            p.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), false);

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

                if(matchFound == false) {
                    String aliasValue = "";
                    if (descendentPair.getColumnType().equals("struct") || ancestorPair.getColumnType().equals("struct")) {
                        System.out.println("Will check in aggrMap for match...");
                        for (ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()) {
                            if(descendentPair.getColumnType().equals("struct") == false){
                                if(aggPair.getLatestAltCastType().equals(descendentPair.getColumnType()) == false){
                                    continue;
                                }
                            }
                            for (StringParameter sP : aggPair.getAltAliasPairs()) {
                                if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                    aliasValue = sP.getValue();
                                    if (sP.getValue().equals(ancestorPair.getColumnName())) {
                                        String extraAlias = "";

                                        String properAlias = fetchlatestRequiredAncestorOfAggr(currentOperatorNode, currentOpQuery, aggPair.getColumnName(),  aggPair.getAltAliasPairs(), ancestorPair.getColumnName());
                                        matchFound = true;
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Match found! - Pair: "+extraAlias+properAlias+" - "+descendentPair.getColumnName());
                                        if(predCols != null){
                                            if(predCols.size() > 0){
                                                for(String c : predCols){
                                                    if(c.equals(descendentPair.getColumnName())){
                                                        if(predicateString != null){
                                                            if(predicateString.contains(c)){
                                                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": This column also exists in the predicate! Replacing!");
                                                                if(predicateString.contains(c+" ")){
                                                                    predicateString = predicateString.replace(c+" ", extraAlias+properAlias+" ");
                                                                }
                                                                else if(predicateString.contains(c+",")){
                                                                    predicateString = predicateString.replace(c+",", extraAlias+properAlias+",");
                                                                }
                                                                else if(predicateString.contains(c+")")){
                                                                    predicateString = predicateString.replace(c+")", extraAlias+properAlias+")");
                                                                }
                                                                else{
                                                                    System.out.println("Failed to replace: "+c+" with alias: "+extraAlias+properAlias+" in predicate string!");
                                                                    System.exit(0);
                                                                }
                                                                c = c.replace(c, extraAlias+properAlias);
                                                            }
                                                        }

                                                        //Add used column
                                                        currentOpQuery.addUsedColumn(extraAlias+properAlias, latestAncestorTableName1);
                                                        currentOpQuery.addUsedColumn(extraAlias+properAlias, latestAncestorTableName2);
                                                        //System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+extraAlias+properAlias+" , "+latestAncestorTableName1+")");
                                                    }
                                                }
                                            }
                                        }

                                        aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), true);

                                    }

                                    if (matchFound) break;
                                }
                            }

                            if (matchFound) break;
                        }
                    }

                    if(matchFound == false){ //EXTRA SEARCH IN AGGR FOR COLUMNS WITHOUT ALT CAST TYPES
                        System.out.println("EXTRA SEARCH: DESC TYPE: "+descendentPair.getColumnType() + " ANC TYPE: "+ancestorPair.getColumnType());
                        if(ancestorPair.getColumnType().equals("struct") && (descendentPair.getColumnType().equals("struct") == false)){
                            System.out.println("Extra search in attempt to match with struct while having type: "+descendentPair.getColumnType());
                            for (ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()) {
                                if(aggPair.getLatestAltCastType().equals(descendentPair.getColumnType())) continue; //We want to locate aggPair who doesn't have the proper cast type
                                for (StringParameter sP : aggPair.getAltAliasPairs()) {
                                    if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                        aliasValue = sP.getValue();
                                        if (sP.getValue().equals(ancestorPair.getColumnName())) { //Located
                                            String properAlias = fetchlatestRequiredAncestorOfAggr(currentOperatorNode, currentOpQuery, aggPair.getColumnName(), aggPair.getAltAliasPairs(), ancestorPair.getColumnName());
                                            matchFound = true;
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Match found! - Pair: "+properAlias+" - "+descendentPair.getColumnName());
                                            if(predCols != null){
                                                if(predCols.size() > 0){
                                                    for(String c : predCols){
                                                        if(c.equals(descendentPair.getColumnName())){
                                                            if(predicateString != null){
                                                                if(predicateString.contains(c)){
                                                                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": This column also exists in the predicate! Replacing!");
                                                                    if(predicateString.contains(c+" ")){
                                                                        predicateString = predicateString.replace(c+" ", properAlias+" ");
                                                                    }
                                                                    else if(predicateString.contains(c+",")){
                                                                        predicateString = predicateString.replace(c+",", properAlias+",");
                                                                    }
                                                                    else if(predicateString.contains(c+")")){
                                                                        predicateString = predicateString.replace(c+")", properAlias+")");
                                                                    }
                                                                    else{
                                                                        System.out.println("Failed to replace: "+c+" with alias: "+properAlias+" in predicate string!");
                                                                        System.exit(0);
                                                                    }
                                                                    c = c.replace(c, properAlias);
                                                                }
                                                            }

                                                            //Add used column
                                                            currentOpQuery.addUsedColumn(properAlias, latestAncestorTableName1);
                                                            currentOpQuery.addUsedColumn(properAlias, latestAncestorTableName2);
                                                            System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+properAlias+" , "+latestAncestorTableName1+")");
                                                        }
                                                    }
                                                }
                                            }

                                            aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), true);
                                        }

                                        if (matchFound) break;
                                    }
                                }

                                if (matchFound) break;
                            }
                        }
                    }

                    if(matchFound == false) {
                        if ((descendentPair.getColumnType().contains("_col") == false) && (currentOperatorNode.getOperator() instanceof FileSinkOperator)) { //Case of fileSink ending with real aliases

                            System.out.println("Will check in aggrMap for match...");
                            for (ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()) {
                                for (StringParameter sP : aggPair.getAltAliasPairs()) {
                                    if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                        aliasValue = sP.getValue();
                                        if (sP.getValue().equals(ancestorPair.getColumnName())) {
                                            System.out.println("Located value in altAliases of: " + aggPair.getColumnName() + " now find the right alias for it...");
                                            matchFound = true;
                                            if (sP.getValue().equals(descendentPair.getColumnName())) { //Father has the same value as descendent
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Match found for Alias: " + descendentPair.getColumnName() + " - Real Name is: " + sP.getValue() + " - through Father: " + fatherOperatorNode.getOperatorName());
                                                matchFound = true;
                                                aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), true);
                                                break;
                                            } else {

                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Match found for Alias: " + descendentPair.getColumnName() + " - Real Name is: " + sP.getValue() + " - through Father: " + fatherOperatorNode.getOperatorName());
                                                matchFound = true;
                                                aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), true);
                                                break;

                                            }
                                        }

                                        if (matchFound) break;
                                    }
                                }

                                if (matchFound) break;
                            }

                        }
                    }

                }

                if(matchFound == false){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Failed to find match for : "+descendentPair.getColumnName());
                    System.exit(0);
                }

            }
            else{

                if(matchFound == false) {
                    String aliasValue = "";
                    if (descendentPair.getColumnType().equals("struct") || ancestorPair.getColumnType().equals("struct")) {
                        System.out.println("Will check in aggrMap for match...");
                        for (ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()) {
                            if(descendentPair.getColumnType().equals("struct") == false){
                                if(aggPair.getLatestAltCastType().equals(descendentPair.getColumnType()) == false){
                                    continue;
                                }
                            }
                            for (StringParameter sP : aggPair.getAltAliasPairs()) {
                                if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                    aliasValue = sP.getValue();
                                    if (sP.getValue().equals(ancestorPair.getColumnName())) {
                                        String extraAlias = "";

                                        String properAlias = fetchlatestRequiredAncestorOfAggr(currentOperatorNode, currentOpQuery, aggPair.getColumnName(), aggPair.getAltAliasPairs(), ancestorPair.getColumnName());
                                        matchFound = true;
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Match found! - Pair: "+extraAlias+properAlias+" - "+descendentPair.getColumnName());
                                        if(predCols != null){
                                            if(predCols.size() > 0){
                                                for(String c : predCols){
                                                    if(c.equals(descendentPair.getColumnName())){
                                                        if(predicateString != null){
                                                            if(predicateString.contains(c)){
                                                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": This column also exists in the predicate! Replacing!");
                                                                if(predicateString.contains(c+" ")){
                                                                    predicateString = predicateString.replace(c+" ", extraAlias+properAlias+" ");
                                                                }
                                                                else if(predicateString.contains(c+",")){
                                                                    predicateString = predicateString.replace(c+",", extraAlias+properAlias+",");
                                                                }
                                                                else if(predicateString.contains(c+")")){
                                                                    predicateString = predicateString.replace(c+")", extraAlias+properAlias+")");
                                                                }
                                                                else{
                                                                    System.out.println("Failed to replace: "+c+" with alias: "+extraAlias+properAlias+" in predicate string!");
                                                                    System.exit(0);
                                                                }
                                                                c = c.replace(c, extraAlias+properAlias);
                                                            }
                                                        }

                                                        //Add used column
                                                        currentOpQuery.addUsedColumn(extraAlias+properAlias, latestAncestorTableName1);
                                                        currentOpQuery.addUsedColumn(extraAlias+properAlias, latestAncestorTableName2);
                                                        //System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+extraAlias+properAlias+" , "+latestAncestorTableName1+")");
                                                    }
                                                }
                                            }
                                        }

                                        aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), true);

                                    }

                                    if (matchFound) break;
                                }
                            }

                            if (matchFound) break;
                        }
                    }

                }

                if(matchFound == false){ //EXTRA SEARCH IN AGGR FOR COLUMNS WITHOUT ALT CAST TYPES
                    System.out.println("EXTRA SEARCH: DESC TYPE: "+descendentPair.getColumnType() + " ANC TYPE: "+ancestorPair.getColumnType());
                    if(ancestorPair.getColumnType().equals("struct") && (descendentPair.getColumnType().equals("struct") == false)){
                        System.out.println("Extra search in attempt to match with struct while having type: "+descendentPair.getColumnType());
                        for (ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()) {
                            if(aggPair.getLatestAltCastType().equals(descendentPair.getColumnType())) continue; //We want to locate aggPair who doesn't have the proper cast type
                            for (StringParameter sP : aggPair.getAltAliasPairs()) {
                                if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                    if (sP.getValue().equals(ancestorPair.getColumnName())) { //Located
                                        String properAlias = fetchlatestRequiredAncestorOfAggr(currentOperatorNode, currentOpQuery, aggPair.getColumnName(), aggPair.getAltAliasPairs(), ancestorPair.getColumnName());
                                        matchFound = true;
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Match found! - Pair: "+properAlias+" - "+descendentPair.getColumnName());
                                        if(predCols != null){
                                            if(predCols.size() > 0){
                                                for(String c : predCols){
                                                    if(c.equals(descendentPair.getColumnName())){
                                                        if(predicateString != null){
                                                            if(predicateString.contains(c)){
                                                                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": This column also exists in the predicate! Replacing!");
                                                                if(predicateString.contains(c+" ")){
                                                                    predicateString = predicateString.replace(c+" ", properAlias+" ");
                                                                }
                                                                else if(predicateString.contains(c+",")){
                                                                    predicateString = predicateString.replace(c+",", properAlias+",");
                                                                }
                                                                else if(predicateString.contains(c+")")){
                                                                    predicateString = predicateString.replace(c+")", properAlias+")");
                                                                }
                                                                else{
                                                                    System.out.println("Failed to replace: "+c+" with alias: "+properAlias+" in predicate string!");
                                                                    System.exit(0);
                                                                }
                                                                c = c.replace(c, properAlias);
                                                            }
                                                        }

                                                        //Add used column
                                                        currentOpQuery.addUsedColumn(properAlias, latestAncestorTableName1);
                                                        currentOpQuery.addUsedColumn(properAlias, latestAncestorTableName2);
                                                        System.out.println(currentOperatorNode.getOperatorName()+": USED Column Addition: ("+properAlias+" , "+latestAncestorTableName1+")");
                                                    }
                                                }
                                            }
                                        }

                                        aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), true);
                                    }

                                    if (matchFound) break;
                                }
                            }

                            if (matchFound) break;
                        }
                    }
                }

                if(matchFound == false) {
                    if ((descendentPair.getColumnType().contains("_col") == false) && (currentOperatorNode.getOperator() instanceof FileSinkOperator)) { //Case of fileSink ending with real aliases

                        System.out.println("Will check in aggrMap for match...");
                        for (ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()) {
                            for (StringParameter sP : aggPair.getAltAliasPairs()) {
                                if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                    if (sP.getValue().equals(ancestorPair.getColumnName())) {
                                        System.out.println("Located value in altAliases of: " + aggPair.getColumnName() + " now find the right alias for it...");
                                        matchFound = true;
                                        if (sP.getValue().equals(descendentPair.getColumnName())) { //Father has the same value as descendent
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Match found for Alias: " + descendentPair.getColumnName() + " - Real Name is: " + sP.getValue() + " - through Father: " + fatherOperatorNode.getOperatorName());
                                            matchFound = true;
                                            aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), true);
                                            break;
                                        } else {

                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Match found for Alias: " + descendentPair.getColumnName() + " - Real Name is: " + sP.getValue() + " - through Father: " + fatherOperatorNode.getOperatorName());
                                            matchFound = true;
                                            aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), true);
                                            break;

                                        }
                                    }

                                    if (matchFound) break;
                                }
                            }

                            if (matchFound) break;
                        }

                    }
                }

                if(matchFound == false){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Possible match Pair does not have same Type - Pair: "+ancestorPair.getColumnName()+" - "+descendentPair.getColumnName());
                    System.exit(0);
                }
            }
        }

        if (predicateString != null) {
            if(currentOpQuery.getLocalQueryString().contains(" group by ")){
                currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString().concat(" having " + predicateString + " "));
                currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString().concat(" having " + predicateString + " "));
            }
            else {
                currentOpQuery.setLocalQueryString(currentOpQuery.getLocalQueryString().concat(" where " + predicateString + " "));
                currentOpQuery.setExaremeQueryString(currentOpQuery.getExaremeQueryString().concat(" where " + predicateString + " "));
            }
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

                                        pair.modifyAltAlias(opName, sP.getValue(), newAlias, false);

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
            System.out.println("useSchemaToFillAliasesForTable: Table with alias: "+tableName+" was never found...");
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
            if(descendentSchema.contains("decimal(")){
                descendentSchema = fixSchemaContainingDecimals(descendentSchema);
            }
            descendentSchema = extractColsFromTypeName(currentOperatorNode.getOperator().getSchema().toString(), descendentMap, currentOperatorNode.getOperator().getSchema().toString(), true);
        }

        System.out.println("DescendentSchema: "+descendentSchema);
        System.out.println("AncestorSchema: "+latestAncestorSchema);
        if(ancestorMap.getColumnAndTypeList().size() != descendentMap.getColumnAndTypeList().size()){
            System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Maps do not have equal size!");
            return latestAncestorSchema;
        }

        for(int i = 0; i < ancestorMap.getColumnAndTypeList().size(); i++){
            ColumnTypePair ancestorPair = ancestorMap.getColumnAndTypeList().get(i);
            ColumnTypePair descendentPair = descendentMap.getColumnAndTypeList().get(i);

            boolean matchFound = false;

            //Fetch possible latest type for ancestorPair
            String altType = "";
            for(TableRegEntry tableRegEntry : tableRegistry.getEntries()) {
                for (ColumnTypePair p : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()) {
                    if (p.getColumnType().equals(ancestorPair.getColumnType()) || p.hasLatestAltCastType(ancestorPair.getColumnType())) {
                        if(p.getColumnName().equals(ancestorPair.getColumnName())) {
                            List<StringParameter> altAliases = p.getAltAliasPairs();
                            for (StringParameter sP : altAliases) {
                                if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                    altType = p.getLatestAltCastType();
                                }
                            }
                        }
                    }
                }
            }

            if(ancestorPair.getColumnType().equals(descendentPair.getColumnType()) || (altType.equals(descendentPair.getColumnType()))){
                for(TableRegEntry entry : tableRegistry.getEntries()){
                    for(ColumnTypePair p : entry.getColumnTypeMap().getColumnAndTypeList()){
                        if(p.getColumnType().equals(ancestorPair.getColumnType()) || p.hasLatestAltCastType(ancestorPair.getColumnType())) {
                            if(p.getColumnName().equals(ancestorPair.getColumnName())){ //Located, now check if Father is inline
                                List<StringParameter> altAliases = p.getAltAliasPairs();
                                for(StringParameter sP : altAliases){
                                    if(sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())){
                                        System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Match found for Alias: "+descendentPair.getColumnName()+" - Real Name is: "+p.getColumnName()+" - through Father: "+fatherOperatorNode.getOperatorName());
                                        matchFound = true;
                                        p.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), false);
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

                if(matchFound == false) {
                    String aliasValue = "";
                    if (descendentPair.getColumnType().equals("struct") || ancestorPair.getColumnType().equals("struct")) {
                        System.out.println("Will check in aggrMap for match...");
                        for (ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()) {
                            if (descendentPair.getColumnType().equals("struct") == false) {
                                if (aggPair.getLatestAltCastType().equals(descendentPair.getColumnType()) == false) {
                                    continue;
                                }
                            }
                            for (StringParameter sP : aggPair.getAltAliasPairs()) {
                                if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                    aliasValue = sP.getValue();
                                    if (sP.getValue().equals(ancestorPair.getColumnName())) {
                                        System.out.println("Located value in altAliases of: " + aggPair.getColumnName() + " now find the right alias for it...");
                                        matchFound = true;
                                        if (sP.getValue().equals(descendentPair.getColumnName())) { //Father has the same value as descendent
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Match found for Alias: " + descendentPair.getColumnName() + " - Real Name is: " + sP.getValue() + " - through Father: " + fatherOperatorNode.getOperatorName());
                                            matchFound = true;
                                            aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), true);
                                            break;
                                        } else {
                                            if (descendentPair.getColumnName().contains("KEY.") || (descendentPair.getColumnName().contains("VALUE."))) {
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Match found for Alias: " + descendentPair.getColumnName() + " - Real Name is: " + sP.getValue() + " - through Father: " + fatherOperatorNode.getOperatorName());
                                                matchFound = true;
                                                aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), true);
                                                break;
                                            }
                                        }
                                    }

                                    if (matchFound) break;
                                }
                            }

                            if (matchFound) break;
                        }
                    }

                    if (matchFound == false) { //EXTRA SEARCH IN AGGR FOR COLUMNS WITHOUT ALT CAST TYPES
                        if (ancestorPair.getColumnType().equals("struct") && (descendentPair.getColumnType().equals("struct") == false)) {
                            for (ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()) {
                                if (aggPair.getLatestAltCastType().equals(descendentPair.getColumnType()))
                                    continue; //We want to locate aggPair who doesn't have the proper cast type
                                for (StringParameter sP : aggPair.getAltAliasPairs()) {
                                    if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                        aliasValue = sP.getValue();
                                        if (sP.getValue().equals(ancestorPair.getColumnName())) { //Located
                                            System.out.println("Located value in altAliases of: " + aggPair.getColumnName() + " now find the right alias for it...");
                                            matchFound = true;

                                            //Add new alt cast type
                                            aggPair.addCastType(descendentPair.getColumnType());

                                            if (sP.getValue().equals(descendentPair.getColumnName())) { //Father has the same value as descendent
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Match found for Alias: " + descendentPair.getColumnName() + " - Real Name is: " + sP.getValue() + " - through Father: " + fatherOperatorNode.getOperatorName());
                                                matchFound = true;
                                                aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), true);
                                                break;
                                            } else {
                                                if (descendentPair.getColumnName().contains("KEY.") || (descendentPair.getColumnName().contains("VALUE."))) {
                                                    System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Match found for Alias: " + descendentPair.getColumnName() + " - Real Name is: " + sP.getValue() + " - through Father: " + fatherOperatorNode.getOperatorName());
                                                    matchFound = true;
                                                    aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), true);
                                                    break;
                                                }
                                            }
                                        }

                                        if (matchFound) break;
                                    }
                                }

                                if (matchFound) break;
                            }
                        }
                    }

                    if (matchFound == false) {
                        if ((descendentPair.getColumnType().contains("_col") == false) && (currentOperatorNode.getOperator() instanceof FileSinkOperator)) { //Case of fileSink ending with real aliases

                            System.out.println("Will check in aggrMap for match...");
                            for (ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()) {
                                for (StringParameter sP : aggPair.getAltAliasPairs()) {
                                    if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                        aliasValue = sP.getValue();
                                        if (sP.getValue().equals(ancestorPair.getColumnName())) {
                                            System.out.println("Located value in altAliases of: " + aggPair.getColumnName() + " now find the right alias for it...");
                                            matchFound = true;
                                            if (sP.getValue().equals(descendentPair.getColumnName())) { //Father has the same value as descendent
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Match found for Alias: " + descendentPair.getColumnName() + " - Real Name is: " + sP.getValue() + " - through Father: " + fatherOperatorNode.getOperatorName());
                                                matchFound = true;
                                                aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), true);
                                                break;
                                            } else {

                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Match found for Alias: " + descendentPair.getColumnName() + " - Real Name is: " + sP.getValue() + " - through Father: " + fatherOperatorNode.getOperatorName());
                                                matchFound = true;
                                                aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), true);
                                                break;

                                            }
                                        }

                                        if (matchFound) break;
                                    }
                                }

                                if (matchFound) break;
                            }

                        }
                    }
                }

            }
            else{

                if(matchFound == false) {
                    String aliasValue = "";
                    if (descendentPair.getColumnType().equals("struct") || ancestorPair.getColumnType().equals("struct")) {
                        System.out.println("Will check in aggrMap for match...");
                        for (ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()) {
                            if(descendentPair.getColumnType().equals("struct") == false){
                                if(aggPair.getLatestAltCastType().equals(descendentPair.getColumnType()) == false){
                                    continue;
                                }
                            }
                            for (StringParameter sP : aggPair.getAltAliasPairs()) {
                                if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                    aliasValue = sP.getValue();
                                    if (sP.getValue().equals(ancestorPair.getColumnName())) {
                                        System.out.println("Located value in altAliases of: " + aggPair.getColumnName() + " now find the right alias for it...");
                                        matchFound = true;
                                        if (sP.getValue().equals(descendentPair.getColumnName())) { //Father has the same value as descendent
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Match found for Alias: " + descendentPair.getColumnName() + " - Real Name is: " + sP.getValue() + " - through Father: " + fatherOperatorNode.getOperatorName());
                                            matchFound = true;
                                            aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), true);
                                            break;
                                        } else {
                                            if (descendentPair.getColumnName().contains("KEY.") || (descendentPair.getColumnName().contains("VALUE."))) {
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Match found for Alias: " + descendentPair.getColumnName() + " - Real Name is: " + sP.getValue() + " - through Father: " + fatherOperatorNode.getOperatorName());
                                                matchFound = true;
                                                aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), true);
                                                break;
                                            }
                                        }
                                    }

                                    if (matchFound) break;
                                }
                            }

                            if (matchFound) break;
                        }
                    }

                }

                if(matchFound == false){ //EXTRA SEARCH IN AGGR FOR COLUMNS WITHOUT ALT CAST TYPES
                    if(ancestorPair.getColumnType().equals("struct") && (descendentPair.getColumnType().equals("struct") == false)){
                        for (ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()) {
                            if(aggPair.getLatestAltCastType().equals(descendentPair.getColumnType())) continue; //We want to locate aggPair who doesn't have the proper cast type
                            for (StringParameter sP : aggPair.getAltAliasPairs()) {
                                if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                    if (sP.getValue().equals(ancestorPair.getColumnName())) { //Located
                                        System.out.println("Located value in altAliases of: " + aggPair.getColumnName() + " now find the right alias for it...");
                                        matchFound = true;

                                        //Add new alt cast type
                                        aggPair.addCastType(descendentPair.getColumnType());

                                        if (sP.getValue().equals(descendentPair.getColumnName())) { //Father has the same value as descendent
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Match found for Alias: " + descendentPair.getColumnName() + " - Real Name is: " + sP.getValue() + " - through Father: " + fatherOperatorNode.getOperatorName());
                                            matchFound = true;
                                            aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), true);
                                            break;
                                        } else {
                                            if (descendentPair.getColumnName().contains("KEY.") || (descendentPair.getColumnName().contains("VALUE."))) {
                                                System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Match found for Alias: " + descendentPair.getColumnName() + " - Real Name is: " + sP.getValue() + " - through Father: " + fatherOperatorNode.getOperatorName());
                                                matchFound = true;
                                                aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), true);
                                                break;
                                            }
                                        }
                                    }

                                    if (matchFound) break;
                                }
                            }

                            if (matchFound) break;
                        }
                    }
                }

                if (matchFound == false) {
                    if ((descendentPair.getColumnType().contains("_col") == false) && (currentOperatorNode.getOperator() instanceof FileSinkOperator)) { //Case of fileSink ending with real aliases

                        System.out.println("Will check in aggrMap for match...");
                        for (ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()) {
                            for (StringParameter sP : aggPair.getAltAliasPairs()) {
                                if (sP.getParemeterType().equals(fatherOperatorNode.getOperatorName())) {
                                    if (sP.getValue().equals(ancestorPair.getColumnName())) {
                                        System.out.println("Located value in altAliases of: " + aggPair.getColumnName() + " now find the right alias for it...");
                                        matchFound = true;
                                        if (sP.getValue().equals(descendentPair.getColumnName())) { //Father has the same value as descendent
                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Match found for Alias: " + descendentPair.getColumnName() + " - Real Name is: " + sP.getValue() + " - through Father: " + fatherOperatorNode.getOperatorName());
                                            matchFound = true;
                                            aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), true);
                                            break;
                                        } else {

                                            System.out.println(currentOperatorNode.getOperator().getOperatorId() + ": Match found for Alias: " + descendentPair.getColumnName() + " - Real Name is: " + sP.getValue() + " - through Father: " + fatherOperatorNode.getOperatorName());
                                            matchFound = true;
                                            aggPair.modifyAltAlias(currentOperatorNode.getOperatorName(), sP.getValue(), descendentPair.getColumnName(), true);
                                            break;

                                        }
                                    }

                                    if (matchFound) break;
                                }
                            }

                            if (matchFound) break;
                        }

                    }
                }

                if(matchFound == false){
                    System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Possible match Pair does not have same Type - Pair: "+ancestorPair.getColumnName()+" - "+descendentPair.getColumnName());
                    System.exit(0);
                }
            }

            if(matchFound == false){
                System.out.println(currentOperatorNode.getOperator().getOperatorId()+": Failed to find match for: "+descendentPair.getColumnName());
                System.exit(0);
            }


        }

        return descendentSchema;

    }

    public String fetchlatestRequiredAncestorOfAggr(OperatorNode currentOperatorNode, OperatorQuery currentOpQuery, String aggName, List<StringParameter> altAliases, String value){

        List<MyTable> inputTables = currentOpQuery.getInputTables();

        System.out.println("fetchlatestRequiredAncestorOfAggr: ");

        if(inputTables.size() == 0){
            System.out.println("Can't determine proper aggr value due to no input tables set!");
            System.exit(0);
        }

        if(altAliases.size() == 1){
            return altAliases.get(0).getExtraValue();
        }

        boolean neverFound = true;

        for(MyTable inputT : inputTables){
            System.out.println("InputTable: "+inputT.getTableName());
        }

        boolean located = false;
        boolean aggrsExist = false;
        String wantedAlias = "";
        for(MyTable inputT : inputTables){
            for(FieldSchema col : inputT.getAllCols()){
                if(col.getName().contains("agg_")){ //Change name for aggregations
                    aggrsExist = true;
                    for(StringParameter altAlias : altAliases){
                        if(col.getName().equals(altAlias.getExtraValue())){
                            wantedAlias =  altAlias.getExtraValue();
                            located = true;
                        }
                    }
                }
            }
        }

        if(located) return wantedAlias;

        if(currentOpQuery.getLocalQueryString().contains("select ")){
            for(StringParameter altAlias: altAliases){
                if( currentOpQuery.getLocalQueryString().contains((aggName + " as " + altAlias.getExtraValue()))){
                    return altAlias.getExtraValue();
                }
            }
        }

        if(neverFound == true){
            System.out.println("Never found a match for aggr alias: "+value+" through the input tables...Returning default alias");
            return "agg_"+currentOperatorNode.getOperatorName()+"_"+value;
        }

        return "fail";



    }

    public List<String> extractColsFromSelectPhrase(String selectPhrase){

        List<String> allCols = new LinkedList<>();

        String newSelectPhraseCopy = new String(selectPhrase.toCharArray());

        newSelectPhraseCopy = newSelectPhraseCopy.trim();
        if(newSelectPhraseCopy.contains("select ")){
            newSelectPhraseCopy = newSelectPhraseCopy.replace("select ", "");
        }

        String[] splitInFrom = newSelectPhraseCopy.split(" from ");
        if(splitInFrom.length != 2){
            System.out.println("extractColsFromSelectPhrase: selectPhrase string possible contains more than 1 FROM statement...splitInFrom: "+splitInFrom.length);
        }

        if(splitInFrom.length == 2) {
            String selectColumnsPart = splitInFrom[0]; //This contains all select columns
            System.out.println("extractColsFromSelectPhrase: SelectColumnsPart: " + selectColumnsPart);

            if (selectColumnsPart.contains(",")) {
                String[] commaParts = selectColumnsPart.split(",");
                System.out.println("extractColsFromSelectPhrase: We seem to have: " + commaParts.length + " columns...CommaParts: " + commaParts.toString());
                for (String commaPart : commaParts) {
                    commaPart = commaPart.trim();
                    if (commaPart.contains(" as ")) {
                        String[] partsOfAs = commaPart.split(" as ");
                        if (partsOfAs.length != 2) {
                            System.out.println("extractColsFromSelectPhrase: Attempted to split: " + commaPart + " in parts for 'as' delimiter but failed! Length: " + partsOfAs.length);
                            System.exit(0);
                        }
                        allCols.add(partsOfAs[1]);
                        System.out.println("extractColsFromSelectPhrase: Located select column: " + partsOfAs[1]);
                    } else {
                        allCols.add(commaPart);
                        System.out.println("extractColsFromSelectPhrase: Located select column: " + commaPart);
                    }
                }
            } else {
                System.out.println("extractColsFromSelectPhrase: Only 1 columns seems to exists...");
                selectColumnsPart = selectColumnsPart.trim();
                if (selectColumnsPart.contains(" as ")) {
                    String[] partsOfAs = selectColumnsPart.split(" as ");
                    if (partsOfAs.length != 2) {
                        System.out.println("extractColsFromSelectPhrase: Attempted to split: " + selectColumnsPart + " in parts for 'as' delimiter but failed! Length: " + partsOfAs.length);
                        System.exit(0);
                    }
                    allCols.add(partsOfAs[1]);
                    System.out.println("extractColsFromSelectPhrase: Located select column: " + partsOfAs[1]);
                } else {
                    allCols.add(selectColumnsPart);
                    System.out.println("extractColsFromSelectPhrase: Located select column: " + selectColumnsPart);
                }
            }
        }
        else{
            if(splitInFrom.length > 2){
                if(newSelectPhraseCopy.contains(" union all ")){
                    String[] unionParts = newSelectPhraseCopy.split(" union all ");
                    boolean firstSubQuery = true;
                    for(String query : unionParts){
                        String[] fromParts = query.split(" from ");
                        System.out.println("extractColsFromSelectPhrase: Working on subQuery: "+query);
                        if(fromParts.length == 2) {
                            String selectColumnsPart = fromParts[0]; //This contains all select columns
                            System.out.println("extractColsFromSelectPhrase: SelectColumnsPart: " + selectColumnsPart);

                            if(firstSubQuery) {
                                if (selectColumnsPart.contains(",")) {
                                    String[] commaParts = selectColumnsPart.split(",");
                                    System.out.println("extractColsFromSelectPhrase: We seem to have: " + commaParts.length + " columns...CommaParts: " + commaParts.toString());
                                    for (String commaPart : commaParts) {
                                        commaPart = commaPart.trim();
                                        if (commaPart.contains(" as ")) {
                                            String[] partsOfAs = commaPart.split(" as ");
                                            if (partsOfAs.length != 2) {
                                                System.out.println("extractColsFromSelectPhrase: Attempted to split: " + commaPart + " in parts for 'as' delimiter but failed! Length: " + partsOfAs.length);
                                                System.exit(0);
                                            }
                                            allCols.add(partsOfAs[1]);
                                            System.out.println("extractColsFromSelectPhrase: Located select column: " + partsOfAs[1]);
                                        } else {
                                            allCols.add(commaPart);
                                            System.out.println("extractColsFromSelectPhrase: Located select column: " + commaPart);
                                        }
                                    }
                                } else {
                                    System.out.println("extractColsFromSelectPhrase: Only 1 columns seems to exists...");
                                    selectColumnsPart = selectColumnsPart.trim();
                                    if (selectColumnsPart.contains(" as ")) {
                                        String[] partsOfAs = selectColumnsPart.split(" as ");
                                        if (partsOfAs.length != 2) {
                                            System.out.println("extractColsFromSelectPhrase: Attempted to split: " + selectColumnsPart + " in parts for 'as' delimiter but failed! Length: " + partsOfAs.length);
                                            System.exit(0);
                                        }
                                        allCols.add(partsOfAs[1]);
                                        System.out.println("extractColsFromSelectPhrase: Located select column: " + partsOfAs[1]);
                                    } else {
                                        allCols.add(selectColumnsPart);
                                        System.out.println("extractColsFromSelectPhrase: Located select column: " + selectColumnsPart);
                                    }
                                }
                                firstSubQuery = false;
                            }
                            else{
                                System.out.println("extractColsFromSelectPhrase: Since UNION all statements have the same cols omitting this subQuery...");
                            }
                        }
                        else{
                            System.out.println("extractColsFromSelectPhrase: Union subQuery has not 2 from parts: "+fromParts.length);
                            System.exit(0);
                        }
                    }
                }
                else{
                    System.out.println("extractColsFromSelectPhrase: More than 1 FROM but non UNION ALL query!");
                    System.exit(0);
                }
            }
            else{
                System.out.println("extractColsFromSelectPhrase: LESS THAN 1 FROM ERROR!");
                System.exit(0);
            }
        }

        return allCols;

    }

    public void ensureFinalColsMatchWithSelectCols(OperatorQuery opQuery, List<FieldSchema> currentFinalCols){ //TODO enable correct check when same col but different type in input tables

        if(opQuery.getLocalQueryString().contains(" select ")){
            List<String> allCols = extractColsFromSelectPhrase(opQuery.getLocalQueryString());

            if(allCols.size() == 0){
                System.out.println("ensureFinalColsMatchWithSelectCols: Select Cols are 0!");
                System.exit(0);
            }

            System.out.println("ensureFinalColsMatchWithSelectCols: Select Cols are: "+allCols.toString());

            for(String col : allCols){
                System.out.println("ensureFinalColsMatchWithSelectCols: Attempting to locate owner of column: "+col);
                if(col.contains("agg_")){
                    System.out.println("ensureFinalColsMatchWithSelectCols: "+col+" is an aggregation column...Looking at input tables first...");
                    boolean found = false;
                    String type = "";
                    for (MyTable inputT : opQuery.getInputTables()) {
                        for (FieldSchema f : inputT.getAllCols()) {
                            if (f.getName().equals(col)) {
                                System.out.println("ensureFinalColsMatchWithSelectCols: " + col + " was located in table: " + inputT.getTableName() + " with type: " + f.getType());
                                type = f.getType();
                                found = true;
                                break;
                            }
                        }

                        if (found == true) break;
                    }

                    if (found == false) { //Aggregation column has not existed before
                        System.out.println("ensureFinalColsMatchWithSelectCols: Column: " + col + " does not exist in any input table! Input Tables: ");
                        for (MyTable inputT : opQuery.getInputTables()) {
                            System.out.println(inputT.getTableName());
                        }
                        System.out.println("ensureFinalColsMatchWithSelectCols: Assuming this is its first appearance! Nothing we can do from here!");
                    }
                    else { //Aggregation column exists in an input table

                        FieldSchema newF = new FieldSchema();
                        newF.setName(col);
                        newF.setType(type);

                        boolean alreadyAddedInOutputTable = false;
                        for (FieldSchema f2 : currentFinalCols) {
                            if (f2.getName().equals(col)) {
                                if (f2.getType().equals(type)) {
                                    alreadyAddedInOutputTable = true;
                                    break;
                                }
                            }
                        }

                        if (alreadyAddedInOutputTable) {
                            System.out.println("ensureFinalColsMatchWithSelectCols: Column: " + col + " with type: " + type + " already exists in output table cols!");
                        } else {
                            currentFinalCols.add(newF);
                        }
                    }
                }
                else if(col.contains("casteddecimal_")){
                    System.out.println("ensureFinalColsMatchWithSelectCols: "+col+" is a casted column alias...Looking at input tables first...");
                    boolean found = false;
                    String type = "";
                    for (MyTable inputT : opQuery.getInputTables()) {
                        for (FieldSchema f : inputT.getAllCols()) {
                            if (f.getName().equals(col)) {
                                System.out.println("ensureFinalColsMatchWithSelectCols: " + col + " was located in table: " + inputT.getTableName() + " with type: " + f.getType());
                                type = f.getType();
                                found = true;
                                break;
                            }
                        }

                        if (found == true) break;
                    }

                    if (found == false) { //Casted column alias has not existed before
                        System.out.println("ensureFinalColsMatchWithSelectCols: Column: " + col + " does not exist in any input table! Input Tables: ");
                        for (MyTable inputT : opQuery.getInputTables()) {
                            System.out.println(inputT.getTableName());
                        }
                        boolean notAdded = true;
                        String typeForCast = "decimal";
                        boolean existsInMap = false;
                        for(Map.Entry<String, MyMap> entry : operatorCastMap.entrySet()){
                            for(ColumnTypePair castPair : entry.getValue().getColumnAndTypeList()){
                                if(castPair.getAltAliasPairs().get(0).getExtraValue().equals(col)){
                                    System.out.println("ensureFinalColsMatchWithSelectCols: Located in castMap!");
                                    existsInMap = true;
                                    typeForCast = castPair.getLatestAltCastType();
                                    break;
                                }
                            }
                            if(existsInMap) break;
                        }

                        if(existsInMap == false){
                            System.out.println("ensureFinalColsMatchWithSelectCols: Failed to locate: "+col+" in castMap!");
                            System.exit(0);
                        }

                        for(FieldSchema oldField : currentFinalCols){
                            if(oldField.getName().equals(col)){
                                if(oldField.getType().equals(typeForCast)){
                                    notAdded = false;
                                }
                            }
                        }

                        if(notAdded){
                            FieldSchema theNewField = new FieldSchema();
                            theNewField.setName(col);
                            theNewField.setType(typeForCast);
                            currentFinalCols.add(theNewField);
                        }
                        System.out.println("ensureFinalColsMatchWithSelectCols: Assuming this is its first appearance! Nothing we can do from here!");
                    }
                    else { //Aggregation column exists in an input table

                        FieldSchema newF = new FieldSchema();
                        newF.setName(col);
                        newF.setType(type);

                        boolean alreadyAddedInOutputTable = false;
                        for (FieldSchema f2 : currentFinalCols) {
                            if (f2.getName().equals(col)) {
                                if (f2.getType().equals(type)) {
                                    alreadyAddedInOutputTable = true;
                                    break;
                                }
                            }
                        }

                        if (alreadyAddedInOutputTable) {
                            System.out.println("ensureFinalColsMatchWithSelectCols: Column: " + col + " with type: " + type + " already exists in output table cols!");
                        } else {
                            currentFinalCols.add(newF);
                        }
                    }
                }
                else {
                    boolean found = false;
                    String type = "";
                    for (MyTable inputT : opQuery.getInputTables()) {
                        for (FieldSchema f : inputT.getAllCols()) {
                            if (f.getName().equals(col)) {
                                System.out.println("ensureFinalColsMatchWithSelectCols: " + col + " was located in table: " + inputT.getTableName() + " with type: " + f.getType());
                                type = f.getType();
                                found = true;
                                break;
                            }
                        }

                        if (found == true) break;
                    }

                    if (found == false) {
                        System.out.println("ensureFinalColsMatchWithSelectCols: Column: " + col + " does not exist in any input table! Input Tables: ");
                        for (MyTable inputT : opQuery.getInputTables()) {
                            System.out.println(inputT.getTableName());
                        }
                        System.exit(0);
                    }

                    FieldSchema newF = new FieldSchema();
                    newF.setName(col);
                    newF.setType(type);

                    boolean alreadyAddedInOutputTable = false;
                    for (FieldSchema f2 : currentFinalCols) {
                        if (f2.getName().equals(col)) {
                            if (f2.getType().equals(type)) {
                                alreadyAddedInOutputTable = true;
                                break;
                            }
                        }
                    }

                    if (alreadyAddedInOutputTable) {
                        System.out.println("ensureFinalColsMatchWithSelectCols: Column: " + col + " with type: " + type + " already exists in output table cols!");
                    } else {
                        currentFinalCols.add(newF);
                    }
                }
            }
        }
        else{
            System.out.println("ensureFinalColsMatchWithSelectCols: Select phrase does not exist!");
            System.exit(0);
        }

    }

    public void addAnyExtraColsToOutputTable(OperatorQuery currentOpQuery, List<FieldSchema> currentFinalCols, List<FieldSchema> extraCols){

        int originalSize = currentFinalCols.size();

        if(extraCols != null){
            if(extraCols.size() > 0){

                    System.out.println("addAnyExtraColsToOutputTable: Extra Cols: ");

                    for (FieldSchema extraCol : extraCols) {
                        System.out.println("addAnyExtraColsToOutputTable: "+extraCol.getName());
                        boolean columnThatDoesNotExist = false;
                        for (FieldSchema f : currentFinalCols) {
                            if (extraCol.getName().equals(f.getName())) {
                                if (f.getType().equals(extraCol.getType())) {
                                    columnThatDoesNotExist = true;
                                    break;
                                }
                            }
                        }

                        if(columnThatDoesNotExist == false){
                            System.out.println("addAnyExtraColsToOutputTable: Column: "+extraCol.getName()+" does not exist in output table but will be needed later! Add it!");
                            currentFinalCols.add(extraCol);
                        }

                    }

                if(currentFinalCols.size() > originalSize){
                    System.out.println("addAnyExtraColsToOutputTable: We must create a new SELECT statement for this OperatorQuery and replace the old one!");
                    if(currentOpQuery.getLocalQueryString().contains(" from ")){
                        String[] parts = currentOpQuery.getLocalQueryString().split(" from ");
                        if(parts.length > 2){
                            System.out.println("addAnyExtraColsToOutputTable: MORE THAN 2 SELECT STATEMENTS MIGHT EXIST! UNSUPPORTED!");
                            System.exit(0);
                        }
                        String afterFromPhrase = parts[1];
                        afterFromPhrase = " from " + afterFromPhrase;

                        String selectString = "";
                        for(int i = 0; i < currentFinalCols.size(); i++){
                            if(i == currentFinalCols.size() - 1){
                                selectString = selectString + " " + currentFinalCols.get(i).getName()+ " ";
                            }
                            else{
                                selectString = selectString + " " + currentFinalCols.get(i).getName() + ",";
                            }

                        }

                        currentOpQuery.setLocalQueryString(" select"+selectString+afterFromPhrase);
                        currentOpQuery.setExaremeQueryString(" select"+selectString+afterFromPhrase);

                        System.out.println("addAnyExtraColsToOutputTable: Fixed Query: "+currentOpQuery.getLocalQueryString());

                    }
                    else{
                        System.out.println("addAnyExtraColsToOutputTable: NO FROM statement in Query!");
                        System.exit(0);
                    }
                }

            }
        }

    }

    public String finaliseOutputTable(OperatorNode currentOperatorNode, OperatorNode fatherOperatorNode, OperatorNode otherFatherNode, OperatorQuery currentOpQuery, String stringToExtractFrom, String outputTableName, MyTable outputTable, List<FieldSchema> newCols, String latestTable1, String latestTable2, List<FieldSchema> extraNeededCols){

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
            String actualType = "";
            if(pair.getColumnName().contains("_col")){ //If it contains _col (aka Unknown column)
                boolean foundMatch = false;
                if(pair.getColumnType().equals("struct")){ //If it is an aggregation column
                    boolean structFound = false;
                    for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){ //Get an aggreagationPair from the List
                        for(StringParameter sP : aggPair.getAltAliasPairs()){ //Get its alt aliases
                            if(sP.getParemeterType().equals(currentOperatorNode.getOperatorName())){ //Locate the currentNode in the List

                                if(sP.getValue().equals(pair.getColumnName())) { //Locate the value as an altAlias in the List

                                    structFound = true;
                                     //The latest altAlias is different from the first - This means that we need the altAlias of the father to refer to
                                        boolean aliasLocated = false;
                                        String properAliasName = "";
                                        properAliasName = fetchlatestRequiredAncestorOfAggr(currentOperatorNode, currentOpQuery, aggPair.getColumnName(), aggPair.getAltAliasPairs(), pair.getColumnName());

                                        if(stringToExtractFrom.contains(","+pair.getColumnName()+":")){
                                            stringToExtractFrom = stringToExtractFrom.replace(","+pair.getColumnName()+":", ","+properAliasName+":");
                                        }
                                        else if(stringToExtractFrom.contains("("+pair.getColumnName())){
                                            stringToExtractFrom = stringToExtractFrom.replace("("+pair.getColumnName()+":", "("+properAliasName+":");
                                        }

                                        foundMatch = true;

                                        System.out.println("Real Alias for: "+pair.getColumnName()+" is: "+properAliasName);

                                        pair.setColumnName(properAliasName);

                                        if(aggPair.getAltCastColumnTypes().size() > 0){
                                            int j = 0;
                                            for(String realType : aggPair.getAltCastColumnTypes()){
                                                if(j == aggPair.getAltCastColumnTypes().size() - 1){
                                                    pair.setColumnType(realType);
                                                }
                                                j++;
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
                                        foundMatch = true;
                                        break;
                                    }
                                }
                                else if ((otherFatherNode != null) && (sP.getParemeterType().equals(otherFatherNode.getOperatorName()))) {
                                    if (sP.getValue().equals(pair.getColumnName())) {
                                        System.out.println("Alias: " + pair.getColumnName() + " is a Constant value and will be ommited...");
                                        pair.setColumnName("__ommited_by_author_of_code__1213");
                                        foundToBeConstant = true;
                                        foundMatch = true;
                                        break;
                                    }
                                }
                                else if ((sP.getParemeterType().equals(currentOperatorNode.getOperatorName()))) {
                                    if (sP.getValue().equals(pair.getColumnName())) {
                                        System.out.println("Alias: " + pair.getColumnName() + " is a Constant value and will be ommited...");
                                        pair.setColumnName("__ommited_by_author_of_code__1213");
                                        foundToBeConstant = true;
                                        foundMatch = true;
                                        break;
                                    }
                                }
                            }

                            if (foundToBeConstant) break;
                        }
                    }

                    if(foundMatch == false){ //Extra search in aggrMap
                        for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){ //Get an aggreagationPair from the List
                            for(StringParameter sP : aggPair.getAltAliasPairs()){ //Get its alt aliases
                                if(sP.getParemeterType().equals(currentOperatorNode.getOperatorName())){ //Locate the currentNode in the List

                                    if(sP.getValue().equals(pair.getColumnName())) { //Locate the value as an altAlias in the List

                                        //The latest altAlias is different from the first - This means that we need the altAlias of the father to refer to
                                        boolean aliasLocated = false;
                                        String properAliasName = "";
                                        properAliasName = fetchlatestRequiredAncestorOfAggr(currentOperatorNode, currentOpQuery, aggPair.getColumnName(), aggPair.getAltAliasPairs(), pair.getColumnName());

                                        if(stringToExtractFrom.contains(","+pair.getColumnName()+":")){
                                            stringToExtractFrom = stringToExtractFrom.replace(","+pair.getColumnName()+":", ","+properAliasName+":");
                                        }
                                        else if(stringToExtractFrom.contains("("+pair.getColumnName())){
                                            stringToExtractFrom = stringToExtractFrom.replace("("+pair.getColumnName()+":", "("+properAliasName+":");
                                        }

                                        foundMatch = true;

                                        System.out.println("Real Alias for: "+pair.getColumnName()+" is: "+properAliasName);

                                        pair.setColumnName(properAliasName);

                                        if(aggPair.getAltCastColumnTypes().size() > 0){
                                            int j = 0;
                                            for(String realType : aggPair.getAltCastColumnTypes()){
                                                if(j == aggPair.getAltCastColumnTypes().size() - 1){
                                                    pair.setColumnType(realType);
                                                }
                                                j++;
                                            }
                                        }

                                    }

                                }
                            }
                        }
                    }

                }
            }
            else{
                boolean checkColumnIsValid = false;

                for(TableRegEntry tableRegEntry : tableRegistry.getEntries()){
                    for(ColumnTypePair cP : tableRegEntry.getColumnTypeMap().getColumnAndTypeList()){
                        if(cP.getColumnType().equals(pair.getColumnType()) || (cP.getLatestAltCastType().equals(pair.getColumnType()) )){
                            if(cP.getColumnName().equals(pair.getColumnName())){
                                String extraAlias="";
                                if(tableRegEntry.getAlias().equals(tableRegEntry.getAssociatedTable().getTableName()) == false){
                                    extraAlias = tableRegEntry.getAlias() + ".";
                                }
                                pair.setColumnName(extraAlias+pair.getColumnName());

                                if(cP.getLatestAltCastType().equals(pair.getColumnType())){
                                    if(pair.getColumnType().equals(cP.getColumnType()) == false) {
                                        boolean foundCast = false;
                                        for (Map.Entry<String, MyMap> entry : operatorCastMap.entrySet()) {
                                            if (entry.getKey().equals(currentOperatorNode.getOperatorName())) {
                                                for (ColumnTypePair entryCol : entry.getValue().getColumnAndTypeList()) {
                                                    if (entryCol.getColumnName().equals(cP.getColumnName())) {
                                                        pair.setColumnName(entryCol.getAltAliasPairs().get(0).getExtraValue());
                                                        System.out.println("addSelectStatementToQuery: FIX - " + cP.getColumnName() + " becomes: " + entryCol.getAltAliasPairs().get(0).getExtraValue());
                                                        foundCast = true;
                                                        break;
                                                    }
                                                }

                                                if(foundCast) break;
                                            }
                                        }
                                    }
                                }

                                checkColumnIsValid = true;
                                break;
                            }
                            else{
                                for(StringParameter sP : cP.getAltAliasPairs()){
                                    if(sP.getParemeterType().equals(currentOperatorNode.getOperatorName())){
                                        if(sP.getValue().equals(pair.getColumnName())){
                                            String extraAlias="";
                                            if(tableRegEntry.getAlias().equals(tableRegEntry.getAssociatedTable().getTableName()) == false){
                                                extraAlias = tableRegEntry.getAlias() + ".";
                                            }
                                            pair.setColumnName(extraAlias+pair.getColumnName());

                                            if(cP.getLatestAltCastType().equals(pair.getColumnType())){
                                                if(pair.getColumnType().equals(cP.getColumnType()) == false) {
                                                    boolean foundCast = false;
                                                    for (Map.Entry<String, MyMap> entry : operatorCastMap.entrySet()) {
                                                        if (entry.getKey().equals(currentOperatorNode.getOperatorName())) {
                                                            for (ColumnTypePair entryCol : entry.getValue().getColumnAndTypeList()) {
                                                                if (entryCol.getColumnName().equals(cP.getColumnName())) {
                                                                    pair.setColumnName(entryCol.getAltAliasPairs().get(0).getExtraValue());
                                                                    System.out.println("addSelectStatementToQuery: FIX - " + cP.getColumnName() + " becomes: " + entryCol.getAltAliasPairs().get(0).getExtraValue());
                                                                    foundCast = true;
                                                                    break;
                                                                }
                                                            }

                                                            if(foundCast) break;
                                                        }
                                                    }
                                                }
                                            }

                                            currentOpQuery.addUsedColumn(extraAlias+pair.getColumnName(), latestTable1);
                                            currentOpQuery.addUsedColumn(extraAlias+pair.getColumnName(), latestTable2);
                                            checkColumnIsValid = true;
                                            break;
                                        }
                                    }
                                }

                                if(checkColumnIsValid) break;
                            }
                        }
                    }

                    if(checkColumnIsValid) break;
                }

                if(checkColumnIsValid == false){
                    //Check in aggregation map then
                    for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){ //Get an aggreagationPair from the List
                        for(StringParameter sP : aggPair.getAltAliasPairs()){ //Get its alt aliases
                            if(sP.getParemeterType().equals(currentOperatorNode.getOperatorName())){ //Locate the currentNode in the List

                                if(sP.getValue().equals(pair.getColumnName())) { //Locate the value as an altAlias in the List

                                    //The latest altAlias is different from the first - This means that we need the altAlias of the father to refer to
                                    boolean aliasLocated = false;
                                    String properAliasName = "";
                                    properAliasName = fetchlatestRequiredAncestorOfAggr(currentOperatorNode, currentOpQuery, aggPair.getColumnName(), aggPair.getAltAliasPairs(), pair.getColumnName());

                                    if(stringToExtractFrom.contains(","+pair.getColumnName()+":")){
                                        stringToExtractFrom = stringToExtractFrom.replace(","+pair.getColumnName()+":", ","+properAliasName+":");
                                    }
                                    else if(stringToExtractFrom.contains("("+pair.getColumnName())){
                                        stringToExtractFrom = stringToExtractFrom.replace("("+pair.getColumnName()+":", "("+properAliasName+":");
                                    }

                                    checkColumnIsValid = true;

                                    System.out.println("Real Alias for: "+pair.getColumnName()+" is: "+properAliasName);

                                    pair.setColumnName(properAliasName);

                                    if(aggPair.getAltCastColumnTypes().size() > 0){
                                        int j = 0;
                                        for(String realType : aggPair.getAltCastColumnTypes()){
                                            if(j == aggPair.getAltCastColumnTypes().size() - 1){
                                                pair.setColumnType(realType);
                                            }
                                            j++;
                                        }
                                    }

                                }

                            }
                        }
                    }

                }

                if(checkColumnIsValid == false){
                    System.out.println("Alias: "+pair.getColumnName()+" is not valid!");
                    System.exit(0);
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
        ensureFinalColsMatchWithSelectCols(currentOpQuery, newCols);
        addAnyExtraColsToOutputTable(currentOpQuery, newCols, extraNeededCols);

        System.out.println("OutputCols: "+newCols.toString());

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

        System.out.println("Will translate the following cols: ");

        List<FieldSchema> allCols = someTable.getAllCols();

        for(FieldSchema c : allCols){
            System.out.println("Name: "+c.getName()+" - Type: "+c.getType());
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
                else if(type.contains("struct")){
                    for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                        if(aggPair.getAltCastColumnTypes().size() > 0){
                            for(StringParameter sP : aggPair.getAltAliasPairs()){
                                if(sP.getExtraValue().equals(allCols.get(i).getName())){
                                    for(int k = 0; k < aggPair.getAltCastColumnTypes().size(); k++){
                                        if(k == aggPair.getAltCastColumnTypes().size() - 1){
                                            String altType = aggPair.getAltCastColumnTypes().get(k);
                                            if(altType.contains("int")){
                                                definition = definition.concat("INT");
                                            }
                                            else if(altType.contains("string")){
                                                definition = definition.concat("TEXT");
                                            }
                                            else if(altType.contains("decimal")){
                                                definition = definition.concat("DECIMAL(10,5)");
                                            }
                                            else if(altType.contains("char")){
                                                definition = definition.concat("TEXT");
                                            }
                                            else if(altType.contains("varchar")){
                                                definition = definition.concat("TEXT");
                                            }
                                            else if(altType.contains("date")){
                                                definition = definition.concat("DATE");
                                            }
                                            else if(altType.contains("float")){
                                                definition = definition.concat("FLOAT");
                                            }
                                            else if(altType.contains("double")){
                                                definition = definition.concat("REAL");
                                            }
                                            else if(altType.contains("double precision")){
                                                definition = definition.concat("REAL");
                                            }
                                            else if(altType.contains("bigint")){
                                                definition = definition.concat("BIGINT");
                                            }
                                            else if(altType.contains("smallint")){
                                                definition = definition.concat("SMALLINT");
                                            }
                                            else if(altType.contains("tinyint")){
                                                definition = definition.concat("TINYINT");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
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
                else if(type.contains("struct")){
                    for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                        if(aggPair.getAltCastColumnTypes().size() > 0){
                            for(StringParameter sP : aggPair.getAltAliasPairs()){
                                if(sP.getExtraValue().equals(allCols.get(i).getName())){
                                    for(int k = 0; k < aggPair.getAltCastColumnTypes().size(); k++){
                                        if(k == aggPair.getAltCastColumnTypes().size() - 1){
                                            String altType = aggPair.getAltCastColumnTypes().get(k);
                                            if(altType.contains("int")){
                                                definition = definition.concat("INT,");
                                            }
                                            else if(altType.contains("string")){
                                                definition = definition.concat("TEXT,");
                                            }
                                            else if(altType.contains("decimal")){
                                                definition = definition.concat("DECIMAL(10,5),");
                                            }
                                            else if(altType.contains("char")){
                                                definition = definition.concat("TEXT,");
                                            }
                                            else if(altType.contains("varchar")){
                                                definition = definition.concat("TEXT,");
                                            }
                                            else if(altType.contains("date")){
                                                definition = definition.concat("DATE,");
                                            }
                                            else if(altType.contains("float")){
                                                definition = definition.concat("FLOAT,");
                                            }
                                            else if(altType.contains("double")){
                                                definition = definition.concat("REAL,");
                                            }
                                            else if(altType.contains("double precision")){
                                                definition = definition.concat("REAL,");
                                            }
                                            else if(altType.contains("bigint")){
                                                definition = definition.concat("BIGINT,");
                                            }
                                            else if(altType.contains("smallint")){
                                                definition = definition.concat("SMALLINT,");
                                            }
                                            else if(altType.contains("tinyint")){
                                                definition = definition.concat("TINYINT,");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                else{
                    System.out.println("exaremeTableDefinition: Unsupported Hive Type! Type: "+type);
                    System.exit(0);
                }
            }
        }

        definition = "create table "+someTable.getTableName().toLowerCase()+" ("+definition+" )";

        System.out.println("TABLE: "+someTable.getTableName()+" - DEFINITION: "+definition);

        return definition;
    }

    public String exaremeTableDefinition(MyPartition somePartition){
        String definition = "";

        List<FieldSchema> allCols = somePartition.getAllFields();

        System.out.println("Will translate the following cols: ");

        for(FieldSchema c : allCols){
            System.out.println("Name: "+c.getName()+" - Type: "+c.getType());
        }

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
                    definition = definition.concat("FLOAT");
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
                else if(type.contains("struct")){
                    for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                        if(aggPair.getAltCastColumnTypes().size() > 0){
                            for(StringParameter sP : aggPair.getAltAliasPairs()){
                                if(sP.getExtraValue().equals(allCols.get(i).getName())){
                                    for(int k = 0; k < aggPair.getAltCastColumnTypes().size(); k++){
                                        if(k == aggPair.getAltCastColumnTypes().size() - 1){
                                            String altType = aggPair.getAltCastColumnTypes().get(k);
                                            if(altType.contains("int")){
                                                definition = definition.concat("INT");
                                            }
                                            else if(altType.contains("string")){
                                                definition = definition.concat("TEXT");
                                            }
                                            else if(altType.contains("decimal")){
                                                definition = definition.concat("DECIMAL(10,5)");
                                            }
                                            else if(altType.contains("char")){
                                                definition = definition.concat("TEXT");
                                            }
                                            else if(altType.contains("varchar")){
                                                definition = definition.concat("TEXT");
                                            }
                                            else if(altType.contains("date")){
                                                definition = definition.concat("DATE");
                                            }
                                            else if(altType.contains("float")){
                                                definition = definition.concat("FLOAT");
                                            }
                                            else if(altType.contains("double")){
                                                definition = definition.concat("REAL");
                                            }
                                            else if(altType.contains("double precision")){
                                                definition = definition.concat("REAL");
                                            }
                                            else if(altType.contains("bigint")){
                                                definition = definition.concat("BIGINT");
                                            }
                                            else if(altType.contains("smallint")){
                                                definition = definition.concat("SMALLINT");
                                            }
                                            else if(altType.contains("tinyint")){
                                                definition = definition.concat("TINYINT");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
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
                else if(type.contains("struct")){
                    for(ColumnTypePair aggPair : aggregationsMap.getColumnAndTypeList()){
                        if(aggPair.getAltCastColumnTypes().size() > 0){
                            for(StringParameter sP : aggPair.getAltAliasPairs()){
                                if(sP.getExtraValue().equals(allCols.get(i).getName())){
                                    for(int k = 0; k < aggPair.getAltCastColumnTypes().size(); k++){
                                        if(k == aggPair.getAltCastColumnTypes().size() - 1){
                                            String altType = aggPair.getAltCastColumnTypes().get(k);
                                            if(altType.contains("int")){
                                                definition = definition.concat("INT,");
                                            }
                                            else if(altType.contains("string")){
                                                definition = definition.concat("TEXT,");
                                            }
                                            else if(altType.contains("decimal")){
                                                definition = definition.concat("DECIMAL(10,5),");
                                            }
                                            else if(altType.contains("char")){
                                                definition = definition.concat("TEXT,");
                                            }
                                            else if(altType.contains("varchar")){
                                                definition = definition.concat("TEXT,");
                                            }
                                            else if(altType.contains("date")){
                                                definition = definition.concat("DATE,");
                                            }
                                            else if(altType.contains("float")){
                                                definition = definition.concat("FLOAT,");
                                            }
                                            else if(altType.contains("double")){
                                                definition = definition.concat("REAL,");
                                            }
                                            else if(altType.contains("double precision")){
                                                definition = definition.concat("REAL,");
                                            }
                                            else if(altType.contains("bigint")){
                                                definition = definition.concat("BIGINT,");
                                            }
                                            else if(altType.contains("smallint")){
                                                definition = definition.concat("SMALLINT,");
                                            }
                                            else if(altType.contains("tinyint")){
                                                definition = definition.concat("TINYINT,");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
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

    public boolean fileOrDirExistsInHDFSDirectory(String path, String directory){ //Directory example: /base/warehouse/tpcds_db.db
                                                                                  //Seeking file example: /base/warehouse/tpcds_db.db/catalog_sales.0.db
        Path thePath = new Path(directory);

        System.out.print("\nPrinting FileStatuses for given path: ");
        try {
            FileStatus[] fileStatusArray = hadoopFS.listStatus(thePath); //Run ls on directory
            if (fileStatusArray != null) {
                for (FileStatus fileStatus : fileStatusArray) { //Look every result line
                    System.out.println("FileStatus: " + fileStatus.toString());
                    String[] parts = fileStatus.toString().split(";"); //Ommit beggining part of FileStatus
                    String tmpPath = parts[0].replace("FileStatus{path=hdfs://localhost:", "");
                    char[] oldCharArray = tmpPath.toCharArray();
                    char[] finalCharArray = new char[tmpPath.length() - 5];
                    for(int j = 0; j < finalCharArray.length; j++){
                        finalCharArray[j] = oldCharArray[j+5];
                    }

                    String finalPath = new String(finalCharArray); //If string are equal we have located the file
                    if(finalPath.equals(path)){
                        return true;
                    }
                }
            }
        } catch(java.io.IOException ex){
            System.out.print("\nHadoop LS Exception:"+ex.getMessage());
            System.exit(0);
        }

        return false;

    }

    private static void execAndPrintResults(String query, MadisProcess proc) throws Exception {

        System.out.println("execAndPrintResults: Query = "+query);
        QueryResultStream stream = proc.execQuery(query);
        System.out.println(stream.getSchema());
        String record = stream.getNextRecord();
        while (record != null) {
            System.out.println("execAndPrintResults: "+record);
            record = stream.getNextRecord();
        }

    }

    public void deleteOnExitFiles(List<String> filesList){

        for(String filePath : filesList){
            File tempFile = new File(filePath);
            if(tempFile.exists()){
                tempFile.delete();
            }
        }

    }

    public String clearColumnFromUDFToLong(String phrase){

        if(phrase.contains("UDFToLong")) {
            System.out.println("Phrase: "+phrase+" contains UDFToLong, removing...");
            phrase = phrase.replace("UDFToLong", "");
            //phrase = phrase.replace("(", "");
            //phrase = phrase.replace(")", "");
        }

        return phrase;

    }

    public String clearColumnFromUDFToDouble(String phrase){

        if(phrase.contains("UDFToDouble")) {
            System.out.println("Phrase: "+phrase+" contains UDFToDouble, removing...");
            phrase = phrase.replace("UDFToDouble", "");
            //phrase = phrase.replace("(", "");
            //phrase = phrase.replace(")", "");
        }

        return phrase;

    }

    public String clearColumnFromUDFUpper(String phrase){

        if(phrase.contains("upper(")) {
            System.out.println("Phrase: "+phrase+" contains UDFupper, removing...");
            phrase = phrase.replace("upper", "");
            //phrase = phrase.replace("(", "");
            //phrase = phrase.replace(")", "");
        }

        return phrase;

    }

    public String clearColumnFromUDFLower(String phrase){

        if(phrase.contains("lower(")) {
            System.out.println("Phrase: "+phrase+" contains UDFlower, removing...");
            phrase = phrase.replace("lower", "");
            //phrase = phrase.replace("(", "");
            //phrase = phrase.replace(")", "");
        }

        return phrase;

    }

    public void createSQLiteTableFromHiveTable(String dbPath, String targetDBName , String targetTableName, String hiveLocation, MyTable inputTable){

        List<String> filesToDeleteList = new LinkedList<>();

        //BasicConfigurator.configure();
        //Logger.getRootLogger().setLevel(Level.ERROR);

        try {
            /*---Fetch .dat from HDFS----*/
            System.out.println("createSQLiteTableFromHiveTable: Will fetch HDFS file: "+dbPath + hiveLocation);

            String localPathForDat = "/tmp/adpHive/" + hiveLocation.replace("/", "_");
            try {
                hadoopFS.copyToLocalFile(new Path(dbPath + hiveLocation), new Path(localPathForDat));
            } catch(java.io.IOException fetchEx){
                System.out.println("HDFS fetch exception: "+fetchEx.getMessage());
                System.exit(0);
            }

            File testFile = new File(localPathForDat);

            if(testFile.exists()){
                System.out.println("createSQLiteTableFromHiveTable: File successfully transferred to path: "+localPathForDat);
            }
            else{
                System.out.println("createSQLiteTableFromHiveTable: Failed to transfer file to path: "+localPathForDat);
                System.exit(0);
            }

            filesToDeleteList.add(localPathForDat);

            /*----Create the .db file -----*/
            String url = "jdbc:sqlite:" + "/tmp/adpHive/" + targetDBName; //The location of the new .db

            System.out.println("createSQLiteTableFromHiveTable: Will create new SQLite .db file in location: "+url);

            //Open connection
            Class.forName("org.sqlite.JDBC");
            Connection conn = DriverManager.getConnection(url);
            System.out.println("createSQLiteTableFromHiveTable: Successfully Opened Database!");

            filesToDeleteList.add("/tmp/adpHive/" + targetDBName);

            System.out.println("createSQLiteTableFromHiveTable:Now Running MadisProcess to fill the new table!");

            MadisProcess madisProcess = new MadisProcess("",madisPath);
            //Start Madis
            try {
                System.out.println("Start Madis...");
                madisProcess.start();
            } catch(java.io.IOException madisStartEx){
                System.out.println("IO Exception in madisProcess.start(): "+madisStartEx.getMessage());
                deleteOnExitFiles(filesToDeleteList);
                System.exit(0);
            }
            catch(Exception simpleEx){
                System.out.println("Exception in madisProcess.start(): "+simpleEx.getMessage());
                deleteOnExitFiles(filesToDeleteList);
                System.exit(0);
            }

            try {
                System.out.println("Attach...");
                execAndPrintResults("attach database '" + "/tmp/adpHive/" + targetDBName + "' as " + targetTableName + "; \n", madisProcess);
            } catch(java.lang.Exception q1){
                System.out.println("Exceptin in attach: "+q1.getMessage());
                deleteOnExitFiles(filesToDeleteList);
                System.exit(0);
            }

            try {
                System.out.println("Drop...");
                execAndPrintResults("drop table if exists " + targetTableName + "." + targetTableName + "; \n", madisProcess);
            } catch(java.lang.Exception q1){
                System.out.println("Exception in drop: "+q1.getMessage());
                deleteOnExitFiles(filesToDeleteList);
                System.exit(0);
            }

            try {
                System.out.println("Create...");
                execAndPrintResults("create table " + targetTableName + "." + targetTableName + " as " + inputTable.getRootHiveTableDefinition() + "from (file '"+ localPathForDat +"' delimiter:| fast:1) ; \n", madisProcess);
            } catch(java.lang.Exception q1){
                System.out.println("Exception in create: "+q1.getMessage());
                deleteOnExitFiles(filesToDeleteList);
                System.exit(0);
            }

            try {
                System.out.println("Stop Madis...");
                madisProcess.stop();
            } catch(java.io.IOException ioEx){
                System.out.println("IO Exception in madisProcess.stop(): "+ioEx.getMessage());
                deleteOnExitFiles(filesToDeleteList);
                System.exit(0);
            }
            catch(java.lang.InterruptedException interEx){
                System.out.println("java.lang.interruptedEx madisProcess.stop(): "+interEx.getMessage());
                deleteOnExitFiles(filesToDeleteList);
                System.exit(0);
            }
            catch(Exception stopEx){
                System.out.println("Exception in madisProcess.start(): "+stopEx.getMessage());
                deleteOnExitFiles(filesToDeleteList);
                System.exit(0);
            }


            //Close the connection
            conn.close();

        }



        //"Multicatch":
        catch (SQLException | ClassNotFoundException e) {
            System.err.println("Database problem: " + e);
        }

        /*---Put .db in HDFS----*/
        System.out.println("createSQLiteTableFromHiveTable: Putting the .db in HDFS for Exareme to find...");
        String localPathForDB = "/tmp/adpHive/" + targetDBName;
        String hdfsPathForDB = dbPath + targetDBName;

        try {
            hadoopFS.copyFromLocalFile(new Path(localPathForDB), new Path(hdfsPathForDB));
        } catch(java.io.IOException fetchEx){
            System.out.println("HDFS put exception: "+fetchEx.getMessage());
            deleteOnExitFiles(filesToDeleteList);
            System.exit(0);
        }

        deleteOnExitFiles(filesToDeleteList);

    }

    public void createSQLiteTableFromHivePartition(String dbPath, String targetDBName , String targetTableName, String hiveLocation, MyPartition inputPartition){

        List<String> filesToDeleteList = new LinkedList<>();

        //BasicConfigurator.configure();
        //Logger.getRootLogger().setLevel(Level.ERROR);

        try {
            /*---Fetch .dat from HDFS----*/
            System.out.println("createSQLiteTableFromHiveTable: Will fetch HDFS file: "+dbPath + hiveLocation);

            String localPathForDat = "/tmp/adpHive/" + hiveLocation.replace("/", "_");
            try {
                hadoopFS.copyToLocalFile(new Path(dbPath + hiveLocation), new Path(localPathForDat));
            } catch(java.io.IOException fetchEx){
                System.out.println("HDFS fetch exception: "+fetchEx.getMessage());
                System.exit(0);
            }

            File testFile = new File(localPathForDat);

            if(testFile.exists()){
                System.out.println("createSQLiteTableFromHiveTable: File successfully transferred to path: "+localPathForDat);
            }
            else{
                System.out.println("createSQLiteTableFromHiveTable: Failed to transfer file to path: "+localPathForDat);
                System.exit(0);
            }

            filesToDeleteList.add(localPathForDat);

            /*----Create the .db file -----*/
            String url = "jdbc:sqlite:" + "/tmp/adpHive/" + targetDBName; //The location of the new .db

            System.out.println("createSQLiteTableFromHiveTable: Will create new SQLite .db file in location: "+url);

            //Open connection
            Class.forName("org.sqlite.JDBC");
            Connection conn = DriverManager.getConnection(url);
            System.out.println("createSQLiteTableFromHiveTable: Successfully Opened Database!");

            filesToDeleteList.add("/tmp/adpHive/" + targetDBName);

            System.out.println("createSQLiteTableFromHiveTable:Now Running MadisProcess to fill the new table!");

            MadisProcess madisProcess = new MadisProcess("", madisPath);
            //Start Madis
            try {
                madisProcess.start();
            } catch(java.io.IOException madisStartEx){
                System.out.println("IO Exception in madisProcess.start(): "+madisStartEx.getMessage());
                deleteOnExitFiles(filesToDeleteList);
                System.exit(0);
            }

            try {
                execAndPrintResults("attach database '" + "/tmp/adpHive/" + targetDBName + "' as " + targetTableName + "; \n", madisProcess);
            } catch(java.lang.Exception q1){
                System.out.println("Exceptin in attach: "+q1.getMessage());
                deleteOnExitFiles(filesToDeleteList);
                System.exit(0);
            }

            try {
                execAndPrintResults("drop table if exists " + targetTableName + "." + targetTableName + "; \n", madisProcess);
            } catch(java.lang.Exception q1){
                System.out.println("Exception in drop: "+q1.getMessage());
                deleteOnExitFiles(filesToDeleteList);
                System.exit(0);
            }

            try {
                execAndPrintResults("create table " + targetTableName + "." + targetTableName + " as " + inputPartition.getRootHiveTableDefinition() + "from (file '"+ localPathForDat +"' delimiter:| fast:1) ; \n", madisProcess);
            } catch(java.lang.Exception q1){
                System.out.println("Exception in create: "+q1.getMessage());
                deleteOnExitFiles(filesToDeleteList);
                System.exit(0);
            }

            if (inputPartition.getSecondaryNeededQueries().size() > 0) { //Extra Queries that have to be issued to finalise the transition from Hive Partition
                for (String q : inputPartition.getSecondaryNeededQueries()) {
                    if (q.contains("add column")) { //ALTER ADD COLUMN
                        try {
                            execAndPrintResults("alter table " + targetTableName + "." + targetTableName + " " + q + "; \n", madisProcess);
                        } catch(java.lang.Exception q1){
                            System.out.println("Exception in alter: "+q1.getMessage());
                            deleteOnExitFiles(filesToDeleteList);
                            System.exit(0);
                        }
                    } else { //UPDATE
                        try {
                            execAndPrintResults("update " + targetTableName + "." + targetTableName + " " + q + "; \n", madisProcess);
                        } catch(java.lang.Exception q1){
                            System.out.println("Exception in update: "+q1.getMessage());
                            deleteOnExitFiles(filesToDeleteList);
                            System.exit(0);
                        }
                    }
                }
            }

            try {
                madisProcess.stop();
            } catch(java.io.IOException ioEx){
                System.out.println("IO Exception in madisProcess.stop(): "+ioEx.getMessage());
                deleteOnExitFiles(filesToDeleteList);
                System.exit(0);
            }
            catch(java.lang.InterruptedException interEx){
                System.out.println("java.lang.interruptedEx madisProcess.stop(): "+interEx.getMessage());
                deleteOnExitFiles(filesToDeleteList);
                System.exit(0);
            }

            //Close the connection
            conn.close();

        }

        //"Multicatch":
        catch (SQLException | ClassNotFoundException e) {
            System.err.println("Database problem: " + e);
        }

        /*---Put .db in HDFS----*/
        System.out.println("createSQLiteTableFromHiveTable: Putting the .db in HDFS for Exareme to find...");
        String localPathForDB = "/tmp/adpHive/" + targetDBName;
        String hdfsPathForDB = dbPath + targetDBName;

        try {
            hadoopFS.copyFromLocalFile(new Path(localPathForDB), new Path(hdfsPathForDB));
        } catch(java.io.IOException fetchEx){
            System.out.println("HDFS put exception: "+fetchEx.getMessage());
            deleteOnExitFiles(filesToDeleteList);
            System.exit(0);
        }

        deleteOnExitFiles(filesToDeleteList);

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
            if(opQuery.getExaremeQueryString().contains(" decimal)")){
                opQuery.setExaremeQueryString(opQuery.getExaremeQueryString().replace(" decimal)", " decimal(10,5))"));
            }
            List<String> inputNames = new LinkedList<>();
            String outputCols = "";
            String usedColstring = "";
            for(MyTable inputT : opQuery.getInputTables()){
                inputNames.add(inputT.getTableName());
            }
            System.out.println("translateToExaremeOps: Inputs: "+inputNames.toString());
            System.out.println("translateToExaremeOps: Output: "+opQuery.getOutputTable().getTableName());
            for(int i = 0; i < opQuery.getOutputTable().getAllCols().size(); i++){
                FieldSchema f = opQuery.getOutputTable().getAllCols().get(i);
                if(i == 0){
                    outputCols = "("+f.getName()+","+f.getType()+") , ";
                }
                else{
                    outputCols = outputCols + "("+f.getName()+","+f.getType()+") ";
                }
            }


            System.out.println("translateToExaremeOps: OutputCols: "+outputCols);
            //opQuery.getUsedColumns().printMap();
            String usedColsString = "";
            List<ColumnTypePair> usedColsList = opQuery.getUsedColumns().getColumnAndTypeList();
            for(int i = 0; i < usedColsList.size(); i++){
                ColumnTypePair f = usedColsList.get(i);
                if(i == 0){
                    usedColsString = "("+f.getColumnName()+","+f.getColumnType()+") , ";
                }
                else{
                    usedColsString = usedColsString + "("+f.getColumnName()+","+f.getColumnType()+") ";
                }
            }
            System.out.println("translateToExaremeOps: Used cols: "+usedColsString);
            System.out.flush();
        }

        System.out.println();
        for(OperatorQuery opQuery : allQueries){
            System.out.println("translateToExaremeOps: \tOperatorQuery: ["+opQuery.getExaremeQueryString()+" ]");
        }

        String dbPath = "";

        /*if(currentDatabasePath.toCharArray()[currentDatabasePath.length() - 1] == '/'){
            dbPath = currentDatabasePath;
        }
        else{
            dbPath = currentDatabasePath + "/";
        }

        //Ensure input tables are converted to SQLITE .db files in hdfs
        System.out.println("translateToExaremeOps: -----------Converting .dat tables to .db ------------\n");
        for(OperatorQuery opQuery : allQueries){
            for(MyTable inputTable : opQuery.getInputTables()) {
                if (inputTable.getIsRootInput()) {
                    if (inputTable.getHasPartitions()) {
                        int j = 0;
                        for (MyPartition inputPart : inputTable.getAllPartitions()) {
                            if (fileOrDirExistsInHDFSDirectory(dbPath + inputPart.getBelogingTableName() + "." + j + ".db", dbPath) == false) { //Input .db does not exist
                                createSQLiteTableFromHivePartition(dbPath, inputPart.getBelogingTableName() + "." + j + ".db", inputPart.getBelogingTableName(), inputPart.getRootHiveLocationPath(), inputPart);
                            } else {
                                System.out.println("Input .db: " + inputTable.getTableName() + "." + j + ".db exists! Must be deleted first!");
                                try {
                                    hadoopFS.delete(new Path(dbPath + inputPart.getBelogingTableName() + "." + j + ".db"), false);
                                } catch (java.io.IOException delEx) {
                                    System.out.println("Exception while deleting: " + dbPath + inputPart.getBelogingTableName() + "." + j + ".db - Message: " + delEx.getMessage());
                                    System.exit(0);
                                }
                                createSQLiteTableFromHivePartition(dbPath, inputPart.getBelogingTableName() + "." + j + ".db", inputPart.getBelogingTableName(), inputPart.getRootHiveLocationPath(), inputPart);
                            }
                            j++;
                        }
                    } else {
                        if (fileOrDirExistsInHDFSDirectory(dbPath + inputTable.getTableName() + ".0.db", dbPath) == false) { //Input .db does not exist
                            createSQLiteTableFromHiveTable(dbPath, inputTable.getTableName() + ".0.db", inputTable.getTableName(), inputTable.getRootHiveLocationPath(), inputTable);
                        } else {
                            System.out.println("Input .db: " + inputTable.getTableName() + ".0.db exists! Ready!");
                        }
                    }
                }
            }
        }

        System.out.println("After the above operations the directory: "+dbPath+" now contains: ");
        try {
            FileStatus[] fileStatusArray = hadoopFS.listStatus(new Path(dbPath)); //Run ls on directory
            if (fileStatusArray != null) {
                for (FileStatus fileStatus : fileStatusArray) { //Look every result line
                    System.out.println("FileStatus: " + fileStatus.toString());
                }
            }
        } catch(java.io.IOException ex){
            System.out.print("\nHadoop LS Exception:"+ex.getMessage());
            System.exit(0);
        }*/


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
