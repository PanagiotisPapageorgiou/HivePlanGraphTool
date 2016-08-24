package com.inmobi.hive.test;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created by panos on 19/7/2016.
 */

//TODO CHECK NOT ONLY KEY BUT VALUE ALSO IF ENTRIES ARE EQUAL
//TODO INCORPORATE ALTERNATE ALIASES FOR JOIN RETURN PROBLEM

public class QueryBuilder {

    ExaremeGraph exaremeGraph;
    MyMap columnAndTypeMap; //_col0 - int .etc
    List<OperatorQuery> allQueries;
    List<MyTable> inputTables;
    List<MyTable> outputTables;

    public QueryBuilder(ExaremeGraph graph, List<MyTable> inputT, List<MyPartition> inputP, List<MyTable> outputT, List<MyPartition> outputP){
        exaremeGraph = graph;
        allQueries = new LinkedList<>();
        columnAndTypeMap = new MyMap();
        inputTables = inputT;
        outputTables = outputT;

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
                        for (FieldSchema f : inputTable.getAllCols()) {
                            System.out.println("\t\tColName: " + f.getName() + " - ColType: " + f.getType());
                        }
                    } else {
                        System.out.println("InputTable has no PartitionKeys!");
                        System.exit(0);
                    }
                } else {
                    System.out.println("InputTable has no PartitionKeys!");
                    System.exit(0);
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
                System.out.println("\tBelongingDataBase: " + outputTable.getBelongingDataBaseName());
                System.out.println("\tTableHDFSPath: " + outputTable.getTableHDFSPath());
                System.out.println("\tHasPartitions: " + outputTable.getHasPartitions());

                if (outputTable.getAllCols() != null) {
                    System.out.println("\tAllCols: ");
                    if (outputTable.getAllCols().size() > 0) {
                        for (FieldSchema f : outputTable.getAllCols()) {
                            System.out.println("\t\tColName: " + f.getName() + " - ColType: " + f.getType());
                        }
                    } else {
                        System.out.println("OutputTable has no Columns!");
                        System.exit(0);
                    }
                } else {
                    System.out.println("OutputTable has no Columns!");
                    System.exit(0);
                }

                if (outputTable.getAllFields() != null) {
                    System.out.println("\tAllFields: ");
                    if (outputTable.getAllFields().size() > 0) {
                        for (StructField f : outputTable.getAllFields()) {
                            System.out.println("\t\tFieldName: " + f.getFieldName());
                        }
                    } else {
                        System.out.println("OutputTable has no Columns!");
                        System.exit(0);
                    }
                } else {
                    System.out.println("OutputTable has no Fields!");
                    System.exit(0);
                }

                if (outputTable.getHasPartitions() == true) {
                    System.out.println("\tOutputTable IS partitioned!");
                } else {
                    System.out.println("\tOutputTable IS NOT partitioned!");
                }

                if (outputTable.getAllPartitionKeys() != null) {
                    System.out.println("\tAllPartitionKeys: ");
                    if (outputTable.getAllCols().size() > 0) {
                        for (FieldSchema f : outputTable.getAllCols()) {
                            System.out.println("\t\tColName: " + f.getName() + " - ColType: " + f.getType());
                        }
                    } else {
                        System.out.println("OutputTable has no PartitionKeys!");
                        System.exit(0);
                    }
                } else {
                    System.out.println("OutputTable has no PartitionKeys!");
                    System.exit(0);
                }

                LinkedHashMap<List<FieldSchema>, LinkedHashMap<List<String>, MyPartition>> mapFieldValueCombos = outputTable.getPartitionKeysValuesMap();
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

        /*System.out.println("Now creating newMap with Keys having format table.col where applicable...");

        List<ColumnTypePair> columnTypePairList = columnAndTypeMap.getColumnAndTypeList();
        for(ColumnTypePair pair: columnTypePairList){
            boolean existsInSchema = false;
            for(String tuple : parts){
                if(tuple != null){
                    if(tuple.contains(":")) {
                        String[] tupleParts = tuple.split(":");
                        if(pair.getColumnName().equals(tupleParts[0])){
                            if(pair.getColumnType().equals(tupleParts[1])) {
                                existsInSchema = true;
                                break;
                            }
                        }
                    }
                }
            }
            if(existsInSchema == true){
                System.out.println("Adding to new Map the updated entry: "+tableAlias+"."+pair.getColumnName());
                String theNewKey = tableAlias+"."+pair.getColumnName();
                ColumnTypePair pair2 = new ColumnTypePair(theNewKey, pair.getColumnType());
                theNewMap.addPair(pair2);
            }
            else{
                System.out.println("Adding to new Map without update the entry: "+pair.getColumnName());
                ColumnTypePair pair2 = new ColumnTypePair(pair.getColumnName(), pair.getColumnType());
                theNewMap.addPair(pair2);
            }
        }*/

        //columnAndTypeMap = theNewMap;

        System.out.println("After...updateColumnAndTypesFromTableScan...columnAndType has become: ");
        printColumnAndTypeMap();

    }

    public String extractColsFromTypeName(String typeName, MyMap aMap, String schema){

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
                    if(tupleParts[0].contains("KEY.")){
                        tupleParts[0] = tupleParts[0].replace("KEY.", "");
                    }
                    if(tupleParts[0].contains("VALUE.")){
                        tupleParts[0] = tupleParts[0].replace("VALUE.", "");
                    }
                    System.out.println("New Column-Value Part located...Key: "+tupleParts[0]+" Value: "+tupleParts[1]);
                    ColumnTypePair pair = new ColumnTypePair(tupleParts[0], tupleParts[1]);
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

    public String findPossibleColumnAliases(Operator<?> currentNode, String currentSchema, MyMap newColumnMap, Map<String, ExprNodeDesc> columnExprMap, MyMap changeMap, MyMap currentSchemaMap){

        boolean foundMatch = false;

        System.out.println("Before...findPossibleColumnAliases...columnAndType has become: ");
        printColumnAndTypeMap();

        MyMap endMap = new MyMap();
        List<ColumnTypePair> columnTypePairList = columnAndTypeMap.getColumnAndTypeList();

        for(ColumnTypePair pair : columnTypePairList){
            foundMatch = false;
            for(Map.Entry<String, ExprNodeDesc> entry2 : columnExprMap.entrySet()){
                if(entry2 != null){
                    if(entry2.getKey() != null){
                        if(entry2.getValue() != null){
                            if((entry2.getKey().equals("ROW__ID")) || entry2.getKey().equals("BLOCK__OFFSET__INSIDE__FILE") || entry2.getKey().equals("INPUT__FILE__NAME")){
                                System.out.println(currentNode.getOperatorId()+": "+"Detected: "+entry2.getKey()+" skipping!");
                                continue;
                            }
                            if((pair.getColumnName().equals(entry2.getKey())) || ("KEY.".concat(pair.getColumnName()).equals(entry2.getKey())) || ("VALUE.".concat(pair.getColumnName()).equals(entry2.getKey())) || (pair.getColumnName().equals(entry2.getKey().replace("KEY", currentNode.getOperatorId()))) || ((pair.hasAlternateAlias() == true) && (pair.getAlternateAlias().equals(entry2.getKey().replace("KEY", currentNode.getOperatorId()))))){
                                if(currentSchemaMap != null){
                                    String type1 = pair.getColumnType();
                                    String type2 = null;
                                    String proper2Name = new String(entry2.getKey());
                                    if(proper2Name.contains("KEY.")) proper2Name = proper2Name.replace("KEY.", "");
                                    if(proper2Name.contains("VALUE.")) proper2Name = proper2Name.replace("VALUE.", "");

                                    List<ColumnTypePair> listForTypes = currentSchemaMap.getColumnAndTypeList();
                                    for(ColumnTypePair typePair : listForTypes){
                                        if(typePair.getColumnName().equals(proper2Name)){
                                            if(typePair.getColumnName().contains("reducesinkkey")){
                                                type2 = "OK";
                                            }
                                            else {
                                                type2 = typePair.getColumnType();
                                            }
                                            break;
                                        }
                                    }
                                    if(type2 == null){
                                        System.out.println("Failed to find Type for MapExprNodeDesc entry: "+entry2.getKey());
                                        System.exit(0);
                                    }
                                    if(type1.equals(type2)){
                                        System.out.println("Types and names match...");
                                    }
                                    else{
                                        if(type2.equals("OK")){
                                            System.out.println("Names Match for reducesinkkey...");
                                        }
                                        else {
                                            continue;
                                        }
                                    }
                                }
                                if(pair.getColumnName().contains("KEY.reducesinkkey")){
                                    if(pair.getColumnName().equals(entry2.getKey())){
                                        System.out.println("KEY.reducesinkkey matches with simply another KEY.reducesinkkey...Simply skipping!");
                                        continue;
                                    }
                                }
                                System.out.println(currentNode.getOperatorId()+": "+"Found possible Match for: "+pair.getColumnName());
                                List<String> exprCols = entry2.getValue().getCols();
                                if(exprCols != null){
                                    if(exprCols.size() == 1){
                                        String exprColumn = exprCols.get(0);
                                        if(exprColumn.contains("KEY.")){
                                            exprColumn = exprColumn.replace("KEY.", "");
                                        }
                                        else if(exprColumn.contains("VALUE.")){
                                            exprColumn = exprColumn.replace("VALUE.", "");
                                        }
                                        if(exprColumn.equals(pair.getColumnName()) == false){
                                            if(exprColumn.contains("reducesinkkey")){
                                                System.out.println(currentNode.getOperatorId()+": "+"Located Match with ReduceSinkKey!");
                                                currentSchema = currentSchema.replace(pair.getColumnName(), exprColumn);
                                                if(currentNode instanceof JoinOperator){
                                                    List<Operator<?>> parents = currentNode.getParentOperators();
                                                    ColumnTypePair newPair = new ColumnTypePair(pair.getColumnName(), pair.getColumnType(), parents.get(0).getOperatorId()+"."+exprColumn);
                                                    newColumnMap.addPair(newPair);
                                                    ColumnTypePair secondPair = new ColumnTypePair(parents.get(1).getOperatorId()+"."+exprColumn, pair.getColumnType());
                                                    endMap.addPair(secondPair);
                                                    ColumnTypePair changePair = new ColumnTypePair(pair.getColumnName(), exprColumn);
                                                    changeMap.addPair(changePair);
                                                }
                                                else{
                                                    System.out.println(currentNode.getOperatorId()+": "+"Located match with ReduceSinkKey in non Join Operator! Not supported yet!");
                                                    System.exit(0);
                                                }
                                            }
                                            else {
                                                System.out.println(currentNode.getOperatorId()+": "+"Found possible Match for: " + pair.getColumnName() + " Match: " + exprColumn);
                                                currentSchema = currentSchema.replace(pair.getColumnName(), exprColumn);
                                                ColumnTypePair pair2 = new ColumnTypePair(exprColumn, pair.getColumnType());
                                                newColumnMap.addPair(pair2);
                                                ColumnTypePair pair3 = new ColumnTypePair(pair.getColumnName(), exprColumn);
                                                changeMap.addPair(pair3);
                                            }
                                        }
                                        else{
                                            ColumnTypePair pair2 = new ColumnTypePair(pair.getColumnName(), pair.getColumnType());
                                            newColumnMap.addPair(pair2);
                                        }
                                    }
                                    else{
                                        System.out.println(currentNode.getOperatorId()+": "+"ExprCols must have size 1!");
                                        System.exit(0);
                                    }
                                }
                                else{
                                    System.out.println(currentNode.getOperatorId()+": "+"ExprCols is NULL!");
                                    System.exit(0);
                                }
                                foundMatch = true;
                                break;
                            }
                        }
                    }
                }
            }
            if(foundMatch == false){
                ColumnTypePair pair2 = new ColumnTypePair(pair.getColumnName(), pair.getColumnType());
                newColumnMap.addPair(pair2);
            }
        }

        boolean hasNoMatch = true;
        for(Map.Entry<String, ExprNodeDesc> entry2 : columnExprMap.entrySet()){
            hasNoMatch = true;
            if(entry2 != null){
                if(entry2.getKey() != null){
                    if(entry2.getValue() != null){
                        List<ColumnTypePair> list2 = columnAndTypeMap.getColumnAndTypeList();
                        for(ColumnTypePair c : list2){
                            if((c.getColumnName().equals(entry2.getKey())) || ("KEY.".concat(c.getColumnName()).equals(entry2.getKey())) || ("VALUE.".concat(c.getColumnName()).equals(entry2.getKey())) || (c.getColumnName().equals(entry2.getKey().replace("KEY", currentNode.getOperatorId()))) || ((c.hasAlternateAlias() == true) && (c.getAlternateAlias().equals(entry2.getKey().replace("KEY", currentNode.getOperatorId()))))){
                                if(currentSchemaMap != null){
                                    String type1 = c.getColumnType();
                                    String type2 = null;
                                    String proper2Name = new String(entry2.getKey());
                                    if(proper2Name.contains("KEY.")) proper2Name = proper2Name.replace("KEY.", "");
                                    if(proper2Name.contains("VALUE.")) proper2Name = proper2Name.replace("VALUE.", "");
                                    List<ColumnTypePair> listForTypes = currentSchemaMap.getColumnAndTypeList();
                                    for(ColumnTypePair typePair : listForTypes){
                                        if(typePair.getColumnName().equals(proper2Name)){
                                            if(typePair.getColumnName().contains("reducesinkkey")){
                                                type2 = "OK";
                                            }
                                            else {
                                                type2 = typePair.getColumnType();
                                            }
                                        }
                                    }
                                    if(type2 == null){
                                        System.out.println("Failed to find Type for MapExprNodeDesc entry: "+entry2.getKey());
                                        System.exit(0);
                                    }
                                    if(type1.equals(type2)){
                                        System.out.println("Types and names match...");
                                    }
                                    else{
                                        if(type2.equals("OK")){
                                            System.out.println("Names match for reducessinkkey0...");
                                        }
                                        else {
                                            continue;
                                        }
                                    }
                                }
                                hasNoMatch = false;
                                break;
                            }
                        }
                    }
                }
            }
            if(hasNoMatch == true){
                if(entry2 != null) {
                    if (entry2.getKey() != null) {
                        if((entry2.getKey().equals("ROW__ID")) || entry2.getKey().equals("BLOCK__OFFSET__INSIDE__FILE") || entry2.getKey().equals("INPUT__FILE__NAME")){
                            System.out.println(currentNode.getOperatorId()+": "+"Detected: "+entry2.getKey()+" skipping!");
                            continue;
                        }
                        if (entry2.getValue() != null) {
                            System.out.println(currentNode.getOperatorId()+": "+"Discovered non existent needed column!: "+entry2.getKey());
                            ColumnTypePair pair2 = new ColumnTypePair(entry2.getKey(), "Unknown-Type");
                            newColumnMap.addPair(pair2);
                        }
                    }
                }
            }
        }

        if(endMap.getColumnAndTypeList().size() > 0){
            List<ColumnTypePair> endList = endMap.getColumnAndTypeList();
            for(ColumnTypePair e : endList){
                newColumnMap.addPair(e);
            }
        }

        columnAndTypeMap = newColumnMap;

        System.out.println("After...findPossibleColumnAliases...columnAndType has become: ");
        printColumnAndTypeMap();

        return currentSchema;

    }


    public OperatorQuery parentIsFileSinkOperator(OperatorNode currentNode, OperatorNode parent, String currentSchema, OperatorQuery currentOperatorQuery, ExaremeGraph exaremeGraph){

        System.out.println(currentNode.getOperator().getOperatorId()+": "+"Parent is a FileSinkOperator!");

        if(currentNode.getOperator() instanceof ListSinkOperator){ //Current Node is FetchOperator
            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Discovered: OP <-- FS connection...");
            RowSchema rowSchema = parent.getOperator().getSchema();
            if(rowSchema == null){
                System.out.println(currentNode.getOperator().getOperatorId()+": "+"OP<---FS Connection! FS RowSchema is null!");
                System.exit(0);
            }
            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Comparing RowSchema...");
            if(currentSchema.equals(rowSchema.toString())){
                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Schemas are equal! Proceeding...");
            }
            else{
                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Schema OP<----FS are not equal!");
                System.exit(0);
            }

            currentOperatorQuery = goToParentOperator(parent, currentSchema, currentOperatorQuery, exaremeGraph);

        }
        else if((currentNode.getOperator() instanceof TableScanOperator) || (currentNode.getOperator() instanceof GroupByOperator)){
            if(currentNode.getOperator() instanceof TableScanOperator)
                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Discovered: TS <-- FS connection...");
            else
                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Discovered: GBY <-- FS connection...");

            RowSchema rowSchema = parent.getOperator().getSchema();
            if(rowSchema == null){
                System.out.println(currentNode.getOperator().getOperatorId()+": "+"FS RowSchema is null!");
                System.exit(0);
            }
            if(currentNode.getOperator() instanceof GroupByOperator) {
                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Comparing RowSchema...");
                if (currentSchema.equals(rowSchema.toString())) {
                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"Schemas are equal! Proceeding...");
                } else {
                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"Schema GBY<----FS are not equal!");
                    System.exit(0);
                }
            }
            System.out.println(currentNode.getOperator().getOperatorId()+": "+"We have reached a FileSink! Creating a new Query!");
            currentOperatorQuery = goToParentOperator(parent, currentSchema, currentOperatorQuery, exaremeGraph);

            currentOperatorQuery.setLocalQueryString("CREATE TABLE "+parent.getOperator().getOperatorId()+" AS ( "+currentOperatorQuery.getLocalQueryString()+" )");
            currentOperatorQuery.addOutputTable(parent.getOperator().getOperatorId());
            currentOperatorQuery.setExaremeOutputTableName("R_"+parent.getOperator().getOperatorId()+"_0");

        }

        System.out.println(currentNode.getOperator().getOperatorId()+": "+" exiting...");

        return currentOperatorQuery;

    }

    public OperatorQuery parentIsLimitOperator(OperatorNode currentNode, OperatorNode parent, String currentSchema, OperatorQuery currentOperatorQuery, ExaremeGraph exaremeGraph){
        System.out.println(currentNode.getOperator().getOperatorId()+": "+"Parent is a LimitOperator!");
        if(currentNode.getOperator() instanceof FileSinkOperator){
            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Discovered: FS <-- LIMIT connection...");
            RowSchema rowSchema = parent.getOperator().getSchema();
            if(rowSchema == null){
                System.out.println(currentNode.getOperator().getOperatorId()+": "+"FS<---LIMIT Connection! LIMIT RowSchema is null!");
                System.exit(0);
            }
            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Comparing RowSchema...");
            if(currentSchema.equals(rowSchema.toString())){
                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Schemas are equal! Proceeding...");
                LimitOperator limitOp = (LimitOperator) parent.getOperator();
                LimitDesc limitDesc = limitOp.getConf();
                if(limitDesc != null){
                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"Limit is: "+limitDesc.getLimit());

                    currentOperatorQuery = goToParentOperator(parent, currentSchema, currentOperatorQuery, exaremeGraph);

                    //currentOperatorQuery.setLocalQueryString("( "+currentOperatorQuery.getLocalQueryString());
                    //currentOperatorQuery.setExaremeQueryString("( "+currentOperatorQuery.getExaremeQueryString());

                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"Appending LIMIT to end of Query...");

                    currentOperatorQuery.setLocalQueryString(currentOperatorQuery.getLocalQueryString().concat(" LIMIT "));
                    currentOperatorQuery.setLocalQueryString(currentOperatorQuery.getLocalQueryString().concat(Integer.toString(limitDesc.getLimit())));

                    currentOperatorQuery.setExaremeQueryString(currentOperatorQuery.getExaremeQueryString().concat(" LIMIT "));
                    currentOperatorQuery.setExaremeQueryString(currentOperatorQuery.getExaremeQueryString().concat(Integer.toString(limitDesc.getLimit())));

                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"QueryString is now: "+currentOperatorQuery.getLocalQueryString());
                }
                else{
                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"LimitDesc is NULL!");
                    System.exit(0);
                }
            }
            else{
                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Schema FS<----LIMIT are not equal!");
                System.exit(0);
            }
        }

        System.out.println(currentNode.getOperator().getOperatorId()+": "+" exiting...");

        return currentOperatorQuery;

    }

    public OperatorQuery goToParentOperator(OperatorNode currentNode, String currentSchema, OperatorQuery currentOperatorQuery, ExaremeGraph exaremeGraph){

        System.out.println(currentNode.getOperator().getOperatorId()+": "+"goToParentOperator: Currently Operating in Node: "+currentNode.getOperatorName());
        System.out.println(currentNode.getOperator().getOperatorId()+": "+"goToParentOperator: CurrentSchema= "+currentSchema);
        System.out.println(currentNode.getOperator().getOperatorId()+": "+"goToParentOperator: CurrentQueryString= "+currentOperatorQuery.getLocalQueryString());

        int numberOfParents = 0;

        List<DirectedEdge> edges = exaremeGraph.getEdges();

        if(edges != null){
            if(edges.size() > 0){
                for(DirectedEdge e : edges){
                    if(e.getToVertex().equals(currentNode.getOperatorName())){
                        numberOfParents++;
                    }
                }

                if(numberOfParents > 0){
                    if(numberOfParents == 1){
                        for(DirectedEdge e : edges){
                            if(e.getToVertex().equals(currentNode.getOperatorName())){ //Found Parent of Operator
                                OperatorNode parent = exaremeGraph.getOperatorNodeByName(e.getFromVertex());
                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"goToParentOperator: Accessing Parent: "+parent.getOperatorName());

                                if(parent.getOperator() instanceof FileSinkOperator){ //Parent is FileSinkOperator
                                    currentOperatorQuery = parentIsFileSinkOperator(currentNode, parent, currentSchema, currentOperatorQuery, exaremeGraph);
                                    break;
                                }
                                else if(parent.getOperator() instanceof JoinOperator){
                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"Parent is a JoinOperator!");
                                    if(currentNode.getOperator() instanceof GroupByOperator){
                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"Discovered: GBY <-- JOIN connection...");
                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"Join Acts as an Leaf Operator that connects two branches!");
                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"In Join Operator...first mark needed indexes for Select and later");
                                        List<Integer> neededIndexes = new LinkedList<>();

                                        JoinOperator joinOp = (JoinOperator) parent.getOperator();
                                        JoinDesc joinDesc = joinOp.getConf();

                                        List<Operator<?>> joinParents = joinOp.getParentOperators();
                                        if(joinParents == null){
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Join has NULL Parents!");
                                            System.exit(0);
                                        }
                                        else{
                                            if(joinParents.size() != 2){
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Join has not 2 exactly parents!");
                                                System.exit(0);
                                            }
                                        }

                                        MyMap outputColsTypeMap = new MyMap();
                                        String someSchema = "";
                                        someSchema = extractColsFromTypeName(joinOp.getSchema().toString(), outputColsTypeMap, someSchema);
                                        //Locate columns for Later
                                        List<String> outputCols = joinDesc.getOutputColumnNames();
                                        for(String wantedCol : outputCols) {
                                            int wantedIndex = 0;
                                            List<ColumnTypePair> columnTypePairList = columnAndTypeMap.getColumnAndTypeList();
                                            boolean found = false;
                                            for (ColumnTypePair pair : columnTypePairList) {
                                                if(wantedCol.equals(pair.getColumnName())){
                                                    List<ColumnTypePair> listForColTypes = outputColsTypeMap.getColumnAndTypeList();
                                                    for(ColumnTypePair pair2 : listForColTypes){
                                                        if(pair2.getColumnName().equals(pair.getColumnName())){
                                                            if(pair2.getColumnType().equals(pair.getColumnType())){
                                                                neededIndexes.add(wantedIndex);
                                                                found = true;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                                if(found == true) break;
                                                wantedIndex++;
                                            }
                                        }

                                        //Find Possible ColumnAliases
                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"Checking for possible Alias Changes...");
                                        MyMap newColumnMap = new MyMap();
                                        Map<String, ExprNodeDesc> columnExprMap = parent.getOperator().getColumnExprMap();
                                        String changedSchema = new String();
                                        MyMap changeMap = new MyMap();

                                        if(columnExprMap != null) {
                                            changedSchema = findPossibleColumnAliases(joinOp, currentSchema, newColumnMap, columnExprMap, changeMap, outputColsTypeMap);
                                            System.out.println(changeMap.getColumnAndTypeList().size() + " Alias Changes happened!");
                                        }

                                        //Create new Query again and move upwards
                                        Operator<?> parent1 = joinParents.get(0);
                                        Operator<?> parent2 = joinParents.get(1);

                                        if(parent1 instanceof ReduceSinkOperator){
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"First parent of Join is ReduceSink: "+parent1.getOperatorId());
                                            if(parent2 instanceof ReduceSinkOperator){
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Second parent of Join is ReduceSink: "+parent2.getOperatorId());
                                            }
                                            else{
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Second parent of Join is not ReduceSink: "+parent2.getOperatorId());
                                                System.exit(0);
                                            }
                                        }
                                        else{
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"First parent of Join is not ReduceSink: "+parent1.getOperatorId());
                                            System.exit(0);
                                        }

                                        currentOperatorQuery = goToParentOperator(parent, currentSchema, currentOperatorQuery, exaremeGraph);

                                        //Deal with the Join type and key
                                        //TODO: Check if Join Keys appear in some other way
                                        JoinCondDesc[] joinCondDescs = joinDesc.getConds();
                                        if (joinCondDescs != null) {
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\t\t\tJoinCondDescs: ");
                                            for (JoinCondDesc j : joinCondDescs) {
                                                if (j != null) {
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\t\t\t\tLeft: " + j.getLeft());
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\t\t\t\tRight: " + j.getRight());
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\t\t\t\tType: " + j.getType());
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\t\t\t\tPreserved: " + j.getPreserved());
                                                    if (j.getJoinCondString() != null) {
                                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\t\t\t\tJoinCondString: " + j.getJoinCondString());
                                                        if(j.getJoinCondString().contains("Inner Join")){
                                                            currentOperatorQuery.setLocalQueryString(currentOperatorQuery.getLocalQueryString().concat(" INNER JOIN "));
                                                            currentOperatorQuery.setExaremeQueryString(currentOperatorQuery.getExaremeQueryString().concat(" INNER JOIN "));
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        boolean parentsLocated = false;
                                        String joinPart1 = "";
                                        String joinPart2 = "";
                                        Map<Byte, String> keys = joinDesc.getKeysString();
                                        if (keys != null) {
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\t\t\tKeys: ");
                                            List<String> keyList = new LinkedList<>();
                                            for (Map.Entry<Byte, String> entry : keys.entrySet()) {
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\t\t\t\tKey: " + entry.getKey() + " : Value: " + entry.getValue());
                                                keyList.add(entry.getValue());
                                            }
                                            Map<String, ExprNodeDesc> parentColumns = parent1.getColumnExprMap();
                                            if(parentColumns != null){
                                                for(Map.Entry<String, ExprNodeDesc> entryColumn : parentColumns.entrySet()){
                                                    if(entryColumn != null){
                                                        ExprNodeDesc value = entryColumn.getValue();
                                                        if(value != null){
                                                            if(entryColumn.getKey() != null){
                                                                if(entryColumn.getKey().contains("KEY.reducesinkkey")){
                                                                    if(entryColumn.getValue().equals("Column["+keyList.get(0)+"]")) {
                                                                        joinPart1 = parent1.getOperatorId() + "." + keyList.get(0);
                                                                        joinPart1 = parent2.getOperatorId() + "." + keyList.get(1);
                                                                        parentsLocated = true;
                                                                        break;
                                                                    }
                                                                    else{
                                                                        joinPart1 = parent1.getOperatorId() + "." + keyList.get(1);
                                                                        joinPart1 = parent2.getOperatorId() + "." + keyList.get(0);
                                                                        parentsLocated = true;
                                                                        break;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        if(parentsLocated == false){
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Failed to locate the tables the keys belong to FOR JOIN!");
                                            System.exit(0);
                                        }

                                        //Build Join
                                        currentOperatorQuery.setLocalQueryString(" FROM "+parent1.getOperatorId()+currentOperatorQuery.getLocalQueryString()+parent2.getOperatorId()+" ON "+joinPart1+" = "+joinPart2+" ");
                                        currentOperatorQuery.setExaremeQueryString(" FROM "+parent1.getOperatorId()+currentOperatorQuery.getExaremeQueryString()+parent2.getOperatorId()+" ON "+joinPart1+" = "+joinPart2+" ");
                                        currentOperatorQuery.addInputTable(parent1.getOperatorId());
                                        currentOperatorQuery.addInputTable(parent2.getOperatorId());

                                        //TODO FIX SELECT OF JOIN
                                        String selectString = null;
                                        int k = 0;
                                        for(String wantedCol : outputCols) {
                                            Integer targetIndex = neededIndexes.get(k);
                                            int l = 0;
                                            List<ColumnTypePair> columnTypePairList = columnAndTypeMap.getColumnAndTypeList();
                                            for(ColumnTypePair pair : columnTypePairList){
                                                if(targetIndex == l){
                                                    if(selectString == null){
                                                        selectString = "";
                                                        selectString = selectString.concat(pair.getColumnName());
                                                    }
                                                    else{
                                                        selectString = selectString.concat(", "+pair.getColumnName());
                                                    }
                                                }
                                                l++;
                                            }
                                            k++;
                                        }

                                        currentOperatorQuery.setLocalQueryString("SELECT "+selectString+" "+currentOperatorQuery.getLocalQueryString());
                                        currentOperatorQuery.setExaremeQueryString("SELECT "+selectString+" "+currentOperatorQuery.getExaremeQueryString());

                                        currentOperatorQuery.setLocalQueryString("CREATE TABLE "+joinOp.getOperatorId()+" AS ( "+currentOperatorQuery.getLocalQueryString()+" )");
                                        currentOperatorQuery.addOutputTable(joinOp.getOperatorId());
                                        currentOperatorQuery.setExaremeOutputTableName("R_"+joinOp.getOperatorId()+"_0");

                                    }
                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+" exiting...");
                                    break;
                                }
                                else if(parent.getOperator() instanceof GroupByOperator){
                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"Parent is a GroupByOperator!");
                                    if((currentNode.getOperator() instanceof SelectOperator) || (currentNode.getOperator() instanceof ReduceSinkOperator) || (currentNode.getOperator() instanceof FileSinkOperator)){
                                        if(currentNode.getOperator() instanceof SelectOperator)
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Discovered: SEL <-- GBY connection...");
                                        else if(currentNode.getOperator() instanceof ReduceSinkOperator)
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Discovered: RS <-- GBY connection...");
                                        else
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Discovered: FS <-- GBY connection...");

                                        RowSchema rowSchema = parent.getOperator().getSchema();
                                        if(rowSchema == null){
                                            if(currentNode.getOperator() instanceof SelectOperator)
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"SEL<---GBY Connection! GBY RowSchema is null!");
                                            else if(currentNode.getOperator() instanceof ReduceSinkOperator)
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"RS<---GBY Connection! GBY RowSchema is null!");
                                            else
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"FS<---GBY Connection! GBY RowSchema is null!");
                                            System.exit(0);
                                        }
                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"Comparing RowSchema...");
                                        if(currentSchema.equals(rowSchema.toString())){
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Schemas are equal!");
                                        }
                                        else{
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"GroupByOperator has different Schema (will become new Schema): [" + rowSchema.toString() + "]");
                                            currentSchema = rowSchema.toString();
                                            if(currentSchema.contains("KEY.")){
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Removing KEY. from Schema");
                                                currentSchema.replace("KEY.", "");
                                            }
                                            if(currentSchema.contains("VALUE.")){
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Removing VALUE. from Schema");
                                                currentSchema.replace("VALUE.", "");
                                            }
                                            addNewColsFromSchema(rowSchema.toString());
                                        }

                                        List<Integer> neededIndexes = new LinkedList<>();

                                        GroupByOperator groupByParent = (GroupByOperator) parent.getOperator();
                                        GroupByDesc groupByDesc = groupByParent.getConf();

                                        if(groupByParent == null){
                                            System.out.println("GroupByDesc is null!");
                                            System.exit(0);
                                        }

                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"Discovering Group By Keys...");
                                        List<String> groupByKeys = new LinkedList<>();
                                        MyMap changeMap = new MyMap();
                                        if(groupByDesc != null){
                                            ArrayList<ExprNodeDesc> keys = groupByDesc.getKeys();
                                            if (keys != null) {
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Keys: ");
                                                for (ExprNodeDesc k : keys) {
                                                    if (k != null) {
                                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"\tKey: ");
                                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\tName: " + k.getName());
                                                        if (k.getCols() != null) {
                                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\tCols: " + k.getCols().toString());
                                                            if(k.getCols().size() > 1){
                                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Key for more than one column?! GROUP BY");
                                                                System.exit(9);
                                                            }
                                                            else if(k.getCols().size() == 1){
                                                                String col = k.getCols().get(0);
                                                                if(col.contains("KEY.")){
                                                                    col = col.replace("KEY.", "");
                                                                }
                                                                else if(col.contains("VALUE.")){
                                                                    col = col.replace("VALUE.", "");
                                                                }


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
                                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\tCols: NULL");
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        MyMap outputColsTypeMap = new MyMap();
                                        String someSchema = "";
                                        someSchema = extractColsFromTypeName(parent.getOperator().getSchema().toString(), outputColsTypeMap, someSchema);

                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"Checking for possible Alias Changes...");
                                        MyMap newColumnMap = new MyMap();
                                        Map<String, ExprNodeDesc> columnExprMap = parent.getOperator().getColumnExprMap();
                                        String changedSchema = new String();
                                        if(columnExprMap != null){
                                            changedSchema = findPossibleColumnAliases(parent.getOperator(),currentSchema, newColumnMap, columnExprMap, changeMap, outputColsTypeMap);
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+changeMap.getColumnAndTypeList().size()+" Alias Changes happened!");
                                            //currentColumns = columnAndTypeMap;
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Marking positions of indexes in ColumnMap for later...");
                                            int i = 0;
                                            for(String col : groupByKeys){
                                                i = 0;
                                                List<ColumnTypePair> columnTypePairList = columnAndTypeMap.getColumnAndTypeList();
                                                boolean found = false;
                                                for(ColumnTypePair pair: columnTypePairList){
                                                    String previousAlias = null;
                                                    if(changeMap.getColumnAndTypeList().size() > 0){
                                                        List<ColumnTypePair> changeList = changeMap.getColumnAndTypeList();
                                                        for(ColumnTypePair c : changeList){
                                                            if(c.getColumnType().equals(col)){
                                                                previousAlias = c.getColumnName();
                                                                List<ColumnTypePair> typeList = outputColsTypeMap.getColumnAndTypeList();
                                                                for(ColumnTypePair somePair : typeList){
                                                                    if(previousAlias.equals(somePair.getColumnName())){
                                                                        somePair.setColumnName(col);
                                                                    }
                                                                }
                                                                break;
                                                            }
                                                        }
                                                    }

                                                        if (pair.getColumnName().equals(col)) {
                                                            List<ColumnTypePair> typeList = outputColsTypeMap.getColumnAndTypeList();
                                                            for (ColumnTypePair somePair : typeList) {
                                                                if (somePair.getColumnName().equals(col)) {
                                                                    if (somePair.getColumnType().equals(pair.getColumnType())) {
                                                                        System.out.println(currentNode.getOperator().getOperatorId() + ": " + "Located required Index for GroupBy: " + i);
                                                                        neededIndexes.add(i);
                                                                        found = true;
                                                                        break;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    if(found == true) break;
                                                    i++;
                                                }
                                            }

                                            boolean queryFinished = false;
                                            List<Operator<? extends OperatorDesc>> parents = parent.getOperator().getParentOperators();
                                            if(parents != null){
                                                if(parents.size() > 0){
                                                    if(parents.size() > 1){
                                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\tGROUP WITH MORE THAN ONE PARENT?");
                                                        System.exit(1);
                                                    }
                                                    else{
                                                        Operator<?> grandpa = parents.get(0);
                                                        if(grandpa != null){
                                                            if(grandpa instanceof ReduceSinkOperator){
                                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\tParent of GroupBy is ReduceSink! Checking next ancestors!");
                                                                List<Operator<?>> ancestorsLv1 = grandpa.getParentOperators();
                                                                if(ancestorsLv1 == null){
                                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\tAncestors of ReduceSink are NULL!");
                                                                    System.exit(1);
                                                                }
                                                                else{
                                                                    if(ancestorsLv1.size() != 1){
                                                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\tReduceSink has more than 1 ancestor!");
                                                                        System.exit(1);
                                                                    }
                                                                    Operator<?> firstAncestor = ancestorsLv1.get(0);
                                                                    if(firstAncestor instanceof TableScanOperator){
                                                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\tAncestor after ReduceSink is TableScan...query is not done yet exactly!");
                                                                        queryFinished = false;
                                                                    }
                                                                    else{
                                                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\tAncestor after ReduceSink is of type: "+firstAncestor.getType().toString());
                                                                        currentOperatorQuery.setLocalQueryString(currentOperatorQuery.getLocalQueryString().concat(" FROM "+grandpa.getOperatorId()));
                                                                        currentOperatorQuery.setExaremeQueryString(currentOperatorQuery.getExaremeQueryString().concat(" FROM "+grandpa.getOperatorId()));
                                                                        currentOperatorQuery.addInputTable(grandpa.getOperatorId());

                                                                        queryFinished = true;
                                                                    }
                                                                }
                                                                /*.out.println("\t\tParent of GroupBy is ReduceSink! Beginning new query!");
                                                                currentQueryString = currentQueryString.concat(" FROM "+grandpa.getOperatorId());
                                                                queryFinished = true;*/
                                                            }
                                                            else if(grandpa instanceof FileSinkOperator){
                                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\tParent of GroupBy is FileSink! Beginning new query!");

                                                                currentOperatorQuery.setLocalQueryString(currentOperatorQuery.getLocalQueryString().concat(" FROM "+grandpa.getOperatorId()));
                                                                currentOperatorQuery.setExaremeQueryString(currentOperatorQuery.getExaremeQueryString().concat(" FROM "+grandpa.getOperatorId()));
                                                                currentOperatorQuery.addInputTable(grandpa.getOperatorId());

                                                                queryFinished = true;
                                                            }
                                                        }
                                                    }
                                                }
                                                else{
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\tGROUP BY WITH NO PARENTS?");
                                                    System.exit(1);
                                                }
                                            }


                                            MyMap columnsForSelect = new MyMap();
                                            List<Integer> selectIndexes = new LinkedList<>();
                                            if(currentNode.getOperator() instanceof FileSinkOperator) { //Find Select Indexes before Moving because Select is ommited by Graph!
                                                List<Operator<?>> grandparents = parent.getOperator().getParentOperators();
                                                if (grandparents == null) {
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"GrandParents are null for FileSinkOperator!");
                                                    System.exit(0);
                                                }
                                                if (grandparents.size() != 1) {
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"GrandParents MORE THAN 1 for FileSinkOperator!");
                                                    System.exit(0);
                                                }
                                                Operator<?> grandParent = grandparents.get(0);
                                                if (grandParent instanceof JoinOperator) {
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"GrandParent of FileSinkOperator is JoinOperator!");
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"Adding SELECT Because sequence FS<---GBY<---JOIN does not include one!");
                                                    String schema = "";

                                                    schema = extractColsFromTypeName(currentNode.getOperator().getSchema().toString(), columnsForSelect, schema);

                                                    List<ColumnTypePair> listColumnTypeSelect = columnsForSelect.getColumnAndTypeList();
                                                    for (ColumnTypePair c : listColumnTypeSelect) {
                                                        if (c != null) {
                                                            int indexTarget = 0;
                                                            List<ColumnTypePair> blaList = columnAndTypeMap.getColumnAndTypeList();
                                                            for (ColumnTypePair blaPair : blaList) {
                                                                if (c.getColumnName().equals(blaPair.getColumnName())) {
                                                                    if(c.getColumnType().equals(blaPair.getColumnType())) {
                                                                        selectIndexes.add(indexTarget);
                                                                        break;
                                                                    }
                                                                }
                                                                indexTarget++;
                                                            }
                                                        }
                                                    }
                                                }
                                            }

                                            if(queryFinished == false) {
                                                currentOperatorQuery = goToParentOperator(parent, currentSchema, currentOperatorQuery, exaremeGraph);
                                            }
                                            else{ //NEW QUERY BEGINS AFTER THIS POINT, WHEN WE RETURN WE MUST ADD IT TO THE LIST
                                                String newQueryString = "";
                                                OperatorQuery newOpQuery = new OperatorQuery();
                                                newOpQuery.setExaremeQueryString("");
                                                newOpQuery.setLocalQueryString("");
                                                newOpQuery = goToParentOperator(parent, currentSchema, newOpQuery, exaremeGraph);
                                                allQueries.add(newOpQuery);
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Query Finished: ["+newOpQuery.getLocalQueryString()+" ]");
                                            }

                                            String columnsString = null;

                                            for(Integer index : neededIndexes){
                                                int count = 0;
                                                List<ColumnTypePair> blaList = columnAndTypeMap.getColumnAndTypeList();
                                                for(ColumnTypePair blaPair : blaList){
                                                    if(count == index){
                                                        if(columnsString == null){
                                                            columnsString = "";
                                                            columnsString = columnsString.concat(blaPair.getColumnName());
                                                        }
                                                        else{
                                                            columnsString = columnsString.concat(" ,"+blaPair.getColumnName());
                                                        }
                                                    }
                                                    count++;
                                                }
                                            }

                                            if(currentNode.getOperator() instanceof FileSinkOperator) {
                                                List<Operator<?>> grandparents = parent.getOperator().getParentOperators();
                                                if (grandparents == null) {
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"GrandParents are null for FileSinkOperator!");
                                                    System.exit(0);
                                                }
                                                if (grandparents.size() != 1) {
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"GrandParents MORE THAN 1 for FileSinkOperator!");
                                                    System.exit(0);
                                                }
                                                Operator<?> grandParent = grandparents.get(0);
                                                if (grandParent instanceof JoinOperator) {
                                                    //System.out.println("GrandParent of FileSinkOperator is JoinOperator!");
                                                    //System.out.println("Adding SELECT Because sequence FS<---GBY<---JOIN does not include one!");
                                                    //String schema = "";

                                                    String selectString = buildColumnNamesFromMap(columnsForSelect);

                                                    int l = 0;
                                                    if(selectIndexes.size() != columnsForSelect.getColumnAndTypeList().size()){
                                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"Not enough needed Indexes for Select in FS!");
                                                        System.exit(0);
                                                    }
                                                    List<ColumnTypePair> blaList = columnsForSelect.getColumnAndTypeList();
                                                    for(ColumnTypePair selectEntry : blaList){
                                                        Integer targetInt = selectIndexes.get(l);
                                                        int k = 0;
                                                        List<ColumnTypePair> columnTypePairList = columnAndTypeMap.getColumnAndTypeList();
                                                        for(ColumnTypePair pair : columnTypePairList){
                                                            if(k == targetInt){
                                                                if(selectString.contains(selectEntry.getColumnName())){
                                                                    selectString = selectString.replace(selectEntry.getColumnName(), pair.getColumnName());
                                                                }
                                                            }
                                                            k++;
                                                        }
                                                        l++;
                                                    }
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"DUE To FS<--GBY<--JOIN Connection a SELECT will be added at the start of the query!");
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"This Query is also Finished! With Join Being a Leaf Node!");

                                                    //Adding FROM
                                                    currentOperatorQuery.setLocalQueryString(currentOperatorQuery.getLocalQueryString().concat(" FROM "+grandParent.getOperatorId()));
                                                    currentOperatorQuery.setExaremeQueryString(currentOperatorQuery.getExaremeQueryString().concat(" FROM "+grandParent.getOperatorId()));
                                                    currentOperatorQuery.addInputTable(grandParent.getOperatorId());

                                                    //Adding Select
                                                    currentOperatorQuery.setLocalQueryString( " SELECT "+selectString+" "+currentOperatorQuery.getLocalQueryString());
                                                    currentOperatorQuery.setExaremeQueryString( " SELECT "+selectString+" "+currentOperatorQuery.getExaremeQueryString());

                                                    OperatorQuery newQueryOperator = new OperatorQuery();
                                                    newQueryOperator = goToParentOperator(parent, currentSchema, newQueryOperator, exaremeGraph);
                                                    allQueries.add(newQueryOperator);
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"Query Finished: ["+newQueryOperator.getLocalQueryString()+" ]");

                                                }
                                                else{
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"GrandParent of FileSink is: "+grandParent.getOperatorId()+" not supported yet!");
                                                    System.exit(0);
                                                }
                                            }

                                            //Add Group By Keys
                                            currentOperatorQuery.setLocalQueryString(currentOperatorQuery.getLocalQueryString().concat(" GROUP BY "+columnsString+" "));
                                            currentOperatorQuery.setExaremeQueryString(currentOperatorQuery.getExaremeQueryString().concat(" GROUP BY "+columnsString+" "));

                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"GROUP-BY Keys: " + columnsString);
                                        }
                                        else{
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Column Expression Map is NULL! Can't check for matches!");
                                            System.exit(0);
                                        }

                                    }
                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+" exiting...");
                                    break;
                                }
                                else if(parent.getOperator() instanceof TableScanOperator){
                                    if((currentNode.getOperator() instanceof SelectOperator) || (currentNode.getOperator() instanceof FilterOperator)){
                                        if(currentNode.getOperator() instanceof SelectOperator) {
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Discovered: SEL <-- RS connection...");
                                        }
                                        else if(currentNode.getOperator() instanceof FilterOperator){
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Discovered: FILTER <-- TS connection...");
                                        }
                                        RowSchema rowSchema = parent.getOperator().getSchema();
                                        if(rowSchema == null){
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"RowSchema is null!");
                                            System.exit(0);
                                        }
                                        //System.out.println("Comparing RowSchema...");
                                        //if(currentSchema.equals(rowSchema.toString())){
                                            //System.out.println("Schemas are equal! Proceeding...");
                                            TableScanOperator tbsOperator = (TableScanOperator) parent.getOperator();
                                            List<Operator<?>> grandparents = tbsOperator.getParentOperators();
                                            if((grandparents == null) || ((grandparents != null) && (grandparents.size() == 0))){
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"TableScan is Root!");
                                                TableScanDesc tableScanDesc = (TableScanDesc) tbsOperator.getConf();
                                                if (tableScanDesc != null) {
                                                    if (tableScanDesc.getAlias() == null) {
                                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"NULL Table Alias in TableScan Root!");
                                                        System.exit(0);
                                                    }
                                                    updateColumnAndTypesFromTableScan(rowSchema.toString(), tableScanDesc.getAlias());

                                                    currentOperatorQuery.setLocalQueryString(currentOperatorQuery.getLocalQueryString().concat(" FROM " + tableScanDesc.getAlias()));
                                                    currentOperatorQuery.setExaremeQueryString(currentOperatorQuery.getExaremeQueryString().concat(" FROM " + tableScanDesc.getAlias()));
                                                    currentOperatorQuery.addInputTable(tableScanDesc.getAlias());

                                                }
                                            }
                                            else{
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Non Root TableScan Discovered!!");
                                                System.exit(0);
                                            }
                                        //}
                                        /*else{
                                            System.out.println("Schema OP<----FS are not equal!");
                                            System.exit(0);
                                        }*/
                                    }
                                    else if(currentNode.getOperator() instanceof ReduceSinkOperator){
                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"Discovered RS<---TS Connection!");
                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"This TableScan is the father of a ReduceSink/QueryEnd!");
                                        RowSchema rowSchema = parent.getOperator().getSchema();
                                        TableScanOperator tbsOperator = (TableScanOperator) parent.getOperator();
                                        List<Operator<?>> grandparents = tbsOperator.getParentOperators();
                                        if((grandparents == null) || ((grandparents != null) && (grandparents.size() == 0))){ //ROOT TableScan
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"TableScan is Root!");
                                            TableScanDesc tableScanDesc = (TableScanDesc) tbsOperator.getConf();
                                            if (tableScanDesc != null) {
                                                if (tableScanDesc.getAlias() == null) {
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"NULL Table Alias in TableScan Root!");
                                                    System.exit(0);
                                                }
                                                updateColumnAndTypesFromTableScan(rowSchema.toString(), tableScanDesc.getAlias());

                                                currentOperatorQuery.setLocalQueryString(currentOperatorQuery.getLocalQueryString().concat(" FROM " + tableScanDesc.getAlias()));
                                                currentOperatorQuery.setExaremeQueryString(currentOperatorQuery.getExaremeQueryString().concat(" FROM " + tableScanDesc.getAlias()));
                                                currentOperatorQuery.addInputTable(tableScanDesc.getAlias());

                                            }
                                        }
                                        else{
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Non Root TableScan Discovered!! Old Query Ends here and new One begins!");
                                            if(grandparents.size() != 1){
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"TableScan has more than 1 parent! Not supported yet!");
                                                System.exit(0);
                                            }
                                            else{
                                                Operator<?> grandpa = grandparents.get(0);
                                                if(grandpa instanceof FileSinkOperator){
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\tParent of TableScan is FileSink! Beginning new query!");

                                                    currentOperatorQuery.setLocalQueryString(currentOperatorQuery.getLocalQueryString().concat(" FROM "+grandpa.getOperatorId()));
                                                    currentOperatorQuery.setExaremeQueryString(currentOperatorQuery.getExaremeQueryString().concat(" FROM "+grandpa.getOperatorId()));
                                                    currentOperatorQuery.addInputTable(grandpa.getOperatorId());

                                                    OperatorQuery newOperatorQuery = new OperatorQuery();
                                                    newOperatorQuery = goToParentOperator(parent, currentSchema, newOperatorQuery, exaremeGraph);
                                                    allQueries.add(newOperatorQuery);
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"Query Finished: ["+newOperatorQuery.getLocalQueryString()+" ]");
                                                }
                                                else{
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"TableScan has parent with different type than FileSink! Type was: "+grandpa.getType().toString());
                                                    System.exit(0);
                                                }
                                            }
                                        }
                                    }
                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+" exiting...");
                                    break;
                                }
                                else if(parent.getOperator() instanceof FilterOperator){
                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"Parent is FilterOperator!");

                                    if((currentNode.getOperator() instanceof SelectOperator) || (currentNode.getOperator() instanceof ReduceSinkOperator)){
                                        if(currentNode.getOperator() instanceof SelectOperator) {
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Discovered: SELECT<--FILTER Connection...");
                                        }
                                        else if(currentNode.getOperator() instanceof ReduceSinkOperator){
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Discovered: RS<--FILTER Connection...");
                                        }
                                        FilterOperator filterOp = (FilterOperator) parent.getOperator();
                                        FilterDesc filterDesc = filterOp.getConf();

                                        currentSchema = filterOp.getSchema().toString();

                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"Using schema of FILTER Operator: "+currentSchema);
                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"Discovering new Columns from this Schema...");
                                        addNewColsFromSchema(currentSchema);

                                        ExprNodeDesc predicate = filterDesc.getPredicate();
                                        if(predicate != null){
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Extracting columns of predicate...");
                                            List<String> filterColumns = predicate.getCols();

                                            List<Integer> neededIndexes = new LinkedList<>();

                                            MyMap neededColsMap = new MyMap();
                                            String someSchema = "";
                                            someSchema = extractColsFromTypeName(parent.getOperator().getSchema().toString(), neededColsMap, someSchema);

                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Marking positions of indexes in ColumnMap for later...");
                                            int i = 0;
                                            for(String col : filterColumns){
                                                i = 0;
                                                List<ColumnTypePair> columnTypePairList = columnAndTypeMap.getColumnAndTypeList();
                                                for(ColumnTypePair pair : columnTypePairList){
                                                    boolean found = false;
                                                    if(pair.getColumnName().equals(col)){
                                                        List<ColumnTypePair> blaList = neededColsMap.getColumnAndTypeList();
                                                        for(ColumnTypePair bla : blaList){
                                                            if(bla.getColumnName().equals(col)){
                                                                if(bla.getColumnType().equals(pair.getColumnType())){
                                                                    neededIndexes.add(i);
                                                                    found = true;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                    }
                                                    if(found == true) break;
                                                    i++;
                                                }
                                            }

                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Move on to parent before adding WHERE predicate...");
                                            currentOperatorQuery = goToParentOperator(parent, currentSchema, currentOperatorQuery, exaremeGraph);

                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Returned from child Operator...Now attempting to modify predicate to use correct column names...");

                                            String predicateString = predicate.getExprString();

                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Predicate Columns are currently: "+filterColumns.toString());

                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Modifying...");

                                            MyMap oldNewColumnAlias = new MyMap();

                                            for(int k = 0; k < neededIndexes.size(); k++){
                                                String currentAlias = null;
                                                int l = 0;
                                                List<ColumnTypePair> columnTypePairList = columnAndTypeMap.getColumnAndTypeList();
                                                for(ColumnTypePair c : columnTypePairList){
                                                    if(neededIndexes.get(k) == l){
                                                        currentAlias = c.getColumnName();
                                                        break;
                                                    }
                                                    l++;
                                                }
                                                if(currentAlias == null){
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"CurrentAlias could not be found (FILTER OP)!");
                                                    System.exit(0);
                                                }
                                                int count = 0;
                                                for(String f : filterColumns){
                                                    if(count == k){
                                                        ColumnTypePair doubleColPair = new ColumnTypePair(f, currentAlias);
                                                        oldNewColumnAlias.addPair(doubleColPair);
                                                        break;
                                                    }
                                                    count++;
                                                }
                                            }

                                            for(String predCol : filterColumns){
                                                if(predicateString.contains(predCol)){
                                                    List<ColumnTypePair> colsTypes = oldNewColumnAlias.getColumnAndTypeList();
                                                    String currentAlias = null;
                                                    for(ColumnTypePair c : colsTypes){
                                                        if(c.getColumnName().equals(predCol)){
                                                            currentAlias = c.getColumnType();
                                                        }
                                                    }
                                                    if(currentAlias == null){
                                                        System.out.println(currentNode.getOperator().getOperatorId()+"CurrentAlias is null in oldNewColumnAlias!");
                                                        System.exit(0);
                                                    }
                                                    predicateString = predicateString.replace(predCol, currentAlias);
                                                }
                                            }

                                            currentOperatorQuery.setLocalQueryString(currentOperatorQuery.getLocalQueryString().concat(" WHERE "+predicateString+" "));
                                            currentOperatorQuery.setExaremeQueryString(currentOperatorQuery.getExaremeQueryString().concat(" WHERE "+predicateString+" "));

                                        }
                                        else{
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Predicate is NULL!");
                                            System.exit(0);
                                        }
                                    }
                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+" exiting...");
                                    break;
                                }
                                else if(parent.getOperator() instanceof ReduceSinkOperator){
                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"Parent is ReduceSinkOperator!");

                                    if(currentNode.getOperator() instanceof GroupByOperator){
                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"Discovered: GBY<---RS connection...");
                                        boolean queryFinished = false;
                                        List<Operator<?>> grandParents = parent.getOperator().getParentOperators();
                                        if(grandParents == null){
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\tFather of ReduceSink is NULL!");
                                            System.exit(1);
                                        }
                                        else{
                                            if(grandParents.size() != 1){
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\tReduceSink has more than 1 parent!");
                                                System.exit(1);
                                            }
                                            Operator<?> grandpa = grandParents.get(0);
                                            if(grandpa instanceof TableScanOperator){
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\tAncestor after ReduceSink is TableScan...query is not done yet exactly!");
                                                queryFinished = true;
                                            }
                                            else {
                                                queryFinished = false;
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"\t\tAncestor is not TableScan!");
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"This means that we will be creating a new Query!");
                                            }
                                            RowSchema rowSchema = parent.getOperator().getSchema();
                                            if(rowSchema == null){
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"GBY<---RS Connection! SELECT RowSchema is null!");
                                                System.exit(0);
                                            }
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Comparing RowSchema...");
                                            String newSchemaRS = rowSchema.toString();
                                            if(newSchemaRS.contains("KEY."))
                                                newSchemaRS = newSchemaRS.replace("KEY.", "");
                                            if(newSchemaRS.contains("VALUE."))
                                                newSchemaRS = newSchemaRS.replace("VALUE.", "");

                                            if(currentSchema.equals(newSchemaRS)){
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Schemas are equal! Proceeding...");
                                                MyMap newColumnMap = new MyMap();
                                                Map<String, ExprNodeDesc> columnExprMap = parent.getOperator().getColumnExprMap();
                                                MyMap changeMap = new MyMap();
                                                String changedSchema = new String();
                                                if(columnExprMap != null){

                                                    String someSchema = "";
                                                    MyMap wantedColsMap = new MyMap();
                                                    someSchema = extractColsFromTypeName(newSchemaRS, wantedColsMap, someSchema);

                                                    changedSchema = findPossibleColumnAliases(parent.getOperator(),currentSchema, newColumnMap, columnExprMap, changeMap, wantedColsMap);
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+changeMap.getColumnAndTypeList().size()+" Alias Changes happened!");
                                                    currentSchema = changedSchema;

                                                    currentOperatorQuery = goToParentOperator(parent, currentSchema, currentOperatorQuery, exaremeGraph);

                                                    if(queryFinished == false) {
                                                        currentOperatorQuery.addOutputTable(parent.getOperator().getOperatorId());
                                                        currentOperatorQuery.setExaremeOutputTableName("R_"+parent.getOperator().getOperatorId()+"_0");
                                                        currentOperatorQuery.setLocalQueryString("CREATE TABLE " + parent.getOperator().getOperatorId() + " AS ( " + currentOperatorQuery.getLocalQueryString() + " )");
                                                    }

                                                }
                                                else{
                                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"Column Expression Map is NULL! Can't check for matches!");
                                                    System.exit(0);
                                                }
                                            }
                                            else{
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Schema GBY<---RS are not equal!");
                                                System.exit(0);
                                            }
                                        }
                                    }
                                    else{
                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"Unsupported Current Operator instance!");
                                        System.exit(0);
                                    }
                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+" exiting...");
                                    break;
                                }
                                else if(parent.getOperator() instanceof SelectOperator){ //Current Parent is SelectOperator
                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"Parent is a SelectOperator!");

                                    if((currentNode.getOperator() instanceof LimitOperator) || (currentNode.getOperator() instanceof ListSinkOperator) || (currentNode.getOperator() instanceof GroupByOperator)){ //CurrentNode is Limit Operator
                                        if(currentNode.getOperator() instanceof LimitOperator) {
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Discovered: LIMIT <-- SELECT connection...");
                                        }
                                        else if(currentNode.getOperator() instanceof ListSinkOperator){
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Discovered: OP <-- SELECT connection...");
                                        }
                                        else{
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Discovered: GBY <-- SELECT connection...");
                                        }

                                        RowSchema rowSchema = parent.getOperator().getSchema();
                                        if(rowSchema == null){
                                            if(currentNode.getOperator() instanceof LimitOperator) {
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"LIMIT<---SELECT Connection! SELECT RowSchema is null!");
                                            }
                                            else if(currentNode.getOperator() instanceof ListSinkOperator){
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"OP<---SELECT Connection! SELECT RowSchema is null!");
                                            }
                                            else{
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"GBY<---SELECT Connection! SELECT RowSchema is null!");
                                            }
                                            System.exit(0);
                                        }
                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"Comparing RowSchema...");
                                        if(currentNode.getOperator() instanceof LimitOperator){
                                            if(currentSchema.equals(rowSchema.toString()) == false){
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Schema LIMIT<----SELECT are not equal!");
                                                System.exit(0);
                                            }
                                        }
                                        else if(currentNode.getOperator() instanceof ListSinkOperator){
                                            if(currentSchema.equals(rowSchema.toString()) == false){
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Schema OP<----SELECT are not equal!");
                                                System.exit(0);
                                            }
                                        }
                                        else{
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Schemas will not be compared! CurrentSchema will become the Select Schema");
                                            currentSchema = rowSchema.toString();
                                        }


                                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"Proceeding to Find Selected Columns...");
                                        MyMap newColumnMap = new MyMap();
                                        Map<String, ExprNodeDesc> columnExprMap = parent.getOperator().getColumnExprMap();
                                        MyMap changeMap = new MyMap();
                                        String changedSchema = new String();

                                        MyMap outputColsMap = new MyMap();
                                        String someSchema = "";
                                        someSchema = extractColsFromTypeName(parent.getOperator().getSchema().toString(), outputColsMap, someSchema);

                                        if(columnExprMap != null){

                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Looking for possible Alias changes...");

                                            changedSchema = findPossibleColumnAliases(parent.getOperator(),currentSchema, newColumnMap, columnExprMap, changeMap, outputColsMap);
                                            System.out.println(changeMap.getColumnAndTypeList().size()+" Alias Changes happened!");
                                            currentSchema = changedSchema;

                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Proceeding to Find Selected Columns...");

                                            List<Integer> neededIndexes = new LinkedList<>();

                                            SelectOperator selectParent = (SelectOperator) parent.getOperator();
                                            SelectDesc selectDesc = selectParent.getConf();

                                            if(selectDesc == null){
                                                System.out.println(currentNode.getOperator().getOperatorId()+": "+"SelectDesc is null!");
                                                System.exit(0);
                                            }

                                            List<String> outputCols = selectDesc.getOutputColumnNames();

                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Marking positions of indexes in ColumnMap for later...");

                                            int i = 0;
                                            for(String col : outputCols){
                                                i = 0;
                                                List<ColumnTypePair> columnTypePairList = columnAndTypeMap.getColumnAndTypeList();
                                                for(ColumnTypePair pair: columnTypePairList){
                                                    boolean found = false;
                                                    if(pair.getColumnName().equals(col)){
                                                        List<ColumnTypePair> blaList = outputColsMap.getColumnAndTypeList();
                                                        for(ColumnTypePair blaPair : blaList){
                                                            if(blaPair.getColumnName().equals(col)){
                                                                if(blaPair.getColumnType().equals(pair.getColumnType())){
                                                                    neededIndexes.add(i);
                                                                    found = true;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                    }
                                                    if(found == true) break;
                                                    i++;
                                                }
                                            }


                                                currentOperatorQuery = goToParentOperator(parent, currentSchema, currentOperatorQuery, exaremeGraph);

                                                String columnsString = null;

                                            for(Integer index : neededIndexes){
                                                    int count = 0;
                                                    List<ColumnTypePair> columnTypePairList = columnAndTypeMap.getColumnAndTypeList();
                                                    for(ColumnTypePair pair: columnTypePairList){
                                                        if(count == index){
                                                            if(columnsString == null){
                                                                columnsString = "";
                                                                columnsString = columnsString.concat(pair.getColumnName());
                                                            }
                                                            else{
                                                                columnsString = columnsString.concat(" ,"+pair.getColumnName());
                                                            }
                                                        }
                                                        count++;
                                                    }
                                            }

                                            currentOperatorQuery.setLocalQueryString(" SELECT "+columnsString+" ".concat(currentOperatorQuery.getLocalQueryString()));
                                            currentOperatorQuery.setExaremeQueryString(" SELECT "+columnsString+" ".concat(currentOperatorQuery.getExaremeQueryString()));

                                            break;
                                        }
                                        else{
                                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Column Expression Map is NULL! Can't check for matches!");
                                            System.exit(0);
                                        }
                                    }
                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+" exiting...");
                                    break;
                                }
                                else if(parent.getOperator() instanceof LimitOperator){
                                    currentOperatorQuery = parentIsLimitOperator(currentNode, parent, currentSchema, currentOperatorQuery, exaremeGraph);
                                    break;
                                }
                                else{
                                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"Current Parent is of unsupported instance! Check it");
                                    System.exit(0);
                                }
                            }
                        }
                    }
                    else if(numberOfParents == 2){
                        System.out.println(currentNode.getOperator().getOperatorId()+" has 2 parents!");
                        if(currentNode.getOperator() instanceof JoinOperator){
                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Discovered: 2 JOIN<---RS connections...");
                            System.out.println(currentNode.getOperator().getOperatorId()+": "+"2 New Queries are beginning here!");
                            List<Operator<?>> parents = currentNode.getOperator().getParentOperators();
                            if(parents == null){
                                System.out.println(currentNode.getOperator().getOperatorId()+" JOIN has null parents!");
                                System.exit(0);
                            }
                            if(parents.size() != 2){
                                System.out.println(currentNode.getOperator().getOperatorId()+" JOIN has not 2 exactly parents!");
                                System.exit(0);
                            }
                            Operator<?> parent1 = parents.get(0);
                            Operator<?> parent2 = parents.get(1);

                            if(((parent1 instanceof ReduceSinkOperator) && (parent2 instanceof ReduceSinkOperator)) == false){
                                System.out.println(currentNode.getOperator().getOperatorId()+" parents of JOIN MUST BE RS as of now...");
                            }

                            OperatorQuery newOperatorQuery1 = new OperatorQuery();
                            OperatorQuery newOperatorQuery2 = new OperatorQuery();

                            newOperatorQuery1 = reduceSinkAfterJoin(currentNode, exaremeGraph.getOperatorNodeByName(parent1.getOperatorId()), newOperatorQuery1, currentSchema);
                            newOperatorQuery2 = reduceSinkAfterJoin(currentNode, exaremeGraph.getOperatorNodeByName(parent2.getOperatorId()), newOperatorQuery2, currentSchema);
                            allQueries.add(newOperatorQuery1);
                            allQueries.add(newOperatorQuery2);
                        }
                        else{
                            System.out.println(currentNode.getOperator().getOperatorId()+" is not supported yet!");
                            System.exit(0);
                        }
                    }
                    else{
                        System.out.println(currentNode.getOperator().getOperatorId()+": "+"CurrentNode has more than 1 Parent! Check it");
                        System.exit(0);
                    }
                }
                else{
                    System.out.println(currentNode.getOperator().getOperatorId()+": "+"CurrentNode has no Parents! Root reached!");
                    System.exit(0);
                }
            }
            else{
                System.out.println(currentNode.getOperator().getOperatorId()+": "+"No Edges in Graph!");
                System.exit(0);
            }
        }
        else{
            System.out.println(currentNode.getOperator().getOperatorId()+": "+"No Edges in Graph!");
            System.exit(0);
        }

        return currentOperatorQuery;

    }

    public OperatorQuery reduceSinkAfterJoin(OperatorNode currentNode, OperatorNode parent, OperatorQuery currentOperatorQuery, String currentSchema){

        String newSchemaRS = parent.getOperator().getSchema().toString();
        if(newSchemaRS.contains("KEY."))
            newSchemaRS = newSchemaRS.replace("KEY.", "");
        if(newSchemaRS.contains("VALUE."))
            newSchemaRS = newSchemaRS.replace("VALUE.", "");

        MyMap newColumnMap = new MyMap();
        Map<String, ExprNodeDesc> columnExprMap = parent.getOperator().getColumnExprMap();
        MyMap changeMap = new MyMap();
        String changedSchema = new String();
        if(columnExprMap != null){

            List<Operator<?>> reduceSinkParents = parent.getOperator().getParentOperators();
            if(reduceSinkParents == null){
                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Parents of RS are NULL!");
            }
            if(reduceSinkParents.size() != 1){
                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Parents of RS MUST be 1!");
            }
            Operator<?> rsParent = reduceSinkParents.get(0);
            List<String> selectColumns = new LinkedList<>();
            MyMap selectMap = new MyMap();
            String anotherS="";
            anotherS = extractColsFromTypeName(newSchemaRS, selectMap, anotherS);

            List<String> reduceSinksList = new LinkedList<>(); //TODO add them to Map
            if((rsParent instanceof SelectOperator) == false) {
                System.out.println(currentNode.getOperator().getOperatorId() + ": " + "Query Needs SELECT addition...");
                Map<String, ExprNodeDesc> mapColumns = parent.getOperator().getColumnExprMap();
                for (Map.Entry<String, ExprNodeDesc> entryColumn : mapColumns.entrySet()) {
                    if (entryColumn != null) {
                        String selectCandidate = entryColumn.getKey();
                        if (selectCandidate.contains("KEY.")) {
                            selectCandidate = selectCandidate.replace("KEY.", "");
                        }
                        if (selectCandidate.contains("VALUE.")) {
                            selectCandidate = selectCandidate.replace("VALUE.", "");
                        }
                        if (selectCandidate.contains("reducesinkkey")) {
                            selectCandidate = parent.getOperator().getOperatorId() + "." + selectCandidate;
                            reduceSinksList.add(selectCandidate);
                            ColumnTypePair newPair = new ColumnTypePair(selectCandidate, "OK");
                            selectMap.addPair(newPair);
                        }
                        selectColumns.add(selectCandidate);
                    }
                }


                System.out.println(currentNode.getOperator().getOperatorId() + "SelectColumns=["+selectColumns+"]");

                List<Integer> selectIndexes = new LinkedList<>();
                List<ColumnTypePair> columnTypePairList = columnAndTypeMap.getColumnAndTypeList();
                for(ColumnTypePair columnTypePair : columnTypePairList){
                    int targetIndex = 0;
                    for(String s : selectColumns){
                        if(s.equals(columnTypePair.getColumnName())){
                            if(s.contains("reducesinkkey")) {
                                selectIndexes.add(targetIndex);
                                System.out.println(currentNode.getOperator().getOperatorId() + " Adding index: " + targetIndex + " corresponding to entry: " + columnTypePair.getColumnName());
                                break;
                            }
                            else{
                                boolean foundMatch = false;
                                List<ColumnTypePair> anotherPairList = selectMap.getColumnAndTypeList();
                                for(ColumnTypePair anotherPair : anotherPairList){
                                    if(anotherPair.getColumnName().equals(s)){
                                        if(anotherPair.getColumnType().equals(columnTypePair.getColumnType())){
                                            selectIndexes.add(targetIndex);
                                            System.out.println(currentNode.getOperator().getOperatorId() + " Adding index: " + targetIndex + " corresponding to entry: " + columnTypePair.getColumnName());
                                            foundMatch = true;
                                            break;
                                        }
                                    }
                                }
                                if(foundMatch == true){
                                    break;
                                }
                            }
                        }
                        targetIndex++;
                    }
                }

                String selectString = null;

                changedSchema = findPossibleColumnAliases(parent.getOperator(),currentSchema, newColumnMap, columnExprMap, changeMap, selectMap);
                System.out.println(currentNode.getOperator().getOperatorId()+": "+changeMap.getColumnAndTypeList().size()+" Alias Changes happened!");
                currentSchema = changedSchema;

                System.out.println(currentNode.getOperator().getOperatorId()+": "+"Moving from RS to parent...");

                currentOperatorQuery = goToParentOperator(parent, currentSchema, currentOperatorQuery, exaremeGraph);

                for(Integer targetIndex : selectIndexes){
                    int k = 0;
                    List<ColumnTypePair> columnTypePairList1 = columnAndTypeMap.getColumnAndTypeList();
                    for(ColumnTypePair entry: columnTypePairList1){
                        if(k == targetIndex){
                            System.out.println(currentNode.getOperator().getOperatorId() + " Using index: "+targetIndex+" corresponding to entry: "+entry.getColumnName());
                            if(selectString == null){
                                selectString = "";
                                selectString = selectString+entry.getColumnName();
                            }
                            else{
                                if(selectString.contains(entry.getColumnName()) == false)
                                    selectString = selectString+", "+entry.getColumnName();
                            }
                        }
                        k++;
                    }
                }

                currentOperatorQuery.setLocalQueryString("SELECT "+selectString+" "+currentOperatorQuery.getLocalQueryString());
                currentOperatorQuery.setExaremeQueryString("SELECT "+selectString+" "+currentOperatorQuery.getExaremeQueryString());
            }
            else{
                System.out.println(currentNode.getOperator().getOperatorId()+": "+"RS after Join has Select as Parent! Not supported yet!");
                System.exit(0);
            }

            currentOperatorQuery.setLocalQueryString("CREATE TABLE " + parent.getOperator().getOperatorId() + " AS ( " + currentOperatorQuery.getLocalQueryString() + " )");
            currentOperatorQuery.addOutputTable(parent.getOperator().getOperatorId());
            currentOperatorQuery.setExaremeOutputTableName("R_"+parent.getOperator().getOperatorId()+"_0");

            System.out.println(currentNode.getOperator().getOperatorId()+" QueryString: [ "+currentOperatorQuery.getLocalQueryString()+" ]");
            System.out.println(currentNode.getOperator().getOperatorId()+" exiting...");

        }
        else{
            System.out.println(currentNode.getOperator().getOperatorId()+": "+"Column Expression Map is NULL! Can't check for matches!");
            System.exit(0);
        }

        return currentOperatorQuery;

    }

    public void createExaremeOperators(PrintWriter outputFile){

        /*----Get Hive Operator Graph Leaves----*/
        List<OperatorNode> leaves = exaremeGraph.getLeaves();

        /*-----New OperatorQuery (used by Exareme in Operators section of Plan)-----*/
        OperatorQuery opQuery = new OperatorQuery();
        opQuery.setDataBasePath("/home/panos/tpcds");
        opQuery.setLocalQueryString("");
        opQuery.setExaremeQueryString("");

        System.out.println("Creating Exareme Operators based on Hive Operator Graph....");
        if(leaves != null){
            if(leaves.size() > 0){
                if(leaves.size() == 1){
                    OperatorNode leaf = leaves.get(0);
                    if(leaf != null){
                        if(leaf.getOperatorName().contains("FS")){ //LEAF IS FS
                            System.out.println("Final Operator is a FileSink! A new table must be created logically!");
                        }
                        else if(leaf.getOperatorName().contains("OP")){ /*----Leaf is FetchOperator aka Query is a Select/Not Create----*/
                            System.out.println("Final Operator is a FetchOperator ! This must be a select query!");
                            System.out.println("Locating output columns...");
                            ObjectInspector outputObjInspector = leaf.getOperator().getOutputObjInspector();
                            if(outputObjInspector != null){
                                if(outputObjInspector.getTypeName() != null) {

                                    /*---Build initial ColumnSchema---*/
                                    String schema = "";
                                    schema = extractColsFromTypeName(outputObjInspector.getTypeName(), columnAndTypeMap, schema);
                                    System.out.println(leaf.getOperator().getOperatorId()+": "+"Schema after extractCols is: "+schema);

                                    /*---Move to Parent Operator---*/
                                    opQuery = goToParentOperator(leaf, schema, opQuery, exaremeGraph);

                                    /*---Returned from Parent---*/
                                    opQuery.addOutputTable(leaf.getOperatorName()); //Add output Table Name
                                    opQuery.setExaremeOutputTableName("R_"+leaf.getOperatorName()+"_0");

                                    /*---Finalize current Local Query String---*/
                                    String createString = "CREATE TABLE "+leaf.getOperatorName()+" AS (";
                                    opQuery.setLocalQueryString(createString.concat(opQuery.getLocalQueryString()+" )"));
                                    System.out.println(leaf.getOperator().getOperatorId()+": "+"LocalQueryString: [ "+opQuery.getLocalQueryString()+" ]");

                                    System.out.println(leaf.getOperator().getOperatorId()+": "+"Adding OperatorQuery to QueryList...");
                                    allQueries.add(opQuery);
                                    System.out.println(leaf.getOperator().getOperatorId()+": "+"Showing current Queries...");
                                    outputFile.println("\n\t++++++++++++++++++++++++++++++++++++++++++++++++++++ EXAREME QUERIES ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
                                    outputFile.flush();
                                    for(OperatorQuery q : allQueries){
                                        q.printOperatorQuery(outputFile);
                                    }
                                }
                                else{
                                    System.out.println("Fetch Task has not output cols!");
                                    System.exit(0);
                                }
                            }
                            else{
                                System.out.println("Fetch Task has not output cols!");
                                System.exit(0);
                            }
                        }
                        else{
                            System.out.println("Not ready to work with other kind of Final Nodes!");
                            System.exit(0);
                        }
                    }
                }
                else{
                    System.out.println("Not ready to work with more than one leaf!");
                    System.exit(0);
                }
            }
            else{
                System.out.println("No Leaves exist!");
                System.exit(0);
            }
        }

    }

}
