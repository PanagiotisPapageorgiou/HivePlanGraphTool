package com.inmobi.hive.test;

import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created by panos on 19/7/2016.
 */
public class QueryBuilder {

    ExaremeGraph exaremeGraph;
    LinkedHashMap<String, String> columnAndTypeMap; //_col0 - int .etc
    List<String> allQueries;

    public QueryBuilder(ExaremeGraph graph){
        exaremeGraph = graph;
        allQueries = new LinkedList<>();
        columnAndTypeMap = new LinkedHashMap<>();
    }

    public List<String> getQueryList(){
        return allQueries;
    }

    public void addQueryToList(String queryString) { allQueries.add(queryString); }

    public ExaremeGraph getExaremeGraph(){
        return exaremeGraph;
    }

    //WARNING STRING ARE IMMUTABLE YOU CAN RETURN OR WRAP OR STRINGBUILD ONLY, NOT CHANGE THE VALUE OF THE REFERENCE

    public LinkedHashMap<String, String> getColumnAndTypeMap(){ return columnAndTypeMap; }

    public void setColumnAndTypeMap(LinkedHashMap<String, String> newMap) { columnAndTypeMap = newMap; }

    public void addNewColsFromSchema(String givenSchema){

        System.out.println("Given Schema: ["+givenSchema+"]");

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
                    for(Map.Entry<String, String> entry : columnAndTypeMap.entrySet()){
                        if(entry.getKey().equals(tupleParts[0])){
                            exists = true;
                            break;
                        }
                    }
                    if(exists == false) {
                        System.out.println("New Column-Value Part located...Key: " + tupleParts[0] + " Value: " + tupleParts[1]);
                        columnAndTypeMap.put(tupleParts[0], tupleParts[1]);
                    }
                }
                else{
                    System.out.println("addNewColsFromSchema: Failed to :!");
                    System.exit(0);
                }
            }
        }

    }

    public void updateColumnAndTypesFromTableScan(String givenSchema, String tableAlias){

        System.out.println("Given Schema: ["+givenSchema+"]");

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
        LinkedHashMap<String, String> theNewMap = new LinkedHashMap<>();
        for(String tuple : parts){
            if(tuple != null){
                if(tuple.contains(":")){
                    String[] tupleParts = tuple.split(":");
                    boolean exists = false;
                    for(Map.Entry<String, String> entry : columnAndTypeMap.entrySet()){
                        if(entry.getKey().equals(tupleParts[0])){
                            exists = true;
                            break;
                        }
                    }
                    if(exists == false) {
                        if(tupleParts[0].equals("BLOCK__OFFSET__INSIDE__FILE") || tupleParts[0].equals("INPUT__FILE__NAME") || tupleParts[0].equals("ROW__ID") || tupleParts[0].equals("bucketid") || tupleParts[0].equals("rowid") ){
                            continue;
                        }
                        System.out.println("New Column-Value Part located...Key: " + tupleParts[0] + " Value: " + tupleParts[1]);
                        columnAndTypeMap.put(tupleParts[0], tupleParts[1]);
                    }
                }
                else{
                    System.out.println("addNewColsFromSchema: Failed to :!");
                    System.exit(0);
                }
            }
        }

        System.out.println("Now creating newMap with Keys having format table.col where applicable...");

        for(Map.Entry<String, String> entry : columnAndTypeMap.entrySet()){
            boolean existsInSchema = false;
            for(String tuple : parts){
                if(tuple != null){
                    if(tuple.contains(":")) {
                        String[] tupleParts = tuple.split(":");
                        if(entry.getKey().equals(tupleParts[0])){
                            existsInSchema = true;
                            break;
                        }
                    }
                }
            }
            if(existsInSchema == true){
                System.out.println("Adding to new Map the updated entry: "+tableAlias+"."+entry.getKey());
                String theNewKey = tableAlias+"."+entry.getKey();
                theNewMap.put(theNewKey, entry.getValue());
            }
            else{
                System.out.println("Adding to new Map without update the entry: "+entry.getKey());
                theNewMap.put(entry.getKey(), entry.getValue());
            }
        }

        columnAndTypeMap = theNewMap;

    }

    public String extractColsFromTypeName(String typeName, Map<String, String> columns, String schema){

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
                    System.out.println("New Column-Value Part located...Key: "+tupleParts[0]+" Value: "+tupleParts[1]);
                    columns.put(tupleParts[0], tupleParts[1]);
                }
                else{
                    System.out.println("extractColsFromTypeName: Failed to :!");
                    System.exit(0);
                }
            }
        }

        return schema;

    }

    public String buildColumnNamesFromMap(LinkedHashMap<String, String> columns){

        String output = "";

        if(columns != null){
            for(Map.Entry<String,String> entry : columns.entrySet()){
                if(!output.isEmpty()){
                    output = output.concat(", ");
                }
                if(entry != null){
                    if(entry.getKey() != null){
                        output = output.concat(entry.getKey());
                    }
                }
            }
        }

        System.out.println("buildColumnNamesFromMap: "+output);

        return output;

    }

    public List<String> findPossibleColumnAliases(String currentSchema, String currentQueryString, Map<String, String> currentColumns, Map<String, String> newColumnMap, Map<String, ExprNodeDesc> columnExprMap, Map<String, String> changeMap){

        List<String> neededStrings = new LinkedList<>();
        boolean foundMatch = false;

        for(Map.Entry<String, String> entry : currentColumns.entrySet()){
            foundMatch = false;
            for(Map.Entry<String, ExprNodeDesc> entry2 : columnExprMap.entrySet()){
                if(entry2 != null){
                    if(entry2.getKey() != null){
                        if(entry2.getValue() != null){
                            if((entry2.getKey().equals("ROW__ID")) || entry2.getKey().equals("BLOCK__OFFSET__INSIDE__FILE") || entry2.getKey().equals("INPUT__FILE__NAME")){
                                System.out.println("Detected: "+entry2.getKey()+" skipping!");
                                continue;
                            }
                            if((entry.getKey().equals(entry2.getKey())) || ("KEY.".concat(entry.getKey()).equals(entry2.getKey())) || ("VALUE.".concat(entry.getKey()).equals(entry2.getKey()))){
                                System.out.println("Found possible Match for: "+entry.getKey());
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
                                        if(exprColumn.equals(entry.getKey()) == false){
                                            System.out.println("Found possible Match for: "+entry.getKey()+" Match: "+exprColumn);
                                            currentSchema = currentSchema.replace(entry.getKey(), exprColumn);
                                            newColumnMap.put(exprColumn, entry.getValue());
                                            changeMap.put(entry.getKey(), exprColumn);
                                        }
                                        else{
                                            newColumnMap.put(entry.getKey(), entry.getValue());
                                        }
                                    }
                                    else{
                                        System.out.println("ExprCols must have size 1!");
                                        System.exit(0);
                                    }
                                }
                                else{
                                    System.out.println("ExprCols is NULL!");
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
                newColumnMap.put(entry.getKey(), entry.getValue());
            }
        }

        boolean hasNoMatch = true;
        for(Map.Entry<String, ExprNodeDesc> entry2 : columnExprMap.entrySet()){
            hasNoMatch = true;
            if(entry2 != null){
                if(entry2.getKey() != null){
                    if(entry2.getValue() != null){
                        for(Map.Entry<String, String> entry : currentColumns.entrySet()){
                            if((entry.getKey().equals(entry2.getKey())) || ("KEY.".concat(entry.getKey()).equals(entry2.getKey())) || ("VALUE.".concat(entry.getKey()).equals(entry2.getKey()))){
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
                            System.out.println("Detected: "+entry2.getKey()+" skipping!");
                            continue;
                        }
                        if (entry2.getValue() != null) {
                            System.out.println("Discovered non existent needed column!: "+entry2.getKey());
                            newColumnMap.put(entry2.getKey(), "Unknown-Type");
                        }
                    }
                }
            }
        }

        columnAndTypeMap = (LinkedHashMap<String,String>) newColumnMap;
        currentColumns = newColumnMap;

        neededStrings.add(currentQueryString);
        neededStrings.add(currentSchema);

        return neededStrings;

    }


    public String parentIsFileSinkOperator(OperatorNode currentNode, OperatorNode parent, String currentSchema, Map<String, String> currentColumns, String currentQueryString, ExaremeGraph exaremeGraph){

        System.out.println("Parent is a FileSinkOperator!");

        if(currentNode.getOperator() instanceof ListSinkOperator){ //Current Node is FetchOperator
            System.out.println("Discovered: OP <-- FS connection...");
            RowSchema rowSchema = parent.getOperator().getSchema();
            if(rowSchema == null){
                System.out.println("OP<---FS Connection! FS RowSchema is null!");
                System.exit(0);
            }
            System.out.println("Comparing RowSchema...");
            if(currentSchema.equals(rowSchema.toString())){
                System.out.println("Schemas are equal! Proceeding...");
            }
            else{
                System.out.println("Schema OP<----FS are not equal!");
                System.exit(0);
            }

            currentQueryString = goToParentOperator(parent, currentColumns, currentSchema, currentQueryString, exaremeGraph);

        }
        else if(currentNode.getOperator() instanceof GroupByOperator){ //Current Node is GroupByOperator
            System.out.println("Discovered: GBY <-- FS connection...");
            RowSchema rowSchema = parent.getOperator().getSchema();
            if(rowSchema == null){
                System.out.println("GBY<---FS Connection! FS RowSchema is null!");
                System.exit(0);
            }
            System.out.println("Comparing RowSchema...");
            if(currentSchema.equals(rowSchema.toString())){
                System.out.println("Schemas are equal! Proceeding...");
            }
            else{
                System.out.println("Schema GBY<----FS are not equal!");
                System.exit(0);
            }
            System.out.println("We have reached a FileSink! Creating a new Query!");
            currentQueryString = goToParentOperator(parent, currentColumns, currentSchema, currentQueryString, exaremeGraph);

            currentQueryString = "CREATE TABLE "+parent.getOperator().getOperatorId()+" AS ( "+currentQueryString+" )";
        }

        return currentQueryString;

    }

    public String parentIsLimitOperator(OperatorNode currentNode, OperatorNode parent, String currentSchema, Map<String, String> currentColumns, String currentQueryString, ExaremeGraph exaremeGraph){
        System.out.println("Parent is a LimitOperator!");
        if(currentNode.getOperator() instanceof FileSinkOperator){
            System.out.println("Discovered: FS <-- LIMIT connection...");
            RowSchema rowSchema = parent.getOperator().getSchema();
            if(rowSchema == null){
                System.out.println("FS<---LIMIT Connection! LIMIT RowSchema is null!");
                System.exit(0);
            }
            System.out.println("Comparing RowSchema...");
            if(currentSchema.equals(rowSchema.toString())){
                System.out.println("Schemas are equal! Proceeding...");
                LimitOperator limitOp = (LimitOperator) parent.getOperator();
                LimitDesc limitDesc = limitOp.getConf();
                if(limitDesc != null){
                    System.out.println("Limit is: "+limitDesc.getLimit());

                    currentQueryString = goToParentOperator(parent, currentColumns, currentSchema, currentQueryString, exaremeGraph);

                    currentQueryString = "( "+currentQueryString;
                    System.out.println("Appending LIMIT to end of Query...");
                    currentQueryString = currentQueryString.concat(" ) LIMIT ");
                    currentQueryString = currentQueryString.concat(Integer.toString(limitDesc.getLimit()));
                    System.out.println("QueryString is now: "+currentQueryString);
                }
                else{
                    System.out.println("LimitDesc is NULL!");
                    System.exit(0);
                }
            }
            else{
                System.out.println("Schema FS<----LIMIT are not equal!");
                System.exit(0);
            }
        }

        return currentQueryString;

    }

    public String goToParentOperator(OperatorNode currentNode, Map<String, String> currentColumns, String currentSchema, String currentQueryString, ExaremeGraph exaremeGraph){

        System.out.println("goToParentOperator: Currently Operating in Node: "+currentNode.getOperatorName());
        System.out.println("goToParentOperator: CurrentSchema= "+currentSchema);
        System.out.println("goToParentOperator: CurrentQueryString= "+currentQueryString);

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
                                System.out.println("goToParentOperator: Accessing Parent: "+parent.getOperatorName());

                                if(parent.getOperator() instanceof FileSinkOperator){ //Parent is FileSinkOperator
                                    currentQueryString = parentIsFileSinkOperator(currentNode, parent, currentSchema, currentColumns, currentQueryString, exaremeGraph);
                                    break;
                                }
                                else if(parent.getOperator() instanceof GroupByOperator){
                                    System.out.println("Parent is a GroupByOperator!");
                                    if((currentNode.getOperator() instanceof SelectOperator) || (currentNode.getOperator() instanceof ReduceSinkOperator)){
                                        if(currentNode.getOperator() instanceof SelectOperator)
                                            System.out.println("Discovered: SEL <-- GBY connection...");
                                        else
                                            System.out.println("Discovered: RS <-- GBY connection...");

                                        RowSchema rowSchema = parent.getOperator().getSchema();
                                        if(rowSchema == null){
                                            if(currentNode.getOperator() instanceof SelectOperator)
                                                System.out.println("SEL<---GBY Connection! GBY RowSchema is null!");
                                            else
                                                System.out.println("RS<---GBY Connection! GBY RowSchema is null!");
                                            System.exit(0);
                                        }
                                        System.out.println("Comparing RowSchema...");
                                        if(currentSchema.equals(rowSchema.toString())){
                                            System.out.println("Schemas are equal!");
                                        }
                                        else{
                                            System.out.println("GroupByOperator has different Schema (will become new Schema): [" + rowSchema.toString() + "]");
                                            currentSchema = rowSchema.toString();
                                            if(currentSchema.contains("KEY.")){
                                                System.out.println("Removing KEY. from Schema");
                                                currentSchema.replace("KEY.", "");
                                            }
                                            if(currentSchema.contains("VALUE.")){
                                                System.out.println("Removing VALUE. from Schema");
                                                currentSchema.replace("VALUE.", "");
                                            }
                                            addNewColsFromSchema(rowSchema.toString());
                                            currentColumns = columnAndTypeMap;
                                        }

                                        List<Integer> neededIndexes = new LinkedList<>();

                                        GroupByOperator groupByParent = (GroupByOperator) parent.getOperator();
                                        GroupByDesc groupByDesc = groupByParent.getConf();

                                        if(groupByParent == null){
                                            System.out.println("GroupByDesc is null!");
                                            System.exit(0);
                                        }

                                        System.out.println("Discovering Group By Keys...");
                                        List<String> groupByKeys = new LinkedList<>();
                                        LinkedHashMap<String, String> changeMap = new LinkedHashMap<>();
                                        if(groupByDesc != null){
                                            ArrayList<ExprNodeDesc> keys = groupByDesc.getKeys();
                                            if (keys != null) {
                                                System.out.println("Keys: ");
                                                for (ExprNodeDesc k : keys) {
                                                    if (k != null) {
                                                        System.out.println("\tKey: ");
                                                        System.out.println("\t\tName: " + k.getName());
                                                        if (k.getCols() != null) {
                                                            System.out.println("\t\tCols: " + k.getCols().toString());
                                                            if(k.getCols().size() > 1){
                                                                System.out.println("Key for more than one column?! GROUP BY");
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
                                                            System.out.println("\t\tCols: NULL");
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        System.out.println("Checking for possible Alias Changes...");
                                        LinkedHashMap<String, String> newColumnMap = new LinkedHashMap<>();
                                        Map<String, ExprNodeDesc> columnExprMap = parent.getOperator().getColumnExprMap();
                                        List<String> changedQueryAndSchema = new LinkedList<>();
                                        if(columnExprMap != null){
                                            changedQueryAndSchema = findPossibleColumnAliases(currentSchema, currentQueryString, currentColumns, newColumnMap, columnExprMap, changeMap);
                                            System.out.println(changeMap.size()+" Alias Changes happened!");
                                            currentQueryString = changedQueryAndSchema.get(0);
                                            currentSchema = changedQueryAndSchema.get(1);
                                            currentColumns = columnAndTypeMap;
                                            //currentColumns = columnAndTypeMap;
                                            System.out.println("Marking positions of indexes in ColumnMap for later...");
                                            int i = 0;
                                            for(String col : groupByKeys){
                                                i = 0;
                                                for(Map.Entry<String, String> entry: currentColumns.entrySet()){
                                                    if(entry.getKey().equals(col)){
                                                        System.out.println("Located required Index for GroupBy: "+i);
                                                        neededIndexes.add(i);
                                                        break;
                                                    }
                                                    i++;
                                                }
                                            }

                                            boolean queryFinished = false;
                                            List<Operator<? extends OperatorDesc>> parents = parent.getOperator().getParentOperators();
                                            if(parents != null){
                                                if(parents.size() > 0){
                                                    if(parents.size() > 1){
                                                        System.out.println("\t\tGROUP WITH MORE THAN ONE PARENT?");
                                                        System.exit(1);
                                                    }
                                                    else{
                                                        Operator<?> grandpa = parents.get(0);
                                                        if(grandpa != null){
                                                            if(grandpa instanceof ReduceSinkOperator){
                                                                System.out.println("\t\tParent of GroupBy is ReduceSink! Beginning new query!");
                                                                currentQueryString = currentQueryString.concat(" FROM "+grandpa.getOperatorId());
                                                                queryFinished = true;
                                                            }
                                                            else if(grandpa instanceof FileSinkOperator){
                                                                System.out.println("\t\tParent of GroupBy is FileSink! Beginning new query!");
                                                                currentQueryString = currentQueryString.concat(" FROM "+grandpa.getOperatorId());
                                                                queryFinished = true;
                                                            }
                                                        }
                                                    }
                                                }
                                                else{
                                                    System.out.println("\t\tGROUP BY WITH NO PARENTS?");
                                                    System.exit(1);
                                                }
                                            }

                                            if(queryFinished == false) {
                                                currentQueryString = goToParentOperator(parent, newColumnMap, currentSchema, currentQueryString, exaremeGraph);
                                            }
                                            else{ //NEW QUERY BEGINS AFTER THIS POINT, WHEN WE RETURN WE MUST ADD IT TO THE LIST
                                                String newQueryString = "";
                                                newQueryString = goToParentOperator(parent, newColumnMap, currentSchema, newQueryString, exaremeGraph);
                                                allQueries.add(newQueryString);
                                                System.out.println("Query Finished: ["+newQueryString+" ]");
                                            }

                                            String columnsString = null;

                                            for(Integer index : neededIndexes){
                                                int count = 0;
                                                for(Map.Entry<String, String> entry : columnAndTypeMap.entrySet()){
                                                    if(count == index){
                                                        if(columnsString == null){
                                                            columnsString = "";
                                                            columnsString = columnsString.concat(entry.getKey());
                                                        }
                                                        else{
                                                            columnsString = columnsString.concat(" ,"+entry.getKey());
                                                        }
                                                    }
                                                    count++;
                                                }
                                            }

                                            currentQueryString = currentQueryString.concat(" GROUP BY "+columnsString+" ");
                                            System.out.println("GROUP-BY Keys: " + columnsString);
                                        }
                                        else{
                                            System.out.println("Column Expression Map is NULL! Can't check for matches!");
                                            System.exit(0);
                                        }

                                    }
                                    break;
                                }
                                else if(parent.getOperator() instanceof TableScanOperator){
                                    if((currentNode.getOperator() instanceof SelectOperator) || (currentNode.getOperator() instanceof FilterOperator)){
                                        if(currentNode.getOperator() instanceof SelectOperator) {
                                            System.out.println("Discovered: TS <-- SEL connection...");
                                        }
                                        else if(currentNode.getOperator() instanceof FilterOperator){
                                            System.out.println("Discovered: FILTER <-- SEL connection...");
                                        }
                                        RowSchema rowSchema = parent.getOperator().getSchema();
                                        if(rowSchema == null){
                                            System.out.println("RowSchema is null!");
                                            System.exit(0);
                                        }
                                        //System.out.println("Comparing RowSchema...");
                                        //if(currentSchema.equals(rowSchema.toString())){
                                            //System.out.println("Schemas are equal! Proceeding...");
                                            TableScanOperator tbsOperator = (TableScanOperator) parent.getOperator();
                                            List<Operator<?>> grandparents = tbsOperator.getParentOperators();
                                            if((grandparents == null) || ((grandparents != null) && (grandparents.size() == 0))){
                                                System.out.println("TableScan is Root!");
                                                TableScanDesc tableScanDesc = (TableScanDesc) tbsOperator.getConf();
                                                if (tableScanDesc != null) {
                                                    if (tableScanDesc.getAlias() == null) {
                                                        System.out.println("NULL Table Alias in TableScan Root!");
                                                        System.exit(0);
                                                    }
                                                    updateColumnAndTypesFromTableScan(rowSchema.toString(), tableScanDesc.getAlias());

                                                    currentQueryString = currentQueryString.concat(" FROM " + tableScanDesc.getAlias());
                                                }
                                            }
                                            else{
                                                System.out.println("Non Root TableScan Discovered!!");
                                                System.exit(0);
                                            }
                                        //}
                                        /*else{
                                            System.out.println("Schema OP<----FS are not equal!");
                                            System.exit(0);
                                        }*/
                                    }
                                    break;
                                }
                                else if(parent.getOperator() instanceof FilterOperator){
                                    System.out.println("Parent is FilterOperator!");

                                    if(currentNode.getOperator() instanceof SelectOperator){
                                        System.out.println("Discovered: SELECT<--FILTER Connection...");
                                        FilterOperator filterOp = (FilterOperator) parent.getOperator();
                                        FilterDesc filterDesc = filterOp.getConf();

                                        currentSchema = filterOp.getSchema().toString();

                                        System.out.println("Using schema of FILTER Operator: "+currentSchema);
                                        System.out.println("Discovering new Columns from this Schema...");
                                        addNewColsFromSchema(currentSchema);
                                        currentColumns = columnAndTypeMap;

                                        ExprNodeDesc predicate = filterDesc.getPredicate();
                                        if(predicate != null){
                                            System.out.println("Extracting columns of predicate...");
                                            List<String> filterColumns = predicate.getCols();

                                            List<Integer> neededIndexes = new LinkedList<>();

                                            System.out.println("Marking positions of indexes in ColumnMap for later...");
                                            int i = 0;
                                            for(String col : filterColumns){
                                                i = 0;
                                                for(Map.Entry<String, String> entry: currentColumns.entrySet()){
                                                    if(entry.getKey().equals(col)){
                                                        neededIndexes.add(i);
                                                        break;
                                                    }
                                                    i++;
                                                }
                                            }

                                            System.out.println("Move on to parent before adding WHERE predicate...");
                                            currentQueryString = goToParentOperator(parent, currentColumns, currentSchema, currentQueryString, exaremeGraph);

                                            System.out.println("Returned from child Operator...Now attempting to modify predicate to use correct column names...");

                                            String predicateString = predicate.getExprString();

                                            System.out.println("Predicate Columns are currently: "+filterColumns.toString());

                                            System.out.println("Modifying...");

                                            LinkedHashMap<String, String> oldNewColumnAlias = new LinkedHashMap<>();

                                            for(int k = 0; k < neededIndexes.size(); k++){
                                                String currentAlias = null;
                                                int l = 0;
                                                for(Map.Entry<String, String > entry: columnAndTypeMap.entrySet()){
                                                    if(neededIndexes.get(k) == l){
                                                        currentAlias = entry.getKey();
                                                        break;
                                                    }
                                                    l++;
                                                }
                                                if(currentAlias == null){
                                                    System.out.println("CurrentAlias could not be found (FILTER OP)!");
                                                    System.exit(0);
                                                }
                                                oldNewColumnAlias.put(filterColumns.get(k), currentAlias);
                                            }

                                            for(String predCol : filterColumns){
                                                if(predicateString.contains(predCol)){
                                                    String currentAlias = oldNewColumnAlias.get(predCol);
                                                    predicateString = predicateString.replace(predCol, currentAlias);
                                                }
                                            }

                                            currentQueryString = currentQueryString.concat(" WHERE "+predicateString+" ");
                                        }
                                        else{
                                            System.out.println("Predicate is NULL!");
                                            System.exit(0);
                                        }
                                    }
                                    break;
                                }
                                else if(parent.getOperator() instanceof ReduceSinkOperator){
                                    System.out.println("Parent is ReduceSinkOperator!");

                                    if(currentNode.getOperator() instanceof GroupByOperator){
                                        System.out.println("Discovered: GBY<---RS connection...");
                                        System.out.println("This means that we will be creating a new Query!");
                                        RowSchema rowSchema = parent.getOperator().getSchema();
                                        if(rowSchema == null){
                                            System.out.println("GBY<---RS Connection! SELECT RowSchema is null!");
                                            System.exit(0);
                                        }
                                        System.out.println("Comparing RowSchema...");
                                        String newSchemaRS = rowSchema.toString();
                                        if(newSchemaRS.contains("KEY."))
                                            newSchemaRS = newSchemaRS.replace("KEY.", "");
                                        if(newSchemaRS.contains("VALUE."))
                                            newSchemaRS = newSchemaRS.replace("VALUE.", "");

                                        if(currentSchema.equals(newSchemaRS)){
                                            System.out.println("Schemas are equal! Proceeding...");
                                            LinkedHashMap<String, String> newColumnMap = new LinkedHashMap<>();
                                            Map<String, ExprNodeDesc> columnExprMap = parent.getOperator().getColumnExprMap();
                                            LinkedHashMap<String, String> changeMap = new LinkedHashMap<>();
                                            List<String> changedQueryAndSchema = new LinkedList<>();
                                            if(columnExprMap != null){

                                                changedQueryAndSchema = findPossibleColumnAliases(currentSchema, currentQueryString, currentColumns, newColumnMap, columnExprMap, changeMap);
                                                System.out.println(changeMap.size()+" Alias Changes happened!");
                                                currentQueryString = changedQueryAndSchema.get(0);
                                                currentSchema = changedQueryAndSchema.get(1);
                                                currentColumns = columnAndTypeMap;

                                                currentQueryString = goToParentOperator(parent, newColumnMap, currentSchema, currentQueryString, exaremeGraph);

                                                currentQueryString = "CREATE TABLE "+parent.getOperator().getOperatorId()+" AS ( "+currentQueryString+" )";

                                            }
                                            else{
                                                System.out.println("Column Expression Map is NULL! Can't check for matches!");
                                                System.exit(0);
                                            }
                                        }
                                        else{
                                            System.out.println("Schema LIMIT<----SELECT are not equal!");
                                            System.exit(0);
                                        }
                                    }
                                    break;
                                }
                                else if(parent.getOperator() instanceof SelectOperator){ //Current Parent is SelectOperator
                                    System.out.println("Parent is a SelectOperator!");

                                    if((currentNode.getOperator() instanceof LimitOperator) || (currentNode.getOperator() instanceof ListSinkOperator) || (currentNode.getOperator() instanceof GroupByOperator)){ //CurrentNode is Limit Operator
                                        if(currentNode.getOperator() instanceof LimitOperator) {
                                            System.out.println("Discovered: LIMIT <-- SELECT connection...");
                                        }
                                        else if(currentNode.getOperator() instanceof ListSinkOperator){
                                            System.out.println("Discovered: OP <-- SELECT connection...");
                                        }
                                        else{
                                            System.out.println("Discovered: GBY <-- SELECT connection...");
                                        }

                                        RowSchema rowSchema = parent.getOperator().getSchema();
                                        if(rowSchema == null){
                                            if(currentNode.getOperator() instanceof LimitOperator) {
                                                System.out.println("LIMIT<---SELECT Connection! SELECT RowSchema is null!");
                                            }
                                            else if(currentNode.getOperator() instanceof ListSinkOperator){
                                                System.out.println("OP<---SELECT Connection! SELECT RowSchema is null!");
                                            }
                                            else{
                                                System.out.println("GBY<---SELECT Connection! SELECT RowSchema is null!");
                                            }
                                            System.exit(0);
                                        }
                                        System.out.println("Comparing RowSchema...");
                                        if(currentNode.getOperator() instanceof LimitOperator){
                                            if(currentSchema.equals(rowSchema.toString()) == false){
                                                System.out.println("Schema LIMIT<----SELECT are not equal!");
                                                System.exit(0);
                                            }
                                        }
                                        else if(currentNode.getOperator() instanceof ListSinkOperator){
                                            if(currentSchema.equals(rowSchema.toString()) == false){
                                                System.out.println("Schema OP<----SELECT are not equal!");
                                                System.exit(0);
                                            }
                                        }
                                        else{
                                            System.out.println("Schemas will not be compared! CurrentSchema will become the Select Schema");
                                            currentSchema = rowSchema.toString();
                                        }


                                        System.out.println("Proceeding to Find Selected Columns...");
                                        LinkedHashMap<String, String> newColumnMap = new LinkedHashMap<>();
                                        Map<String, ExprNodeDesc> columnExprMap = parent.getOperator().getColumnExprMap();
                                        LinkedHashMap<String, String> changeMap = new LinkedHashMap<>();
                                        List<String> changedQueryAndSchema = new LinkedList<>();
                                        if(columnExprMap != null){

                                            System.out.println("Looking for possible Alias changes...");

                                            changedQueryAndSchema = findPossibleColumnAliases(currentSchema, currentQueryString, currentColumns, newColumnMap, columnExprMap, changeMap);
                                            System.out.println(changeMap.size()+" Alias Changes happened!");
                                            currentQueryString = changedQueryAndSchema.get(0);
                                            currentSchema = changedQueryAndSchema.get(1);
                                            currentColumns = columnAndTypeMap;

                                            System.out.println("Proceeding to Find Selected Columns...");

                                            List<Integer> neededIndexes = new LinkedList<>();

                                            SelectOperator selectParent = (SelectOperator) parent.getOperator();
                                            SelectDesc selectDesc = selectParent.getConf();

                                            if(selectDesc == null){
                                                System.out.println("SelectDesc is null!");
                                                System.exit(0);
                                            }

                                            List<String> outputCols = selectDesc.getOutputColumnNames();

                                            System.out.println("Marking positions of indexes in ColumnMap for later...");

                                            int i = 0;
                                            for(String col : outputCols){
                                                i = 0;
                                                for(Map.Entry<String, String> entry: currentColumns.entrySet()){
                                                    if(entry.getKey().equals(col)){
                                                        neededIndexes.add(i);
                                                        break;
                                                    }
                                                    i++;
                                                }
                                            }


                                                currentQueryString = goToParentOperator(parent, newColumnMap, currentSchema, currentQueryString, exaremeGraph);

                                                String columnsString = null;

                                            for(Integer index : neededIndexes){
                                                    int count = 0;
                                                    for(Map.Entry<String, String> entry : columnAndTypeMap.entrySet()){
                                                        if(count == index){
                                                            if(columnsString == null){
                                                                columnsString = "";
                                                                columnsString = columnsString.concat(entry.getKey());
                                                            }
                                                            else{
                                                                columnsString = columnsString.concat(" ,"+entry.getKey());
                                                            }
                                                        }
                                                        count++;
                                                    }
                                            }

                                            currentQueryString = " SELECT "+columnsString+" ".concat(currentQueryString);
                                            break;
                                        }
                                        else{
                                            System.out.println("Column Expression Map is NULL! Can't check for matches!");
                                            System.exit(0);
                                        }
                                    }
                                }
                                else if(parent.getOperator() instanceof LimitOperator){
                                    currentQueryString = parentIsLimitOperator(currentNode, parent, currentSchema, currentColumns, currentQueryString, exaremeGraph);
                                    break;
                                }
                                else{
                                    System.out.println("Current Parent is of unsupported instance! Check it");
                                    System.exit(0);
                                }
                            }
                        }
                    }
                    else{
                        System.out.println("CurrentNode has more than 1 Parent! Check it");
                        System.exit(0);
                    }
                }
                else{
                    System.out.println("CurrentNode has no Parents! Root reached!");
                    System.exit(0);
                }
            }
            else{
                System.out.println("No Edges in Graph!");
                System.exit(0);
            }
        }
        else{
            System.out.println("No Edges in Graph!");
            System.exit(0);
        }

        return currentQueryString;

    }

    public void createExaremeOperators(PrintWriter outputFile){
        List<OperatorNode> leaves = exaremeGraph.getLeaves();
        String queryString = "";
        System.out.println("Creating Exareme Plan based on Operator Graph....");
        if(leaves != null){
            if(leaves.size() > 0){
                if(leaves.size() == 1){
                    OperatorNode leaf = leaves.get(0);
                    if(leaf != null){
                        if(leaf.getOperatorName().contains("FS")){ //LEAF IS FS
                            System.out.println("Final Operator is a FileSink! A new table must be created logically!");
                        }
                        else if(leaf.getOperatorName().contains("OP")){ //LEAF IS OP
                            System.out.println("Final Operator is a FetchOperator ! This must be a select query!");
                            System.out.println("Locating output columns...");
                            ObjectInspector outputObjInspector = leaf.getOperator().getOutputObjInspector();
                            if(outputObjInspector != null){
                                if(outputObjInspector.getTypeName() != null) {
                                    String schema = "";
                                    schema = extractColsFromTypeName(outputObjInspector.getTypeName(), columnAndTypeMap, schema);
                                    System.out.println("Schema after extractCols is: "+schema);
                                    queryString = goToParentOperator(leaf, columnAndTypeMap, schema, queryString, exaremeGraph);
                                    String createString = "CREATE TABLE "+leaf.getOperatorName()+" AS (";
                                    queryString = createString.concat(queryString+" )");
                                    System.out.println("QueryString: [ "+queryString+" ]");
                                    System.out.println("Adding Query to QueryList...");
                                    allQueries.add(queryString);
                                    System.out.println("Showing current Queries...");
                                    outputFile.println("\n\t++++++++++++++++++++++++++++++++++++++++++++++++++++ EXAREME QUERIES ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
                                    outputFile.flush();
                                    for(String query : allQueries){
                                        System.out.println("\t\tQuery: ["+query+" ]");
                                        outputFile.println("\t\tQuery: ["+query+" ]");
                                        outputFile.flush();
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

        System.out.println("QueryString for Exareme: ["+queryString+" ]");

    }

}
