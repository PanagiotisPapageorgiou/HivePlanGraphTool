package com.inmobi.hive.test;

import madgik.exareme.common.app.engine.AdpDBSelectOperator;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by panos on 17/8/2016.
 */

/* This class helps translate a set of MapReduce Operators (contained in the ExaremeGraph)
   to an Exareme Plan Operator (such as the AdpDBSelectOperator). Similarly to an actual
   Exareme Operator it keeps track of InputTables and OutputTable. It also contains the query
   that runs on the specific input tables in order to produce the output table and the used
   columns.

   Instance of this class are created and filled by the QueryBuilder class.

*/

public class OperatorQuery {
    String dataBasePath;
    String localQueryString;
    String exaremeQueryString;
    String exaremeOutputTableName;
    String assignedContainer;
    MyTable outputTable;
    MyMap usedColumns;
    List<MyTable> inputTables;

    public OperatorQuery() {
        assignedContainer = "";
        dataBasePath = "";
        localQueryString = "";
        exaremeQueryString = "";
        exaremeOutputTableName = "";
        outputTable = new MyTable();
        inputTables = new LinkedList<>();
        usedColumns = new MyMap(false);
    }

    public void addUsedColumn(String c, String tableName){

        if(tableName == null) return;

        List<ColumnTypePair> usedColumnList = usedColumns.getColumnAndTypeList();

        if(usedColumnList.size() > 0){
            for(ColumnTypePair p : usedColumnList){
                if(p.getColumnName().equals(c)){
                    if(p.getColumnType().equals(tableName)){
                        return;
                    }
                }
            }
        }

        boolean existsAsInput = false;
        MyTable targetInput = new MyTable();
        for(MyTable inputT : inputTables){
            if(inputT.getTableName().toLowerCase().equals(tableName.toLowerCase())){
                existsAsInput = true;
                targetInput = inputT;
                break;
            }
        }

        if(existsAsInput == false){
            if( ( (c.contains("agg_")) && (c.contains("_col")) ) == false){
                System.out.println("addNewColumn: Attempt to add pair (" + c + " , " + tableName + ") fails because OpQuery does not have input table: " + tableName);
                return;
            }
            else{
                ColumnTypePair pair = new ColumnTypePair(c, tableName.toLowerCase());
                usedColumns.addPair(pair);
                System.out.println("addUsedColumn: ("+c+" , "+tableName+")");
                return;
            }
        }

        boolean colExistsInTarget = false;
        for(FieldSchema f : targetInput.getAllCols()){
            if(f.getName().equals(c)){
                colExistsInTarget = true;
                break;
            }
        }

        if(colExistsInTarget == false){ //Attempt auto correct
            for(MyTable inputT : inputTables){
                for(FieldSchema f : inputT.getAllCols()){
                    if(f.getName().equals(c)){
                        ColumnTypePair pair = new ColumnTypePair(c, inputT.getTableName().toLowerCase());
                        usedColumns.addPair(pair);
                        System.out.println("addUsedColumn: ("+c+" , "+tableName+")");
                    }
                }
            }
        }
        else{
            ColumnTypePair pair = new ColumnTypePair(c, tableName.toLowerCase());
            usedColumns.addPair(pair);
            System.out.println("addUsedColumn: ("+c+" , "+tableName+")");
        }


    }

    public MyMap getUsedColumns() { return usedColumns; }

    public String getAssignedContainer() { return assignedContainer; }

    public void setAssignedContainer(String c) { assignedContainer = c; }

    public String getDataBasePath() { return dataBasePath; }

    public String getExaremeOutputTableName() { return exaremeOutputTableName; }

    public String getLocalQueryString() { return localQueryString; }

    public String getExaremeQueryString() { return exaremeQueryString; }

    public MyTable getOutputTable() { return outputTable; }

    public void setOutputTable(MyTable out) { outputTable = out; }

    public List<MyTable> getInputTables() { return inputTables; }

    public void setDataBasePath(String p) { dataBasePath = p; }

    public void setLocalQueryString(String q) { localQueryString = q; }

    public void setExaremeOutputTableName(String s) { exaremeOutputTableName = s; }

    public void setExaremeQueryString(String e) { exaremeQueryString = e; }

    public void addInputTable(MyTable n){

        //if(inputTables.size() > 0){
        //    for(MyTable m : inputTables){
        //        if(m.getTableName().equals(n.getTableName())) {
        //            if(m.getBelongingDataBaseName().equals(n.getBelongingDataBaseName()))
        //            return;
        //        }
        //    }
        //}

        inputTables.add(n);

    }

    public void printOperatorQuery(PrintWriter outputFile){
        System.out.println("\t\t--------Operator Query--------");
        outputFile.println("\t\t--------Operator Query--------");
        outputFile.flush();
        System.out.println("\t\t\tDatabasePath: ["+dataBasePath+"]");
        outputFile.println("\t\t\tDatabasePath: ["+dataBasePath+"]");
        outputFile.flush();
        System.out.println("\t\t\tlocalQueryString: ["+localQueryString+"]");
        outputFile.println("\t\t\tlocalQueryString: ["+localQueryString+"]");
        outputFile.flush();
        System.out.println("\t\t\texaremeQueryString: ["+exaremeQueryString+"]");
        outputFile.println("\t\t\texaremeQueryString: ["+exaremeQueryString+"]");
        outputFile.flush();
        System.out.println("\t\t\tOutputTable(Name): "+outputTable.getTableName());
        outputFile.println("\t\t\tOutputTable(Name): "+outputTable.getTableName());
        outputFile.flush();
        List<String> inputNames = new LinkedList<>();
        for(MyTable i : inputTables){
            inputNames.add(i.getTableName());
        }
        System.out.println("\t\t\tInputTables(Names): "+inputNames.toString());
        outputFile.println("\t\t\tInputTables(Names): "+inputNames.toString());
        outputFile.flush();
        System.out.println();
        outputFile.println();
        outputFile.flush();
    }
}
