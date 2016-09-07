package com.inmobi.hive.test;

import madgik.exareme.common.app.engine.AdpDBSelectOperator;

import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by panos on 17/8/2016.
 */
public class OperatorQuery {
    String dataBasePath;
    String localQueryString;
    String exaremeQueryString;
    String exaremeOutputTableName;
    String assignedContainer;
    MyTable outputTable;
    List<String> usedColumns;
    List<MyTable> inputTables;

    public OperatorQuery() {
        assignedContainer = "";
        dataBasePath = "";
        localQueryString = "";
        exaremeQueryString = "";
        exaremeOutputTableName = "";
        outputTable = new MyTable();
        inputTables = new LinkedList<>();
        usedColumns = new LinkedList<>();
    }

    public void addUsedColumn(String c){

        if(usedColumns.size() > 0){
            for(String s : usedColumns){
                if(s.equals(c)){
                    return;
                }
            }
        }

        usedColumns.add(c);

    }

    public List<String> getUsedColumns() { return usedColumns; }

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

        if(inputTables.size() > 0){
            for(MyTable m : inputTables){
                if(m.getTableName().equals(n.getTableName())) {
                    if(m.getBelongingDataBaseName().equals(n.getBelongingDataBaseName()))
                    return;
                }
            }
        }

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
        outputFile.println("			OutputTable(Name): "+outputTable.getTableName());
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
