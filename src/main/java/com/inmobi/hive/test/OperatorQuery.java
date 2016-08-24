package com.inmobi.hive.test;

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
    List<String> outputTableNames;
    List<String> inputTableNames;

    public OperatorQuery() {
        dataBasePath = "/home/panos/tpcds";
        localQueryString = "";
        exaremeQueryString = "";
        exaremeOutputTableName = "";
        outputTableNames = new LinkedList<>();
        inputTableNames = new LinkedList<>();
    }

    public String getDataBasePath() { return dataBasePath; }

    public String getExaremeOutputTableName() { return exaremeOutputTableName; }

    public String getLocalQueryString() { return localQueryString; }

    public String getExaremeQueryString() { return exaremeQueryString; }

    public List<String> getOutputTableNames() { return outputTableNames; }

    public List<String> getInputTableNames() { return inputTableNames; }

    public void setDataBasePath(String p) { dataBasePath = p; }

    public void setLocalQueryString(String q) { localQueryString = q; }

    public void setExaremeOutputTableName(String s) { exaremeOutputTableName = s; }

    public void setExaremeQueryString(String e) { exaremeQueryString = e; }

    public void setOutputTableNames(List<String> l) { outputTableNames = l; }

    public void setInputTableNames(List<String> l) { inputTableNames = l; }

    public void addOutputTable(String o){

        if(outputTableNames.size() > 0){
            for(String s : outputTableNames){
                if(s.equals(o)) return;
            }
        }

        outputTableNames.add(o);

    }

    public void addInputTable(String o){

        if(inputTableNames.size() > 0){
            for(String s : inputTableNames){
                if(s.equals(o)) return;
            }
        }

        inputTableNames.add(o);

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
        System.out.println("\t\t\tOutputTableNames: "+outputTableNames.toString());
        outputFile.println("\t\t\tOutputTableNames: "+outputTableNames.toString());
        outputFile.flush();
        System.out.println("\t\t\tInputTableNames: "+inputTableNames.toString());
        outputFile.println("\t\t\tInputTableNames: "+inputTableNames.toString());
        outputFile.flush();
        System.out.println();
        outputFile.println();
        outputFile.flush();
    }
}
