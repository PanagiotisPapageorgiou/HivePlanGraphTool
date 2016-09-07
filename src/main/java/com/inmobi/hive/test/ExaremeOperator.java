package com.inmobi.hive.test;

import java.util.List;

import madgik.exareme.common.app.engine.AdpDBOperatorType;
import madgik.exareme.common.app.engine.AdpDBSelectOperator;
import madgik.exareme.common.schema.Select;
import madgik.exareme.common.schema.Table;
import madgik.exareme.common.schema.TableView;
import madgik.exareme.common.schema.expression.SQLSelect;
import madgik.exareme.utils.encoding.Base64Util;

/**
 * Created by panos on 1/7/2016.
 */
public class ExaremeOperator {
    String containerName;
    String operatorName;
    String resultsName;
    String queryString;
    List<Parameter> parameters;
    List<String> inputTables;
    List<String> outputTables;
    AdpDBSelectOperator trueExaremeOperator;

    public ExaremeOperator(){
        containerName = null;
        operatorName = null;
        resultsName = null;
        queryString = null;
        parameters = null;
        trueExaremeOperator = null;
    }

    public ExaremeOperator(String containerName, String opName, String rName, AdpDBSelectOperator exaSelectOp, List<Parameter> pList, int serialNumber){
        this.containerName = containerName;
        this.operatorName = opName;
        this.resultsName = rName;
        this.parameters = pList;

        try {
            queryString = "{"+Base64Util.encodeBase64(exaSelectOp)+"};";
        }
        catch(java.io.IOException ex){
           System.out.println("Failed to convert to true Exareme Operator!");
            System.exit(0);
        }

    }

    public List<String> getOutputTables() { return outputTables; }

    public List<String> getInputTables() { return inputTables; }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public String getContainerName() {
        return containerName;
    }

    public void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    public String getOperatorName() {
        return operatorName;
    }

    public String getQueryString() {
        return queryString;
    }

    public String getResultsName() {
        return resultsName;
    }

    public void setOperatorName(String operatorName) {
        this.operatorName = operatorName;
    }

    public void setResultsName(String resultsName) {
        this.resultsName = resultsName;
    }

    public void setParameters(List<Parameter> parameters) {
        this.parameters = parameters;
    }

    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

}
