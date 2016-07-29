package com.inmobi.hive.test;

import java.util.List;

/**
 * Created by panos on 1/7/2016.
 */
public class ExaremeOperator {
    String containerName;
    String operatorName;
    String resultsName;
    List<String> inputTableNames;
    List<String> outputTableNames;
    String queryString;
    List<Parameter> parameters;

    public ExaremeOperator(){
        containerName = null;
        operatorName = null;
        resultsName = null;
        inputTableNames = null;
        outputTableNames = null;
        queryString = null;
        parameters = null;
    }

    public ExaremeOperator(String containerName, String opName, String rName, List<String> input, List<String> output, String queryString, List<Parameter> pList){
        this.containerName = containerName;
        this.operatorName = opName;
        this.resultsName = rName;
        this.inputTableNames = input;
        this.outputTableNames = output;
        this.queryString = queryString;
        this.parameters = pList;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public List<String> getInputTableNames() {
        return inputTableNames;
    }

    public List<String> getOutputTableNames() {
        return outputTableNames;
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

    public void setInputTableNames(List<String> inputTableNames) {
        this.inputTableNames = inputTableNames;
    }

    public void setOperatorName(String operatorName) {
        this.operatorName = operatorName;
    }

    public void setOutputTableNames(List<String> outputTableNames) {
        this.outputTableNames = outputTableNames;
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
