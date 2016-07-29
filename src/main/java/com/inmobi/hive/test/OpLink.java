package com.inmobi.hive.test;

import java.util.List;

/**
 * Created by panos on 1/7/2016.
 */
public class OpLink {
    String containerName;
    String fromTable;
    String toTable;
    List<Parameter> parameters;

    public OpLink(){
        containerName = null;
        fromTable = null;
        toTable = null;
        parameters = null;
    }

    public OpLink(String c, String f, String t, List<Parameter> pList){
        containerName = c;
        fromTable = f;
        toTable = t;
        parameters = pList;
    }

    public void setParameters(List<Parameter> parameters) {
        this.parameters = parameters;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public String getContainerName() {
        return containerName;
    }

    public String getFromTable() {
        return fromTable;
    }

    public String getToTable() {
        return toTable;
    }

    public void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    public void setFromTable(String fromTable) {
        this.fromTable = fromTable;
    }

    public void setToTable(String toTable) {
        this.toTable = toTable;
    }

}
