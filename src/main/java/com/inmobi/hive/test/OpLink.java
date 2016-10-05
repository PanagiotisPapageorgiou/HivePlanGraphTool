package com.inmobi.hive.test;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by panos on 1/7/2016.
 */

/*----Class to help print an OperatorLink for the Exareme Plan----*/

public class OpLink {
    String containerName;
    String fromTable;
    String toTable;
    List<Parameter> parameters;
    List<OpLink> brothers = new LinkedList<>(); //Extra OpLinks created because an input Table has multiple partitions

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

    public List<OpLink> getBrothers() { return brothers; }

    public void addBrother(OpLink brother ) {

        if(brothers.size() > 0){
            for(OpLink op : brothers){
                if(op.getFromTable().equals(brother.getFromTable())){
                    if(op.getToTable().equals(brother.getToTable())){
                        return;
                    }
                }
            }
        }

        brothers.add(brother);
    }

}
