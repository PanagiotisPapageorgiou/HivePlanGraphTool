package com.inmobi.hive.test;

import java.io.PrintWriter;
import java.util.List;

/**
 * Created by panos on 1/7/2016.
 */
public class ExaremePlan {
    List<Container> containers;
    List<ExaremeOperator> operators;
    List<OpLink> opLinks;

    public ExaremePlan(List<Container> cList, List<ExaremeOperator> eList, List<OpLink> oList){
        containers = cList;
        operators = eList;
        opLinks = oList;
    }

    public List<Container> getContainers() {
        return containers;
    }

    public List<ExaremeOperator> getOperators() {
        return operators;
    }

    public List<OpLink> getOpLinks() {
        return opLinks;
    }

    public void setContainers(List<Container> containers) {
        this.containers = containers;
    }

    public void setOperators(List<ExaremeOperator> operators) {
        this.operators = operators;
    }

    public void setOpLinks(List<OpLink> opLinks) {
        this.opLinks = opLinks;
    }

    public void printPragmaSubObject(PrintWriter outputFile, String name, String value,boolean finalPrint){

        if(finalPrint == true) {
            outputFile.println("\n\t\t{\n");
            outputFile.flush();
            outputFile.println("\t\t\t\"name\": " + "\"" + name + "\",\n");
            outputFile.flush();
            outputFile.println("\t\t\t\"value\": " + "\"" + value + "\",\n");
            outputFile.flush();
            outputFile.println("\t\t}");
            outputFile.flush();
        }
        else{
            outputFile.println("\n\t\t{\n");
            outputFile.flush();
            outputFile.println("\t\t\t\"name\": " + "\"" + name + "\",\n");
            outputFile.flush();
            outputFile.println("\t\t\t\"value\": " + "\"" + value + "\",\n");
            outputFile.flush();
            outputFile.println("\t\t},");
            outputFile.flush();
        }
    }

    public void outputPragma(PrintWriter outputFile){ //Output Pragma Section

        outputFile.println();
        outputFile.flush();
        outputFile.println("\t"+"\"pragma\": [");
        outputFile.flush();

        printPragmaSubObject(outputFile, "materialized_reader", "madgik.exareme.master.engine.executor.remote.operator.data.MaterializedReader", false);

        printPragmaSubObject(outputFile, "materialized_writer", "madgik.exareme.master.engine.executor.remote.operator.data.MaterializedWriter", false);

        printPragmaSubObject(outputFile, "inter_container_mediator_from", "madgik.exareme.master.engine.executor.remote.operator.data.InterContainerMediatorFrom", false);

        printPragmaSubObject(outputFile, "inter_container_mediator_to", "madgik.exareme.master.engine.executor.remote.operator.data.InterContainerMediatorTo", false);

        printPragmaSubObject(outputFile, "inter_container_data_transfer", "madgik.exareme.master.engine.executor.remote.operator.data.DataTransferRegister", true);

        outputFile.println("\n\t]\n");
        outputFile.flush();

    }

    /*public void printExaremePlan(PrintWriter outputFile){
        outputFile.println("[\n");
        if(containers != null){
            outputFile.println("\t\"containers\": [\n");
            for(Container c : containers){
                if(c != null){
                    c.printContainer();
                    outputFile.println();
                }
            }
        }
        outputFile.println("]\n");
    }*/

}
