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
            outputFile.println("\t\t\t\"value\": " + "\"" + value + "\"\n");
            outputFile.flush();
            outputFile.println("\t\t}");
            outputFile.flush();
        }
        else{
            outputFile.println("\n\t\t{\n");
            outputFile.flush();
            outputFile.println("\t\t\t\"name\": " + "\"" + name + "\",\n");
            outputFile.flush();
            outputFile.println("\t\t\t\"value\": " + "\"" + value + "\"\n");
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

    public void printExaremePlan(PrintWriter outputFile) {

        outputFile.println("{\n");
        outputFile.flush();

        //Containers
            outputFile.println("\t\"containers:\": [\n");
            outputFile.flush();

            for(Container c : containers){
                outputFile.println("\t\t{\n");
                outputFile.flush();

                    outputFile.println("\t\t\t\"name\": \""+c.getName()+"\",\n");
                    outputFile.flush();
                    outputFile.println("\t\t\t\"IP\": \""+c.getIP()+"\",\n");
                    outputFile.flush();
                    outputFile.println("\t\t\t\"port\": \""+c.getPort()+"\",\n");
                    outputFile.flush();
                    outputFile.println("\t\t\t\"data_transfer_port\": \""+c.getData_transfer_port()+"\"\n");
                    outputFile.flush();

                outputFile.println("\t\t}\n");
                outputFile.flush();
            }

            outputFile.println("\t],\n");
            outputFile.flush();

        //Operators
            outputFile.println("\t\"operators:\": [\n");
            outputFile.flush();

            int i = 0;
            for(ExaremeOperator e : operators){
                outputFile.println("\t\t{\n");
                outputFile.flush();

                    outputFile.println("\t\t\t\"container\": \""+e.getContainerName()+"\",\n");
                    outputFile.flush();
                    outputFile.println("\t\t\t\"operator\": \""+e.getOperatorName()+"\",\n");
                    outputFile.flush();
                    outputFile.println("\t\t\t\"name\": \""+e.getResultsName()+"\",\n");
                    outputFile.flush();
                    outputFile.println("\t\t\t\"queryString\": \""+e.getQueryString()+"\",\n");
                    outputFile.flush();

                    outputFile.println("\t\t\t\"parameters\": [\n");
                    outputFile.flush();

                    //Print parameters
                    int j = 0;
                    for(Parameter p : e.getParameters()){
                        outputFile.println("\t\t\t\t[\n");
                        outputFile.flush();

                        if(p instanceof NumParameter){
                            NumParameter nP = (NumParameter) p;
                            outputFile.println("\t\t\t\t\t\""+nP.getParemeterType()+"\",\n");
                            outputFile.flush();
                            outputFile.println("\t\t\t\t\t\""+nP.getValue()+"\"\n");
                            outputFile.flush();
                        }
                        else{
                            StringParameter sP = (StringParameter) p;
                            outputFile.println("\t\t\t\t\t\""+sP.getParemeterType()+"\",\n");
                            outputFile.flush();
                            outputFile.println("\t\t\t\t\t\""+sP.getValue()+"\"\n");
                            outputFile.flush();
                        }

                        if(j == e.getParameters().size() - 1)
                            outputFile.println("\t\t\t\t]\n");
                        else
                            outputFile.println("\t\t\t\t],\n");
                        outputFile.flush();
                        j++;
                    }

                    outputFile.println("\t\t\t]\n");
                    outputFile.flush();

                if(i == operators.size() - 1)
                    outputFile.println("\t\t}\n");
                else
                    outputFile.println("\t\t},\n");
                outputFile.flush();
                i++;
            }

            outputFile.println("\t],\n");
            outputFile.flush();

        //Op_Links
            outputFile.println("\t\"op_links:\": [\n");
            outputFile.flush();

            int k = 0;
            for(OpLink o : opLinks){
                outputFile.println("\t\t{\n");
                outputFile.flush();

                    outputFile.println("\t\t\t\"container\": \""+o.getContainerName()+"\",\n");
                    outputFile.flush();
                    outputFile.println("\t\t\t\"from\": \""+o.getFromTable()+"\",\n");
                    outputFile.flush();
                    outputFile.println("\t\t\t\"to\": \""+o.getToTable()+"\",\n");
                    outputFile.flush();

                    outputFile.println("\t\t\t\"parameters\": [\n");
                    outputFile.flush();

                    //Print parameters
                    int j = 0;
                    for(Parameter p : o.getParameters()){
                        outputFile.println("\t\t\t\t[\n");
                        outputFile.flush();

                        if(p instanceof NumParameter){
                            NumParameter nP = (NumParameter) p;
                            outputFile.println("\t\t\t\t\t\""+nP.getParemeterType()+"\",\n");
                            outputFile.flush();
                            outputFile.println("\t\t\t\t\t\""+nP.getValue()+"\"\n");
                            outputFile.flush();
                        }
                        else{
                            StringParameter sP = (StringParameter) p;
                            outputFile.println("\t\t\t\t\t\""+sP.getParemeterType()+"\",\n");
                            outputFile.flush();
                            outputFile.println("\t\t\t\t\t\""+sP.getValue()+"\"\n");
                            outputFile.flush();
                        }

                        if(j == o.getParameters().size() - 1)
                            outputFile.println("\t\t\t\t]\n");
                        else
                            outputFile.println("\t\t\t\t],\n");
                        outputFile.flush();
                        j++;
                    }

                    outputFile.println("\t\t\t]\n");
                    outputFile.flush();


                if(k == opLinks.size() - 1)
                    outputFile.println("\t\t}\n");
                else
                    outputFile.println("\t\t},\n");
                outputFile.flush();
                k++;
            }

            outputFile.println("\t],\n");
            outputFile.flush();

        //Pragma
        outputPragma(outputFile);

        outputFile.println("}");
        outputFile.flush();
    }

}
