package com.inmobi.hive.test;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by panos on 10/10/2016.
 */
public class UnionPoint { //Helps connect branches of Operator Trees who link at UNION
    String id;
    String createdById;
    List<String> latestAncestorTableNames = new LinkedList<>();
    OperatorQuery associatedQuery;
    int passesMade = 0;
    List<OperatorNode> otherFathers = new LinkedList<>();
    String unionPhrase = "";
    int numOfInputs = 0; //the Number of Branches this Union connects

    List<String> inputTableNames = new LinkedList<>();
    List<List<FieldSchema>> listOfUsedColsList = new LinkedList<>();

    public UnionPoint(String i, String createdBy, OperatorQuery q, String latest, OperatorNode father, String unionP, int numOfIn){
        id = i;
        createdById = createdBy;
        numOfInputs = numOfIn;
        otherFathers.add(father);
        associatedQuery = q;
        latestAncestorTableNames.add(latest);
        unionPhrase = unionP;
    }

    public void addInputTableAndUsedCols(String input, List<FieldSchema> usedCols){ //Duplicate input tables allowed in this case

        inputTableNames.add(input);

        listOfUsedColsList.add(usedCols);

        passesMade++;

    }

    public int getPassesMade() { return passesMade; }

    public void setAssociatedQuery(OperatorQuery q) { associatedQuery = q; }

    public List<List<FieldSchema>> getListOfUsedColsList() { return listOfUsedColsList; }

    public List<String> getListOfInputTableNames() { return inputTableNames; }

    public String getUnionPhrase() { return unionPhrase; }

    public String getId() { return id; }

    public String getCreatedById() { return createdById; }

    public OperatorQuery getAssociatedQuery() { return associatedQuery; }

    public List<String> getLatestAncestorTableNames() { return latestAncestorTableNames; }

    public List<OperatorNode> getOtherFatherNodes() { return otherFathers; }

    public int getNumOfInputs() { return numOfInputs; }
}
