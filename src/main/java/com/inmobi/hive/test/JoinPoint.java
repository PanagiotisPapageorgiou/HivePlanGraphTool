package com.inmobi.hive.test;

/**
 * Created by panos on 23/9/2016.
 */
public class JoinPoint { //Helps connect two branches of Operator Trees
    String id;
    String createdById;
    String latestAncestorTableName;
    OperatorQuery associatedOpQuery;
    OperatorNode otherFather;
    String joinPhrase = "";

    public JoinPoint(String i, String createdBy, OperatorQuery associatedQuery, String latest, OperatorNode father, String joinP){
        id = i;
        createdById = createdBy;
        associatedOpQuery = associatedQuery;
        latestAncestorTableName = latest;
        otherFather = father;
        joinPhrase = joinP;
    }

    public String getJoinPhrase() { return joinPhrase; }

    public String getId() { return id; }

    public String getCreatedById() { return createdById; }

    public OperatorQuery getAssociatedOpQuery() { return associatedOpQuery; }

    public String getLatestAncestorTableName() { return latestAncestorTableName; }

    public OperatorNode getOtherFatherNode() { return otherFather; }


}
