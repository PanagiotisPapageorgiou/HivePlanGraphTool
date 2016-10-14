package com.inmobi.hive.test;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by panos on 3/8/2016.
 */

/* This is a custom class that is used primarily for keeping track of the
   different tables used in the Exareme Graph and to help in translating
   the Exareme Graph into an Exareme Plan
 */

public class MyMap {
    List<ColumnTypePair> columnAndTypeList;
    boolean allowDuplicates = false;

    public MyMap(boolean allowDups){

        columnAndTypeList = new LinkedList<>();

        allowDuplicates = allowDups;

    }

    public List<ColumnTypePair> getColumnAndTypeList() { return columnAndTypeList; }

    public void setColumnAndTypeList(List<ColumnTypePair> l) { columnAndTypeList = l; }

    public void addPair(ColumnTypePair c){

        boolean exists = false;

        if(c.getColumnName().contains("BLOCK__OFFSET__INSIDE__FILE")){
            return;
        }
        else if(c.getColumnName().contains("INPUT__FILE__NAME")){
            return;
        }
        else if(c.getColumnName().contains("ROW__ID")){
            return;
        }
        else if(c.getColumnName().contains("rowid")){
            return;
        }
        else if(c.getColumnName().contains("bucketid")){
            return;
        }

        if(allowDuplicates == false) {
            for (ColumnTypePair pair : columnAndTypeList) {
                if (pair.getColumnName().equals(c.getColumnName())) {
                    if (pair.getColumnType().equals(c.getColumnType())) {
                        exists = true;
                        break;
                    }
                }
            }

            if (exists == true) {
                System.out.println("PAIR: (" + c.getColumnName() + " , " + c.getColumnType() + " already exists!");
                return;
            }

        }

        columnAndTypeList.add(c);
        System.out.println("PAIR: ("+c.getColumnName()+" , "+c.getColumnType()+" added successfully!");

    }

    public void printMap(){

        System.out.println("\t------------- columnAndTypeMap ---------------");
        int k = 0;
        for(ColumnTypePair c : columnAndTypeList){
            if(c.hasAlternateAlias() == false) {
                System.out.println("\t\tIndex: " + k + " - Column: " + c.getColumnName() + " - Entry: " + c.getColumnType()+" - AltAliases: NULL");
            }
            else{
                System.out.println("\t\tIndex: " + k + " - Column: " + c.getColumnName() + " - Entry: " + c.getColumnType()+" - AltAliases: ");
                List<StringParameter> altAliases = c.getAltAliasPairs();
                for(StringParameter sP : altAliases) {
                    System.out.println("\t\t\tAltAlias - Operator: "+sP.getParemeterType()+" - Column: "+sP.getValue());
                }
            }
            k++;
        }

    }

}
