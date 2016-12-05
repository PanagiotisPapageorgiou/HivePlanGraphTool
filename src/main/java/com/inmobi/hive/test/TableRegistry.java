package com.inmobi.hive.test;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by panos on 8/10/2016.
 */
public class TableRegistry {
    List<TableRegEntry> entries = new LinkedList<>();

    public void addEntry(TableRegEntry entry, String operatorName){

        if(entries.size() > 0){
            for(TableRegEntry regEntry : entries){
                if(regEntry.getAlias().equals(entry.getAlias())){ //Entry with same alias name exists
                    if(regEntry.getAssociatedTable().getTableName().equals(entry.getAssociatedTable().getTableName())){ //Entry with same alias and input table
                        MyMap mapOfEntry = regEntry.getColumnTypeMap();

                        //Traverse through the columnTypePair Map of this entry and find if this operator name already exists
                        ColumnTypePair pair  = mapOfEntry.getColumnAndTypeList().get(0);
                        List<StringParameter> alternateAliases = pair.getAltAliasPairs();

                        if(alternateAliases == null){
                            System.out.println("addEntry: Alternate aliases should never be null!");
                            System.exit(0);
                        }
                        else if(alternateAliases.size() == 0){
                            System.out.println("addEntry: Alternate aliases should never be empty!");
                            System.exit(0);
                        }

                        for(StringParameter aliasPair : alternateAliases){
                            if(aliasPair.getParemeterType().equals(operatorName)){
                                System.out.println("addEntry: TableScanOperator: "+operatorName+" has been accessed before...returning!");
                                return;
                            }
                        }

                        System.out.println("addEntry: TableScanOperator: "+operatorName+" is a new ROOT Operator using Table: "+regEntry.getAlias()+" ! Adding as altAlias in all columns...");
                        for(ColumnTypePair somePair : mapOfEntry.getColumnAndTypeList()){
                            somePair.addAltAlias(operatorName, somePair.getColumnName(), false);
                        }

                        return;
                    }
                    else{
                        System.out.println("addEntry: TableRegEntry with alias= "+entry.getAlias()+" already exists for different Input Table! Error! Unsupported!");
                        System.exit(0);
                    }
                }
            }
        }

        //New RegistryEntry
        List<ColumnTypePair> entryMapList = entry.getColumnTypeMap().getColumnAndTypeList();

        for(ColumnTypePair pair : entryMapList){
            pair.addAltAlias(operatorName, pair.getColumnName(), false);
        }

        entries.add(entry);

        System.out.println("addEntry: TableRegEntry with alias= "+entry.getAlias()+" added to Registry! First TableScan to access it: "+operatorName);

    }

    public MyTable fetchTableByAlias(String tableAlias){

        MyTable wantedTable = new MyTable();
        boolean tableFound = false;

        if(entries.size() > 0){
            for(TableRegEntry regEntry : entries)   {
                if(regEntry.getAlias().equals(tableAlias)){
                    wantedTable = regEntry.getAssociatedTable();
                    tableFound = true;
                    break;
                }
            }
        }

        if(tableFound == false){
            System.out.println("addEntry: Table with alias= "+tableAlias+" was not found!");
            System.exit(0);
        }

        return wantedTable;

    }

    public void printRegistry(){

        if(entries.size() > 0) {
            System.out.println("\n-----------------------TABLE REGISTRY-----------------------\n");
            for(TableRegEntry entry : entries){
                System.out.println("\tTableRegEntry: ");
                System.out.println("\t\tAlias: "+entry.getAlias());
                System.out.println("\t\tReal TableName: "+entry.getAssociatedTable().getTableName());
                System.out.println("\t\tAssociated Map: ");
                entry.getColumnTypeMap().printMap();
                System.out.println();
            }
        }
        else{
            System.out.println("Table Registry is empty! Nothing to print!");
        }

    }

    public List<TableRegEntry> getEntries() { return entries; }

}
