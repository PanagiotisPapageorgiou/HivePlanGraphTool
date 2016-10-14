package com.inmobi.hive.test;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

/**
 * Created by panos on 8/10/2016.
 */
public class TableRegEntry {
    String aliasOfTable;
    MyTable associatedInputTable;
    MyMap columnTypeMap;

    public TableRegEntry(String alias, MyTable inputT){
        aliasOfTable = alias;
        associatedInputTable = inputT;
        columnTypeMap = new MyMap(false);

        for(FieldSchema f : inputT.getAllCols()){
            ColumnTypePair pair = new ColumnTypePair(f.getName(), f.getType());
            columnTypeMap.addPair(pair);
        }

    }

    public String getAlias() { return aliasOfTable; }

    public MyTable getAssociatedTable() { return associatedInputTable; }

    public MyMap getColumnTypeMap() { return columnTypeMap; }

}
