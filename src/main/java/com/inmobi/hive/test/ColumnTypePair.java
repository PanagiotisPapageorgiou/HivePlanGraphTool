package com.inmobi.hive.test;

/**
 * Created by panos on 3/8/2016.
 */
public class ColumnTypePair {
    String columnName;
    String columnType;
    String alternateAlias;
    boolean hasAlt;

    public ColumnTypePair(String n, String t){
        columnName = n;
        columnType = t;
        hasAlt = false;
    }

    public ColumnTypePair(String n, String t, String alt){
        columnName = n;
        columnType = t;
        alternateAlias = alt;
        hasAlt = true;
    }

    public boolean hasAlternateAlias(){
        return hasAlt;
    }

    public String getAlternateAlias() { return alternateAlias; }

    public void setAlternateAlias(String alt){
        alternateAlias = alt;
    }

    public String getColumnName(){
        return columnName;
    }

    public String getColumnType(){
        return columnType;
    }

    public void setColumnName(String c){
        columnName = c;
    }

    public void setColumnType(String t){
        columnType = t;
    }

    public boolean equalsColumnTypePair(ColumnTypePair other){
        if(columnName.equals(other.getColumnName())){
            if(columnType.equals(other.getColumnType())){
                return true;
            }
        }

        return false;
    }
}
