package com.inmobi.hive.test;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by panos on 3/8/2016.
 */
public class ColumnTypePair {
    String columnName;
    String columnType;
    String alternateAlias;
    List<StringParameter> altAliasPairs;
    boolean hasAlt;

    public ColumnTypePair(String n, String t){
        columnName = n;
        columnType = t;
        hasAlt = false;
        altAliasPairs = new LinkedList<>();
    }

    public ColumnTypePair(String n, String t, String alt){
        columnName = n;
        columnType = t;
        alternateAlias = alt;
        hasAlt = true;
    }

    public void addAltAlias(String operator, String alias){
        if(hasAlt == false)
            hasAlt = true;

        boolean exists = false;
        if(altAliasPairs.size() > 0) {
            for (StringParameter sP : altAliasPairs) {
                if (sP.getParemeterType().equals(operator)) {
                    if (sP.getValue().equals(alias)) {
                        exists = true;
                        break;
                    }
                }
            }
        }

        if(exists == false){
            StringParameter sP = new StringParameter(operator, alias);
            altAliasPairs.add(sP);
        }
    }

    public List<StringParameter> getAltAliasPairs() { return altAliasPairs; }

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
