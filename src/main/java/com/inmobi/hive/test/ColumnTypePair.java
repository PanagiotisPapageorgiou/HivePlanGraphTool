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
    List<String> parameterValues = new LinkedList<>();
    List<String> parameterTypes = new LinkedList<>();
    List<StringParameter> altAliasPairs = new LinkedList<>();
    boolean hasAlt;

    List<String> altCastColumnTypes = new LinkedList<>(); //Types given after cast

    public ColumnTypePair(String n, String t){
        columnName = n;
        columnType = t.toLowerCase();
        hasAlt = false;
        altAliasPairs = new LinkedList<>();
    }

    public ColumnTypePair(String n, String t, String alt){
        columnName = n;
        columnType = t.toLowerCase();
        alternateAlias = alt;
        hasAlt = true;
    }

    public void setParameterValueAndType(List<String> v, List<String> t){
        parameterValues = v;
        parameterTypes = t;
    }

    public List<String> getParameterValues(){
        return parameterValues;
    }

    public List<String> getParameterTypes(){
        return parameterTypes;
    }

    public void addAltAlias(String operator, String alias, boolean extraValFlag){
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
            if(extraValFlag){
                sP = new StringParameter(operator, alias, "agg_"+operator+"_"+alias);
            }
            altAliasPairs.add(sP);
        }

    }

    public String getLatestAltCastType(){

        String returnType = getColumnType();

        if(this.getAltCastColumnTypes().size() > 0){
            int i = 0;
            for(String t : this.getAltCastColumnTypes()){
                if(i == this.getAltCastColumnTypes().size() - 1){
                    return t;
                }
                i++;
            }
        }

        return returnType;

    }

    public boolean hasLatestAltCastType(String type){

        if(this.getAltCastColumnTypes().size() > 0){
            int i = 0;
            for(String t : this.getAltCastColumnTypes()){
                if(i == this.getAltCastColumnTypes().size() - 1){
                    if(t.equals(type)){
                        return true;
                    }
                }
                i++;
            }
        }

        return false;
    }

    public void modifyAltAlias(String operator, String oldAlias, String newAlias, boolean extraValFlag){

        boolean exists = false;
        if(altAliasPairs.size() > 0) {
            for (StringParameter sP : altAliasPairs) {
                if (sP.getParemeterType().equals(operator)) {
                    if (sP.getValue().equals(oldAlias)) {
                        sP.setValue(newAlias);
                        if(extraValFlag){
                            sP.setExtraValue("agg_"+operator+"_"+newAlias);
                        }
                        exists = true;
                        break;
                    }
                }
            }
        }

        if(exists == false){
            System.out.println("modifyAltAlias: Can't locate Pair( "+operator+ ", "+oldAlias+" )");
            System.exit(0);
        }

    }

    public void addCastType(String t){

        for(String c : altCastColumnTypes){
            if(c.equals(t)) return;
        }

        altCastColumnTypes.add(t);

    }

    public void duplicateAndmodifyAltAlias(String operator, String oldAlias, String newAlias, boolean extraValFlag){

        String oldValue = "";

        boolean exists = false;
        if(altAliasPairs.size() > 0) {
            for (StringParameter sP : altAliasPairs) {
                if (sP.getParemeterType().equals(operator)) {
                    if (sP.getValue().equals(oldAlias)) {
                        oldValue = oldAlias;
                        sP.setValue(newAlias);
                        if(extraValFlag){
                            sP.setExtraValue("agg_"+operator+"_"+newAlias);
                        }
                        exists = true;
                        break;
                    }
                }
            }
        }

        StringParameter extraModification = new StringParameter(operator, oldValue);
        extraModification.setExtraValue("agg_"+operator+"_"+oldValue);

        altAliasPairs.add(extraModification);

        if(exists == false){
            System.out.println("modifyAltAlias: Can't locate Pair( "+operator+ ", "+oldAlias+" )");
            System.exit(0);
        }

    }

    public List<String> getAltCastColumnTypes() { return altCastColumnTypes; }

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
