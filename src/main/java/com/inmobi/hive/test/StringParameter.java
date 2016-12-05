package com.inmobi.hive.test;

/**
 * Created by panos on 1/7/2016.
 */

/* Used to assist in printing the JSON file of the Exareme Plan  */

public class StringParameter extends Parameter{
    String value;
    String extraValue = "";
    String castExpr = "";
    boolean hasCastExpr = false;

    public StringParameter(String p, String v){
        super(p);
        value = v;
    }

    public StringParameter(String p, String v, String extraV){
        super(p);
        value = v;
        extraValue = extraV;
    }

    public boolean getHasCastExpr() { return hasCastExpr; }

    public String getCastExpr() { return castExpr; }

    public void setCastExpr(String c) {
        hasCastExpr = true;
        castExpr = c;
    }

    public String getExtraValue() {
        return extraValue;
    }

    public void setExtraValue(String value) {
        this.extraValue = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
