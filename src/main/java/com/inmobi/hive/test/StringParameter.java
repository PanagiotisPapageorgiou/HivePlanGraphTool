package com.inmobi.hive.test;

/**
 * Created by panos on 1/7/2016.
 */

/* Used to assist in printing the JSON file of the Exareme Plan  */

public class StringParameter extends Parameter{
    String value;

    public StringParameter(String p, String v){
        super(p);
        value = v;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
