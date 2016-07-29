package com.inmobi.hive.test;

/**
 * Created by panos on 1/7/2016.
 */
public class NumParameter extends Parameter{
    int value;

    public NumParameter(String p, int v){
        super(p);
        value = v;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
