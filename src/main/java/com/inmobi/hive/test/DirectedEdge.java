package com.inmobi.hive.test;

/**
 * Created by panos on 21/4/2016.
 */
public class DirectedEdge {

    String label;
    String from, to;

    public DirectedEdge(String f, String t, String l){
        from = f;
        to = t;
        label = l;
    }

    public String getLabel(){
        return label;
    }

    public String getFromVertex(){
        return from;
    }

    public String getToVertex(){
        return to;
    }

    public boolean isEqualTo(DirectedEdge edge){

            if(getFromVertex().equals(edge.getFromVertex())){
                if(getToVertex().equals(edge.getToVertex())){
                    return true;
                }
                else return false;
            }
            else return false;
    }

}
