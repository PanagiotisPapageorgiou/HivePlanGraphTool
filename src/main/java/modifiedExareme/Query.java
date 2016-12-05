/**
 * Copyright MaDgIK Group 2010 - 2015.
 */
package madgik.exareme.common.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author herald
 */
public class Query implements Serializable {
    private static final long serialVersionUID = 1L;
    private int id = -1;
    private String databaseDir = null;
    private String query = null;
    private String comments = null;
    private String mappings;

    private ArrayList<Integer> runOnParts = null;
    private List<Integer> runOnPartsView = null;
    private boolean sorted = false;

    public Query(int id, String query, String comments) {
        this.id = id;
        this.query = query;
        this.comments = comments;
        this.runOnParts = new ArrayList<Integer>();
        this.runOnPartsView = Collections.unmodifiableList(runOnParts);
    }

    public int getId() {
        return id;
    }

    public String getComments() {
        return comments;
    }

    public String getMappings() {
        return mappings;
    }

    public void setMappings(String mappings) {
        this.mappings = mappings;
    }

    public String getDatabaseDir() {
        return databaseDir;
    }

    public void setDatabaseDir(String databaseDir) {
        this.databaseDir = databaseDir;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void addRunOnPart(int pNum) {
        runOnParts.add(pNum);
        sorted = false;
    }

    /**
     * @return a sorted list with the partitions that are used.
     */
    public List<Integer> getRunOnParts() {
        if (sorted == false) {
            Collections.sort(runOnParts);
            sorted = true;
        }

        return runOnPartsView;
    }

    public void printQuery(){
        System.out.println("\t\t-------------------Query------------------");
        System.out.println("\t\t\tserialVersionUID: "+serialVersionUID);
        System.out.println("\t\t\tdatabaseDir: "+databaseDir);
        System.out.println("\t\t\tquery: "+query);
        System.out.println("\t\t\tcomments: "+comments);
        System.out.println("\t\t\tmappings: "+mappings);
        if(runOnParts != null){
            System.out.println("\t\t\trunOnParts: ");
            for(Integer i : runOnParts){
                System.out.println("\t\t\t\trunOnPart: "+i);
            }
        }
        else{
            System.out.println("\t\t\trunOnParts: NULL");
        }
        if(runOnPartsView != null){
            System.out.println("\t\t\trunOnPartsView: ");
            for(Integer i : runOnPartsView){
                System.out.println("\t\t\t\trunOnPartsView: "+i);
            }
        }
        else{
            System.out.println("\t\t\trunOnPartsView: NULL");
        }

        System.out.println("\t\t\tSorted: "+sorted);

    }

}
