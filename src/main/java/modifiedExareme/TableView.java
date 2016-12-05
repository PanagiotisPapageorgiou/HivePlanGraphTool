package madgik.exareme.common.schema;


import madgik.exareme.common.schema.expression.DataPattern;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @author herald
 */
public class TableView implements Serializable {
    private static final long serialVersionUID = 1L;

    private Table table = null;
    private DataPattern pattern = null;
    private ArrayList<String> patternColumnNames = null;
    private ArrayList<String> usedColumns = null;
    private int numOfPartitions = -1;

    //Added for Hive Tables/Partitions
    private boolean isHiveTable = false;
    private String hiveTableLocation = null;
    String hiveCreateTableQuery = "";
    List<String> hiveSecondaryNeededQueries = new LinkedList<>();

    public TableView(Table table, String hiveLocation, String hiveCreateQuery, List<String> secondaryQueries){
        isHiveTable = true;
        hiveTableLocation = hiveLocation;
        hiveCreateTableQuery = hiveCreateQuery;

        if(secondaryQueries != null)
            hiveSecondaryNeededQueries = secondaryQueries;

        this.table = table;
        this.usedColumns = new ArrayList<String>();
        this.patternColumnNames = new ArrayList<String>();
    }

    public TableView(Table table) {
        this.table = table;
        this.usedColumns = new ArrayList<String>();
        this.patternColumnNames = new ArrayList<String>();
    }

    /*public void fixDuplicateUsedColumns(){

        if(usedColumns.size() > 0){

            ArrayList<String> newColsList = new ArrayList<>();
            List<String> alreadyCheckedList = new LinkedList<>();

            for(String s : usedColumns){
                int countDuplicates = 0;
                for(String s2 : usedColumns){
                    boolean alreadyChecked = false;
                    for(String s3 : alreadyCheckedList){
                        if(s3.equals(s2)){
                            alreadyChecked = true;
                            break;
                        }
                    }

                    if(alreadyChecked == true) continue;

                    if(s.equals(s2)){
                        countDuplicates++;
                    }

                }

                if(countDuplicates > 1){

                }
                else{
                    newColsList.add(s);
                }

            }

        }

    }*/

    public void setUsedColumns(ArrayList newUsedColsList) { usedColumns = newUsedColsList; }

    public List<String> getHiveSecondaryNeededQueries() { return hiveSecondaryNeededQueries; }

    public boolean getIsHiveTable() { return isHiveTable; }

    public String getHiveCreateTableQuery() { return hiveCreateTableQuery; }

    public String getHiveTableLocation() { return hiveTableLocation; }

    public Table getTable() {
        return table;
    }

    public String getName() {
        return table.getName();
    }

    public DataPattern getPattern() {
        return pattern;
    }

    public void setPattern(DataPattern pattern) {
        this.pattern = pattern;
    }

    public void addUsedColumn(String column) {
        usedColumns.add(column);
    }

    public void addPatternColumn(String column) {
        patternColumnNames.add(column);
    }

    public List<String> getPatternColumnNames() {
        return Collections.unmodifiableList(patternColumnNames);
    }

    public List<String> getUsedColumnNames() {
        return Collections.unmodifiableList(usedColumns);
    }

    public int getNumOfPartitions() {
        return numOfPartitions;
    }

    public void setNumOfPartitions(int numOfPartitions) {
        this.numOfPartitions = numOfPartitions;
    }

    @Override public String toString() {
        return "Partitions: " + numOfPartitions;
    }
}
