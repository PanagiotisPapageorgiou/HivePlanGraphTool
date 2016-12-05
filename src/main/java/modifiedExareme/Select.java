/**
 * Copyright MaDgIK Group 2010 - 2015.
 */
package madgik.exareme.common.schema;

import madgik.exareme.common.schema.expression.DataPattern;
import madgik.exareme.common.schema.expression.SQLSelect;

import java.util.*;

/**
 * @author herald
 * @author Christoforos Svingos
 */
public class Select extends Query {
    private static final long serialVersionUID = 1L;

    private String comments = null;
    private TableView outputTable = null;
    private ArrayList<TableView> inputTables = null;
    private HashMap<String, TableView> inputTablesMap = null;

    private SQLSelect parsedSqlQuery = null;
    private List<String> queryStatements = null;

    public Select(int id, SQLSelect sqlQuery, TableView outputTable) {
        super(id, sqlQuery.getSql(), sqlQuery.getComments().toString());
        this.outputTable = outputTable;
        this.parsedSqlQuery = sqlQuery;
        this.comments = parsedSqlQuery.getComments().toString();

        this.inputTables = new ArrayList<>();
        this.inputTablesMap = new HashMap<>();
        this.queryStatements = new ArrayList<>();
    }

    public void addInput(TableView input) {
        inputTables.add(input);
        inputTablesMap.put(input.getName(), input);
    }

    public void addQueryStatement(String query) {
        this.queryStatements.add(query);
    }

    public void clearQueryStatement() {
        this.queryStatements.clear();
    }

    public List<String> getQueryStatements() {
        return this.queryStatements;
    }

    public String getSelectQueryStatement() {
        String query;
        if (this.queryStatements.size() == 0) {
            query = getQuery();
        } else {
            query = this.queryStatements.get(this.queryStatements.size() - 1);
        }

        return query;
    }

    public TableView getOutputTable() {
        return outputTable;
    }

    public void setOutputTable(TableView outputTable) {
        this.outputTable = outputTable;
    }

    public List<TableView> getInputTables() {
        return Collections.unmodifiableList(inputTables);
    }

    public void clearInputTables() {
        inputTables.clear();
        inputTablesMap.clear();
    }

    public SQLSelect getParsedSqlQuery() {
        return parsedSqlQuery;
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Database   : " + getDatabaseDir() + "\n");
        if (outputTable != null) {
            sb.append("Output     : " + outputTable.toString() + "\n");
        } else {
            sb.append("Output     : --\n");
        }
        sb.append("Inputs     : " + inputTables.size() + "\n");
        for (TableView in : inputTables) {
            sb.append(" -> : " + in.toString() + "\n");
        }
        if (comments != null) {
            sb.append(comments);
        }
        sb.append(getQuery() + ";");

        return sb.toString();
    }

    public void print(){
        System.out.println("\t----------------Select--------------");
        System.out.println("\tserialVersionUID: "+serialVersionUID);
        if(comments != null){
            System.out.println("\tcomments: "+comments);
        }
        else{
            System.out.println("\tcomments: is null");
        }
        if(outputTable != null){
            System.out.println("\tOutput TableView:");
            System.out.println("\t\tName: " + outputTable.getName());
            System.out.println("\t\tNumOfPartitions: " + outputTable.getNumOfPartitions());
            System.out.println("\t\tisHiveTable: " + outputTable.getIsHiveTable());
            if(outputTable.getIsHiveTable() == true){
                System.out.println("\t\tHiveTableLocation: " + outputTable.getHiveTableLocation());
            }
            DataPattern dataPattern = outputTable.getPattern();
            if (dataPattern != null) {
                System.out.println("\t\tDataPattern: " + dataPattern.toString());
            }
            else{
                System.out.println("\t\tDataPattern: is NULL");
            }
            List<String> patternColNames = outputTable.getPatternColumnNames();
            if (patternColNames != null) {
                System.out.println("\t\tPatternColNames: " + patternColNames.toString());
            }
            else{
                System.out.println("\t\tPatternColNames: is NULL");
            }
            List<String> usedColNames = outputTable.getUsedColumnNames();
            if (usedColNames != null) {
                System.out.println("\t\tUsedColNames: " + usedColNames.toString());
            }
            else{
                System.out.println("\t\tUsedColNames: is NULL");
            }
            Table outputT = outputTable.getTable();
            if (outputT != null) {
                System.out.println("\t\tTable: ");
                System.out.println("\t\t\tName: " + outputT.getName());
                System.out.println("\t\t\tLevel: " + outputT.getLevel());
                System.out.println("\t\t\tSQLDefinition: " + outputT.getSqlDefinition());
                System.out.println("\t\t\tisTemp: " + outputT.isTemp());
                System.out.println("\t\t\thasSQLDefinition: " + outputT.hasSQLDefinition());
            }
            else{
                System.out.println("\t\toutputT: is NULL");
            }
        }
        else{
            System.out.println("\toutputTable: is null");
        }

        List<TableView> inputTablesView = this.getInputTables();
        if (inputTablesView != null) {
            System.out.println("\tInput Tables (TableViews): ");
            for (TableView inputTableView : inputTablesView) {
                if (inputTableView != null) {
                    System.out.println("\t\tInput TableView:");
                    System.out.println("\t\t\tName: " + inputTableView.getName());
                    System.out.println("\t\t\tNumOfPartitions: " + inputTableView.getNumOfPartitions());
                    System.out.println("\t\tisHiveTable: " + inputTableView.getIsHiveTable());
                    if(inputTableView.getIsHiveTable() == true){
                        System.out.println("\t\tHiveTableLocation: " + inputTableView.getHiveTableLocation());
                        System.out.println("\t\tHiveCreateTableQuery: " + inputTableView.getHiveCreateTableQuery());
                    }
                    DataPattern dataPattern = inputTableView.getPattern();
                    if (dataPattern != null) {
                        System.out.println("\t\t\tDataPattern: " + dataPattern.toString());
                    }
                    else{
                        System.out.println("\t\t\tDataPattern: is NULL");
                    }
                    List<String> patternColNames = inputTableView.getPatternColumnNames();
                    if (patternColNames != null) {
                        System.out.println("\t\t\tPatternColNames: " + patternColNames.toString());
                    }
                    else{
                        System.out.println("\t\t\tPatternColNames: is NULL");
                    }
                    List<String> usedColNames = inputTableView.getUsedColumnNames();
                    if (usedColNames != null) {
                        System.out.println("\t\t\tUsedColNames: " + usedColNames.toString());
                    }
                    else{
                        System.out.println("\t\t\tUsedColNames: is NULL");
                    }
                    Table outputT = inputTableView.getTable();
                    if (outputT != null) {
                        System.out.println("\t\t\tTable: ");
                        System.out.println("\t\t\t\tName: " + outputT.getName());
                        System.out.println("\t\t\t\tLevel: " + outputT.getLevel());
                        System.out.println("\t\t\t\tSQLDefinition: " + outputT.getSqlDefinition());
                        System.out.println("\t\t\t\tisTemp: " + outputT.isTemp());
                        System.out.println("\t\t\t\thasSQLDefinition: " + outputT.hasSQLDefinition());
                    }
                    else{
                        System.out.println("\t\t\tinputT: is NULL");
                    }
                }
                else{
                    System.out.println("\t\tInputTableView: is NULL");
                }
            }
        }
        else{
            System.out.println("\tInput Tables (TableViews): is NULL");
        }

        if(inputTablesMap != null){
            System.out.println("\tInputTablesMap: ");
            for(Map.Entry<String, TableView> entry : inputTablesMap.entrySet()){
                System.out.println("\t\tEntry: "+entry.getKey());
                if(entry.getValue() != null){
                    TableView inputTableView = entry.getValue();
                    System.out.println("\t\t\tInput TableView:");
                    System.out.println("\t\t\t\tName: " + inputTableView.getName());
                    System.out.println("\t\t\t\tNumOfPartitions: " + inputTableView.getNumOfPartitions());
                    DataPattern dataPattern = inputTableView.getPattern();
                    if (dataPattern != null) {
                        System.out.println("\t\t\t\tDataPattern: " + dataPattern.toString());
                    }
                    else{
                        System.out.println("\t\t\t\tDataPattern: is NULL");
                    }
                    List<String> patternColNames = inputTableView.getPatternColumnNames();
                    if (patternColNames != null) {
                        System.out.println("\t\t\t\tPatternColNames: " + patternColNames.toString());
                    }
                    else{
                        System.out.println("\t\t\t\tPatternColNames: is NULL");
                    }
                    List<String> usedColNames = inputTableView.getUsedColumnNames();
                    if (usedColNames != null) {
                        System.out.println("\t\t\t\tUsedColNames: " + usedColNames.toString());
                    }
                    else{
                        System.out.println("\t\t\t\tUsedColNames: is NULL");
                    }
                    Table outputT = inputTableView.getTable();
                    if (outputT != null) {
                        System.out.println("\t\t\t\tTable: ");
                        System.out.println("\t\t\t\t\tName: " + outputT.getName());
                        System.out.println("\t\t\t\t\tLevel: " + outputT.getLevel());
                        System.out.println("\t\t\t\t\tSQLDefinition: " + outputT.getSqlDefinition());
                        System.out.println("\t\t\t\t\tisTemp: " + outputT.isTemp());
                        System.out.println("\t\t\t\t\thasSQLDefinition: " + outputT.hasSQLDefinition());
                    }
                    else{
                        System.out.println("\t\t\t\tinputT: is NULL");
                    }
                }
                else{
                    System.out.println("\t\t\tEntryGetValue: is NULL");
                }
            }
        }
        else{
            System.out.println("\tInputTablesMap: is NULL");
        }

        if(queryStatements != null){
            System.out.println("\tqueryStatements: ");
            for(String q : queryStatements){
                System.out.println("\t\tQueryStatement: "+q);
            }
        }
        else{
            System.out.println("\tqueryStatements: is NULL");
        }

        System.out.println("\tExtra Query information: ");
        this.printQuery();

        if(parsedSqlQuery != null){
            System.out.println("\tparsedSQLQuery: ");
            parsedSqlQuery.print();
        }
        else{
            System.out.println("\tparsedSQLQuery: is NULL!");
        }

    }

}
