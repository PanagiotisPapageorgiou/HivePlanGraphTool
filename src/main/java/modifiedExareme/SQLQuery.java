/**
 * Copyright MaDgIK Group 2010 - 2015.
 */
package madgik.exareme.common.schema.expression;

import java.io.Serializable;

/**
 * @author herald
 */
public class SQLQuery implements Serializable {
    private static final long serialVersionUID = 1L;
    private Comments comments = null;
    private String sql = null;
    private String partsDefn = null;

    public SQLQuery() {
    }

    public Comments getComments() {
        return comments;
    }

    public void setComments(Comments comments) {
        this.comments = comments;
    }

    public String getPartsDefn() {
        return partsDefn;
    }

    public void setPartsDefn(String partitionDefn) {
        this.partsDefn = partitionDefn;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public void printSQLQuery() {
        System.out.println("\t\t\t\t-----------------SQL Query---------------------");
        System.out.println("\t\t\t\t\tserialVersion: "+serialVersionUID);
        if(comments != null)
            System.out.println("\t\t\t\t\tcomments: "+comments.toString());
        else
            System.out.println("\t\t\t\t\tcomments: IS NULL");
        System.out.println("\t\t\t\t\tsql: "+sql);
        System.out.println("\t\t\t\t\tpartsDefn: "+partsDefn);
    }
}
