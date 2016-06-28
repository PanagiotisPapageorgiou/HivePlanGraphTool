package com.inmobi.hive.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by panos on 5/5/2016.
 */
//OperatorNode carries the Operator Object from a Stage of the QueryPlan
//Each Operator object has access to its children and its parents in the same stage
//As of now, children and parents in other stages are not yet included
//Their connections can be seen in the Edges of the ExaremeGraph only

public class OperatorNode {

    Operator<? extends Serializable> operator;
    String operatorType;
    Task<? extends Serializable> ownerStage;

    public OperatorNode(Operator<? extends Serializable> op, Task<? extends Serializable> owner){
        operator = op;
        ownerStage = owner;
        operatorType = operator.getType().toString();
    }

    public Operator<? extends Serializable> getOperator() {
        return operator;
    }

    public Task<? extends Serializable> getOwnerStage() { return ownerStage; }
    
    public void setOperator(Operator<? extends Serializable> op) {
        this.operator = op;
    }

    public String getOperatorName(){
        return operator.getOperatorId();
    }

    public boolean compareOperatorNames(OperatorNode op){
        return this.operator.getOperatorId().equals(op.getOperator().getOperatorId());
    }

    public String getOperatorType(){
        return operatorType;
    }

    public void printOperatorInstance(PrintWriter outputFile){
        outputFile.println("\t\t------------------------OPERATOR: "+operator.getOperatorId()+" ----------------------------------");
        outputFile.flush();
        outputFile.println("\t\t\tOperatorName: "+operator.getName());
        outputFile.flush();
        outputFile.println("\t\t\tOperatorIdentifier: "+operator.getIdentifier());
        outputFile.flush();
        outputFile.println("\t\t\tToString: "+operator.toString());
        outputFile.flush();
        Map<String, ExprNodeDesc> mapExprNodeDesc = operator.getColumnExprMap();
        if(mapExprNodeDesc != null) {
            outputFile.println("\t\t\tPrinting MapExprNodeDesc...");
            outputFile.flush();
            for (Map.Entry<String, ExprNodeDesc> entry : mapExprNodeDesc.entrySet()) {
                ExprNodeDesc tmp = entry.getValue();
                if(tmp != null) {
                    outputFile.println("\t\t\t\tPriting Key: " + entry.getKey() + " with Value(ToString): " + tmp.toString());
                    outputFile.flush();
                }
            }
        }
        else{
            outputFile.println("\t\t\tColumnExprMap is null...");
            outputFile.flush();
        }

        OperatorType opType = operator.getType();
        if(opType != null){
            outputFile.println("\t\t\tOperatorType(toString): "+opType.toString());
            outputFile.flush();
        }
        else{
            outputFile.println("\t\t\tOperatorType is null...");
            outputFile.flush();
        }

        List<org.apache.hadoop.hive.ql.exec.Operator <? extends OperatorDesc>> childOperators = operator.getChildOperators();
        if(childOperators != null){
            if(childOperators.size() > 0)
                outputFile.println("\t\t\tIsLeaf: NO");
            else
                outputFile.println("\t\t\tIsLeaf: YES");
            for(org.apache.hadoop.hive.ql.exec.Operator <? extends OperatorDesc> ch : childOperators) {
                if (ch != null) {
                    outputFile.println("\t\t\t\tChildID: " + ch.getOperatorId());
                    outputFile.flush();
                }
                else{
                    System.out.println("Child is NULL...?");
                }
            }
        }
        else{
            outputFile.println("\t\t\tIsLeaf: YES");
            outputFile.println("\t\t\tOperator has no children...");
            outputFile.flush();
        }

        List<org.apache.hadoop.hive.ql.exec.Operator <? extends OperatorDesc>> parentOperators = operator.getParentOperators();
        if(parentOperators != null){
            if(parentOperators.size() > 0)
                outputFile.println("\t\t\tIsRoot: NO");
            else
                outputFile.println("\t\t\tIsRoot: YES");

            for(org.apache.hadoop.hive.ql.exec.Operator <? extends Serializable> p : parentOperators)
                if(p != null) {
                    outputFile.println("\t\t\t\tParentID: " + p.getOperatorId());
                    outputFile.flush();
                }
        }
        else{
            outputFile.println("\t\t\tIsRoot: YES");
            outputFile.println("\t\t\tOperator has no parent...");
            outputFile.flush();
        }

        ExecMapperContext execMapperContext = operator.getExecContext();
        if(execMapperContext != null) {
            outputFile.println("\t\t\tExecMapperContext(toString): "+execMapperContext.toString());
            outputFile.flush();
        }
        else{
            outputFile.println("\t\t\tExecContext is null...");
            outputFile.flush();
        }

        ObjectInspector[] objInspectors = operator.getInputObjInspectors();
        if(objInspectors != null){
            outputFile.println("\t\t\tAccessing InputObjectInspectors...");
            outputFile.flush();
            for(ObjectInspector objInspector : objInspectors){
                if(objInspector != null) {
                    ObjectInspector.Category category = objInspector.getCategory();
                    if (category != null) {
                        outputFile.println("\t\t\t\tCategory: " + category.toString());
                        outputFile.flush();
                    }
                    if (objInspector.getTypeName() != null) {
                        outputFile.println("\t\t\t\tTypeName: " + objInspector.getTypeName());
                        outputFile.flush();
                    }
                    outputFile.println("\t\t\t\tObjInspector(toString): " + objInspector.toString());
                    outputFile.flush();
                }
                else{
                    outputFile.println("\t\t\t\tObjectInspector is null...");
                    outputFile.flush();
                }
            }
        }
        else{
            outputFile.println("\t\t\t\tInputObjectInspectors are null...");
            outputFile.flush();
        }

        ObjectInspector outputObjInspector = operator.getOutputObjInspector();
        if(outputObjInspector != null){
            outputFile.println("\t\t\tAccessing OutputObjectInspector...");
            outputFile.flush();
            outputFile.println("\t\t\t\tOutputObject(toString): "+outputObjInspector.toString());
            outputFile.flush();
            if(outputObjInspector.getCategory() != null){
                outputFile.println("\t\t\t\t: "+outputObjInspector.getCategory().toString());
                outputFile.flush();
            }
            else{
                outputFile.println("\t\t\t\tCategory is null!");
                outputFile.flush();
            }
            if(outputObjInspector.getTypeName() != null) {
                outputFile.println("\t\t\t\tTypeName: "+outputObjInspector.getTypeName());
                outputFile.flush();
            }
            else{
                outputFile.println("\t\t\t\tTypeName is null!");
                outputFile.flush();
            }
        }

        Configuration configuration = operator.getConfiguration();
        if(configuration != null){
            outputFile.println("\t\t\tConfiguration(toString): "+configuration.toString());
            outputFile.flush();
        }
        else{
            outputFile.println("\t\t\tConfiguration is null!");
            outputFile.flush();
        }

        OpTraits opTraits = operator.getOpTraits();
        if(opTraits != null){
            outputFile.println("\t\t\tOpTraits(toString): "+opTraits.toString());
            outputFile.flush();
        }
        else{
            outputFile.println("\t\t\tOpTraits is null!");
            outputFile.flush();
        }

        RowSchema rowSchema = operator.getSchema();
        if(rowSchema != null){
            outputFile.println("\t\t\tRowSchema: "+rowSchema.toString());
            outputFile.flush();
        }
        else{
            outputFile.println("\t\t\tRowSchema is null!");
            outputFile.flush();
        }

        Statistics statistics = operator.getStatistics();
        if(statistics != null){
            outputFile.println("\t\t\tStatistics: "+statistics.toString());
            outputFile.flush();
        }
        else{
            outputFile.println("\t\t\tStatistics is null!");
            outputFile.flush();
        }

        Map<String, Long> statsMap = operator.getStats();
        if(statsMap != null){
            outputFile.println("\t\t\tAccessing stats map...");
            outputFile.flush();
            for(Map.Entry<String, Long> entry : statsMap.entrySet()){
                if(entry != null){
                    if(entry.getValue() != null){
                        if(entry.getKey() != null){
                            outputFile.println("\t\t"+entry.getKey()+" : "+entry.getValue());
                            outputFile.flush();
                        }
                    }
                }
            }
        }
        else{
            outputFile.println("\t\t\tStatsMap is null!");
            outputFile.flush();
        }

        if(opType != null){
            if(opType.toString().equals("TABLESCAN")){
                if(operator instanceof TableScanOperator){
                    outputFile.println("\n\t\t\tTableScan information...");
                    outputFile.flush();
                    TableScanOperator tbsOperator = (TableScanOperator) operator;
                    if(tbsOperator != null){
                        List<String> neededColumns = tbsOperator.getNeededColumns();
                        if(neededColumns != null){
                            outputFile.println("\t\t\t\tNeeded Columns: "+neededColumns.toString());
                            outputFile.flush();
                        }
                        List<Integer> neededColumnIDs = tbsOperator.getNeededColumnIDs();
                        if(neededColumnIDs != null){
                            outputFile.println("\t\t\t\tNeeded Columns IDs: "+neededColumnIDs.toString());
                            outputFile.flush();
                        }
                        List<String> referencedColumns = tbsOperator.getReferencedColumns();
                        if(referencedColumns != null){
                            outputFile.println("\t\t\t\tReferenced Columns: "+referencedColumns.toString());
                            outputFile.flush();
                        }
                        TableDesc tableDesc = tbsOperator.getTableDesc();
                        if(tableDesc != null){
                            String tableName = tableDesc.getTableName();
                            if(tableName != null){
                                outputFile.println("\t\t\t\tTableName: "+tableName);
                                outputFile.flush();
                            }
                            String serdeClassName = tableDesc.getSerdeClassName();
                            if(serdeClassName != null){
                                outputFile.println("\t\t\t\tSerdeClassName: "+serdeClassName);
                                outputFile.flush();
                            }
                            Map propertiesExplain = tableDesc.getPropertiesExplain();
                            if(propertiesExplain != null){
                                outputFile.println("\t\t\t\tPropertiesMap(Explain): "+propertiesExplain.toString());
                                outputFile.flush();
                            }
                            Properties properties = tableDesc.getProperties();
                            if(properties != null){
                                outputFile.println("\t\t\t\tProperties: "+properties.toString());
                                outputFile.flush();
                            }
                            String inputFileFormatClassName = tableDesc.getInputFileFormatClassName();
                            if(inputFileFormatClassName != null){
                                outputFile.println("\t\t\t\tInputFileFormatClassName: "+inputFileFormatClassName);
                                outputFile.flush();
                            }
                            String outputFileFormatClassName = tableDesc.getOutputFileFormatClassName();
                            if(outputFileFormatClassName != null){
                                outputFile.println("\t\t\t\tOutputFileFormatClassName: "+outputFileFormatClassName);
                                outputFile.flush();
                            }
                            Map<String, String> jobProperties = tableDesc.getJobProperties();
                            if(jobProperties != null){
                                outputFile.println("\t\t\t\tJobProperties: ");
                                outputFile.flush();
                                for (Map.Entry<String, String> entry : jobProperties.entrySet()) {
                                    outputFile.println("\t\t\t\t\tPriting Key: " + entry.getKey() + " with Value(ToString): " + entry.getValue());
                                    outputFile.flush();
                                }
                            }
                        }
                    }
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else if(opType.toString().equals("FILTER")) {
                if(operator instanceof FilterOperator){
                    outputFile.println("\n\t\t\tFilter information...");
                    outputFile.flush();
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else if(opType.toString().equals("FILESINK")) {
                if(operator instanceof FileSinkOperator){
                    outputFile.println("\n\t\t\tFileSink information...");
                    outputFile.flush();
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else if(opType.toString().equals("DEMUX")) {
                if(operator instanceof DemuxOperator){
                    outputFile.println("\n\t\t\tDemux information...");
                    outputFile.flush();
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else if(opType.toString().equals("EXTRACT")) {
                if(operator instanceof ExtractOperator){
                    outputFile.println("\n\t\t\tExtract information...");
                    outputFile.flush();
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else if(opType.toString().equals("FORWARD")){
                if(operator instanceof ForwardOperator){
                    outputFile.println("\n\t\t\tForward information...");
                    outputFile.flush();
                    ForwardOperator forwardOperator = (ForwardOperator) operator;
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else if(opType.toString().equals("GROUPBY")){
                if(operator instanceof GroupByOperator){
                    outputFile.println("\n\t\t\tGroupBy information...");
                    outputFile.flush();
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else if(opType.toString().equals("HASHTABLEDUMMY")){
                if(operator instanceof HashTableDummyOperator){
                    outputFile.println("\n\t\t\tHashTableDummy information...");
                    outputFile.flush();
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else if(opType.toString().equals("HASHTABLESINK")){
                if(operator instanceof HashTableSinkOperator){
                    outputFile.println("\n\t\t\tHashTableSink information...");
                    outputFile.flush();
                    HashTableSinkOperator hashTableSinkOperator = (HashTableSinkOperator) operator;
                    if(hashTableSinkOperator != null){
                        MapJoinTableContainer[] mapJoinTableContainers = hashTableSinkOperator.getMapJoinTables();
                        if(mapJoinTableContainers != null){
                            outputFile.println("\n\t\t\t\tMapJoinTableContainers: ");
                            outputFile.flush();
                            for(MapJoinTableContainer mapJoinTableContainer : mapJoinTableContainers){
                                if(mapJoinTableContainer != null){
                                    outputFile.println("\n\t\t\t\t\t"+mapJoinTableContainer.toString());
                                }
                            }
                        }
                    }
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else if(opType.toString().equals("JOIN")){
                if(operator instanceof JoinOperator){
                    outputFile.println("\n\t\t\tJoin information...");
                    outputFile.flush();
                    JoinOperator joinOperator = (JoinOperator) operator;
                    outputFile.println("\t\t\t\topAllowedBeforeSortMergeJoin: "+joinOperator.opAllowedBeforeSortMergeJoin());
                    outputFile.println("\t\t\t\tsupportSkewJoinOptimization: "+joinOperator.supportSkewJoinOptimization());
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else if(opType.toString().equals("LATERALVIEWFORWARD")){
                if(operator instanceof LateralViewForwardOperator){
                    outputFile.println("\n\t\t\tLateralViewForward information...");
                    outputFile.flush();
                    LateralViewForwardOperator lateralViewForwardOperator = (LateralViewForwardOperator) operator;
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else if(opType.toString().equals("LATERALVIEWJOIN")){
                if(operator instanceof LateralViewJoinOperator){
                    outputFile.println("\n\t\t\tLateralViewJoin information...");
                    outputFile.flush();
                    LateralViewJoinOperator lateralViewJoinOperator = (LateralViewJoinOperator) operator;
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else if(opType.toString().equals("LIMIT")){
                if(operator instanceof LimitOperator){
                    outputFile.println("\n\t\t\tLimit information...");
                    outputFile.flush();
                    LimitOperator limitOperator = (LimitOperator) operator;
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else if(opType.toString().equals("MAPJOIN")){
                if(operator instanceof MapJoinOperator){
                    outputFile.println("\n\t\t\tMapJoin information...");
                    outputFile.flush();
                    MapJoinOperator mapJoinOperator = (MapJoinOperator) operator;
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else if(opType.toString().equals("MUX")){
                if(operator instanceof MuxOperator){
                    outputFile.println("\n\t\t\tMux information...");
                    outputFile.flush();
                    MuxOperator muxOperator = (MuxOperator) operator;
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else if(opType.toString().equals("PTF")){
                if(operator instanceof PTFOperator){
                    outputFile.println("\n\t\t\tPTF information...");
                    outputFile.flush();
                    PTFOperator ptfOperator = (PTFOperator) operator;
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else if(opType.toString().equals("REDUCESINK")){
                if(operator instanceof ReduceSinkOperator){
                    outputFile.println("\n\t\t\tReduceSink information...");
                    outputFile.flush();
                    ReduceSinkOperator reduceSinkOperator = (ReduceSinkOperator) operator;
                    if(reduceSinkOperator != null){
                        String[] inputAliases = reduceSinkOperator.getInputAliases();
                        if(inputAliases != null){
                            outputFile.println("\t\t\t\tInputAliases: "+inputAliases.toString());
                            outputFile.flush();
                        }
                        int[] valueIndeces = reduceSinkOperator.getValueIndex();
                        if(valueIndeces != null){
                            outputFile.println("\t\t\t\tValueIndex: "+valueIndeces.toString());
                            outputFile.flush();
                        }
                        outputFile.println("\t\t\t\tOpAllowedBeforeMapJoin: "+reduceSinkOperator.opAllowedBeforeMapJoin());
                        outputFile.flush();
                    }
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else if(opType.toString().equals("SCRIPT")){
                if(operator instanceof ScriptOperator){
                    outputFile.println("\n\t\t\tScriptOperator information...");
                    outputFile.flush();
                    ScriptOperator scriptOperator = (ScriptOperator) operator;
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else if(opType.toString().equals("SELECT")){
                if(operator instanceof SelectOperator){
                    outputFile.println("\n\t\t\tSelect information...");
                    outputFile.flush();
                    SelectOperator selectOperator = (SelectOperator) operator;
                    if(selectOperator != null){
                        outputFile.println("\t\t\t\tsupportSkewJoinOptimization: "+selectOperator.supportSkewJoinOptimization());
                        outputFile.flush();
                        outputFile.println("\t\t\t\tacceptLimitPushdown: "+selectOperator.acceptLimitPushdown());
                        outputFile.flush();
                        outputFile.println("\t\t\t\tcolumnNamesRowResolvedCanBeObtained: "+selectOperator.columnNamesRowResolvedCanBeObtained());
                        outputFile.flush();
                        outputFile.println("\t\t\t\tisIdentitySelect: "+selectOperator.isIdentitySelect());
                        outputFile.flush();
                        outputFile.println("\t\t\t\tsupportAutomaticSortMergeJoin: "+selectOperator.supportAutomaticSortMergeJoin());
                        outputFile.flush();
                        outputFile.println("\t\t\t\tsupportUnionRemoveOptimization: "+selectOperator.supportUnionRemoveOptimization());
                        outputFile.flush();
                    }
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else if(opType.toString().equals("UDTF")){
                if(operator instanceof UDTFOperator){
                    outputFile.println("\n\t\t\tUTDF information...");
                    outputFile.flush();
                    UDTFOperator udtfOperator = (UDTFOperator) operator;
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else if(opType.toString().equals("UNION")){
                if(operator instanceof UnionOperator){
                    outputFile.println("\n\t\t\tUnion information...");
                    outputFile.flush();
                    UnionOperator unionOperator = (UnionOperator) operator;
                    if(unionOperator != null){
                        outputFile.println("\t\t\t\topAllowedAfterMapJoin: "+unionOperator.opAllowedAfterMapJoin());
                        outputFile.flush();
                        outputFile.println("\t\t\t\topAllowedBeforeMapJoin: "+unionOperator.opAllowedBeforeMapJoin());
                        outputFile.flush();
                        outputFile.println("\t\t\t\topAllowedBeforeSortMergeJoin: "+unionOperator.opAllowedBeforeSortMergeJoin());
                        outputFile.flush();
                    }
                }
                else{
                    System.out.println("Operator instance and type do not match!");
                    System.exit(0);
                }
            }
            else {
                outputFile.println("\n\t\t\tUnknown Operator Type: "+opType.toString());
                outputFile.flush();
            }
        }
    }

}
