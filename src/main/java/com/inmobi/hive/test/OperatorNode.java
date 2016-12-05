package com.inmobi.hive.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.*;

/**
 * Created by panos on 5/5/2016.
 */

/*
   An OperatorNode is a Node of the Exareme Graph. It contains a Map Reduce
   Operator extracted from the Hive Plan (such as TableScanOperator, FilterOperator
   GroupByOperator etc.). Since every Operator is found in a Stage of the plan we
   also keep track of the stage (ownerStage) that the Operator was originally found
   to help us connect leaf Operators from one stage to the root operators of the next
   stage.
*/

public class OperatorNode {

    Operator<? extends Serializable> operator;
    String operatorType;
    boolean specialOperator;
    Task<? extends Serializable> ownerStage;
    String specialName;

    public OperatorNode(Operator<? extends Serializable> op, Task<? extends Serializable> owner){
        operator = op;
        ownerStage = owner;
        operatorType = operator.getType().toString();
        specialOperator = false;
    }

    public OperatorNode(Task<? extends Serializable> owner, boolean sOp){
        specialOperator = sOp;
        operator = null;
        ownerStage = owner;
        if(owner instanceof DDLTask) {
            operatorType = "Create Table Operator";
            specialName = "CREATE_OP";
        }
        else if(owner instanceof MoveTask){
            operatorType = "Move Operator";
            specialName = "MOVE_OP";
        }
        else if(owner instanceof StatsTask){
            operatorType = "Stats-Aggr Operator";
            specialName = "STATS_OP";
        }
    }

    public Operator<? extends Serializable> getOperator() {
        return operator;
    }

    public boolean isSpecialOperator() { return specialOperator; }

    public String getSpecialName() { return specialName; }

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

    public void retrieveAllPredicatePhrases(ExprNodeDesc predicate, List<String> predicatePhrases){
        if(predicate != null){
            String exprString = predicate.getExprString();
            if(exprString != null){
                if(predicatePhrases.size() > 0){
                    for(String s : predicatePhrases){
                        if(s.equals(exprString)){
                            return;
                        }
                    }
                }
                predicatePhrases.add(exprString);
                return;
            }
            List<ExprNodeDesc> children = predicate.getChildren();
            if(children != null){
                for(ExprNodeDesc c : children){
                    if(c != null){
                        retrieveAllPredicatePhrases(c, predicatePhrases);
                    }
                }
            }
        }
        return;
    }

    public void printOperatorInstance(PrintWriter outputFile) {

        if(outputFile == null) {
            printOperatorInstanceToConsole();
            return;
        }

        if (this.isSpecialOperator() == false) {
            outputFile.println("\t\t------------------------OPERATOR: " + operator.getOperatorId() + " ----------------------------------");
            outputFile.flush();
            outputFile.println("\t\t\tOperatorName: " + operator.getName());
            outputFile.flush();
            outputFile.println("\t\t\tOperatorIdentifier: " + operator.getIdentifier());
            outputFile.flush();
            outputFile.println("\t\t\tToString: " + operator.toString());
            outputFile.flush();
            Map<String, ExprNodeDesc> mapExprNodeDesc = operator.getColumnExprMap();
            if (mapExprNodeDesc != null) {
                outputFile.println("\t\t\tPrinting MapExprNodeDesc...");
                outputFile.flush();
                for (Map.Entry<String, ExprNodeDesc> entry : mapExprNodeDesc.entrySet()) {
                    ExprNodeDesc tmp = entry.getValue();
                    if (tmp != null) {
                        outputFile.println("\t\t\t\tPriting Key: " + entry.getKey() + " with Value(ToString): " + tmp.toString());
                        outputFile.flush();
                    }
                }
            } else {
                outputFile.println("\t\t\tColumnExprMap is null...");
                outputFile.flush();
            }

            OperatorType opType = operator.getType();
            if (opType != null) {
                outputFile.println("\t\t\tOperatorType(toString): " + opType.toString());
                outputFile.flush();
            } else {
                outputFile.println("\t\t\tOperatorType is null...");
                outputFile.flush();
            }

            List<org.apache.hadoop.hive.ql.exec.Operator<? extends OperatorDesc>> childOperators = operator.getChildOperators();
            if (childOperators != null) {
                if (childOperators.size() > 0)
                    outputFile.println("\t\t\tIsLeaf: NO");
                else
                    outputFile.println("\t\t\tIsLeaf: YES");
                for (org.apache.hadoop.hive.ql.exec.Operator<? extends OperatorDesc> ch : childOperators) {
                    if (ch != null) {
                        outputFile.println("\t\t\t\tChildID: " + ch.getOperatorId());
                        outputFile.flush();
                    } else {
                        System.out.println("Child is NULL...?");
                    }
                }
            } else {
                outputFile.println("\t\t\tIsLeaf: YES");
                outputFile.println("\t\t\tOperator has no children...");
                outputFile.flush();
            }

            List<org.apache.hadoop.hive.ql.exec.Operator<? extends OperatorDesc>> parentOperators = operator.getParentOperators();
            if (parentOperators != null) {
                if (parentOperators.size() > 0)
                    outputFile.println("\t\t\tIsRoot: NO");
                else
                    outputFile.println("\t\t\tIsRoot: YES");

                for (org.apache.hadoop.hive.ql.exec.Operator<? extends Serializable> p : parentOperators)
                    if (p != null) {
                        outputFile.println("\t\t\t\tParentID: " + p.getOperatorId());
                        outputFile.flush();
                    }
            } else {
                outputFile.println("\t\t\tIsRoot: YES");
                outputFile.println("\t\t\tOperator has no parent...");
                outputFile.flush();
            }
            ExecMapperContext execMapperContext = operator.getExecContext();
            if (execMapperContext != null) {
                outputFile.println("\t\t\tExecMapperContext(toString): " + execMapperContext.toString());
                outputFile.flush();
            } else {
                outputFile.println("\t\t\tExecContext is null...");
                outputFile.flush();
            }

            ObjectInspector[] objInspectors = operator.getInputObjInspectors();
            if (objInspectors != null) {
                outputFile.println("\t\t\tAccessing InputObjectInspectors...");
                outputFile.flush();
                for (ObjectInspector objInspector : objInspectors) {
                    if (objInspector != null) {
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
                    } else {
                        outputFile.println("\t\t\t\tObjectInspector is null...");
                        outputFile.flush();
                    }
                }
            } else {
                outputFile.println("\t\t\t\tInputObjectInspectors are null...");
                outputFile.flush();
            }

            ObjectInspector outputObjInspector = operator.getOutputObjInspector();
            if (outputObjInspector != null) {
                outputFile.println("\t\t\tAccessing OutputObjectInspector...");
                outputFile.flush();
                outputFile.println("\t\t\t\tOutputObject(toString): " + outputObjInspector.toString());
                outputFile.flush();
                if (outputObjInspector.getCategory() != null) {
                    outputFile.println("\t\t\t\t: " + outputObjInspector.getCategory().toString());
                    outputFile.flush();
                } else {
                    outputFile.println("\t\t\t\tCategory is null!");
                    outputFile.flush();
                }
                if (outputObjInspector.getTypeName() != null) {
                    outputFile.println("\t\t\t\tTypeName: " + outputObjInspector.getTypeName());
                    outputFile.flush();
                } else {
                    outputFile.println("\t\t\t\tTypeName is null!");
                    outputFile.flush();
                }
            }

            Configuration configuration = operator.getConfiguration();
            if (configuration != null) {
                outputFile.println("\t\t\tConfiguration(toString): " + configuration.toString());
                outputFile.flush();
            } else {
                outputFile.println("\t\t\tConfiguration is null!");
                outputFile.flush();
            }

            OpTraits opTraits = operator.getOpTraits();
            if (opTraits != null) {
                outputFile.println("\t\t\tOpTraits(toString): " + opTraits.toString());
                outputFile.flush();
            } else {
                outputFile.println("\t\t\tOpTraits is null!");
                outputFile.flush();
            }

            RowSchema rowSchema = operator.getSchema();
            if (rowSchema != null) {
                outputFile.println("\t\t\tRowSchema: " + rowSchema.toString());
                outputFile.flush();
            } else {
                outputFile.println("\t\t\tRowSchema is null!");
                outputFile.flush();
            }

            Statistics statistics = operator.getStatistics();
            if (statistics != null) {
                outputFile.println("\t\t\tStatistics: " + statistics.toString());
                outputFile.flush();
            } else {
                outputFile.println("\t\t\tStatistics is null!");
                outputFile.flush();
            }

            Map<String, Long> statsMap = operator.getStats();
            if (statsMap != null) {
                outputFile.println("\t\t\tAccessing stats map...");
                outputFile.flush();
                for (Map.Entry<String, Long> entry : statsMap.entrySet()) {
                    if (entry != null) {
                        if (entry.getValue() != null) {
                            if (entry.getKey() != null) {
                                outputFile.println("\t\t" + entry.getKey() + " : " + entry.getValue());
                                outputFile.flush();
                            }
                        }
                    }
                }
            } else {
                outputFile.println("\t\t\tStatsMap is null!");
                outputFile.flush();
            }

            outputFile.println("\t\t\tcolumnNamesRowResolvedCanBeObtained: " + operator.columnNamesRowResolvedCanBeObtained());
            outputFile.flush();

            outputFile.println("\t\t\tacceptLimitPushdown: " + operator.acceptLimitPushdown());
            outputFile.flush();

            outputFile.println("\t\t\tsupportUnionRemoveOptimization: " + operator.supportUnionRemoveOptimization());
            outputFile.flush();

            outputFile.println("\t\t\topAllowedAfterMapJoin: " + operator.opAllowedAfterMapJoin());
            outputFile.flush();

            outputFile.println("\t\t\tisUseBucketizedHiveInputFormat: " + operator.isUseBucketizedHiveInputFormat());
            outputFile.flush();

            outputFile.println("\t\t\tsupportAutomaticSortMergeJoin: " + operator.supportAutomaticSortMergeJoin());
            outputFile.flush();

            outputFile.println("\t\t\tsupportSkewJoinOptimization: " + operator.supportSkewJoinOptimization());
            outputFile.flush();

            outputFile.println("\t\t\topAllowedConvertMapJoin: " + operator.opAllowedConvertMapJoin());
            outputFile.flush();

            outputFile.println("\t\t\topAllowedBeforeSortMergeJoin: " + operator.opAllowedBeforeSortMergeJoin());
            outputFile.flush();

            outputFile.println("\t\t\topAllowedBeforeSortMergeJoin: " + operator.opAllowedBeforeSortMergeJoin());
            outputFile.flush();

            if (opType != null) {
                if (opType.toString().equals("TABLESCAN")) {
                    if (operator instanceof TableScanOperator) {
                        outputFile.println("\n\t\t\tTableScan information...");
                        outputFile.flush();
                        TableScanOperator tbsOperator = (TableScanOperator) operator;
                        if (tbsOperator != null) {
                            List<String> neededColumns = tbsOperator.getNeededColumns();
                            if (neededColumns != null) {
                                outputFile.println("\t\t\t\tNeeded Columns: " + neededColumns.toString());
                                outputFile.flush();
                            }
                            List<Integer> neededColumnIDs = tbsOperator.getNeededColumnIDs();
                            if (neededColumnIDs != null) {
                                outputFile.println("\t\t\t\tNeeded Columns IDs: " + neededColumnIDs.toString());
                                outputFile.flush();
                            }
                            List<String> referencedColumns = tbsOperator.getReferencedColumns();
                            if (referencedColumns != null) {
                                outputFile.println("\t\t\t\tReferenced Columns: " + referencedColumns.toString());
                                outputFile.flush();
                            }
                            TableDesc tableDesc = tbsOperator.getTableDesc();
                            if (tableDesc != null) {
                                String tableName = tableDesc.getTableName();
                                if (tableName != null) {
                                    outputFile.println("\t\t\t\tTableName: " + tableName);
                                    outputFile.flush();
                                }
                                String serdeClassName = tableDesc.getSerdeClassName();
                                if (serdeClassName != null) {
                                    outputFile.println("\t\t\t\tSerdeClassName: " + serdeClassName);
                                    outputFile.flush();
                                }
                                Map propertiesExplain = tableDesc.getPropertiesExplain();
                                if (propertiesExplain != null) {
                                    outputFile.println("\t\t\t\tPropertiesMap(Explain): " + propertiesExplain.toString());
                                    outputFile.flush();
                                }
                                Properties properties = tableDesc.getProperties();
                                if (properties != null) {
                                    outputFile.println("\t\t\t\tProperties: " + properties.toString());
                                    outputFile.flush();
                                }
                                String inputFileFormatClassName = tableDesc.getInputFileFormatClassName();
                                if (inputFileFormatClassName != null) {
                                    outputFile.println("\t\t\t\tInputFileFormatClassName: " + inputFileFormatClassName);
                                    outputFile.flush();
                                }
                                String outputFileFormatClassName = tableDesc.getOutputFileFormatClassName();
                                if (outputFileFormatClassName != null) {
                                    outputFile.println("\t\t\t\tOutputFileFormatClassName: " + outputFileFormatClassName);
                                    outputFile.flush();
                                }
                                Map<String, String> jobProperties = tableDesc.getJobProperties();
                                if (jobProperties != null) {
                                    outputFile.println("\t\t\t\tJobProperties: ");
                                    outputFile.flush();
                                    for (Map.Entry<String, String> entry : jobProperties.entrySet()) {
                                        outputFile.println("\t\t\t\t\tPrinting Key: " + entry.getKey() + " with Value(ToString): " + entry.getValue());
                                        outputFile.flush();
                                    }
                                }
                            }
                            TableScanDesc tableScanDesc = tbsOperator.getConf();
                            if (tableScanDesc != null) {
                                outputFile.println("\t\t\t\tAlias: " + tableScanDesc.getAlias());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tisStatsReliable: " + tableScanDesc.isStatsReliable());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tIsMetadataOnly: " + tableScanDesc.getIsMetadataOnly());
                                outputFile.flush();
                                /*if(tableScanDesc.getFilterExprString() != null) {
                                    outputFile.println("\t\t\t\tFilterExprString: " + tableScanDesc.getFilterExprString());
                                    outputFile.flush();
                                }*/
                                Table table = tableScanDesc.getTableMetadata();
                                if (table != null) {
                                    outputFile.println("\t\t\t\tTable: ");
                                    outputFile.flush();
                                    outputFile.println("\t\t\t\t\tCompleteName: " + table.getCompleteName());
                                    outputFile.flush();
                                    Path dataLoc = table.getDataLocation();
                                    if (dataLoc != null) {
                                        outputFile.println("\t\t\t\t\tDataLocation: " + dataLoc.toString());
                                        outputFile.flush();
                                    }
                                    List<FieldSchema> partitionKeys = table.getPartitionKeys();
                                    if (partitionKeys != null) {
                                        outputFile.println("\t\t\t\t\tPartitionKeys: ");
                                        outputFile.flush();
                                        for (FieldSchema f : partitionKeys) {
                                            if (f != null) {
                                                outputFile.println("\t\t\t\t\t\tPartitionKey: " + f.toString());
                                                outputFile.flush();
                                            }
                                        }
                                    }
                                    List<FieldSchema> allCols = table.getAllCols();
                                    if (allCols != null) {
                                        outputFile.println("\t\t\t\t\tAllColumns: ");
                                        outputFile.flush();
                                        for (FieldSchema f : allCols) {
                                            if (f != null) {
                                                outputFile.println("\t\t\t\t\t\tColumn: " + f.toString());
                                                outputFile.flush();
                                            }
                                        }
                                    }
                                }
                                List<String> partColumns = tableScanDesc.getPartColumns();
                                if (partColumns != null) {
                                    outputFile.println("\t\t\t\tPartitionColumns: ");
                                    outputFile.flush();
                                    for (String s : partColumns) {
                                        outputFile.println("\t\t\t\t\tColumn: " + s);
                                        outputFile.flush();
                                    }
                                }
                                List<String> needed = tableScanDesc.getNeededColumns();
                                if (needed != null) {
                                    outputFile.println("\t\t\t\tNeededColumns: ");
                                    outputFile.flush();
                                    for (String s : needed) {
                                        outputFile.println("\t\t\t\t\tColumn: " + s);
                                        outputFile.flush();
                                    }
                                }
                                List<String> referenced = tableScanDesc.getReferencedColumns();
                                if (referenced != null) {
                                    outputFile.println("\t\t\t\tReferencedColumns: ");
                                    outputFile.flush();
                                    for (String s : referenced) {
                                        outputFile.println("\t\t\t\t\tColumn: " + s);
                                        outputFile.flush();
                                    }
                                }
                                List<Integer> neededIDs = tableScanDesc.getNeededColumnIDs();
                                if (neededIDs != null) {
                                    outputFile.println("\t\t\t\tNeededIDs: ");
                                    outputFile.flush();
                                    for (Integer i : neededIDs) {
                                        outputFile.println("\t\t\t\t\tID: " + i);
                                        outputFile.flush();
                                    }
                                }

                                List<VirtualColumn> virtualColumns = tableScanDesc.getVirtualCols();
                                if (virtualColumns != null) {
                                    outputFile.println("\t\t\t\tVirtualColumns: ");
                                    outputFile.flush();
                                    for (VirtualColumn v : virtualColumns) {
                                        if (v != null) {
                                            outputFile.println("\t\t\t\t\tColumn: " + v.getName());
                                            outputFile.flush();
                                        }
                                    }
                                }
                                outputFile.println("\t\t\t\tRowLimit: " + tableScanDesc.getRowLimit());
                                outputFile.flush();
                            }
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("FILTER")) {
                    if (operator instanceof FilterOperator) {
                        outputFile.println("\n\t\t\tFilter information...");
                        outputFile.flush();
                        FilterOperator filter = (FilterOperator) operator;
                        outputFile.println("\t\t\t\tsupportSkewJoinOptimization: " + filter.supportSkewJoinOptimization());
                        outputFile.flush();
                        outputFile.println("\t\t\t\tsupportAutomaticSortMergeJoin: " + filter.supportAutomaticSortMergeJoin());
                        outputFile.flush();
                        outputFile.println("\t\t\t\tsupportUnionRemoveOptimization: " + filter.supportUnionRemoveOptimization());
                        outputFile.flush();
                        outputFile.println("\t\t\t\tcolumnNamesRowResolvedCanBeObtained: " + filter.columnNamesRowResolvedCanBeObtained());
                        outputFile.flush();

                        FilterDesc filterDesc = filter.getConf();
                        if (filterDesc != null) {
                            outputFile.println("\t\t\t\tIsSamplingPred: " + filterDesc.getIsSamplingPred());
                            outputFile.flush();
                            outputFile.println("\t\t\t\tisSortedFilter: " + filterDesc.isSortedFilter());
                            outputFile.flush();
                            ExprNodeDesc predicate = filterDesc.getPredicate();
                            if (predicate != null) {
                                outputFile.println("\t\t\t\tPredicate: ");
                                outputFile.flush();
                                if (predicate.getCols() != null) {
                                    outputFile.println("\t\t\t\t\tColumns: " + predicate.getCols().toString());
                                    outputFile.flush();
                                }
                                outputFile.println("\t\t\t\t\tName: " + predicate.getName());
                                outputFile.flush();
                                outputFile.println("\t\t\t\t\tExprString: " + predicate.getExprString());
                                outputFile.flush();
                                outputFile.println("\t\t\t\t\tTypeString: " + predicate.getTypeString());
                                outputFile.flush();

                                List<String> phrases = new LinkedList<String>();

                                retrieveAllPredicatePhrases(predicate, phrases);

                                outputFile.println("\t\t\t\t\tPhrases: ");
                                outputFile.flush();
                                for (String p : phrases) {
                                    outputFile.println("\t\t\t\t\t\tPhrase: " + p);
                                    outputFile.flush();
                                }
                            }
                            outputFile.println("\t\t\t\tPredicateString: " + filterDesc.getPredicateString());
                            outputFile.flush();
                            FilterDesc.sampleDesc sample = filterDesc.getSampleDescr();
                            if (sample != null) {
                                outputFile.println("\t\t\t\tsampleDescToString: " + sample.toString());
                                outputFile.flush();
                                outputFile.println("\t\t\t\t\tinputPruning: " + sample.getInputPruning());
                                outputFile.flush();
                            }
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("FILESINK")) {
                    if (operator instanceof FileSinkOperator) {
                        outputFile.println("\n\t\t\tFileSink information...");
                        outputFile.flush();
                        FileSinkOperator fileSink = (FileSinkOperator) operator;
                        if (fileSink != null) {
                            FileSinkDesc fileSinkDesc = fileSink.getConf();
                            if (fileSinkDesc != null) {
                                outputFile.println("\t\t\t\tisStatsReliable: " + fileSinkDesc.isStatsReliable());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tcanBeMerged: " + fileSinkDesc.canBeMerged());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tgetCompressed: " + fileSinkDesc.getCompressed());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tisGatherStats: " + fileSinkDesc.isGatherStats());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tisLinkedFileSink: " + fileSinkDesc.isLinkedFileSink());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tisMultiFileSpray: " + fileSinkDesc.isMultiFileSpray());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tisRemovedReduceSinkBucketSort: " + fileSinkDesc.isRemovedReduceSinkBucketSort());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tisStatsCollectRawDataSize: " + fileSinkDesc.isStatsCollectRawDataSize());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tisStatsReliable: " + fileSinkDesc.isStatsReliable());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tisTemporary: " + fileSinkDesc.isTemporary());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tCompressCodec: " + fileSinkDesc.getCompressCodec());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tCompressType: " + fileSinkDesc.getCompressType());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tDestTableId: " + fileSinkDesc.getDestTableId());
                                outputFile.flush();

                                Path dirName = fileSinkDesc.getDirName();
                                if (dirName != null) {
                                    outputFile.println("\t\t\t\tdirName: " + dirName.toString());
                                    outputFile.flush();
                                }
                                Path finalDirName = fileSinkDesc.getFinalDirName();
                                if (finalDirName != null) {
                                    outputFile.println("\t\t\t\tfinalDirName: " + finalDirName.toString());
                                    outputFile.flush();
                                }

                                FileSinkDesc.DPSortState dpSortState = fileSinkDesc.getDpSortState();
                                if (dpSortState != null) {
                                    outputFile.println("\t\t\t\tDpSortState: " + dpSortState.toString());
                                    outputFile.flush();
                                }

                                outputFile.println("\t\t\t\tDestTableID: " + fileSinkDesc.getDestTableId());
                                outputFile.flush();
                                DynamicPartitionCtx dynamicPartitionCtx = fileSinkDesc.getDynPartCtx();
                                if (dynamicPartitionCtx != null) {
                                    outputFile.println("\t\t\t\tDynamicPartitionContext: ");
                                    outputFile.flush();
                                    outputFile.println("\t\t\t\t\tdefaultPartitionName: " + dynamicPartitionCtx.getDefaultPartitionName());
                                    outputFile.flush();
                                    if (dynamicPartitionCtx.getDPColNames() != null) {
                                        outputFile.println("\t\t\t\t\tDPColNames: " + dynamicPartitionCtx.getDPColNames().toString());
                                        outputFile.flush();
                                    }
                                    Map<String, String> inputToDPCols = dynamicPartitionCtx.getInputToDPCols();
                                    if (inputToDPCols != null) {
                                        outputFile.println("\t\t\t\t\tinputToDPCols: ");
                                        outputFile.flush();
                                        for (Map.Entry<String, String> entry : inputToDPCols.entrySet()) {
                                            outputFile.println("\t\t\t\t\t\tKey: " + entry.getKey() + " : " + entry.getValue());
                                            outputFile.flush();
                                        }
                                    }
                                    outputFile.println("\t\t\t\t\tNumBuckets: " + dynamicPartitionCtx.getNumBuckets());
                                    outputFile.flush();
                                    outputFile.println("\t\t\t\t\tMaxPartitionsPerNode: " + dynamicPartitionCtx.getMaxPartitionsPerNode());
                                    outputFile.flush();
                                    outputFile.println("\t\t\t\t\tNumSPCols: " + dynamicPartitionCtx.getNumSPCols());
                                    outputFile.flush();
                                    outputFile.println("\t\t\t\t\tNumDPCols: " + dynamicPartitionCtx.getNumDPCols());
                                    outputFile.flush();
                                    if (dynamicPartitionCtx.getRootPath() != null) {
                                        outputFile.println("\t\t\t\t\tRootPath: " + dynamicPartitionCtx.getRootPath());
                                        outputFile.flush();
                                    }
                                }

                                outputFile.println("\t\t\t\tStaticSpec: " + fileSinkDesc.getStaticSpec());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tStatsAggPrefix: " + fileSinkDesc.getStatsAggPrefix());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tTotalFiles: " + fileSinkDesc.getTotalFiles());
                                outputFile.flush();
                                List<FileSinkDesc> linkedFileDescs = fileSinkDesc.getLinkedFileSinkDesc();
                                if (linkedFileDescs != null) {
                                    outputFile.println("\t\t\t\tLinkedFileDescs: More information exists");
                                    outputFile.flush();
                                }
                                if (fileSinkDesc.getTable() != null) {
                                    Table table = fileSinkDesc.getTable();
                                    if (table != null) {
                                        outputFile.println("\t\t\t\tTableName: " + table.getCompleteName());
                                        outputFile.flush();
                                        outputFile.println("\t\t\t\t\tNOTE: MORE INFO CAN BE FOUND ");
                                        outputFile.flush();
                                    }
                                }
                            }
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("DEMUX")) {
                    if (operator instanceof DemuxOperator) {
                        outputFile.println("\n\t\t\tDemux information...");
                        outputFile.flush();
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("EXTRACT")) {
                    if (operator instanceof ExtractOperator) {
                        outputFile.println("\n\t\t\tExtract information...");
                        outputFile.flush();
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("FORWARD")) {
                    if (operator instanceof ForwardOperator) {
                        outputFile.println("\n\t\t\tForward information...");
                        outputFile.flush();
                        ForwardOperator forwardOperator = (ForwardOperator) operator;
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        if (operator instanceof ListSinkOperator) {
                            ListSinkOperator sink = (ListSinkOperator) operator;
                            if (sink != null) {
                                outputFile.println("\n\t\t\tListSink information...");
                                outputFile.flush();
                                outputFile.println("\n\t\t\t\tNumRows: " + sink.getNumRows());
                                outputFile.flush();
                            }
                        }
                    }
                } else if (opType.toString().equals("GROUPBY")) {
                    if (operator instanceof GroupByOperator) {
                        outputFile.println("\n\t\t\tGroupBy information...");
                        outputFile.flush();
                        GroupByOperator groupByOperator = (GroupByOperator) operator;
                        if (groupByOperator != null) {
                            outputFile.println("\t\t\t\tacceptLimitPushdown: " + groupByOperator.acceptLimitPushdown());
                            outputFile.flush();
                            GroupByDesc groupByDesc = groupByOperator.getConf();
                            if (groupByDesc != null) {
                                List<AggregationDesc> aggregationDescList = groupByDesc.getAggregators();
                                if (aggregationDescList != null) {
                                    outputFile.println("\t\t\t\taggregationDescList: ");
                                    outputFile.flush();
                                    for (AggregationDesc a : aggregationDescList) {
                                        if (a != null) {
                                            outputFile.println("\t\t\t\t\tAggregation: ");
                                            outputFile.flush();
                                            outputFile.println("\t\t\t\t\t\tgetDistinct: " + a.getDistinct());
                                            outputFile.flush();
                                            if (a.getExprString() != null) {
                                                outputFile.println("\t\t\t\t\t\tExprString: " + a.getExprString());
                                                outputFile.flush();
                                            }
                                            GenericUDAFEvaluator genericUDAFEvaluator = a.getGenericUDAFEvaluator();
                                            if (genericUDAFEvaluator != null) {
                                                outputFile.println("\t\t\t\t\t\tgenericUDAFEvaluator(toString): " + genericUDAFEvaluator.toString());
                                                outputFile.flush();
                                            }
                                            outputFile.println("\t\t\t\t\t\tGenericUDAFEvaluatorClassName: " + a.getGenericUDAFEvaluatorClassName());
                                            outputFile.flush();
                                            outputFile.println("\t\t\t\t\t\tGenericUDAFName: " + a.getGenericUDAFName());
                                            outputFile.flush();
                                            GenericUDAFEvaluator.Mode mode = a.getMode();
                                            if (mode != null) {
                                                outputFile.println("\t\t\t\t\t\tMode: " + mode.toString());
                                                outputFile.flush();
                                            }
                                            ArrayList<ExprNodeDesc> parameters = a.getParameters();
                                            if (parameters != null) {
                                                outputFile.println("\t\t\t\t\t\tParameters: ");
                                                outputFile.flush();
                                                for (ExprNodeDesc p : parameters) {
                                                    outputFile.println("\t\t\t\t\t\t\tParameter: ");
                                                    outputFile.flush();
                                                    outputFile.println("\t\t\t\t\t\t\t\tName: " + p.getName());
                                                    outputFile.flush();
                                                    List<String> cols = p.getCols();
                                                    if (cols != null) {
                                                        outputFile.println("\t\t\t\t\t\t\t\tCols: " + cols.toString());
                                                        outputFile.flush();
                                                    } else {
                                                        outputFile.println("\t\t\t\t\t\t\t\tCols: NULL");
                                                        outputFile.flush();
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                List<String> aggStrings = groupByDesc.getAggregatorStrings();
                                if (aggStrings != null) {
                                    outputFile.println("\t\t\t\taggregatorStrings: " + aggStrings.toString());
                                    outputFile.flush();
                                }
                                outputFile.println("\t\t\t\tBucketGroup: " + groupByDesc.getBucketGroup());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tGroupingSetPosition: " + groupByDesc.getGroupingSetPosition());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tGroupKeyNotReductionKey: " + groupByDesc.getGroupKeyNotReductionKey());
                                outputFile.flush();
                                ArrayList<ExprNodeDesc> keys = groupByDesc.getKeys();
                                if (keys != null) {
                                    outputFile.println("\t\t\t\tKeys: ");
                                    outputFile.flush();
                                    for (ExprNodeDesc k : keys) {
                                        if (k != null) {
                                            outputFile.println("\t\t\t\t\tKey: ");
                                            outputFile.flush();
                                            outputFile.println("\t\t\t\t\t\tName: " + k.getName());
                                            outputFile.flush();
                                            if (k.getCols() != null) {
                                                outputFile.println("\t\t\t\t\t\tCols: " + k.getCols().toString());
                                                outputFile.flush();
                                            } else {
                                                outputFile.println("\t\t\t\t\t\tCols: NULL");
                                                outputFile.flush();
                                            }
                                        }
                                    }
                                }
                                String keyString = groupByDesc.getKeyString();
                                if (keyString != null) {
                                    outputFile.println("\t\t\t\tKeyString: " + keyString);
                                    outputFile.flush();
                                }
                                String modeString = groupByDesc.getModeString();
                                if (modeString != null) {
                                    outputFile.println("\t\t\t\tModeString: " + modeString);
                                    outputFile.flush();
                                }
                                List<Integer> groupingSets = groupByDesc.getListGroupingSets();
                                if (groupingSets != null) {
                                    outputFile.println("\t\t\t\tGroupingSets: " + groupingSets.toString());
                                    outputFile.flush();
                                }
                                ArrayList<String> outputCols = groupByDesc.getOutputColumnNames();
                                if (outputCols != null) {
                                    outputFile.println("\t\t\t\tOutputColumns: ");
                                    outputFile.flush();
                                    for (String s : outputCols) {
                                        outputFile.println("\t\t\t\t\tColumn " + s);
                                        outputFile.flush();
                                    }
                                }
                                outputFile.println("\t\t\t\tisDistinctLike: " + groupByDesc.isDistinctLike());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tisDontResetAggrsDistinct: " + groupByDesc.isDontResetAggrsDistinct());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tisGroupingSetsPresent: " + groupByDesc.isGroupingSetsPresent());
                                outputFile.flush();
                            }
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("HASHTABLEDUMMY")) {
                    if (operator instanceof HashTableDummyOperator) {
                        outputFile.println("\n\t\t\tHashTableDummy information...");
                        outputFile.flush();
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("HASHTABLESINK")) {
                    if (operator instanceof HashTableSinkOperator) {
                        outputFile.println("\n\t\t\tHashTableSink information...");
                        outputFile.flush();
                        HashTableSinkOperator hashTableSinkOperator = (HashTableSinkOperator) operator;
                        if (hashTableSinkOperator != null) {
                            MapJoinTableContainer[] mapJoinTableContainers = hashTableSinkOperator.getMapJoinTables();
                            if (mapJoinTableContainers != null) {
                                outputFile.println("\t\t\t\tMapJoinTableContainers: ");
                                outputFile.flush();
                                for (MapJoinTableContainer mapJoinTableContainer : mapJoinTableContainers) {
                                    if (mapJoinTableContainer != null) {
                                        outputFile.println("\n\t\t\t\t\t" + mapJoinTableContainer.toString());
                                    }
                                }
                            }
                            HashTableSinkDesc hashDesc = hashTableSinkOperator.getConf();
                            if (hashDesc != null) {
                                outputFile.println("\t\t\t\tisMapSideJoin: " + hashDesc.isMapSideJoin());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tisLeftInputJoin: " + hashDesc.isLeftInputJoin());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tisHandleSkewJoin: " + hashDesc.isHandleSkewJoin());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tisNoOuterJoin: " + hashDesc.isNoOuterJoin());
                                outputFile.flush();
                                Map<Byte, Path> bigKeysDirMap = hashDesc.getBigKeysDirMap();
                                if (bigKeysDirMap != null) {
                                    outputFile.println("\t\t\t\tBigKeysDirMap: ");
                                    outputFile.flush();
                                    for (Map.Entry<Byte, Path> entry : bigKeysDirMap.entrySet()) {
                                        if (entry != null) {
                                            if (entry.getValue() != null) {
                                                outputFile.println("\t\t\t\t\tKey: " + entry.getKey() + " Path: " + entry.getValue().toString());
                                                outputFile.flush();
                                            } else {
                                                outputFile.println("\t\t\t\t\tKey: " + entry.getKey() + " Path: NULL");
                                                outputFile.flush();
                                            }
                                        }
                                    }
                                }
                                JoinCondDesc[] joinDescArray = hashDesc.getConds();
                                if (joinDescArray != null) {
                                    outputFile.println("\t\t\t\tJoinCondDescs: ");
                                    outputFile.flush();
                                    for (JoinCondDesc j : joinDescArray) {
                                        if (j != null) {
                                            outputFile.println("\t\t\t\t\tJoinCondDesc: ");
                                            outputFile.flush();
                                            outputFile.println("\t\t\t\t\t\tJoinCondString: " + j.getJoinCondString());
                                            outputFile.flush();
                                            outputFile.println("\t\t\t\t\t\tLeft: " + j.getLeft());
                                            outputFile.flush();
                                            outputFile.println("\t\t\t\t\t\tRight: " + j.getRight());
                                            outputFile.flush();
                                            outputFile.println("\t\t\t\t\t\tPreserved: " + j.getPreserved());
                                            outputFile.flush();
                                        }
                                    }
                                } //TODO
                                List<String> outputCols = hashDesc.getOutputColumnNames();
                                if (outputCols != null) {
                                    outputFile.println("\t\t\t\toutputCols: " + outputCols.toString());
                                    outputFile.flush();
                                }
                                Map<Byte, List<ExprNodeDesc>> keys = hashDesc.getKeys();
                                if (keys != null) {
                                    outputFile.println("\t\t\t\tKeys: ");
                                    outputFile.flush();
                                    for (Map.Entry<Byte, List<ExprNodeDesc>> entry : keys.entrySet()) {
                                        outputFile.println("\t\t\t\t\tKey: " + entry.getKey());
                                        outputFile.flush();
                                        if (entry.getValue() != null) {
                                            outputFile.println("\t\t\t\t\t\tList of ExprNodeDescs: ");
                                            outputFile.flush();
                                            for (ExprNodeDesc e : entry.getValue()) {
                                                if (e != null) {
                                                    outputFile.println("\t\t\t\t\t\t\tName: " + e.getName());
                                                    outputFile.flush();
                                                    if (e.getCols() != null) {
                                                        outputFile.println("\t\t\t\t\t\t\tCols: " + e.getCols().toString());
                                                        outputFile.flush();
                                                    } else {
                                                        outputFile.println("\t\t\t\t\t\t\tCols: NULL");
                                                        outputFile.flush();
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                Map<Byte, TableDesc> skewKeysValueTables = hashDesc.getSkewKeysValuesTables();
                                if (skewKeysValueTables != null) {
                                    outputFile.println("\t\t\t\tskewKeysValueTables: ");
                                    outputFile.flush();
                                    for (Map.Entry<Byte, TableDesc> entry : skewKeysValueTables.entrySet()) {
                                        if (entry != null) {
                                            outputFile.println("\t\t\t\t\tKey: " + entry.getKey());
                                            outputFile.flush();
                                            if (entry.getValue() != null) {
                                                outputFile.println("\t\t\t\t\t\tValue: " + entry.getValue().getTableName());
                                                outputFile.flush();
                                            }
                                        }
                                    }
                                }
                                List<TableDesc> valueTblDescs = hashDesc.getValueTblDescs();
                                if (valueTblDescs != null) {
                                    outputFile.println("\t\t\t\tValueTblDescs: ");
                                    outputFile.flush();
                                    for (TableDesc v : valueTblDescs) {
                                        if (v != null) {
                                            outputFile.println("\t\t\t\t\tTableDesc: ");
                                            outputFile.flush();
                                            outputFile.println("\t\t\t\t\t\tName: " + v.getTableName());
                                            outputFile.flush();
                                        }
                                    }
                                }
                                List<TableDesc> valueFilteredTblDescs = hashDesc.getValueTblFilteredDescs();
                                if (valueFilteredTblDescs != null) {
                                    outputFile.println("\t\t\t\tValueFilteredTblDescs: ");
                                    outputFile.flush();
                                    for (TableDesc v : valueFilteredTblDescs) {
                                        if (v != null) {
                                            outputFile.println("\t\t\t\t\tTableDesc: ");
                                            outputFile.flush();
                                            outputFile.println("\t\t\t\t\t\tName: " + v.getTableName());
                                            outputFile.flush();
                                        }
                                    }
                                }

                                Map<Byte, String> keys2 = hashDesc.getKeysString();
                                if (keys2 != null) {
                                    outputFile.println("\t\t\t\tKeys (From getKeyString()): ");
                                    outputFile.flush();
                                    for (Map.Entry<Byte, String> entry : keys2.entrySet()) {
                                        outputFile.println("\t\t\t\t\tKey: " + entry.getKey() + " : Value: " + entry.getValue());
                                        outputFile.flush();
                                    }
                                }

                            }
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("JOIN")) {
                    if (operator instanceof JoinOperator) {
                        outputFile.println("\n\t\t\tJoin information...");
                        outputFile.flush();
                        JoinOperator joinOperator = (JoinOperator) operator;
                        outputFile.println("\t\t\t\topAllowedBeforeSortMergeJoin: " + joinOperator.opAllowedBeforeSortMergeJoin());
                        outputFile.println("\t\t\t\tsupportSkewJoinOptimization: " + joinOperator.supportSkewJoinOptimization());
                        JoinDesc joinDesc = joinOperator.getConf();
                        if (joinDesc != null) {
                            List<String> outputCols = joinDesc.getOutputColumnNames();
                            if (outputCols != null) {
                                outputFile.println("\t\t\t\toutputCols: " + outputCols.toString());
                                outputFile.flush();
                            }
                            JoinCondDesc[] joinCondDescs = joinDesc.getConds();
                            if (joinCondDescs != null) {
                                outputFile.println("\t\t\t\tJoinCondDescs: ");
                                outputFile.flush();
                                for (JoinCondDesc j : joinCondDescs) {
                                    if (j != null) {
                                        outputFile.println("\t\t\t\t\tLeft: " + j.getLeft());
                                        outputFile.flush();
                                        outputFile.println("\t\t\t\t\tRight: " + j.getRight());
                                        outputFile.flush();
                                        outputFile.println("\t\t\t\t\tType: " + j.getType());
                                        outputFile.flush();
                                        outputFile.println("\t\t\t\t\tPreserved: " + j.getPreserved());
                                        outputFile.flush();
                                        if (j.getJoinCondString() != null) {
                                            outputFile.println("\t\t\t\t\tJoinCondString: " + j.getJoinCondString());
                                            outputFile.flush();
                                        }
                                    }
                                }
                            }
                            Map<Byte, Path> bigKeysDirMap = joinDesc.getBigKeysDirMap();
                            if (bigKeysDirMap != null) {
                                outputFile.println("\t\t\t\tBigKeysDirMap: ");
                                outputFile.flush();
                                for (Map.Entry<Byte, Path> entry : bigKeysDirMap.entrySet()) {
                                    if (entry != null) {
                                        Path value = entry.getValue();
                                        if (value != null) {
                                            outputFile.println("\t\t\t\t\tKey= " + entry.getKey() + " : Value= " + value.toString());
                                            outputFile.flush();
                                        } else {
                                            outputFile.println("\t\t\t\t\tKey= " + entry.getKey() + " : Value= NULL");
                                            outputFile.flush();
                                        }
                                    }
                                }
                            }
                            Map<Byte, String> keys = joinDesc.getKeysString();
                            if (keys != null) {
                                outputFile.println("\t\t\t\tKeys: ");
                                outputFile.flush();
                                for (Map.Entry<Byte, String> entry : keys.entrySet()) {
                                    outputFile.println("\t\t\t\t\tKey: " + entry.getKey() + " : Value: " + entry.getValue());
                                    outputFile.flush();
                                }
                            }
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("LATERALVIEWFORWARD")) {
                    if (operator instanceof LateralViewForwardOperator) {
                        outputFile.println("\n\t\t\tLateralViewForward information...");
                        outputFile.flush();
                        LateralViewForwardOperator lateralViewForwardOperator = (LateralViewForwardOperator) operator;
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("LATERALVIEWJOIN")) {
                    if (operator instanceof LateralViewJoinOperator) {
                        outputFile.println("\n\t\t\tLateralViewJoin information...");
                        outputFile.flush();
                        LateralViewJoinOperator lateralViewJoinOperator = (LateralViewJoinOperator) operator;
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("LIMIT")) {
                    if (operator instanceof LimitOperator) {
                        outputFile.println("\n\t\t\tLimit information...");
                        outputFile.flush();
                        LimitOperator limitOperator = (LimitOperator) operator;
                        LimitDesc limitDesc = limitOperator.getConf();
                        if (limitDesc != null) {
                            outputFile.println("\t\t\t\tLimit: " + limitDesc.getLimit());
                            outputFile.flush();
                            outputFile.println("\t\t\t\tLimit: " + limitDesc.getLeastRows());
                            outputFile.flush();
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("MAPJOIN")) {
                    if (operator instanceof MapJoinOperator) {
                        outputFile.println("\n\t\t\tMapJoin information...");
                        outputFile.flush();
                        MapJoinOperator mapJoinOperator = (MapJoinOperator) operator;
                        MapJoinDesc mapJoinDesc = mapJoinOperator.getConf();
                        if (mapJoinDesc != null) {
                            List<String> outputCols = mapJoinDesc.getOutputColumnNames();
                            if (outputCols != null) {
                                outputFile.println("\t\t\t\toutputCols: " + outputCols.toString());
                                outputFile.flush();
                            }
                            outputFile.println("\t\t\t\tisBucketMapJoin: " + mapJoinDesc.isBucketMapJoin());
                            outputFile.flush();
                            JoinCondDesc[] joinCondDescs = mapJoinDesc.getConds();
                            if (joinCondDescs != null) {
                                outputFile.println("\t\t\t\tJoinCondDescs: ");
                                outputFile.flush();
                                for (JoinCondDesc j : joinCondDescs) {
                                    if (j != null) {
                                        outputFile.println("\t\t\t\t\tLeft: " + j.getLeft());
                                        outputFile.flush();
                                        outputFile.println("\t\t\t\t\tRight: " + j.getRight());
                                        outputFile.flush();
                                        outputFile.println("\t\t\t\t\tType: " + j.getType());
                                        outputFile.flush();
                                        outputFile.println("\t\t\t\t\tPreserved: " + j.getPreserved());
                                        outputFile.flush();
                                        if (j.getJoinCondString() != null) {
                                            outputFile.println("\t\t\t\t\tJoinCondString: " + j.getJoinCondString());
                                            outputFile.flush();
                                        }
                                    }
                                }
                            }
                            Map<Byte, Path> bigKeysDirMap = mapJoinDesc.getBigKeysDirMap();
                            if (bigKeysDirMap != null) {
                                outputFile.println("\t\t\t\tBigKeysDirMap: ");
                                outputFile.flush();
                                for (Map.Entry<Byte, Path> entry : bigKeysDirMap.entrySet()) {
                                    if (entry != null) {
                                        Path value = entry.getValue();
                                        if (value != null) {
                                            outputFile.println("\t\t\t\t\tKey= " + entry.getKey() + " : Value= " + value.toString());
                                            outputFile.flush();
                                        } else {
                                            outputFile.println("\t\t\t\t\tKey= " + entry.getKey() + " : Value= NULL");
                                            outputFile.flush();
                                        }
                                    }
                                }
                            }
                            outputFile.println("\t\t\t\tisNoOuterJoin: " + mapJoinDesc.isNoOuterJoin());
                            outputFile.flush();
                            outputFile.println("\t\t\t\tBigTableAlias: " + mapJoinDesc.getBigTableAlias());
                            outputFile.flush();
                            if (mapJoinDesc.getKeyCountsExplainDesc() != null) {
                                outputFile.println("\t\t\t\tKeyCountsExplainDesc: " + mapJoinDesc.getKeyCountsExplainDesc());
                                outputFile.flush();
                            }
                            Map<String, List<String>> bigTablePart = mapJoinDesc.getBigTablePartSpecToFileMapping();
                            if (bigTablePart != null) {
                                outputFile.println("\t\t\t\tBigTablePartSpecToFileMapping: ");
                                outputFile.flush();
                                for (Map.Entry<String, List<String>> entry : bigTablePart.entrySet()) {
                                    if (entry != null) {
                                        outputFile.println("\t\t\t\tKey: " + entry.getKey());
                                        outputFile.flush();
                                        List<String> valueList = entry.getValue();
                                        if (valueList != null) {
                                            outputFile.println("\t\t\t\t\tValueList: " + valueList.toString());
                                            outputFile.flush();
                                        } else {
                                            outputFile.println("\t\t\t\t\tValueList: NULL");
                                            outputFile.flush();
                                        }
                                    }
                                }
                            }
                            Map<Byte, List<ExprNodeDesc>> keys = mapJoinDesc.getKeys();
                            if (keys != null) {
                                outputFile.println("\t\t\t\tKeys: ");
                                outputFile.flush();
                                for (Map.Entry<Byte, List<ExprNodeDesc>> entry : keys.entrySet()) {
                                    outputFile.println("\t\t\t\t\tKey: " + entry.getKey());
                                    outputFile.flush();
                                    if (entry.getValue() != null) {
                                        outputFile.println("\t\t\t\t\t\tList of ExprNodeDescs: ");
                                        outputFile.flush();
                                        for (ExprNodeDesc e : entry.getValue()) {
                                            if (e != null) {
                                                outputFile.println("\t\t\t\t\t\t\tName: " + e.getName());
                                                outputFile.flush();
                                                if (e.getCols() != null) {
                                                    outputFile.println("\t\t\t\t\t\t\tCols: " + e.getCols().toString());
                                                    outputFile.flush();
                                                } else {
                                                    outputFile.println("\t\t\t\t\t\t\tCols: NULL");
                                                    outputFile.flush();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Map<Byte, int[]> valueIndices = mapJoinDesc.getValueIndices();
                            if (valueIndices != null) {
                                outputFile.println("\t\t\t\tValueIndices: ");
                                outputFile.flush();
                                for (Map.Entry<Byte, int[]> entry : valueIndices.entrySet()) {
                                    if (entry != null) {
                                        outputFile.println("\t\t\t\t\tKey: " + entry.getKey());
                                        outputFile.flush();
                                        if (entry.getValue() != null) {
                                            outputFile.println("\t\t\t\t\t\tValue: " + entry.getValue().toString());
                                            outputFile.flush();
                                        }
                                    }
                                }
                            }
                            List<TableDesc> valueTblDescs = mapJoinDesc.getValueTblDescs();
                            if (valueTblDescs != null) {
                                outputFile.println("\t\t\t\tValueTblDescs: ");
                                outputFile.flush();
                                for (TableDesc v : valueTblDescs) {
                                    if (v != null) {
                                        outputFile.println("\t\t\t\t\tTableDesc: ");
                                        outputFile.flush();
                                        outputFile.println("\t\t\t\t\t\tName: " + v.getTableName());
                                        outputFile.flush();
                                    }
                                }
                            }
                            List<TableDesc> valueFilteredTblDescs = mapJoinDesc.getValueFilteredTblDescs();
                            if (valueFilteredTblDescs != null) {
                                outputFile.println("\t\t\t\tValueFilteredTblDescs: ");
                                outputFile.flush();
                                for (TableDesc v : valueFilteredTblDescs) {
                                    if (v != null) {
                                        outputFile.println("\t\t\t\t\tTableDesc: ");
                                        outputFile.flush();
                                        outputFile.println("\t\t\t\t\t\tName: " + v.getTableName());
                                        outputFile.flush();
                                    }
                                }
                            }

                            Map<Byte, String> keys2 = mapJoinDesc.getKeysString();
                            if (keys2 != null) {
                                outputFile.println("\t\t\t\tKeys (From getKeyString()): ");
                                outputFile.flush();
                                for (Map.Entry<Byte, String> entry : keys2.entrySet()) {
                                    outputFile.println("\t\t\t\t\tKey: " + entry.getKey() + " : Value: " + entry.getValue());
                                    outputFile.flush();
                                }
                            }

                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("MUX")) {
                    if (operator instanceof MuxOperator) {
                        outputFile.println("\n\t\t\tMux information...");
                        outputFile.flush();
                        MuxOperator muxOperator = (MuxOperator) operator;
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("PTF")) {
                    if (operator instanceof PTFOperator) {
                        outputFile.println("\n\t\t\tPTF information...");
                        outputFile.flush();
                        PTFOperator ptfOperator = (PTFOperator) operator;
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("REDUCESINK")) {
                    if (operator instanceof ReduceSinkOperator) {
                        outputFile.println("\n\t\t\tReduceSink information...");
                        outputFile.flush();
                        ReduceSinkOperator reduceSinkOperator = (ReduceSinkOperator) operator;
                        if (reduceSinkOperator != null) {
                            String[] inputAliases = reduceSinkOperator.getInputAliases();
                            if (inputAliases != null) {
                                outputFile.println("\t\t\t\tInputAliases: " + inputAliases.toString());
                                outputFile.flush();
                            }
                            int[] valueIndeces = reduceSinkOperator.getValueIndex();
                            if (valueIndeces != null) {
                                outputFile.println("\t\t\t\tValueIndex: " + valueIndeces.toString());
                                outputFile.flush();
                            }
                            outputFile.println("\t\t\t\tOpAllowedBeforeMapJoin: " + reduceSinkOperator.opAllowedBeforeMapJoin());
                            outputFile.flush();
                            ReduceSinkDesc reduceSinkDesc = reduceSinkOperator.getConf();
                            if (reduceSinkDesc != null) {
                                outputFile.println("\t\t\t\tisPTFReduceSink: " + reduceSinkDesc.isPTFReduceSink());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tisMapGroupBy: " + reduceSinkDesc.isMapGroupBy());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tisEnforceSort: " + reduceSinkDesc.isEnforceSort());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tisAutoParallel: " + reduceSinkDesc.isAutoParallel());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tgetSkipTag: " + reduceSinkDesc.getSkipTag());
                                outputFile.flush();
                                if (reduceSinkDesc.getKeyColString() != null) {
                                    outputFile.println("\t\t\t\tKeyColString: " + reduceSinkDesc.getKeyColString());
                                    outputFile.flush();
                                }
                                List<ExprNodeDesc> keyCols = reduceSinkDesc.getKeyCols();
                                if (keyCols != null) {
                                    outputFile.println("\t\t\t\tKeyCols: ");
                                    outputFile.flush();
                                    for (ExprNodeDesc v : keyCols) {
                                        if (v != null) {
                                            outputFile.println("\t\t\t\t\tName: " + v.getName());
                                            outputFile.flush();
                                            if (v.getCols() != null)
                                                outputFile.println("\t\t\t\t\tCols: " + v.getCols().toString());
                                            else
                                                outputFile.println("\t\t\t\t\tCols: NULL");
                                            outputFile.flush();
                                        }
                                    }
                                }
                                if (reduceSinkDesc.getValueCols() != null) {
                                    outputFile.println("\t\t\t\tValueColsString: " + reduceSinkDesc.getValueColsString());
                                    outputFile.flush();
                                }
                                ArrayList<ExprNodeDesc> valueCols = reduceSinkDesc.getValueCols();
                                if (valueCols != null) {
                                    outputFile.println("\t\t\t\tValueCols: ");
                                    outputFile.flush();
                                    for (ExprNodeDesc v : valueCols) {
                                        if (v != null) {
                                            outputFile.println("\t\t\t\t\tName: " + v.getName());
                                            outputFile.flush();
                                            if (v.getCols() != null)
                                                outputFile.println("\t\t\t\t\tCols: " + v.getCols().toString());
                                            else
                                                outputFile.println("\t\t\t\t\tCols: NULL");
                                            outputFile.flush();
                                        }
                                    }
                                }

                                List<ExprNodeDesc> bucketCols = reduceSinkDesc.getBucketCols();
                                if (bucketCols != null) {
                                    outputFile.println("\t\t\t\tBucketCols: ");
                                    outputFile.flush();
                                    for (ExprNodeDesc v : bucketCols) {
                                        if (v != null) {
                                            outputFile.println("\t\t\t\t\tName: " + v.getName());
                                            outputFile.flush();
                                            if (v.getCols() != null)
                                                outputFile.println("\t\t\t\t\tCols: " + v.getCols().toString());
                                            else
                                                outputFile.println("\t\t\t\t\tCols: NULL");
                                            outputFile.flush();
                                        }
                                    }
                                }

                                outputFile.println("\t\t\t\tNumBuckets: " + reduceSinkDesc.getNumBuckets());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tNumDistributionKeys: " + reduceSinkDesc.getNumDistributionKeys());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tNumReducers: " + reduceSinkDesc.getNumReducers());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tOrder: " + reduceSinkDesc.getOrder());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tOutputName: " + reduceSinkDesc.getOutputName());
                                outputFile.flush();
                                List<String> outputKeyColumnNames = reduceSinkDesc.getOutputKeyColumnNames();
                                if (outputKeyColumnNames != null) {
                                    outputFile.println("\t\t\t\toutputKeyColumnNames: " + outputKeyColumnNames.toString());
                                    outputFile.flush();
                                }
                                List<String> outputValueColumnNames = reduceSinkDesc.getOutputValueColumnNames();
                                if (outputValueColumnNames != null) {
                                    outputFile.println("\t\t\t\toutputValueColumnNames: " + outputValueColumnNames.toString());
                                    outputFile.flush();
                                }
                                if (reduceSinkDesc.getParitionColsString() != null) {
                                    outputFile.println("\t\t\t\tPartitionColsString: " + reduceSinkDesc.getParitionColsString());
                                    outputFile.flush();
                                }
                            }
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("SCRIPT")) {
                    if (operator instanceof ScriptOperator) {
                        outputFile.println("\n\t\t\tScriptOperator information...");
                        outputFile.flush();
                        //ScriptOperator scriptOperator = (ScriptOperator) operator;
                        //ScriptDesc scriptDesc = scriptOperator.getConf();
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("SELECT")) {
                    if (operator instanceof SelectOperator) {
                        outputFile.println("\n\t\t\tSelect information...");
                        outputFile.flush();
                        SelectOperator selectOperator = (SelectOperator) operator;
                        if (selectOperator != null) {
                            outputFile.println("\t\t\t\tsupportSkewJoinOptimization: " + selectOperator.supportSkewJoinOptimization());
                            outputFile.flush();
                            outputFile.println("\t\t\t\tacceptLimitPushdown: " + selectOperator.acceptLimitPushdown());
                            outputFile.flush();
                            outputFile.println("\t\t\t\tcolumnNamesRowResolvedCanBeObtained: " + selectOperator.columnNamesRowResolvedCanBeObtained());
                            outputFile.flush();
                            outputFile.println("\t\t\t\tisIdentitySelect: " + selectOperator.isIdentitySelect());
                            outputFile.flush();
                            outputFile.println("\t\t\t\tsupportAutomaticSortMergeJoin: " + selectOperator.supportAutomaticSortMergeJoin());
                            outputFile.flush();
                            outputFile.println("\t\t\t\tsupportUnionRemoveOptimization: " + selectOperator.supportUnionRemoveOptimization());
                            outputFile.flush();
                            SelectDesc selectDesc = selectOperator.getConf();
                            if (selectDesc != null) {
                                outputFile.println("\t\t\t\tisSelStarNoCompute: " + selectDesc.isSelStarNoCompute());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tisSelectStar: " + selectDesc.isSelectStar());
                                outputFile.flush();
                                List<String> outputColNames = selectDesc.getOutputColumnNames();
                                if (outputColNames != null) {
                                    outputFile.println("\t\t\t\toutputColNames: " + outputColNames.toString());
                                    outputFile.flush();
                                }
                                outputFile.println("\t\t\t\tcolListString: " + selectDesc.getColListString());
                                outputFile.flush();
                                outputFile.println("\t\t\t\texplainNoCompute: " + selectDesc.explainNoCompute());
                                outputFile.flush();
                            }
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("UDTF")) {
                    if (operator instanceof UDTFOperator) {
                        outputFile.println("\n\t\t\tUTDF information...");
                        outputFile.flush();
                        UDTFOperator udtfOperator = (UDTFOperator) operator;
                        UDTFDesc udtfDesc = udtfOperator.getConf();
                        if (udtfDesc != null) {
                            outputFile.println("\n\t\t\t\tisOuterLV: " + udtfDesc.isOuterLV());
                            outputFile.flush();
                            outputFile.println("\n\t\t\t\tUDTFName: " + udtfDesc.getUDTFName());
                            outputFile.flush();
                            outputFile.println("\n\t\t\t\tisOuterLateralView: " + udtfDesc.isOuterLateralView());
                            outputFile.flush();
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (operator instanceof ListSinkOperator) {
                    outputFile.println("\n\t\t\tListSink information...");
                    outputFile.flush();
                    ListSinkOperator listSinkOperator = (ListSinkOperator) operator;

                    if (listSinkOperator != null) {
                        outputFile.println("\n\t\t\t\tNumRows: " + listSinkOperator.getNumRows());
                        outputFile.flush();
                    }
                } else if (opType.toString().equals("UNION")) {
                    if (operator instanceof UnionOperator) {
                        outputFile.println("\n\t\t\tUnion information...");
                        outputFile.flush();
                        UnionOperator unionOperator = (UnionOperator) operator;
                        if (unionOperator != null) {
                            outputFile.println("\t\t\t\topAllowedAfterMapJoin: " + unionOperator.opAllowedAfterMapJoin());
                            outputFile.flush();
                            outputFile.println("\t\t\t\topAllowedBeforeMapJoin: " + unionOperator.opAllowedBeforeMapJoin());
                            outputFile.flush();
                            outputFile.println("\t\t\t\topAllowedBeforeSortMergeJoin: " + unionOperator.opAllowedBeforeSortMergeJoin());
                            outputFile.flush();
                            UnionDesc unionDesc = unionOperator.getConf();
                            if (unionDesc != null) {
                                outputFile.println("\t\t\t\tNumInputs: " + unionDesc.getNumInputs());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tisAllInputsInSameReducer: " + unionDesc.isAllInputsInSameReducer());
                                outputFile.flush();
                            }
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else {
                    outputFile.println("\n\t\t\tUnknown Operator Type: " + opType.toString());
                    outputFile.flush();
                }
            }
        }
        else{
            outputFile.println("\t\t------------------------OPERATOR: " + specialName + " ----------------------------------");
            outputFile.flush();
            if(this.getOperatorType().equals("Stats-Aggr Operator")){
                if(ownerStage instanceof StatsTask) {
                    StatsTask statsTask = (StatsTask) ownerStage;
                    if (statsTask != null) {
                        StatsWork statsWork = statsTask.getWork();
                        if(statsWork != null){
                            outputFile.println("\t\t\tAggKey: "+statsWork.getAggKey());
                            outputFile.flush();
                            outputFile.println("\t\t\tisStatsReliable: "+statsWork.isStatsReliable());
                            outputFile.flush();
                            outputFile.println("\t\t\tisClearAggregatorStats: "+statsWork.isClearAggregatorStats());
                            outputFile.flush();
                            outputFile.println("\t\t\tisNoScanAnalyzeCommand: "+statsWork.isNoScanAnalyzeCommand());
                            outputFile.flush();
                            outputFile.println("\t\t\tisPartialScanAnalyzeCommand: "+statsWork.isPartialScanAnalyzeCommand());
                            outputFile.flush();
                            outputFile.println("\t\t\tNoStatsAggregator: "+statsWork.getNoStatsAggregator());
                            outputFile.flush();
                            Task<?> sourceTask = statsWork.getSourceTask();
                            if(sourceTask != null){
                                outputFile.println("\t\t\tSourceTask: ");
                                outputFile.flush();
                                outputFile.println("\t\t\t\tID: "+sourceTask.getId());
                                outputFile.flush();
                                List<Task<?>> childTasks = sourceTask.getChildTasks();
                                if(childTasks != null){
                                    outputFile.println("\t\t\t\tChildTasks: "+childTasks.toString());
                                    outputFile.flush();
                                }
                                outputFile.println("\t\t\t\tTop Operators...");
                                outputFile.flush();
                                if (sourceTask.getTopOperators() != null) {
                                    if (sourceTask.getTopOperators().size() > 0) {
                                        for (Object o : sourceTask.getTopOperators()) {
                                            if (o != null) {
                                                outputFile.println("\t\t\t\t\tOperator: " + ((org.apache.hadoop.hive.ql.exec.Operator<? extends Serializable>) o).getOperatorId());
                                                outputFile.flush();
                                            } else {
                                                outputFile.println("\t\t\t\t\tOperator is NULL!");
                                                outputFile.flush();
                                            }
                                        }
                                    }
                                }
                            }
                            LoadFileDesc loadFileDesc = statsWork.getLoadFileDesc();
                            if(loadFileDesc != null){
                                outputFile.println("\t\t\tLoadFileDesc: ");
                                outputFile.flush();
                                outputFile.println("\t\t\t\tColumns: "+loadFileDesc.getColumns());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tColumnTypes: "+loadFileDesc.getColumnTypes());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tDestinationCreateTable: "+loadFileDesc.getDestinationCreateTable());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tIsDfsDir: "+loadFileDesc.getIsDfsDir());
                                outputFile.flush();
                                Path targetDir = loadFileDesc.getTargetDir();
                                if(targetDir != null){
                                    outputFile.println("\t\t\t\tTableDir: "+targetDir.toString());
                                    outputFile.flush();
                                }
                            }
                            LoadTableDesc loadTableDesc = statsWork.getLoadTableDesc();
                            if(loadTableDesc != null){
                                outputFile.println("\t\t\tLoadTableDesc: ");
                                outputFile.flush();
                                TableDesc tableDesc = loadTableDesc.getTable();
                                if(tableDesc != null){
                                    outputFile.println("\t\t\t\tTableName: "+tableDesc.getTableName());
                                    outputFile.flush();
                                }
                                outputFile.println("\t\t\t\tHoldDDLTime: "+loadTableDesc.getHoldDDLTime());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tInheritTableSpecs: "+loadTableDesc.getInheritTableSpecs());
                                outputFile.flush();
                                outputFile.println("\t\t\t\tReplace: "+loadTableDesc.getReplace());
                                outputFile.flush();
                                Map<String, String> partitionSpec = loadTableDesc.getPartitionSpec();
                                if(partitionSpec != null){
                                    outputFile.println("\t\t\t\tPartitionSpec: ");
                                    outputFile.flush();
                                    for(Map.Entry<String, String> entry : partitionSpec.entrySet()){
                                        if(entry != null){
                                            if(entry.getKey() != null) {
                                                if(entry.getValue() != null) {
                                                    outputFile.println("\t\t\t\t\tKey=" + entry.getKey() + " : Value=" + entry.getValue());
                                                    outputFile.flush();
                                                }
                                                else{
                                                    outputFile.println("\t\t\t\t\tKey=" + entry.getKey() + " : Value=NULL");
                                                    outputFile.flush();
                                                }
                                            }
                                        }
                                    }
                                }

                                DynamicPartitionCtx dynamicPartitionCtx = loadTableDesc.getDPCtx();
                                if (dynamicPartitionCtx != null) {
                                    outputFile.println("\t\t\tdynamicPartitionCtx: ");
                                    outputFile.flush();
                                    outputFile.println("\t\t\t\tDynamicPartitionContext: ");
                                    outputFile.flush();
                                    outputFile.println("\t\t\t\t\tdefaultPartitionName: " + dynamicPartitionCtx.getDefaultPartitionName());
                                    outputFile.flush();
                                    if (dynamicPartitionCtx.getDPColNames() != null) {
                                        outputFile.println("\t\t\t\t\tDPColNames: " + dynamicPartitionCtx.getDPColNames().toString());
                                        outputFile.flush();
                                    }
                                    Map<String, String> inputToDPCols = dynamicPartitionCtx.getInputToDPCols();
                                    if (inputToDPCols != null) {
                                        outputFile.println("\t\t\t\t\tinputToDPCols: ");
                                        outputFile.flush();
                                        for (Map.Entry<String, String> entry : inputToDPCols.entrySet()) {
                                            outputFile.println("\t\t\t\t\t\tKey: " + entry.getKey() + " : " + entry.getValue());
                                            outputFile.flush();
                                        }
                                    }
                                    outputFile.println("\t\t\t\t\tNumBuckets: " + dynamicPartitionCtx.getNumBuckets());
                                    outputFile.flush();
                                    outputFile.println("\t\t\t\t\tMaxPartitionsPerNode: " + dynamicPartitionCtx.getMaxPartitionsPerNode());
                                    outputFile.flush();
                                    outputFile.println("\t\t\t\t\tNumSPCols: " + dynamicPartitionCtx.getNumSPCols());
                                    outputFile.flush();
                                    outputFile.println("\t\t\t\t\tNumDPCols: " + dynamicPartitionCtx.getNumDPCols());
                                    outputFile.flush();
                                    if (dynamicPartitionCtx.getRootPath() != null) {
                                        outputFile.println("\t\t\t\t\tRootPath: " + dynamicPartitionCtx.getRootPath());
                                        outputFile.flush();
                                    }
                                }
                            }
                        }
                    }
                }
            }
            else if(this.getOperatorType().equals("Create Table Operator")){

            }
            else if(this.getOperatorType().equals("Move Operator")){

            }
        }
    }

    public void printOperatorInstanceToConsole() {

        if (this.isSpecialOperator() == false) {
            System.out.println("\t\t------------------------OPERATOR: " + operator.getOperatorId() + " ----------------------------------");
            System.out.flush();
            System.out.println("\t\t\tOperatorName: " + operator.getName());
            System.out.flush();
            System.out.println("\t\t\tOperatorIdentifier: " + operator.getIdentifier());
            System.out.flush();
            System.out.println("\t\t\tToString: " + operator.toString());
            System.out.flush();
            Map<String, ExprNodeDesc> mapExprNodeDesc = operator.getColumnExprMap();
            if (mapExprNodeDesc != null) {
                System.out.println("\t\t\tPrinting MapExprNodeDesc...");
                System.out.flush();
                for (Map.Entry<String, ExprNodeDesc> entry : mapExprNodeDesc.entrySet()) {
                    ExprNodeDesc tmp = entry.getValue();
                    if (tmp != null) {
                        System.out.println("\t\t\t\tPriting Key: " + entry.getKey() + " with Value(ToString): " + tmp.toString());
                        System.out.flush();
                    }
                }
            } else {
                System.out.println("\t\t\tColumnExprMap is null...");
                System.out.flush();
            }

            OperatorType opType = operator.getType();
            if (opType != null) {
                System.out.println("\t\t\tOperatorType(toString): " + opType.toString());
                System.out.flush();
            } else {
                System.out.println("\t\t\tOperatorType is null...");
                System.out.flush();
            }

            List<org.apache.hadoop.hive.ql.exec.Operator<? extends OperatorDesc>> childOperators = operator.getChildOperators();
            if (childOperators != null) {
                if (childOperators.size() > 0)
                    System.out.println("\t\t\tIsLeaf: NO");
                else
                    System.out.println("\t\t\tIsLeaf: YES");
                for (org.apache.hadoop.hive.ql.exec.Operator<? extends OperatorDesc> ch : childOperators) {
                    if (ch != null) {
                        System.out.println("\t\t\t\tChildID: " + ch.getOperatorId());
                        System.out.flush();
                    } else {
                        System.out.println("Child is NULL...?");
                    }
                }
            } else {
                System.out.println("\t\t\tIsLeaf: YES");
                System.out.println("\t\t\tOperator has no children...");
                System.out.flush();
            }

            List<org.apache.hadoop.hive.ql.exec.Operator<? extends OperatorDesc>> parentOperators = operator.getParentOperators();
            if (parentOperators != null) {
                if (parentOperators.size() > 0)
                    System.out.println("\t\t\tIsRoot: NO");
                else
                    System.out.println("\t\t\tIsRoot: YES");

                for (org.apache.hadoop.hive.ql.exec.Operator<? extends Serializable> p : parentOperators)
                    if (p != null) {
                        System.out.println("\t\t\t\tParentID: " + p.getOperatorId());
                        System.out.flush();
                    }
            } else {
                System.out.println("\t\t\tIsRoot: YES");
                System.out.println("\t\t\tOperator has no parent...");
                System.out.flush();
            }
            ExecMapperContext execMapperContext = operator.getExecContext();
            if (execMapperContext != null) {
                System.out.println("\t\t\tExecMapperContext(toString): " + execMapperContext.toString());
                System.out.flush();
            } else {
                System.out.println("\t\t\tExecContext is null...");
                System.out.flush();
            }

            ObjectInspector[] objInspectors = operator.getInputObjInspectors();
            if (objInspectors != null) {
                System.out.println("\t\t\tAccessing InputObjectInspectors...");
                System.out.flush();
                for (ObjectInspector objInspector : objInspectors) {
                    if (objInspector != null) {
                        ObjectInspector.Category category = objInspector.getCategory();
                        if (category != null) {
                            System.out.println("\t\t\t\tCategory: " + category.toString());
                            System.out.flush();
                        }
                        if (objInspector.getTypeName() != null) {
                            System.out.println("\t\t\t\tTypeName: " + objInspector.getTypeName());
                            System.out.flush();
                        }
                        System.out.println("\t\t\t\tObjInspector(toString): " + objInspector.toString());
                        System.out.flush();
                    } else {
                        System.out.println("\t\t\t\tObjectInspector is null...");
                        System.out.flush();
                    }
                }
            } else {
                System.out.println("\t\t\t\tInputObjectInspectors are null...");
                System.out.flush();
            }

            ObjectInspector outputObjInspector = operator.getOutputObjInspector();
            if (outputObjInspector != null) {
                System.out.println("\t\t\tAccessing OutputObjectInspector...");
                System.out.flush();
                System.out.println("\t\t\t\tOutputObject(toString): " + outputObjInspector.toString());
                System.out.flush();
                if (outputObjInspector.getCategory() != null) {
                    System.out.println("\t\t\t\t: " + outputObjInspector.getCategory().toString());
                    System.out.flush();
                } else {
                    System.out.println("\t\t\t\tCategory is null!");
                    System.out.flush();
                }
                if (outputObjInspector.getTypeName() != null) {
                    System.out.println("\t\t\t\tTypeName: " + outputObjInspector.getTypeName());
                    System.out.flush();
                } else {
                    System.out.println("\t\t\t\tTypeName is null!");
                    System.out.flush();
                }
            }

            Configuration configuration = operator.getConfiguration();
            if (configuration != null) {
                System.out.println("\t\t\tConfiguration(toString): " + configuration.toString());
                System.out.flush();
            } else {
                System.out.println("\t\t\tConfiguration is null!");
                System.out.flush();
            }

            OpTraits opTraits = operator.getOpTraits();
            if (opTraits != null) {
                System.out.println("\t\t\tOpTraits(toString): " + opTraits.toString());
                System.out.flush();
            } else {
                System.out.println("\t\t\tOpTraits is null!");
                System.out.flush();
            }

            RowSchema rowSchema = operator.getSchema();
            if (rowSchema != null) {
                System.out.println("\t\t\tRowSchema: " + rowSchema.toString());
                System.out.flush();
            } else {
                System.out.println("\t\t\tRowSchema is null!");
                System.out.flush();
            }

            Statistics statistics = operator.getStatistics();
            if (statistics != null) {
                System.out.println("\t\t\tStatistics: " + statistics.toString());
                System.out.flush();
            } else {
                System.out.println("\t\t\tStatistics is null!");
                System.out.flush();
            }

            Map<String, Long> statsMap = operator.getStats();
            if (statsMap != null) {
                System.out.println("\t\t\tAccessing stats map...");
                System.out.flush();
                for (Map.Entry<String, Long> entry : statsMap.entrySet()) {
                    if (entry != null) {
                        if (entry.getValue() != null) {
                            if (entry.getKey() != null) {
                                System.out.println("\t\t" + entry.getKey() + " : " + entry.getValue());
                                System.out.flush();
                            }
                        }
                    }
                }
            } else {
                System.out.println("\t\t\tStatsMap is null!");
                System.out.flush();
            }

            System.out.println("\t\t\tcolumnNamesRowResolvedCanBeObtained: " + operator.columnNamesRowResolvedCanBeObtained());
            System.out.flush();

            System.out.println("\t\t\tacceptLimitPushdown: " + operator.acceptLimitPushdown());
            System.out.flush();

            System.out.println("\t\t\tsupportUnionRemoveOptimization: " + operator.supportUnionRemoveOptimization());
            System.out.flush();

            System.out.println("\t\t\topAllowedAfterMapJoin: " + operator.opAllowedAfterMapJoin());
            System.out.flush();

            System.out.println("\t\t\tisUseBucketizedHiveInputFormat: " + operator.isUseBucketizedHiveInputFormat());
            System.out.flush();

            System.out.println("\t\t\tsupportAutomaticSortMergeJoin: " + operator.supportAutomaticSortMergeJoin());
            System.out.flush();

            System.out.println("\t\t\tsupportSkewJoinOptimization: " + operator.supportSkewJoinOptimization());
            System.out.flush();

            System.out.println("\t\t\topAllowedConvertMapJoin: " + operator.opAllowedConvertMapJoin());
            System.out.flush();

            System.out.println("\t\t\topAllowedBeforeSortMergeJoin: " + operator.opAllowedBeforeSortMergeJoin());
            System.out.flush();

            System.out.println("\t\t\topAllowedBeforeSortMergeJoin: " + operator.opAllowedBeforeSortMergeJoin());
            System.out.flush();

            if (opType != null) {
                if (opType.toString().equals("TABLESCAN")) {
                    if (operator instanceof TableScanOperator) {
                        System.out.println("\n\t\t\tTableScan information...");
                        System.out.flush();
                        TableScanOperator tbsOperator = (TableScanOperator) operator;
                        if (tbsOperator != null) {
                            List<String> neededColumns = tbsOperator.getNeededColumns();
                            if (neededColumns != null) {
                                System.out.println("\t\t\t\tNeeded Columns: " + neededColumns.toString());
                                System.out.flush();
                            }
                            List<Integer> neededColumnIDs = tbsOperator.getNeededColumnIDs();
                            if (neededColumnIDs != null) {
                                System.out.println("\t\t\t\tNeeded Columns IDs: " + neededColumnIDs.toString());
                                System.out.flush();
                            }
                            List<String> referencedColumns = tbsOperator.getReferencedColumns();
                            if (referencedColumns != null) {
                                System.out.println("\t\t\t\tReferenced Columns: " + referencedColumns.toString());
                                System.out.flush();
                            }
                            TableDesc tableDesc = tbsOperator.getTableDesc();
                            if (tableDesc != null) {
                                String tableName = tableDesc.getTableName();
                                if (tableName != null) {
                                    System.out.println("\t\t\t\tTableName: " + tableName);
                                    System.out.flush();
                                }
                                String serdeClassName = tableDesc.getSerdeClassName();
                                if (serdeClassName != null) {
                                    System.out.println("\t\t\t\tSerdeClassName: " + serdeClassName);
                                    System.out.flush();
                                }
                                Map propertiesExplain = tableDesc.getPropertiesExplain();
                                if (propertiesExplain != null) {
                                    System.out.println("\t\t\t\tPropertiesMap(Explain): " + propertiesExplain.toString());
                                    System.out.flush();
                                }
                                Properties properties = tableDesc.getProperties();
                                if (properties != null) {
                                    System.out.println("\t\t\t\tProperties: " + properties.toString());
                                    System.out.flush();
                                }
                                String inputFileFormatClassName = tableDesc.getInputFileFormatClassName();
                                if (inputFileFormatClassName != null) {
                                    System.out.println("\t\t\t\tInputFileFormatClassName: " + inputFileFormatClassName);
                                    System.out.flush();
                                }
                                String outputFileFormatClassName = tableDesc.getOutputFileFormatClassName();
                                if (outputFileFormatClassName != null) {
                                    System.out.println("\t\t\t\tOutputFileFormatClassName: " + outputFileFormatClassName);
                                    System.out.flush();
                                }
                                Map<String, String> jobProperties = tableDesc.getJobProperties();
                                if (jobProperties != null) {
                                    System.out.println("\t\t\t\tJobProperties: ");
                                    System.out.flush();
                                    for (Map.Entry<String, String> entry : jobProperties.entrySet()) {
                                        System.out.println("\t\t\t\t\tPrinting Key: " + entry.getKey() + " with Value(ToString): " + entry.getValue());
                                        System.out.flush();
                                    }
                                }
                            }
                            TableScanDesc tableScanDesc = tbsOperator.getConf();
                            if (tableScanDesc != null) {
                                System.out.println("\t\t\t\tAlias: " + tableScanDesc.getAlias());
                                System.out.flush();
                                System.out.println("\t\t\t\tisStatsReliable: " + tableScanDesc.isStatsReliable());
                                System.out.flush();
                                System.out.println("\t\t\t\tIsMetadataOnly: " + tableScanDesc.getIsMetadataOnly());
                                System.out.flush();
                                /*if(tableScanDesc.getFilterExprString() != null) {
                                    System.out.println("\t\t\t\tFilterExprString: " + tableScanDesc.getFilterExprString());
                                    System.out.flush();
                                }*/
                                Table table = tableScanDesc.getTableMetadata();
                                if (table != null) {
                                    System.out.println("\t\t\t\tTable: ");
                                    System.out.flush();
                                    System.out.println("\t\t\t\t\tCompleteName: " + table.getCompleteName());
                                    System.out.flush();
                                    Path dataLoc = table.getDataLocation();
                                    if (dataLoc != null) {
                                        System.out.println("\t\t\t\t\tDataLocation: " + dataLoc.toString());
                                        System.out.flush();
                                    }
                                    List<FieldSchema> partitionKeys = table.getPartitionKeys();
                                    if (partitionKeys != null) {
                                        System.out.println("\t\t\t\t\tPartitionKeys: ");
                                        System.out.flush();
                                        for (FieldSchema f : partitionKeys) {
                                            if (f != null) {
                                                System.out.println("\t\t\t\t\t\tPartitionKey: " + f.toString());
                                                System.out.flush();
                                            }
                                        }
                                    }
                                    List<FieldSchema> allCols = table.getAllCols();
                                    if (allCols != null) {
                                        System.out.println("\t\t\t\t\tAllColumns: ");
                                        System.out.flush();
                                        for (FieldSchema f : allCols) {
                                            if (f != null) {
                                                System.out.println("\t\t\t\t\t\tColumn: " + f.toString());
                                                System.out.flush();
                                            }
                                        }
                                    }
                                }
                                List<String> partColumns = tableScanDesc.getPartColumns();
                                if (partColumns != null) {
                                    System.out.println("\t\t\t\tPartitionColumns: ");
                                    System.out.flush();
                                    for (String s : partColumns) {
                                        System.out.println("\t\t\t\t\tColumn: " + s);
                                        System.out.flush();
                                    }
                                }
                                List<String> needed = tableScanDesc.getNeededColumns();
                                if (needed != null) {
                                    System.out.println("\t\t\t\tNeededColumns: ");
                                    System.out.flush();
                                    for (String s : needed) {
                                        System.out.println("\t\t\t\t\tColumn: " + s);
                                        System.out.flush();
                                    }
                                }
                                List<String> referenced = tableScanDesc.getReferencedColumns();
                                if (referenced != null) {
                                    System.out.println("\t\t\t\tReferencedColumns: ");
                                    System.out.flush();
                                    for (String s : referenced) {
                                        System.out.println("\t\t\t\t\tColumn: " + s);
                                        System.out.flush();
                                    }
                                }
                                List<Integer> neededIDs = tableScanDesc.getNeededColumnIDs();
                                if (neededIDs != null) {
                                    System.out.println("\t\t\t\tNeededIDs: ");
                                    System.out.flush();
                                    for (Integer i : neededIDs) {
                                        System.out.println("\t\t\t\t\tID: " + i);
                                        System.out.flush();
                                    }
                                }

                                List<VirtualColumn> virtualColumns = tableScanDesc.getVirtualCols();
                                if (virtualColumns != null) {
                                    System.out.println("\t\t\t\tVirtualColumns: ");
                                    System.out.flush();
                                    for (VirtualColumn v : virtualColumns) {
                                        if (v != null) {
                                            System.out.println("\t\t\t\t\tColumn: " + v.getName());
                                            System.out.flush();
                                        }
                                    }
                                }
                                System.out.println("\t\t\t\tRowLimit: " + tableScanDesc.getRowLimit());
                                System.out.flush();
                            }
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("FILTER")) {
                    if (operator instanceof FilterOperator) {
                        System.out.println("\n\t\t\tFilter information...");
                        System.out.flush();
                        FilterOperator filter = (FilterOperator) operator;
                        System.out.println("\t\t\t\tsupportSkewJoinOptimization: " + filter.supportSkewJoinOptimization());
                        System.out.flush();
                        System.out.println("\t\t\t\tsupportAutomaticSortMergeJoin: " + filter.supportAutomaticSortMergeJoin());
                        System.out.flush();
                        System.out.println("\t\t\t\tsupportUnionRemoveOptimization: " + filter.supportUnionRemoveOptimization());
                        System.out.flush();
                        System.out.println("\t\t\t\tcolumnNamesRowResolvedCanBeObtained: " + filter.columnNamesRowResolvedCanBeObtained());
                        System.out.flush();

                        FilterDesc filterDesc = filter.getConf();
                        if (filterDesc != null) {
                            System.out.println("\t\t\t\tIsSamplingPred: " + filterDesc.getIsSamplingPred());
                            System.out.flush();
                            System.out.println("\t\t\t\tisSortedFilter: " + filterDesc.isSortedFilter());
                            System.out.flush();
                            ExprNodeDesc predicate = filterDesc.getPredicate();
                            if (predicate != null) {
                                System.out.println("\t\t\t\tPredicate: ");
                                System.out.flush();
                                if (predicate.getCols() != null) {
                                    System.out.println("\t\t\t\t\tColumns: " + predicate.getCols().toString());
                                    System.out.flush();
                                }
                                System.out.println("\t\t\t\t\tName: " + predicate.getName());
                                System.out.flush();
                                System.out.println("\t\t\t\t\tExprString: " + predicate.getExprString());
                                System.out.flush();
                                System.out.println("\t\t\t\t\tTypeString: " + predicate.getTypeString());
                                System.out.flush();

                                List<String> phrases = new LinkedList<String>();

                                retrieveAllPredicatePhrases(predicate, phrases);

                                System.out.println("\t\t\t\t\tPhrases: ");
                                System.out.flush();
                                for (String p : phrases) {
                                    System.out.println("\t\t\t\t\t\tPhrase: " + p);
                                    System.out.flush();
                                }
                            }
                            System.out.println("\t\t\t\tPredicateString: " + filterDesc.getPredicateString());
                            System.out.flush();
                            FilterDesc.sampleDesc sample = filterDesc.getSampleDescr();
                            if (sample != null) {
                                System.out.println("\t\t\t\tsampleDescToString: " + sample.toString());
                                System.out.flush();
                                System.out.println("\t\t\t\t\tinputPruning: " + sample.getInputPruning());
                                System.out.flush();
                            }
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("FILESINK")) {
                    if (operator instanceof FileSinkOperator) {
                        System.out.println("\n\t\t\tFileSink information...");
                        System.out.flush();
                        FileSinkOperator fileSink = (FileSinkOperator) operator;
                        if (fileSink != null) {
                            FileSinkDesc fileSinkDesc = fileSink.getConf();
                            if (fileSinkDesc != null) {
                                System.out.println("\t\t\t\tisStatsReliable: " + fileSinkDesc.isStatsReliable());
                                System.out.flush();
                                System.out.println("\t\t\t\tcanBeMerged: " + fileSinkDesc.canBeMerged());
                                System.out.flush();
                                System.out.println("\t\t\t\tgetCompressed: " + fileSinkDesc.getCompressed());
                                System.out.flush();
                                System.out.println("\t\t\t\tisGatherStats: " + fileSinkDesc.isGatherStats());
                                System.out.flush();
                                System.out.println("\t\t\t\tisLinkedFileSink: " + fileSinkDesc.isLinkedFileSink());
                                System.out.flush();
                                System.out.println("\t\t\t\tisMultiFileSpray: " + fileSinkDesc.isMultiFileSpray());
                                System.out.flush();
                                System.out.println("\t\t\t\tisRemovedReduceSinkBucketSort: " + fileSinkDesc.isRemovedReduceSinkBucketSort());
                                System.out.flush();
                                System.out.println("\t\t\t\tisStatsCollectRawDataSize: " + fileSinkDesc.isStatsCollectRawDataSize());
                                System.out.flush();
                                System.out.println("\t\t\t\tisStatsReliable: " + fileSinkDesc.isStatsReliable());
                                System.out.flush();
                                System.out.println("\t\t\t\tisTemporary: " + fileSinkDesc.isTemporary());
                                System.out.flush();
                                System.out.println("\t\t\t\tCompressCodec: " + fileSinkDesc.getCompressCodec());
                                System.out.flush();
                                System.out.println("\t\t\t\tCompressType: " + fileSinkDesc.getCompressType());
                                System.out.flush();
                                System.out.println("\t\t\t\tDestTableId: " + fileSinkDesc.getDestTableId());
                                System.out.flush();

                                Path dirName = fileSinkDesc.getDirName();
                                if (dirName != null) {
                                    System.out.println("\t\t\t\tdirName: " + dirName.toString());
                                    System.out.flush();
                                }
                                Path finalDirName = fileSinkDesc.getFinalDirName();
                                if (finalDirName != null) {
                                    System.out.println("\t\t\t\tfinalDirName: " + finalDirName.toString());
                                    System.out.flush();
                                }

                                FileSinkDesc.DPSortState dpSortState = fileSinkDesc.getDpSortState();
                                if (dpSortState != null) {
                                    System.out.println("\t\t\t\tDpSortState: " + dpSortState.toString());
                                    System.out.flush();
                                }

                                System.out.println("\t\t\t\tDestTableID: " + fileSinkDesc.getDestTableId());
                                System.out.flush();
                                DynamicPartitionCtx dynamicPartitionCtx = fileSinkDesc.getDynPartCtx();
                                if (dynamicPartitionCtx != null) {
                                    System.out.println("\t\t\t\tDynamicPartitionContext: ");
                                    System.out.flush();
                                    System.out.println("\t\t\t\t\tdefaultPartitionName: " + dynamicPartitionCtx.getDefaultPartitionName());
                                    System.out.flush();
                                    if (dynamicPartitionCtx.getDPColNames() != null) {
                                        System.out.println("\t\t\t\t\tDPColNames: " + dynamicPartitionCtx.getDPColNames().toString());
                                        System.out.flush();
                                    }
                                    Map<String, String> inputToDPCols = dynamicPartitionCtx.getInputToDPCols();
                                    if (inputToDPCols != null) {
                                        System.out.println("\t\t\t\t\tinputToDPCols: ");
                                        System.out.flush();
                                        for (Map.Entry<String, String> entry : inputToDPCols.entrySet()) {
                                            System.out.println("\t\t\t\t\t\tKey: " + entry.getKey() + " : " + entry.getValue());
                                            System.out.flush();
                                        }
                                    }
                                    System.out.println("\t\t\t\t\tNumBuckets: " + dynamicPartitionCtx.getNumBuckets());
                                    System.out.flush();
                                    System.out.println("\t\t\t\t\tMaxPartitionsPerNode: " + dynamicPartitionCtx.getMaxPartitionsPerNode());
                                    System.out.flush();
                                    System.out.println("\t\t\t\t\tNumSPCols: " + dynamicPartitionCtx.getNumSPCols());
                                    System.out.flush();
                                    System.out.println("\t\t\t\t\tNumDPCols: " + dynamicPartitionCtx.getNumDPCols());
                                    System.out.flush();
                                    if (dynamicPartitionCtx.getRootPath() != null) {
                                        System.out.println("\t\t\t\t\tRootPath: " + dynamicPartitionCtx.getRootPath());
                                        System.out.flush();
                                    }
                                }

                                System.out.println("\t\t\t\tStaticSpec: " + fileSinkDesc.getStaticSpec());
                                System.out.flush();
                                System.out.println("\t\t\t\tStatsAggPrefix: " + fileSinkDesc.getStatsAggPrefix());
                                System.out.flush();
                                System.out.println("\t\t\t\tTotalFiles: " + fileSinkDesc.getTotalFiles());
                                System.out.flush();
                                List<FileSinkDesc> linkedFileDescs = fileSinkDesc.getLinkedFileSinkDesc();
                                if (linkedFileDescs != null) {
                                    System.out.println("\t\t\t\tLinkedFileDescs: More information exists");
                                    System.out.flush();
                                }
                                if (fileSinkDesc.getTable() != null) {
                                    Table table = fileSinkDesc.getTable();
                                    if (table != null) {
                                        System.out.println("\t\t\t\tTableName: " + table.getCompleteName());
                                        System.out.flush();
                                        System.out.println("\t\t\t\t\tNOTE: MORE INFO CAN BE FOUND ");
                                        System.out.flush();
                                    }
                                }
                            }
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("DEMUX")) {
                    if (operator instanceof DemuxOperator) {
                        System.out.println("\n\t\t\tDemux information...");
                        System.out.flush();
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("EXTRACT")) {
                    if (operator instanceof ExtractOperator) {
                        System.out.println("\n\t\t\tExtract information...");
                        System.out.flush();
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("FORWARD")) {
                    if (operator instanceof ForwardOperator) {
                        System.out.println("\n\t\t\tForward information...");
                        System.out.flush();
                        ForwardOperator forwardOperator = (ForwardOperator) operator;
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        if (operator instanceof ListSinkOperator) {
                            ListSinkOperator sink = (ListSinkOperator) operator;
                            if (sink != null) {
                                System.out.println("\n\t\t\tListSink information...");
                                System.out.flush();
                                System.out.println("\n\t\t\t\tNumRows: " + sink.getNumRows());
                                System.out.flush();
                            }
                        }
                    }
                } else if (opType.toString().equals("GROUPBY")) {
                    if (operator instanceof GroupByOperator) {
                        System.out.println("\n\t\t\tGroupBy information...");
                        System.out.flush();
                        GroupByOperator groupByOperator = (GroupByOperator) operator;
                        if (groupByOperator != null) {
                            System.out.println("\t\t\t\tacceptLimitPushdown: " + groupByOperator.acceptLimitPushdown());
                            System.out.flush();
                            GroupByDesc groupByDesc = groupByOperator.getConf();
                            if (groupByDesc != null) {
                                List<AggregationDesc> aggregationDescList = groupByDesc.getAggregators();
                                if (aggregationDescList != null) {
                                    System.out.println("\t\t\t\taggregationDescList: ");
                                    System.out.flush();
                                    for (AggregationDesc a : aggregationDescList) {
                                        if (a != null) {
                                            System.out.println("\t\t\t\t\tAggregation: ");
                                            System.out.flush();
                                            System.out.println("\t\t\t\t\t\tgetDistinct: " + a.getDistinct());
                                            System.out.flush();
                                            if (a.getExprString() != null) {
                                                System.out.println("\t\t\t\t\t\tExprString: " + a.getExprString());
                                                System.out.flush();
                                            }
                                            GenericUDAFEvaluator genericUDAFEvaluator = a.getGenericUDAFEvaluator();
                                            if (genericUDAFEvaluator != null) {
                                                System.out.println("\t\t\t\t\t\tgenericUDAFEvaluator(toString): " + genericUDAFEvaluator.toString());
                                                System.out.flush();
                                            }
                                            System.out.println("\t\t\t\t\t\tGenericUDAFEvaluatorClassName: " + a.getGenericUDAFEvaluatorClassName());
                                            System.out.flush();
                                            System.out.println("\t\t\t\t\t\tGenericUDAFName: " + a.getGenericUDAFName());
                                            System.out.flush();
                                            GenericUDAFEvaluator.Mode mode = a.getMode();
                                            if (mode != null) {
                                                System.out.println("\t\t\t\t\t\tMode: " + mode.toString());
                                                System.out.flush();
                                            }
                                            ArrayList<ExprNodeDesc> parameters = a.getParameters();
                                            if (parameters != null) {
                                                System.out.println("\t\t\t\t\t\tParameters: ");
                                                System.out.flush();
                                                for (ExprNodeDesc p : parameters) {
                                                    System.out.println("\t\t\t\t\t\t\tParameter: ");
                                                    System.out.flush();
                                                    System.out.println("\t\t\t\t\t\t\t\tName: " + p.getName());
                                                    System.out.flush();
                                                    List<String> cols = p.getCols();
                                                    if (cols != null) {
                                                        System.out.println("\t\t\t\t\t\t\t\tCols: " + cols.toString());
                                                        System.out.flush();
                                                    } else {
                                                        System.out.println("\t\t\t\t\t\t\t\tCols: NULL");
                                                        System.out.flush();
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                List<String> aggStrings = groupByDesc.getAggregatorStrings();
                                if (aggStrings != null) {
                                    System.out.println("\t\t\t\taggregatorStrings: " + aggStrings.toString());
                                    System.out.flush();
                                }
                                System.out.println("\t\t\t\tBucketGroup: " + groupByDesc.getBucketGroup());
                                System.out.flush();
                                System.out.println("\t\t\t\tGroupingSetPosition: " + groupByDesc.getGroupingSetPosition());
                                System.out.flush();
                                System.out.println("\t\t\t\tGroupKeyNotReductionKey: " + groupByDesc.getGroupKeyNotReductionKey());
                                System.out.flush();
                                ArrayList<ExprNodeDesc> keys = groupByDesc.getKeys();
                                if (keys != null) {
                                    System.out.println("\t\t\t\tKeys: ");
                                    System.out.flush();
                                    for (ExprNodeDesc k : keys) {
                                        if (k != null) {
                                            System.out.println("\t\t\t\t\tKey: ");
                                            System.out.flush();
                                            System.out.println("\t\t\t\t\t\tName: " + k.getName());
                                            System.out.flush();
                                            if (k.getCols() != null) {
                                                System.out.println("\t\t\t\t\t\tCols: " + k.getCols().toString());
                                                System.out.flush();
                                            } else {
                                                System.out.println("\t\t\t\t\t\tCols: NULL");
                                                System.out.flush();
                                            }
                                        }
                                    }
                                }
                                String keyString = groupByDesc.getKeyString();
                                if (keyString != null) {
                                    System.out.println("\t\t\t\tKeyString: " + keyString);
                                    System.out.flush();
                                }
                                String modeString = groupByDesc.getModeString();
                                if (modeString != null) {
                                    System.out.println("\t\t\t\tModeString: " + modeString);
                                    System.out.flush();
                                }
                                List<Integer> groupingSets = groupByDesc.getListGroupingSets();
                                if (groupingSets != null) {
                                    System.out.println("\t\t\t\tGroupingSets: " + groupingSets.toString());
                                    System.out.flush();
                                }
                                ArrayList<String> outputCols = groupByDesc.getOutputColumnNames();
                                if (outputCols != null) {
                                    System.out.println("\t\t\t\tOutputColumns: ");
                                    System.out.flush();
                                    for (String s : outputCols) {
                                        System.out.println("\t\t\t\t\tColumn " + s);
                                        System.out.flush();
                                    }
                                }
                                System.out.println("\t\t\t\tisDistinctLike: " + groupByDesc.isDistinctLike());
                                System.out.flush();
                                System.out.println("\t\t\t\tisDontResetAggrsDistinct: " + groupByDesc.isDontResetAggrsDistinct());
                                System.out.flush();
                                System.out.println("\t\t\t\tisGroupingSetsPresent: " + groupByDesc.isGroupingSetsPresent());
                                System.out.flush();
                            }
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("HASHTABLEDUMMY")) {
                    if (operator instanceof HashTableDummyOperator) {
                        System.out.println("\n\t\t\tHashTableDummy information...");
                        System.out.flush();
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("HASHTABLESINK")) {
                    if (operator instanceof HashTableSinkOperator) {
                        System.out.println("\n\t\t\tHashTableSink information...");
                        System.out.flush();
                        HashTableSinkOperator hashTableSinkOperator = (HashTableSinkOperator) operator;
                        if (hashTableSinkOperator != null) {
                            MapJoinTableContainer[] mapJoinTableContainers = hashTableSinkOperator.getMapJoinTables();
                            if (mapJoinTableContainers != null) {
                                System.out.println("\t\t\t\tMapJoinTableContainers: ");
                                System.out.flush();
                                for (MapJoinTableContainer mapJoinTableContainer : mapJoinTableContainers) {
                                    if (mapJoinTableContainer != null) {
                                        System.out.println("\n\t\t\t\t\t" + mapJoinTableContainer.toString());
                                    }
                                }
                            }
                            HashTableSinkDesc hashDesc = hashTableSinkOperator.getConf();
                            if (hashDesc != null) {
                                System.out.println("\t\t\t\tisMapSideJoin: " + hashDesc.isMapSideJoin());
                                System.out.flush();
                                System.out.println("\t\t\t\tisLeftInputJoin: " + hashDesc.isLeftInputJoin());
                                System.out.flush();
                                System.out.println("\t\t\t\tisHandleSkewJoin: " + hashDesc.isHandleSkewJoin());
                                System.out.flush();
                                System.out.println("\t\t\t\tisNoOuterJoin: " + hashDesc.isNoOuterJoin());
                                System.out.flush();
                                Map<Byte, Path> bigKeysDirMap = hashDesc.getBigKeysDirMap();
                                if (bigKeysDirMap != null) {
                                    System.out.println("\t\t\t\tBigKeysDirMap: ");
                                    System.out.flush();
                                    for (Map.Entry<Byte, Path> entry : bigKeysDirMap.entrySet()) {
                                        if (entry != null) {
                                            if (entry.getValue() != null) {
                                                System.out.println("\t\t\t\t\tKey: " + entry.getKey() + " Path: " + entry.getValue().toString());
                                                System.out.flush();
                                            } else {
                                                System.out.println("\t\t\t\t\tKey: " + entry.getKey() + " Path: NULL");
                                                System.out.flush();
                                            }
                                        }
                                    }
                                }
                                JoinCondDesc[] joinDescArray = hashDesc.getConds();
                                if (joinDescArray != null) {
                                    System.out.println("\t\t\t\tJoinCondDescs: ");
                                    System.out.flush();
                                    for (JoinCondDesc j : joinDescArray) {
                                        if (j != null) {
                                            System.out.println("\t\t\t\t\tJoinCondDesc: ");
                                            System.out.flush();
                                            System.out.println("\t\t\t\t\t\tJoinCondString: " + j.getJoinCondString());
                                            System.out.flush();
                                            System.out.println("\t\t\t\t\t\tLeft: " + j.getLeft());
                                            System.out.flush();
                                            System.out.println("\t\t\t\t\t\tRight: " + j.getRight());
                                            System.out.flush();
                                            System.out.println("\t\t\t\t\t\tPreserved: " + j.getPreserved());
                                            System.out.flush();
                                        }
                                    }
                                } //TODO
                                List<String> outputCols = hashDesc.getOutputColumnNames();
                                if (outputCols != null) {
                                    System.out.println("\t\t\t\toutputCols: " + outputCols.toString());
                                    System.out.flush();
                                }
                                Map<Byte, List<ExprNodeDesc>> keys = hashDesc.getKeys();
                                if (keys != null) {
                                    System.out.println("\t\t\t\tKeys: ");
                                    System.out.flush();
                                    for (Map.Entry<Byte, List<ExprNodeDesc>> entry : keys.entrySet()) {
                                        System.out.println("\t\t\t\t\tKey: " + entry.getKey());
                                        System.out.flush();
                                        if (entry.getValue() != null) {
                                            System.out.println("\t\t\t\t\t\tList of ExprNodeDescs: ");
                                            System.out.flush();
                                            for (ExprNodeDesc e : entry.getValue()) {
                                                if (e != null) {
                                                    System.out.println("\t\t\t\t\t\t\tName: " + e.getName());
                                                    System.out.flush();
                                                    if (e.getCols() != null) {
                                                        System.out.println("\t\t\t\t\t\t\tCols: " + e.getCols().toString());
                                                        System.out.flush();
                                                    } else {
                                                        System.out.println("\t\t\t\t\t\t\tCols: NULL");
                                                        System.out.flush();
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                Map<Byte, TableDesc> skewKeysValueTables = hashDesc.getSkewKeysValuesTables();
                                if (skewKeysValueTables != null) {
                                    System.out.println("\t\t\t\tskewKeysValueTables: ");
                                    System.out.flush();
                                    for (Map.Entry<Byte, TableDesc> entry : skewKeysValueTables.entrySet()) {
                                        if (entry != null) {
                                            System.out.println("\t\t\t\t\tKey: " + entry.getKey());
                                            System.out.flush();
                                            if (entry.getValue() != null) {
                                                System.out.println("\t\t\t\t\t\tValue: " + entry.getValue().getTableName());
                                                System.out.flush();
                                            }
                                        }
                                    }
                                }
                                List<TableDesc> valueTblDescs = hashDesc.getValueTblDescs();
                                if (valueTblDescs != null) {
                                    System.out.println("\t\t\t\tValueTblDescs: ");
                                    System.out.flush();
                                    for (TableDesc v : valueTblDescs) {
                                        if (v != null) {
                                            System.out.println("\t\t\t\t\tTableDesc: ");
                                            System.out.flush();
                                            System.out.println("\t\t\t\t\t\tName: " + v.getTableName());
                                            System.out.flush();
                                        }
                                    }
                                }
                                List<TableDesc> valueFilteredTblDescs = hashDesc.getValueTblFilteredDescs();
                                if (valueFilteredTblDescs != null) {
                                    System.out.println("\t\t\t\tValueFilteredTblDescs: ");
                                    System.out.flush();
                                    for (TableDesc v : valueFilteredTblDescs) {
                                        if (v != null) {
                                            System.out.println("\t\t\t\t\tTableDesc: ");
                                            System.out.flush();
                                            System.out.println("\t\t\t\t\t\tName: " + v.getTableName());
                                            System.out.flush();
                                        }
                                    }
                                }

                                Map<Byte, String> keys2 = hashDesc.getKeysString();
                                if (keys2 != null) {
                                    System.out.println("\t\t\t\tKeys (From getKeyString()): ");
                                    System.out.flush();
                                    for (Map.Entry<Byte, String> entry : keys2.entrySet()) {
                                        System.out.println("\t\t\t\t\tKey: " + entry.getKey() + " : Value: " + entry.getValue());
                                        System.out.flush();
                                    }
                                }

                            }
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("JOIN")) {
                    if (operator instanceof JoinOperator) {
                        System.out.println("\n\t\t\tJoin information...");
                        System.out.flush();
                        JoinOperator joinOperator = (JoinOperator) operator;
                        System.out.println("\t\t\t\topAllowedBeforeSortMergeJoin: " + joinOperator.opAllowedBeforeSortMergeJoin());
                        System.out.println("\t\t\t\tsupportSkewJoinOptimization: " + joinOperator.supportSkewJoinOptimization());
                        JoinDesc joinDesc = joinOperator.getConf();
                        if (joinDesc != null) {
                            List<String> outputCols = joinDesc.getOutputColumnNames();
                            if (outputCols != null) {
                                System.out.println("\t\t\t\toutputCols: " + outputCols.toString());
                                System.out.flush();
                            }
                            JoinCondDesc[] joinCondDescs = joinDesc.getConds();
                            if (joinCondDescs != null) {
                                System.out.println("\t\t\t\tJoinCondDescs: ");
                                System.out.flush();
                                for (JoinCondDesc j : joinCondDescs) {
                                    if (j != null) {
                                        System.out.println("\t\t\t\t\tLeft: " + j.getLeft());
                                        System.out.flush();
                                        System.out.println("\t\t\t\t\tRight: " + j.getRight());
                                        System.out.flush();
                                        System.out.println("\t\t\t\t\tType: " + j.getType());
                                        System.out.flush();
                                        System.out.println("\t\t\t\t\tPreserved: " + j.getPreserved());
                                        System.out.flush();
                                        if (j.getJoinCondString() != null) {
                                            System.out.println("\t\t\t\t\tJoinCondString: " + j.getJoinCondString());
                                            System.out.flush();
                                        }
                                    }
                                }
                            }
                            Map<Byte, Path> bigKeysDirMap = joinDesc.getBigKeysDirMap();
                            if (bigKeysDirMap != null) {
                                System.out.println("\t\t\t\tBigKeysDirMap: ");
                                System.out.flush();
                                for (Map.Entry<Byte, Path> entry : bigKeysDirMap.entrySet()) {
                                    if (entry != null) {
                                        Path value = entry.getValue();
                                        if (value != null) {
                                            System.out.println("\t\t\t\t\tKey= " + entry.getKey() + " : Value= " + value.toString());
                                            System.out.flush();
                                        } else {
                                            System.out.println("\t\t\t\t\tKey= " + entry.getKey() + " : Value= NULL");
                                            System.out.flush();
                                        }
                                    }
                                }
                            }
                            Map<Byte, String> keys = joinDesc.getKeysString();
                            if (keys != null) {
                                System.out.println("\t\t\t\tKeys: ");
                                System.out.flush();
                                for (Map.Entry<Byte, String> entry : keys.entrySet()) {
                                    System.out.println("\t\t\t\t\tKey: " + entry.getKey() + " : Value: " + entry.getValue());
                                    System.out.flush();
                                }
                            }
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("LATERALVIEWFORWARD")) {
                    if (operator instanceof LateralViewForwardOperator) {
                        System.out.println("\n\t\t\tLateralViewForward information...");
                        System.out.flush();
                        LateralViewForwardOperator lateralViewForwardOperator = (LateralViewForwardOperator) operator;
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("LATERALVIEWJOIN")) {
                    if (operator instanceof LateralViewJoinOperator) {
                        System.out.println("\n\t\t\tLateralViewJoin information...");
                        System.out.flush();
                        LateralViewJoinOperator lateralViewJoinOperator = (LateralViewJoinOperator) operator;
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("LIMIT")) {
                    if (operator instanceof LimitOperator) {
                        System.out.println("\n\t\t\tLimit information...");
                        System.out.flush();
                        LimitOperator limitOperator = (LimitOperator) operator;
                        LimitDesc limitDesc = limitOperator.getConf();
                        if (limitDesc != null) {
                            System.out.println("\t\t\t\tLimit: " + limitDesc.getLimit());
                            System.out.flush();
                            System.out.println("\t\t\t\tLimit: " + limitDesc.getLeastRows());
                            System.out.flush();
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("MAPJOIN")) {
                    if (operator instanceof MapJoinOperator) {
                        System.out.println("\n\t\t\tMapJoin information...");
                        System.out.flush();
                        MapJoinOperator mapJoinOperator = (MapJoinOperator) operator;
                        MapJoinDesc mapJoinDesc = mapJoinOperator.getConf();
                        if (mapJoinDesc != null) {
                            List<String> outputCols = mapJoinDesc.getOutputColumnNames();
                            if (outputCols != null) {
                                System.out.println("\t\t\t\toutputCols: " + outputCols.toString());
                                System.out.flush();
                            }
                            System.out.println("\t\t\t\tisBucketMapJoin: " + mapJoinDesc.isBucketMapJoin());
                            System.out.flush();
                            JoinCondDesc[] joinCondDescs = mapJoinDesc.getConds();
                            if (joinCondDescs != null) {
                                System.out.println("\t\t\t\tJoinCondDescs: ");
                                System.out.flush();
                                for (JoinCondDesc j : joinCondDescs) {
                                    if (j != null) {
                                        System.out.println("\t\t\t\t\tLeft: " + j.getLeft());
                                        System.out.flush();
                                        System.out.println("\t\t\t\t\tRight: " + j.getRight());
                                        System.out.flush();
                                        System.out.println("\t\t\t\t\tType: " + j.getType());
                                        System.out.flush();
                                        System.out.println("\t\t\t\t\tPreserved: " + j.getPreserved());
                                        System.out.flush();
                                        if (j.getJoinCondString() != null) {
                                            System.out.println("\t\t\t\t\tJoinCondString: " + j.getJoinCondString());
                                            System.out.flush();
                                        }
                                    }
                                }
                            }
                            Map<Byte, Path> bigKeysDirMap = mapJoinDesc.getBigKeysDirMap();
                            if (bigKeysDirMap != null) {
                                System.out.println("\t\t\t\tBigKeysDirMap: ");
                                System.out.flush();
                                for (Map.Entry<Byte, Path> entry : bigKeysDirMap.entrySet()) {
                                    if (entry != null) {
                                        Path value = entry.getValue();
                                        if (value != null) {
                                            System.out.println("\t\t\t\t\tKey= " + entry.getKey() + " : Value= " + value.toString());
                                            System.out.flush();
                                        } else {
                                            System.out.println("\t\t\t\t\tKey= " + entry.getKey() + " : Value= NULL");
                                            System.out.flush();
                                        }
                                    }
                                }
                            }
                            System.out.println("\t\t\t\tisNoOuterJoin: " + mapJoinDesc.isNoOuterJoin());
                            System.out.flush();
                            System.out.println("\t\t\t\tBigTableAlias: " + mapJoinDesc.getBigTableAlias());
                            System.out.flush();
                            if (mapJoinDesc.getKeyCountsExplainDesc() != null) {
                                System.out.println("\t\t\t\tKeyCountsExplainDesc: " + mapJoinDesc.getKeyCountsExplainDesc());
                                System.out.flush();
                            }
                            Map<String, List<String>> bigTablePart = mapJoinDesc.getBigTablePartSpecToFileMapping();
                            if (bigTablePart != null) {
                                System.out.println("\t\t\t\tBigTablePartSpecToFileMapping: ");
                                System.out.flush();
                                for (Map.Entry<String, List<String>> entry : bigTablePart.entrySet()) {
                                    if (entry != null) {
                                        System.out.println("\t\t\t\tKey: " + entry.getKey());
                                        System.out.flush();
                                        List<String> valueList = entry.getValue();
                                        if (valueList != null) {
                                            System.out.println("\t\t\t\t\tValueList: " + valueList.toString());
                                            System.out.flush();
                                        } else {
                                            System.out.println("\t\t\t\t\tValueList: NULL");
                                            System.out.flush();
                                        }
                                    }
                                }
                            }
                            Map<Byte, List<ExprNodeDesc>> keys = mapJoinDesc.getKeys();
                            if (keys != null) {
                                System.out.println("\t\t\t\tKeys: ");
                                System.out.flush();
                                for (Map.Entry<Byte, List<ExprNodeDesc>> entry : keys.entrySet()) {
                                    System.out.println("\t\t\t\t\tKey: " + entry.getKey());
                                    System.out.flush();
                                    if (entry.getValue() != null) {
                                        System.out.println("\t\t\t\t\t\tList of ExprNodeDescs: ");
                                        System.out.flush();
                                        for (ExprNodeDesc e : entry.getValue()) {
                                            if (e != null) {
                                                System.out.println("\t\t\t\t\t\t\tName: " + e.getName());
                                                System.out.flush();
                                                if (e.getCols() != null) {
                                                    System.out.println("\t\t\t\t\t\t\tCols: " + e.getCols().toString());
                                                    System.out.flush();
                                                } else {
                                                    System.out.println("\t\t\t\t\t\t\tCols: NULL");
                                                    System.out.flush();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Map<Byte, int[]> valueIndices = mapJoinDesc.getValueIndices();
                            if (valueIndices != null) {
                                System.out.println("\t\t\t\tValueIndices: ");
                                System.out.flush();
                                for (Map.Entry<Byte, int[]> entry : valueIndices.entrySet()) {
                                    if (entry != null) {
                                        System.out.println("\t\t\t\t\tKey: " + entry.getKey());
                                        System.out.flush();
                                        if (entry.getValue() != null) {
                                            System.out.println("\t\t\t\t\t\tValue: " + entry.getValue().toString());
                                            System.out.flush();
                                        }
                                    }
                                }
                            }
                            List<TableDesc> valueTblDescs = mapJoinDesc.getValueTblDescs();
                            if (valueTblDescs != null) {
                                System.out.println("\t\t\t\tValueTblDescs: ");
                                System.out.flush();
                                for (TableDesc v : valueTblDescs) {
                                    if (v != null) {
                                        System.out.println("\t\t\t\t\tTableDesc: ");
                                        System.out.flush();
                                        System.out.println("\t\t\t\t\t\tName: " + v.getTableName());
                                        System.out.flush();
                                    }
                                }
                            }
                            List<TableDesc> valueFilteredTblDescs = mapJoinDesc.getValueFilteredTblDescs();
                            if (valueFilteredTblDescs != null) {
                                System.out.println("\t\t\t\tValueFilteredTblDescs: ");
                                System.out.flush();
                                for (TableDesc v : valueFilteredTblDescs) {
                                    if (v != null) {
                                        System.out.println("\t\t\t\t\tTableDesc: ");
                                        System.out.flush();
                                        System.out.println("\t\t\t\t\t\tName: " + v.getTableName());
                                        System.out.flush();
                                    }
                                }
                            }

                            Map<Byte, String> keys2 = mapJoinDesc.getKeysString();
                            if (keys2 != null) {
                                System.out.println("\t\t\t\tKeys (From getKeyString()): ");
                                System.out.flush();
                                for (Map.Entry<Byte, String> entry : keys2.entrySet()) {
                                    System.out.println("\t\t\t\t\tKey: " + entry.getKey() + " : Value: " + entry.getValue());
                                    System.out.flush();
                                }
                            }

                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("MUX")) {
                    if (operator instanceof MuxOperator) {
                        System.out.println("\n\t\t\tMux information...");
                        System.out.flush();
                        MuxOperator muxOperator = (MuxOperator) operator;
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("PTF")) {
                    if (operator instanceof PTFOperator) {
                        System.out.println("\n\t\t\tPTF information...");
                        System.out.flush();
                        PTFOperator ptfOperator = (PTFOperator) operator;
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("REDUCESINK")) {
                    if (operator instanceof ReduceSinkOperator) {
                        System.out.println("\n\t\t\tReduceSink information...");
                        System.out.flush();
                        ReduceSinkOperator reduceSinkOperator = (ReduceSinkOperator) operator;
                        if (reduceSinkOperator != null) {
                            String[] inputAliases = reduceSinkOperator.getInputAliases();
                            if (inputAliases != null) {
                                System.out.println("\t\t\t\tInputAliases: " + inputAliases.toString());
                                System.out.flush();
                            }
                            int[] valueIndeces = reduceSinkOperator.getValueIndex();
                            if (valueIndeces != null) {
                                System.out.println("\t\t\t\tValueIndex: " + valueIndeces.toString());
                                System.out.flush();
                            }
                            System.out.println("\t\t\t\tOpAllowedBeforeMapJoin: " + reduceSinkOperator.opAllowedBeforeMapJoin());
                            System.out.flush();
                            ReduceSinkDesc reduceSinkDesc = reduceSinkOperator.getConf();
                            if (reduceSinkDesc != null) {
                                System.out.println("\t\t\t\tisPTFReduceSink: " + reduceSinkDesc.isPTFReduceSink());
                                System.out.flush();
                                System.out.println("\t\t\t\tisMapGroupBy: " + reduceSinkDesc.isMapGroupBy());
                                System.out.flush();
                                System.out.println("\t\t\t\tisEnforceSort: " + reduceSinkDesc.isEnforceSort());
                                System.out.flush();
                                System.out.println("\t\t\t\tisAutoParallel: " + reduceSinkDesc.isAutoParallel());
                                System.out.flush();
                                System.out.println("\t\t\t\tgetSkipTag: " + reduceSinkDesc.getSkipTag());
                                System.out.flush();
                                if (reduceSinkDesc.getKeyColString() != null) {
                                    System.out.println("\t\t\t\tKeyColString: " + reduceSinkDesc.getKeyColString());
                                    System.out.flush();
                                }
                                List<ExprNodeDesc> keyCols = reduceSinkDesc.getKeyCols();
                                if (keyCols != null) {
                                    System.out.println("\t\t\t\tKeyCols: ");
                                    System.out.flush();
                                    for (ExprNodeDesc v : keyCols) {
                                        if (v != null) {
                                            System.out.println("\t\t\t\t\tName: " + v.getName());
                                            System.out.flush();
                                            if (v.getCols() != null)
                                                System.out.println("\t\t\t\t\tCols: " + v.getCols().toString());
                                            else
                                                System.out.println("\t\t\t\t\tCols: NULL");
                                            System.out.flush();
                                        }
                                    }
                                }
                                if (reduceSinkDesc.getValueCols() != null) {
                                    System.out.println("\t\t\t\tValueColsString: " + reduceSinkDesc.getValueColsString());
                                    System.out.flush();
                                }
                                ArrayList<ExprNodeDesc> valueCols = reduceSinkDesc.getValueCols();
                                if (valueCols != null) {
                                    System.out.println("\t\t\t\tValueCols: ");
                                    System.out.flush();
                                    for (ExprNodeDesc v : valueCols) {
                                        if (v != null) {
                                            System.out.println("\t\t\t\t\tName: " + v.getName());
                                            System.out.flush();
                                            if (v.getCols() != null)
                                                System.out.println("\t\t\t\t\tCols: " + v.getCols().toString());
                                            else
                                                System.out.println("\t\t\t\t\tCols: NULL");
                                            System.out.flush();
                                        }
                                    }
                                }

                                List<ExprNodeDesc> bucketCols = reduceSinkDesc.getBucketCols();
                                if (bucketCols != null) {
                                    System.out.println("\t\t\t\tBucketCols: ");
                                    System.out.flush();
                                    for (ExprNodeDesc v : bucketCols) {
                                        if (v != null) {
                                            System.out.println("\t\t\t\t\tName: " + v.getName());
                                            System.out.flush();
                                            if (v.getCols() != null)
                                                System.out.println("\t\t\t\t\tCols: " + v.getCols().toString());
                                            else
                                                System.out.println("\t\t\t\t\tCols: NULL");
                                            System.out.flush();
                                        }
                                    }
                                }

                                System.out.println("\t\t\t\tNumBuckets: " + reduceSinkDesc.getNumBuckets());
                                System.out.flush();
                                System.out.println("\t\t\t\tNumDistributionKeys: " + reduceSinkDesc.getNumDistributionKeys());
                                System.out.flush();
                                System.out.println("\t\t\t\tNumReducers: " + reduceSinkDesc.getNumReducers());
                                System.out.flush();
                                System.out.println("\t\t\t\tOrder: " + reduceSinkDesc.getOrder());
                                System.out.flush();
                                System.out.println("\t\t\t\tOutputName: " + reduceSinkDesc.getOutputName());
                                System.out.flush();
                                List<String> outputKeyColumnNames = reduceSinkDesc.getOutputKeyColumnNames();
                                if (outputKeyColumnNames != null) {
                                    System.out.println("\t\t\t\toutputKeyColumnNames: " + outputKeyColumnNames.toString());
                                    System.out.flush();
                                }
                                List<String> outputValueColumnNames = reduceSinkDesc.getOutputValueColumnNames();
                                if (outputValueColumnNames != null) {
                                    System.out.println("\t\t\t\toutputValueColumnNames: " + outputValueColumnNames.toString());
                                    System.out.flush();
                                }
                                if (reduceSinkDesc.getParitionColsString() != null) {
                                    System.out.println("\t\t\t\tPartitionColsString: " + reduceSinkDesc.getParitionColsString());
                                    System.out.flush();
                                }
                            }
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("SCRIPT")) {
                    if (operator instanceof ScriptOperator) {
                        System.out.println("\n\t\t\tScriptOperator information...");
                        System.out.flush();
                        //ScriptOperator scriptOperator = (ScriptOperator) operator;
                        //ScriptDesc scriptDesc = scriptOperator.getConf();
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("SELECT")) {
                    if (operator instanceof SelectOperator) {
                        System.out.println("\n\t\t\tSelect information...");
                        System.out.flush();
                        SelectOperator selectOperator = (SelectOperator) operator;
                        if (selectOperator != null) {
                            System.out.println("\t\t\t\tsupportSkewJoinOptimization: " + selectOperator.supportSkewJoinOptimization());
                            System.out.flush();
                            System.out.println("\t\t\t\tacceptLimitPushdown: " + selectOperator.acceptLimitPushdown());
                            System.out.flush();
                            System.out.println("\t\t\t\tcolumnNamesRowResolvedCanBeObtained: " + selectOperator.columnNamesRowResolvedCanBeObtained());
                            System.out.flush();
                            System.out.println("\t\t\t\tisIdentitySelect: " + selectOperator.isIdentitySelect());
                            System.out.flush();
                            System.out.println("\t\t\t\tsupportAutomaticSortMergeJoin: " + selectOperator.supportAutomaticSortMergeJoin());
                            System.out.flush();
                            System.out.println("\t\t\t\tsupportUnionRemoveOptimization: " + selectOperator.supportUnionRemoveOptimization());
                            System.out.flush();
                            SelectDesc selectDesc = selectOperator.getConf();
                            if (selectDesc != null) {
                                System.out.println("\t\t\t\tisSelStarNoCompute: " + selectDesc.isSelStarNoCompute());
                                System.out.flush();
                                System.out.println("\t\t\t\tisSelectStar: " + selectDesc.isSelectStar());
                                System.out.flush();
                                List<String> outputColNames = selectDesc.getOutputColumnNames();
                                if (outputColNames != null) {
                                    System.out.println("\t\t\t\toutputColNames: " + outputColNames.toString());
                                    System.out.flush();
                                }
                                System.out.println("\t\t\t\tcolListString: " + selectDesc.getColListString());
                                System.out.flush();
                                System.out.println("\t\t\t\texplainNoCompute: " + selectDesc.explainNoCompute());
                                System.out.flush();
                            }
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (opType.toString().equals("UDTF")) {
                    if (operator instanceof UDTFOperator) {
                        System.out.println("\n\t\t\tUTDF information...");
                        System.out.flush();
                        UDTFOperator udtfOperator = (UDTFOperator) operator;
                        UDTFDesc udtfDesc = udtfOperator.getConf();
                        if (udtfDesc != null) {
                            System.out.println("\n\t\t\t\tisOuterLV: " + udtfDesc.isOuterLV());
                            System.out.flush();
                            System.out.println("\n\t\t\t\tUDTFName: " + udtfDesc.getUDTFName());
                            System.out.flush();
                            System.out.println("\n\t\t\t\tisOuterLateralView: " + udtfDesc.isOuterLateralView());
                            System.out.flush();
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else if (operator instanceof ListSinkOperator) {
                    System.out.println("\n\t\t\tListSink information...");
                    System.out.flush();
                    ListSinkOperator listSinkOperator = (ListSinkOperator) operator;

                    if (listSinkOperator != null) {
                        System.out.println("\n\t\t\t\tNumRows: " + listSinkOperator.getNumRows());
                        System.out.flush();
                    }
                } else if (opType.toString().equals("UNION")) {
                    if (operator instanceof UnionOperator) {
                        System.out.println("\n\t\t\tUnion information...");
                        System.out.flush();
                        UnionOperator unionOperator = (UnionOperator) operator;
                        if (unionOperator != null) {
                            System.out.println("\t\t\t\topAllowedAfterMapJoin: " + unionOperator.opAllowedAfterMapJoin());
                            System.out.flush();
                            System.out.println("\t\t\t\topAllowedBeforeMapJoin: " + unionOperator.opAllowedBeforeMapJoin());
                            System.out.flush();
                            System.out.println("\t\t\t\topAllowedBeforeSortMergeJoin: " + unionOperator.opAllowedBeforeSortMergeJoin());
                            System.out.flush();
                            UnionDesc unionDesc = unionOperator.getConf();
                            if (unionDesc != null) {
                                System.out.println("\t\t\t\tNumInputs: " + unionDesc.getNumInputs());
                                System.out.flush();
                                System.out.println("\t\t\t\tisAllInputsInSameReducer: " + unionDesc.isAllInputsInSameReducer());
                                System.out.flush();
                            }
                        }
                    } else {
                        System.out.println("Operator instance and type do not match!");
                        System.exit(0);
                    }
                } else {
                    System.out.println("\n\t\t\tUnknown Operator Type: " + opType.toString());
                    System.out.flush();
                }
            }
        }
        else{
            System.out.println("\t\t------------------------OPERATOR: " + specialName + " ----------------------------------");
            System.out.flush();
            if(this.getOperatorType().equals("Stats-Aggr Operator")){
                if(ownerStage instanceof StatsTask) {
                    StatsTask statsTask = (StatsTask) ownerStage;
                    if (statsTask != null) {
                        StatsWork statsWork = statsTask.getWork();
                        if(statsWork != null){
                            System.out.println("\t\t\tAggKey: "+statsWork.getAggKey());
                            System.out.flush();
                            System.out.println("\t\t\tisStatsReliable: "+statsWork.isStatsReliable());
                            System.out.flush();
                            System.out.println("\t\t\tisClearAggregatorStats: "+statsWork.isClearAggregatorStats());
                            System.out.flush();
                            System.out.println("\t\t\tisNoScanAnalyzeCommand: "+statsWork.isNoScanAnalyzeCommand());
                            System.out.flush();
                            System.out.println("\t\t\tisPartialScanAnalyzeCommand: "+statsWork.isPartialScanAnalyzeCommand());
                            System.out.flush();
                            System.out.println("\t\t\tNoStatsAggregator: "+statsWork.getNoStatsAggregator());
                            System.out.flush();
                            Task<?> sourceTask = statsWork.getSourceTask();
                            if(sourceTask != null){
                                System.out.println("\t\t\tSourceTask: ");
                                System.out.flush();
                                System.out.println("\t\t\t\tID: "+sourceTask.getId());
                                System.out.flush();
                                List<Task<?>> childTasks = sourceTask.getChildTasks();
                                if(childTasks != null){
                                    System.out.println("\t\t\t\tChildTasks: "+childTasks.toString());
                                    System.out.flush();
                                }
                                System.out.println("\t\t\t\tTop Operators...");
                                System.out.flush();
                                if (sourceTask.getTopOperators() != null) {
                                    if (sourceTask.getTopOperators().size() > 0) {
                                        for (Object o : sourceTask.getTopOperators()) {
                                            if (o != null) {
                                                System.out.println("\t\t\t\t\tOperator: " + ((org.apache.hadoop.hive.ql.exec.Operator<? extends Serializable>) o).getOperatorId());
                                                System.out.flush();
                                            } else {
                                                System.out.println("\t\t\t\t\tOperator is NULL!");
                                                System.out.flush();
                                            }
                                        }
                                    }
                                }
                            }
                            LoadFileDesc loadFileDesc = statsWork.getLoadFileDesc();
                            if(loadFileDesc != null){
                                System.out.println("\t\t\tLoadFileDesc: ");
                                System.out.flush();
                                System.out.println("\t\t\t\tColumns: "+loadFileDesc.getColumns());
                                System.out.flush();
                                System.out.println("\t\t\t\tColumnTypes: "+loadFileDesc.getColumnTypes());
                                System.out.flush();
                                System.out.println("\t\t\t\tDestinationCreateTable: "+loadFileDesc.getDestinationCreateTable());
                                System.out.flush();
                                System.out.println("\t\t\t\tIsDfsDir: "+loadFileDesc.getIsDfsDir());
                                System.out.flush();
                                Path targetDir = loadFileDesc.getTargetDir();
                                if(targetDir != null){
                                    System.out.println("\t\t\t\tTableDir: "+targetDir.toString());
                                    System.out.flush();
                                }
                            }
                            LoadTableDesc loadTableDesc = statsWork.getLoadTableDesc();
                            if(loadTableDesc != null){
                                System.out.println("\t\t\tLoadTableDesc: ");
                                System.out.flush();
                                TableDesc tableDesc = loadTableDesc.getTable();
                                if(tableDesc != null){
                                    System.out.println("\t\t\t\tTableName: "+tableDesc.getTableName());
                                    System.out.flush();
                                }
                                System.out.println("\t\t\t\tHoldDDLTime: "+loadTableDesc.getHoldDDLTime());
                                System.out.flush();
                                System.out.println("\t\t\t\tInheritTableSpecs: "+loadTableDesc.getInheritTableSpecs());
                                System.out.flush();
                                System.out.println("\t\t\t\tReplace: "+loadTableDesc.getReplace());
                                System.out.flush();
                                Map<String, String> partitionSpec = loadTableDesc.getPartitionSpec();
                                if(partitionSpec != null){
                                    System.out.println("\t\t\t\tPartitionSpec: ");
                                    System.out.flush();
                                    for(Map.Entry<String, String> entry : partitionSpec.entrySet()){
                                        if(entry != null){
                                            if(entry.getKey() != null) {
                                                if(entry.getValue() != null) {
                                                    System.out.println("\t\t\t\t\tKey=" + entry.getKey() + " : Value=" + entry.getValue());
                                                    System.out.flush();
                                                }
                                                else{
                                                    System.out.println("\t\t\t\t\tKey=" + entry.getKey() + " : Value=NULL");
                                                    System.out.flush();
                                                }
                                            }
                                        }
                                    }
                                }

                                DynamicPartitionCtx dynamicPartitionCtx = loadTableDesc.getDPCtx();
                                if (dynamicPartitionCtx != null) {
                                    System.out.println("\t\t\tdynamicPartitionCtx: ");
                                    System.out.flush();
                                    System.out.println("\t\t\t\tDynamicPartitionContext: ");
                                    System.out.flush();
                                    System.out.println("\t\t\t\t\tdefaultPartitionName: " + dynamicPartitionCtx.getDefaultPartitionName());
                                    System.out.flush();
                                    if (dynamicPartitionCtx.getDPColNames() != null) {
                                        System.out.println("\t\t\t\t\tDPColNames: " + dynamicPartitionCtx.getDPColNames().toString());
                                        System.out.flush();
                                    }
                                    Map<String, String> inputToDPCols = dynamicPartitionCtx.getInputToDPCols();
                                    if (inputToDPCols != null) {
                                        System.out.println("\t\t\t\t\tinputToDPCols: ");
                                        System.out.flush();
                                        for (Map.Entry<String, String> entry : inputToDPCols.entrySet()) {
                                            System.out.println("\t\t\t\t\t\tKey: " + entry.getKey() + " : " + entry.getValue());
                                            System.out.flush();
                                        }
                                    }
                                    System.out.println("\t\t\t\t\tNumBuckets: " + dynamicPartitionCtx.getNumBuckets());
                                    System.out.flush();
                                    System.out.println("\t\t\t\t\tMaxPartitionsPerNode: " + dynamicPartitionCtx.getMaxPartitionsPerNode());
                                    System.out.flush();
                                    System.out.println("\t\t\t\t\tNumSPCols: " + dynamicPartitionCtx.getNumSPCols());
                                    System.out.flush();
                                    System.out.println("\t\t\t\t\tNumDPCols: " + dynamicPartitionCtx.getNumDPCols());
                                    System.out.flush();
                                    if (dynamicPartitionCtx.getRootPath() != null) {
                                        System.out.println("\t\t\t\t\tRootPath: " + dynamicPartitionCtx.getRootPath());
                                        System.out.flush();
                                    }
                                }
                            }
                        }
                    }
                }
            }
            else if(this.getOperatorType().equals("Create Table Operator")){

            }
            else if(this.getOperatorType().equals("Move Operator")){

            }
        }
    }

}
