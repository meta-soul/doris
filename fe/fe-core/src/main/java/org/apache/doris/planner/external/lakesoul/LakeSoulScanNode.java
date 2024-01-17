// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.planner.external.lakesoul;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.LakeSoulExternalTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.external.FileQueryScanNode;
import org.apache.doris.planner.external.TableFormatType;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TLakeSoulFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import lombok.Setter;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class LakeSoulScanNode extends FileQueryScanNode {

    protected final LakeSoulExternalTable lakeSoulExternalTable;

    protected final TableInfo table;

    @Setter
    private LogicalFileScan.SelectedPartitions selectedPartitions = null;

    /**
     * External file scan node for Query lakesoul table
     * needCheckColumnPriv: Some of ExternalFileScanNode do not need to check column priv
     * eg: s3 tvf
     * These scan nodes do not have corresponding catalog/database/table info, so no need to do priv check
     *
     * @param id
     * @param desc
     * @param needCheckColumnPriv
     */
    public LakeSoulScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        super(id, desc, "planNodeName", StatisticalType.LAKESOUL_SCAN_NODE, needCheckColumnPriv);
        lakeSoulExternalTable = (LakeSoulExternalTable) desc.getTable();
        table = lakeSoulExternalTable.getLakeSoulTableInfo();
    }

    @Override
    protected TFileType getLocationType() throws UserException {
        String location = table.getTablePath();
        return getLocationType(location);
    }

    @Override
    protected TFileType getLocationType(String location) throws UserException {
        return getTFileType(location).orElseThrow(() ->
                new DdlException("Unknown file location " + location + " for iceberg table " + table.getTableName()));
    }

    @Override
    protected TFileFormatType getFileFormatType() throws UserException {
        return TFileFormatType.FORMAT_PARQUET;
    }

    @Override
    protected List<String> getPathPartitionKeys() throws UserException {
        return DBUtil.parseTableInfoPartitions(table.getPartitions()).rangeKeys;
    }

    @Override
    protected TableIf getTargetTable() throws UserException {
        return lakeSoulExternalTable;
    }

    @Override
    protected Map<String, String> getLocationProperties() throws UserException {
        return lakeSoulExternalTable.getHadoopProperties();
    }

    public static void setLakeSoulParams(TFileRangeDesc rangeDesc, LakeSoulSplit lakeSoulSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(lakeSoulSplit.getTableFormatType().value());
        TLakeSoulFileDesc fileDesc = new TLakeSoulFileDesc();
        fileDesc.setBasePath(lakeSoulSplit.getPath().toString());
        fileDesc.setColumnNames(lakeSoulSplit.getLakeSoulColumnNames());
        fileDesc.setTableSchema(lakeSoulSplit.getTableSchema());
        tableFormatFileDesc.setLakesoulParams(fileDesc);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    protected List<Split> getSplits() throws UserException {
        List<Split> splits = new ArrayList<>();
        List<String> tablePartitions = getPathPartitionKeys();
        if (tablePartitions.size() > 0) {
            boolean isPartitionPruned = selectedPartitions == null ? false : selectedPartitions.isPruned;
            if (!isPartitionPruned) {
                System.out.println("aaa");
            } else {
                this.totalPartitionNum = selectedPartitions.totalPartitionNum;
                Collection<PartitionItem> partitionItems = selectedPartitions.selectedPartitions.values();
                for (PartitionItem item : partitionItems) {
                    List<PartitionKey> items = item.getItems();
                    for (PartitionKey p : items) {
                        System.out.println(p.toString());
                    }
                }

            }
        } else {
            DataFileInfo[] tableDataInfo = DataOperation.getTableDataInfo(table.getTableId());
            for (DataFileInfo dataFileInfo : tableDataInfo) {
                String filePath = dataFileInfo.path();
                long fileSize = dataFileInfo.size();
                //                List<String> partitionValues = getPathPartitionKeys();
                LakeSoulSplit lakeSoulSplit = new LakeSoulSplit(new Path(filePath), 0, fileSize, fileSize,
                        new String[0], null);
                lakeSoulSplit.setTableSchema(table.getTableSchema());
                lakeSoulSplit.setLakeSoulColumnNames(Arrays.asList(dataFileInfo.file_exist_cols().split(",")));
                lakeSoulSplit.setTableFormatType(TableFormatType.LAKESOUL);
                splits.add(lakeSoulSplit);

            }
        }
        return splits;
    }
}

