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

package org.apache.doris.catalog.external;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.lakesoul.LakeSoulExternalCatalog;
import org.apache.doris.thrift.TLakeSoulTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.dmetasoul.lakesoul.meta.dao.TableInfoDao;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.google.common.collect.Lists;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.arrow.ArrowUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LakeSoulExternalTable extends ExternalTable {

    public static final int LAKESOUL_TIMESTAMP_SCALE_MS = 6;

    public LakeSoulExternalTable(long id, String name, String dbName, LakeSoulExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.LAKESOUl_EXTERNAL_TABLE);
    }

    private Type lakeSoulTypeToDorisType(DataType dt) {

        if (dt.equals(DataTypes.BooleanType)) {
            return Type.BOOLEAN;
        } else if (dt.equals(DataTypes.ByteType)) {
            return Type.TINYINT;
        } else if (dt.equals(Type.SMALLINT)) {
            return Type.SMALLINT;
        } else if (dt.equals(DataTypes.IntegerType)) {
            return Type.INT;
        } else if (dt.equals(DataTypes.LongType)) {
            return Type.BIGINT;
        } else if (dt.equals(DataTypes.FloatType)) {
            return Type.FLOAT;
        } else if (dt.equals(DataTypes.DoubleType)) {
            return Type.DOUBLE;
        } else if (dt.equals(DataTypes.StringType) || dt.equals(DataTypes.BinaryType)) {
            return Type.STRING;
        } else if (dt instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) dt;
            return ScalarType.createDecimalV3Type(decimalType.precision(), decimalType.scale());
        } else if (dt.equals(DataTypes.DateType)) {
            return ScalarType.createDateV2Type();
        } else if (dt.equals(DataTypes.TimestampType)) {
            return ScalarType.createDatetimeV2Type(LAKESOUL_TIMESTAMP_SCALE_MS);
        } else if (dt instanceof StructType) {
            ArrayList<org.apache.doris.catalog.StructField> fields = new ArrayList<>();
            for (StructField structField : ((StructType) dt).fields()) {
                fields.add(new org.apache.doris.catalog.StructField(structField.name(),
                        lakeSoulTypeToDorisType(structField.dataType())));
            }
            return new org.apache.doris.catalog.StructType(fields);
        }
        throw new IllegalArgumentException("Cannot transform unknown type: " + dt);
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        TLakeSoulTable tLakeSoulTable = new TLakeSoulTable(dbName, name, new HashMap<>());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.LAKESOUL_TABLE, schema.size(), 0,
                getName(), dbName);
        tTableDescriptor.setLakesoulTable(tLakeSoulTable);
        return tTableDescriptor;

    }

    @Override
    public List<Column> initSchema() {
        String tableSchema = ((LakeSoulExternalCatalog) catalog).getLakeSoulTable(dbName, name).getTableSchema();
        StructType schema = null;
        System.out.println(tableSchema);
        if (TableInfoDao.isArrowKindSchema(tableSchema)) {
            try {
                schema = ArrowUtils.fromArrowSchema(Schema.fromJSON(tableSchema));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            schema = (StructType) DataType.fromJson(tableSchema);
        }
        List<Column> tmpSchema = Lists.newArrayListWithCapacity(schema.size());
        for (StructField field : schema.fields()) {
            tmpSchema.add(new Column(new Column(field.name(), lakeSoulTypeToDorisType(field.dataType()),
                    true, null, true,
                    field.metadata().contains("comment") ? field.metadata().getString("comment") : null,
                    true, schema.fieldIndex(field.name()))));
        }
        return tmpSchema;
    }

    public TableInfo getLakeSoulTableInfo() {
        return ((LakeSoulExternalCatalog) catalog).getLakeSoulTable(dbName, name);
    }

    public String tablePath() {
        return ((LakeSoulExternalCatalog) catalog).getLakeSoulTable(dbName, name).getTablePath();
    }

    public Map<String, String> getHadoopProperties() {
        return catalog.getCatalogProperty().getHadoopProperties();
    }
}

