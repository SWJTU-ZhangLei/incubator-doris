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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.utframe.TestWithFeService;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Lists;
import com.selectdb.cloud.proto.SelectdbCloud.StagePB;
import com.selectdb.cloud.proto.SelectdbCloud.StagePB.StageType;
import mockit.Expectations;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CopyIntoTest extends TestWithFeService {

    private static final String OBJ_INFO = "properties (\"bucket\" = \"tmp_bucket\", "
            + "\"endpoint\" = \"cos.ap-beijing.myqcloud.com\", "
            + "\"prefix\" = \"tmp_prefix\", "
            + "\"sk\" = \"tmp_sk\", "
            + "\"ak\" = \"tmp_ak\", "
            + "\"provider\" = \"s3\", "
            + "\"region\" = \"ap-beijing\" ";
    private List<String> tableColumnNames = Lists.newArrayList("id", "name", "score");

    private static final String INTERNAL_STAGE_ID = "test_in_stage_id";
    private StagePB externalStagePB;
    private StagePB internalStagePB;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        String varcharTable = "CREATE TABLE t2 (\n" + "id INT,\n" + "name varchar(20),\n" + "score INT\n" + ")\n"
                + "DUPLICATE KEY(id, name)\n" + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1');";
        createTable(varcharTable);

        String query = "create stage if not exists ex_stage_1 " + OBJ_INFO
                + ", 'default.file.type' = 'csv', 'default.file.column_separator'=\",\" "
                + ", 'default.copy.on_error' = 'max_filter_ratio_0.4', 'default.copy.size_limit' = '100')";
        externalStagePB = ((CreateStageStmt) UtFrameUtils.parseAndAnalyzeStmt(query, connectContext)).toStageProto();
        internalStagePB = StagePB.newBuilder().setType(StageType.INTERNAL).addMysqlUserName("test")
                .setStageId(INTERNAL_STAGE_ID).setObjInfo(externalStagePB.getObjInfo()).build();
    }

    @Test
    public void testCopyInto() throws Exception {
        String query1 = "create stage if not exists ex_stage_2 " + OBJ_INFO + ")";
        StagePB stagePB1 = ((CreateStageStmt) UtFrameUtils.parseAndAnalyzeStmt(query1, connectContext)).toStageProto();
        List<StagePB> stages1 = Lists.newArrayList(stagePB1);
        String query2 = "create stage if not exists ex_stage_3 " + OBJ_INFO
                + ", 'default.file.type' = 'csv', 'default.file.column_separator' = ',' "
                + ", 'default.copy.on_error' = 'continue', 'default.copy.size_limit' = '100'"
                + ", 'default.copy.strict_mode' = 'false')";
        StagePB stagePB2 = ((CreateStageStmt) UtFrameUtils.parseAndAnalyzeStmt(query2, connectContext)).toStageProto();
        List<StagePB> stages2 = Lists.newArrayList(stagePB2);

        new Expectations(connectContext.getEnv(), connectContext.getEnv().getInternalCatalog()) {
            {
                Env.getCurrentInternalCatalog().getStage(StageType.EXTERNAL, anyString, "ex_stage_2");
                minTimes = 0;
                result = stages1;

                Env.getCurrentInternalCatalog().getStage(StageType.EXTERNAL, anyString, "ex_stage_3");
                minTimes = 0;
                result = stages2;
            }
        };

        String copySqlPrefix = "copy into t2 from @ex_stage_2 ";
        checkCopyInto(copySqlPrefix, null, 0, true, 0, 0, false);
        String copySqlPrefix2 = "copy into t2 from @ex_stage_3 ";
        checkCopyInto(copySqlPrefix2, "csv", 100, true, 5, 1, false);

        String copyProperties = "properties ('file.type' = 'json', 'file.fuzzy_parse'='true', 'file.json_root'=\"{\", "
                + "'copy.on_error' = 'continue', 'copy.size_limit' = '200', " + "'copy.async' = 'false')";
        String copySql = copySqlPrefix + copyProperties;
        checkCopyInto(copySql, "json",  200, false, 6, 0, false);
        copySql = copySqlPrefix2 + copyProperties;
        checkCopyInto(copySql, "json",  200, false, 8, 1, false);

        copyProperties = "properties ('file.type' = 'csv', 'file.fuzzy_parse'='true', 'file.json_root'=\"{\", "
                + "'copy.on_error' = 'continue', 'copy.size_limit' = '300')";
        copySql = copySqlPrefix + copyProperties;
        checkCopyInto(copySql, "csv",  300, true, 5, 0, false);
        copySql = copySqlPrefix2 + copyProperties;
        checkCopyInto(copySql, "csv",  300, true, 7, 1, false);

        copyProperties = "properties ('file.type' = 'csv', 'file.fuzzy_parse'='true', 'file.json_root'=\"{\") ";
        copySql = copySqlPrefix + copyProperties;
        checkCopyInto(copySql, "csv",  0, true, 3, 0, false);
        copySql = copySqlPrefix2 + copyProperties;
        checkCopyInto(copySql, "csv",  100, true, 7, 1, false);

        copyProperties = "properties ('copy.on_error' = 'continue', 'copy.size_limit' = '400')";
        copySql = copySqlPrefix + copyProperties;
        checkCopyInto(copySql, null,  400, true, 2, 0, false);
        copySql = copySqlPrefix2 + copyProperties;
        checkCopyInto(copySql, "csv",  400, true, 5, 1, false);

        copyProperties = "properties('copy.async' = 'false')";
        copySql = copySqlPrefix + copyProperties;
        checkCopyInto(copySql, null,  0, false, 1, 0, false);
        copySql = copySqlPrefix2 + copyProperties;
        checkCopyInto(copySql, "csv",  100, false, 6, 1, false);

        copyProperties = "properties ('file.compression' = 'gz') ";
        copySql = copySqlPrefix + copyProperties;
        checkCopyInto(copySql, null,  0, true, 1, 0, false);
        copySql = copySqlPrefix2 + copyProperties;
        checkCopyInto(copySql, null,  100, true, 6, 1, false);

        copyProperties = "properties ('file.type' = 'csv', 'file.compression' = 'gz') ";
        copySql = copySqlPrefix + copyProperties;
        checkCopyInto(copySql, null,  0, true, 2, 0, false);
        copySql = copySqlPrefix2 + copyProperties;
        checkCopyInto(copySql, null,  100, true, 6, 1, false);

        copyProperties = "properties('copy.strict_mode' = 'true')";
        copySql = copySqlPrefix + copyProperties;
        checkCopyInto(copySql, null,  0, true, 1, 1, true);
        copySql = copySqlPrefix2 + copyProperties;
        checkCopyInto(copySql, "csv",  100, true, 5, 1, true);
    }

    private void checkCopyInto(String sql, String fileType, long sizeLimit, boolean async, int propertiesNum,
            int execPropertiesNum, boolean strictMode) {
        try {
            CopyStmt copyStmt = (CopyStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
            System.out.println("original sql: " + sql);
            System.out.println("parsed sql: " + copyStmt.toSql());
            Assert.assertEquals(propertiesNum, copyStmt.getCopyIntoProperties().getProperties().size());
            Assert.assertEquals(fileType, copyStmt.getCopyIntoProperties().getFileType());
            Assert.assertEquals(sizeLimit, copyStmt.getCopyIntoProperties().getSizeLimit());
            Assert.assertEquals(async, copyStmt.isAsync());
            Assert.assertEquals(execPropertiesNum, copyStmt.getCopyIntoProperties().getExecProperties().size());
            if (execPropertiesNum > 0) {
                Assert.assertEquals(strictMode,
                        Boolean.parseBoolean(copyStmt.getCopyIntoProperties().getExecProperties().get("strict_mode")));
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("must be success.");
        }
    }

    @Test
    public void testCopyFromInternalStage() throws Exception {
        List<StagePB> stages = Lists.newArrayList(internalStagePB);
        new Expectations(connectContext.getEnv().getInternalCatalog()) {
            {
                Env.getCurrentInternalCatalog().getStage(StageType.INTERNAL, anyString, null);
                minTimes = 0;
                result = stages;
            }
        };

        String sql = "copy into t2 from @~";
        CopyStmt copyStmt = parseAndAnalyze(sql);
        Assert.assertEquals("~", copyStmt.getStage());
        Assert.assertEquals(INTERNAL_STAGE_ID, copyStmt.getStageId());
        Assert.assertEquals(StageType.INTERNAL, copyStmt.getStageType());
        Assert.assertEquals("tmp_ak", copyStmt.getObjectInfo().getAk());
    }

    @Test
    public void testCopyWithPattern() throws Exception {
        List<StagePB> stages = Lists.newArrayList(internalStagePB);
        new Expectations(connectContext.getEnv().getInternalCatalog()) {
            {
                Env.getCurrentInternalCatalog().getStage(StageType.INTERNAL, anyString, null);
                minTimes = 0;
                result = stages;
            }
        };

        String sql1 = "copy into t2 from @~('/*.csv')";
        String sql2 = "copy into t2 from (select $3, $1, $2 from @~('/*.csv'))";
        for (String sql : Lists.newArrayList(sql1, sql2)) {
            CopyStmt copyStmt = parseAndAnalyze(sql);
            Assert.assertEquals("/*.csv", copyStmt.getPattern());
        }
    }

    @Test
    public void testCopyIntoWithSelect() throws Exception {
        List<StagePB> stages = Lists.newArrayList(externalStagePB);
        new Expectations(connectContext.getEnv(), connectContext.getEnv().getInternalCatalog()) {
            {
                Env.getCurrentInternalCatalog().getStage(StageType.EXTERNAL, anyString, "ex_stage_1");
                minTimes = 0;
                result = stages;
            }
        };

        String copySql = "copy into t2 from (select from @ex_stage_1) ";
        checkEmptyDataDescription(copySql);

        copySql = "copy into t2 from (select * from @ex_stage_1) ";
        checkEmptyDataDescription(copySql);

        copySql = "copy into t2 from (select $2, $1, $3 from @ex_stage_1) ";
        checkDataDescription(copySql, Lists.newArrayList("$2", "$1", "$3"));

        copySql = "copy into t2 from (select $3, $1 from @ex_stage_1) ";
        checkDataDescriptionWithException(copySql);

        copySql = "copy into t2 from (select $2, $1+100, $3 from @ex_stage_1) ";
        checkDataDescription(copySql, Lists.newArrayList("$2", "$1", "$3"));

        copySql = "copy into t2 from "
                + "(select $1, str_to_date($3, '%Y-%m-%d'), $2 + 1 from @ex_stage_1 where $2 > $1) ";
        checkDataDescription(copySql, Lists.newArrayList("$1", "$3", "$2"));

        copySql = "copy into t2 from "
                + "(select $1, str_to_date($3, '%Y-%m-%d'), $2 + 1 from @ex_stage_1 where $2 > $1) ";
        checkDataDescription(copySql, Lists.newArrayList("$1", "$3", "$2"));

        copySql = "copy into t2 from (select $2, NULL, $3 from @ex_stage_1) ";
        checkDataDescriptionWithNull(copySql, Lists.newArrayList("$2", "", "$3"), 1);

        copySql = "copy into t2 from (select $3, $1, $a from @ex_stage_1) ";
        checkDataDescriptionWithException(copySql);
    }

    private void checkDataDescription(String sql, List<String> filedColumns) {
        try {
            CopyStmt copyStmt = (CopyStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
            System.out.println("original sql: " + sql);
            System.out.println("parsed sql: " + copyStmt.toSql());
            List<DataDescription> dataDescriptions = copyStmt.getDataDescriptions();
            Assert.assertEquals(1, dataDescriptions.size());
            DataDescription dataDescription = dataDescriptions.get(0);
            // check file field names
            List<String> fileFieldNames = dataDescription.getFileFieldNames();
            Assert.assertEquals(3, fileFieldNames.size());
            for (int i = 0; i < fileFieldNames.size(); i++) {
                Assert.assertEquals("$" + (i + 1), fileFieldNames.get(i));
            }
            // check column mapping
            List<Expr> columnMappingList = dataDescription.getColumnMappingList();
            Assert.assertNotNull(columnMappingList);
            Assert.assertEquals(3, columnMappingList.size());
            for (int i = 0; i < columnMappingList.size(); i++) {
                Expr expr = columnMappingList.get(i);
                List<SlotRef> slotRefs = Lists.newArrayList();
                Expr.collectList(Lists.newArrayList(expr), SlotRef.class, slotRefs);
                Assert.assertEquals(2, slotRefs.size());
                Assert.assertEquals(tableColumnNames.get(i), slotRefs.get(0).getColumnName());
                Assert.assertEquals(filedColumns.get(i), slotRefs.get(1).getColumnName());
            }
        } catch (Exception e) {
            Assert.fail("must be success.");
        }
    }

    private void checkDataDescriptionWithNull(String sql, List<String> filedColumns, int nullId) {
        try {
            CopyStmt copyStmt = (CopyStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
            System.out.println("original sql: " + sql);
            System.out.println("parsed sql: " + copyStmt.toSql());
            List<DataDescription> dataDescriptions = copyStmt.getDataDescriptions();
            Assert.assertEquals(1, dataDescriptions.size());
            DataDescription dataDescription = dataDescriptions.get(0);
            // check file field names
            List<String> fileFieldNames = dataDescription.getFileFieldNames();
            Assert.assertEquals(3, fileFieldNames.size());
            for (int i = 0; i < fileFieldNames.size(); i++) {
                Assert.assertEquals("$" + (i + 1), fileFieldNames.get(i));
            }
            // check column mapping
            List<Expr> columnMappingList = dataDescription.getColumnMappingList();
            Assert.assertNotNull(columnMappingList);
            Assert.assertEquals(3, columnMappingList.size());
            for (int i = 0; i < columnMappingList.size(); i++) {
                Expr expr = columnMappingList.get(i);
                System.out.println("expr = " + expr.debugString());
                List<SlotRef> slotRefs = Lists.newArrayList();
                Expr.collectList(Lists.newArrayList(expr), SlotRef.class, slotRefs);
                if (i == nullId) {
                    Assert.assertEquals(1, slotRefs.size());
                    Assert.assertEquals(tableColumnNames.get(i), slotRefs.get(0).getColumnName());
                } else {
                    Assert.assertEquals(2, slotRefs.size());
                    Assert.assertEquals(tableColumnNames.get(i), slotRefs.get(0).getColumnName());
                    Assert.assertEquals(filedColumns.get(i), slotRefs.get(1).getColumnName());
                }
            }
        } catch (Exception e) {
            Assert.fail("must be success.");
        }
    }

    private void checkEmptyDataDescription(String sql) {
        try {
            CopyStmt copyStmt = (CopyStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
            System.out.println("original sql: " + sql);
            System.out.println("parsed sql: " + copyStmt.toSql());
            List<DataDescription> dataDescriptions = copyStmt.getDataDescriptions();
            Assert.assertEquals(1, dataDescriptions.size());
            DataDescription dataDescription = dataDescriptions.get(0);
            // check file field names
            Assert.assertNull(dataDescription.getFileFieldNames());
            Assert.assertNull(dataDescription.getPrecdingFilterExpr());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("must be success.");
        }
    }

    private void checkDataDescriptionWithException(String sql) {
        do {
            try {
                UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
                Assert.fail("should not come here");
            } catch (AnalysisException e) {
                Assert.assertTrue(true);
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while (false);
    }

    private CopyStmt parseAndAnalyze(String sql) throws Exception {
        try {
            CopyStmt stmt = (CopyStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
            return stmt;
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("must be success.");
            throw e;
        }
    }
}
