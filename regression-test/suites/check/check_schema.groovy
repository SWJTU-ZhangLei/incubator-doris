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

import groovy.json.JsonSlurper

suite("check_schema") {
   def httpGet = { url ->
        def conn = new URL(url).openConnection()
        conn.setRequestMethod("GET")
        if (conn.getResponseCode() == 200) {
            return conn.getInputStream().getText()
        }
        return null;
    }

    def checkColumnNullable = { tbSchema, tabletColumns ->
        // tbSchema and tabletColumns
        for (tbColumn : tbSchema) {
            for (tabletColumn : tabletColumns) {
                if (tbColumn[0].equalsIgnoreCase(tabletColumn["name"])) {
                    if (tbColumn[2].equalsIgnoreCase("Yes") && tabletColumn["is_nullable"] == false) {
                        return false;
                    }
                    if (tbColumn[2].equalsIgnoreCase("No") && tabletColumn["is_nullable"] == true) {
                        return false;
                    }
                    break;
                }
            }
        }
        return true;
    }

    def List<String> exculudeDatabases = ["__internal_schema", "information_schema", "mysql"]
    List<List<Object>> databases = sql "show databases;"
    logger.info("db size:{}", databases.size());
    logger.info("dbNames:{}", databases);
    for (List<Object> databaseEntry : databases) {
        try {
            String databaseName = databaseEntry[0]
            if (exculudeDatabases.contains(databaseName)) {
                continue;
            }
            logger.info("=================================================\n")
            List<List<Object>> tables = sql "show tables from ${databaseName};"
            logger.info("db:{} tb size:{}", databaseName, tables.size())
            logger.info("db:{} tbNames:{}", databaseName, tables)
            for (List<Object> tableEntry : tables) {
                try {
                    logger.info("-------------------------------------------\n")
                    String tableName = tableEntry[0]
                    List<List<Object>> tablets = sql "show tablets from ${databaseName}.${tableName};"
                    logger.info("db:{} tb:{} tablets size:{}", databaseName, tableName, tablets.size())
                    logger.debug("db:{} tb:{} tablets:{}", databaseName, tableName, tablets)

                    List<List<Object>> tbSchema = sql "desc ${databaseName}.${tableName};"
                    logger.debug("db:{} tb:{} tbSchema:{}", databaseName, tableName, tbSchema)

                    for (List<Object> tabletEntry : tablets) {
                        try {
                            String tabletId = tabletEntry[0]
                            String replicaId = tabletEntry[1]
                            String metaUrl = tabletEntry[17]
                            logger.debug("tabletId:{} replicaId:{} metaUrl:{}", tabletId, replicaId, metaUrl)

                            def result = httpGet(metaUrl)
                            result = profiles = new JsonSlurper().parseText(result)
                            logger.debug("result:{}", result)
                            def columns = result["schema"]["column"]

                            logger.debug("columns:{}", columns)
                            if (!checkColumnNullable(tbSchema, columns)) {
                                logger.warn("db:{} tb:{} tabletId:{} replicaId:{} check failed", databaseName, tableName, tabletId, replicaId)
                            } else {
                                logger.info("db:{} tb:{} tabletId:{} replicaId:{} check pass", databaseName, tableName, tabletId, replicaId)
                            }
                        } catch (Exception e) {
                            logger.warn("db:{} tb:{} tabletEntry:{} exception:", databaseName, tableName, tabletEntry, e)
                        }
                    }
                } catch(Exception e) {
                    logger.warn("databaseEntry:{} tableEntry:{} exception:", databaseEntry, tableEntry, e)
                }
            }
        } catch(Exception e) {
            logger.warn("databaseEntry:{} exception:", databaseEntry, e)
        }
    }
}
