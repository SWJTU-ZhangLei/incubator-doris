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

/* ******* Do not commit this file unless you know what you are doing ******* */

// **Note**: default db will be create if not exist
defaultDb = "regression_test"

// add useLocalSessionState so that the jdbc will not send
// init cmd like: select @@session.tx_read_only
// at each time we connect.
jdbcUrl = "jdbc:mysql://127.0.0.1:9030/?useLocalSessionState=true"
jdbcUser = "root"
jdbcPassword = ""

feHttpAddress = "127.0.0.1:8030"
feHttpUser = "root"
feHttpPassword = ""

feCloudHttpAddress = "127.0.0.1:8035"
feCloudHttpUser = "root"
feCloudHttpPassword = ""

// for cloud mode
instanceId = "clickbench"
cloudUniqueId = "cloud_unique_id_xxx"
metaServiceHttpAddress = "127.0.0.1:5000"
recycleServiceHttpAddress = "127.0.0.1:5001"

// set DORIS_HOME by system properties
// e.g. java -DDORIS_HOME=./
suitePath = "${DORIS_HOME}/regression-test/suites"
dataPath = "${DORIS_HOME}/regression-test/data"
pluginPath = "${DORIS_HOME}/regression-test/plugins"
realDataPath = "${DORIS_HOME}/regression-test/realdata"
// sf1DataPath can be url like "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com" or local path like "/data"
sf1DataPath = "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com"

// will test <group>/<suite>.groovy
// empty group will test all group
testGroups = ""
// empty suite will test all suite
testSuites = ""
// empty directories will test all directories
testDirectories = ""

// this groups will not be executed
excludeGroups = ""
// this suites will not be executed
excludeSuites = "test_date_function,create_table_use_partition_policy"
// this directories will not be executed
excludeDirectories = ""

customConf1 = "test_custom_conf_value"

// for test csv with header
enableHdfs=false // set to true if hdfs is ready
hdfsFs = "hdfs://127.0.0.1:9000"
hdfsUser = "doris-test"
hdfsPasswd = ""
brokerName = "broker_name"

// broker load test config
enableBrokerLoad=true
ak=""
sk=""

// jdbc connector test config
// To enable jdbc test, you need first start mysql/pg container.
// See `docker/thirdparties/start-thirdparties-docker.sh`
enableJdbcTest=false
mysql_57_port=3316
pg_14_port=5442

// hive catalog test config
// To enable jdbc test, you need first start hive container.
// See `docker/thirdparties/start-thirdparties-docker.sh`
enableHiveTest=false
hms_port=9183
hdfs_port=8120

// elasticsearch catalog test config
// See `docker/thirdparties/start-thirdparties-docker.sh`
enableEsTest=false
es_6_port=19200
es_7_port=29200
es_8_port=39200

// be1_ip:be1_heartbeat_port:be1_http_port:be1_unique_id,be2_ip:be2_heartbeat_port:be2_http_port:be2_unique_id
multiClusterBes = "127.0.0.1:9712:9711:be_1_id,127.0.0.1:9732:9731:be_2_id"
metaServiceToken = "greedisgood9999"
multiClusterInstance = "test_instance_id"
cacheDataPath = "/tmp"

s3Endpoint = "cos.ap-hongkong.myqcloud.com"
s3BucketName = "doris-build-hk-1308700295"
s3Region = "ap-hongkong"

// stage iam test
stageIamEndpoint = ""
stageIamRegion = ""
stageIamBucket = ""

// used for oss and obs, which does not support external id. Create role (or agency) and use for all instances.
stageIamRole = ""
stageIamArn = ""

// used for cos and s3, which support external id.
// this is policy arn for s3
stageIamPolicy = "smoke_test_policy"
stageIamAk = ""
stageIamSk = ""

// used for cos, which sdk does not return arn, so construct arn by user id.
stageIamUserId = ""