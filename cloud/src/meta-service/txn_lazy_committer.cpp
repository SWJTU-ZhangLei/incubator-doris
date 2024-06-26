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

#include "meta-service/txn_lazy_committer.h"

#include <gen_cpp/cloud.pb.h>

#include "common/logging.h"
#include "common/util.h"
#include "keys.h"
#include "meta-service/meta_service_helper.h"

namespace doris::cloud {

static constexpr size_t MAX_ROWSETS_PER_BATCH = 256;

extern void scan_tmp_rowset(
        const std::string& instance_id, int64_t txn_id, std::shared_ptr<TxnKv> txn_kv,
        MetaServiceCode& code, std::string& msg, std::stringstream& ss, int64_t* db_id,
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>* tmp_rowsets_meta);

extern void convert_tmp_rowsets(
        const std::string& instance_id, int64_t txn_id, std::shared_ptr<TxnKv> txn_kv,
        MetaServiceCode& code, std::string& msg, std::stringstream& ss, int64_t db_id,
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>& tmp_rowsets_meta);

TxnLazyCommitTask::TxnLazyCommitTask(const std::string& instance_id, int64_t txn_id,
                                     std::shared_ptr<TxnKv> txn_kv) {
    task = [&]() {
        MetaServiceCode code = MetaServiceCode::OK;
        std::stringstream ss;
        std::string msg;
        int64_t db_id;
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>> tmp_rowsets_meta;
        scan_tmp_rowset(instance_id, txn_id, txn_kv, code, msg, ss, &db_id, &tmp_rowsets_meta);

        for (size_t i = 0; i < tmp_rowsets_meta.size(); i += MAX_ROWSETS_PER_BATCH) {
            size_t end = (i + MAX_ROWSETS_PER_BATCH) > tmp_rowsets_meta.size()
                                 ? tmp_rowsets_meta.size()
                                 : i + MAX_ROWSETS_PER_BATCH;
            std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>> sub_tmp_rowsets_meta(
                    tmp_rowsets_meta.begin() + i, tmp_rowsets_meta.begin() + end);
            convert_tmp_rowsets(instance_id, txn_id, txn_kv, code, msg, ss, db_id,
                                tmp_rowsets_meta);
        }
        return;
    };
}

void TxnLazyCommitTask::wait() {
    std::unique_lock<std::mutex> lock(_mutex);
    _cond.wait(lock, [&]() { return finished.load() == true; });
}

TxnLazyCommitter::TxnLazyCommitter() {
    worker_pool = std::make_unique<SimpleThreadPool>(config::txn_lazy_commit_worker_num);
    worker_pool->start();
}

std::shared_ptr<TxnLazyCommitTask> TxnLazyCommitter::submit(const std::string& instance_id,
                                                            int64_t txn_id,
                                                            std::shared_ptr<TxnKv> txn_kv) {
    std::unique_lock<std::mutex> lock(_mutex);
    auto iter = running_tasks.find(txn_id);
    if (iter != running_tasks.end()) {
        return iter->second;
    }

    std::shared_ptr<TxnLazyCommitTask> task =
            std::make_shared<TxnLazyCommitTask>(instance_id, txn_id, txn_kv);
    running_tasks.emplace(txn_id, task);
    worker_pool->submit(task->task);
    return task;
}

void TxnLazyCommitter::remove(int64_t txn_id) {
    std::unique_lock<std::mutex> lock(_mutex);
}

} // namespace doris::cloud