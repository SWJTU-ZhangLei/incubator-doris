
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

#pragma once

#include "common/config.h"
#include "common/simple_thread_pool.h"
#include "meta-service/txn_kv.h"

namespace doris::cloud {

class TxnLazyCommitTask {
public:
    TxnLazyCommitTask(const std::string& instance_id, int64_t txn_id,
                      std::shared_ptr<TxnKv> txn_kv);

    void wait();

private:
    friend class TxnLazyCommitter;
    std::function<void()> task;
    std::mutex _mutex;
    std::condition_variable _cond;
    std::atomic_bool finished = false;
};

class TxnLazyCommitter {
public:
    TxnLazyCommitter() {
        worker_pool = std::make_unique<SimpleThreadPool>(config::txn_lazy_commit_worker_num);
        worker_pool->start();
    };
    std::shared_ptr<TxnLazyCommitTask> submit_task(const std::string& instance_id, int64_t txn_id,
                                                   std::shared_ptr<TxnKv> txn_kv);
    void remove_task(int64_t txn_id);

private:
    std::unique_ptr<SimpleThreadPool> worker_pool;

    std::mutex _mutex;
    // <txn_id, TxnLazyCommitTask>
    std::unordered_map<int64_t, std::shared_ptr<TxnLazyCommitTask>> running_tasks;
};
} // namespace doris::cloud