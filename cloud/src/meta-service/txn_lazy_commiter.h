

#include "common/simple_thread_pool.h"
#include "meta-service/txn_kv.h"

namespace doris::cloud {

class TxnLazyCommitTask {
public:
    TxnLazyCommitTask(const std::string& instance_id, int64_t txn_id,
                      std::shared_ptr<TxnKv> txn_kv);

    void wait();

private:
    friend class TxnLazyCommiter;
    std::function<void()> task;
    std::mutex _mutex;
    std::condition_variable _cond;
    std::atomic_bool finished = false;
};

class TxnLazyCommiter {
public:
    TxnLazyCommiter() {
        worker_pool = std::make_unique<SimpleThreadPool>(2);
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