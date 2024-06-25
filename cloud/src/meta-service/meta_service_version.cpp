#include "common/logging.h"
#include "meta-service/meta_service.h"
#include "meta-service/meta_service_helper.h"

namespace doris::cloud {

void MetaServiceImpl::get_version(::google::protobuf::RpcController* controller,
                                  const GetVersionRequest* request, GetVersionResponse* response,
                                  ::google::protobuf::Closure* done) {
    get_version_v1(controller, request, response, done);
}

void MetaServiceImpl::get_version_v2(::google::protobuf::RpcController* controller,
                                     const GetVersionRequest* request, GetVersionResponse* response,
                                     ::google::protobuf::Closure* done) {
    if (request->batch_mode()) {
        batch_get_version(controller, request, response, done);
        return;
    }

    RPC_PREPROCESS(get_version);
    std::string cloud_unique_id;
    if (request->has_cloud_unique_id()) {
        cloud_unique_id = request->cloud_unique_id();
    }

    bool is_table_version = false;
    if (request->has_is_table_version()) {
        is_table_version = request->is_table_version();
    }

    int64_t db_id = request->has_db_id() ? request->db_id() : -1;
    int64_t table_id = request->has_table_id() ? request->table_id() : -1;
    int64_t partition_id = request->has_partition_id() ? request->partition_id() : -1;
    if (db_id == -1 || table_id == -1 || (!is_table_version && partition_id == -1)) {
        msg = "params error, db_id=" + std::to_string(db_id) +
              " table_id=" + std::to_string(table_id) +
              " partition_id=" + std::to_string(partition_id) +
              " is_table_version=" + std::to_string(is_table_version);
        code = MetaServiceCode::INVALID_ARGUMENT;
        LOG(WARNING) << msg;
        return;
    }

    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(get_version)

    bool need_retry = false;
    do {
        std::string ver_key;
        if (is_table_version) {
            table_version_key({instance_id, db_id, table_id}, &ver_key);
        } else {
            partition_version_key({instance_id, db_id, table_id, partition_id}, &ver_key);
        }

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            msg = "failed to create txn";
            code = cast_as<ErrCategory::CREATE>(err);
            return;
        }

        std::string ver_val;
        // 0 for success get a key, 1 for key not found, negative for error
        err = txn->get(ver_key, &ver_val);
        VLOG_DEBUG << "xxx get version_key=" << hex(ver_key);
        if (err == TxnErrorCode::TXN_OK) {
            if (is_table_version) {
                int64_t version = 0;
                if (!txn->decode_atomic_int(ver_val, &version)) {
                    code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                    msg = "malformed table version value";
                    return;
                }
                response->set_version(version);
            } else {
                VersionPB version_pb;
                if (!version_pb.ParseFromString(ver_val)) {
                    code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                    msg = "malformed version value";
                    return;
                }

                if (version_pb.has_txn_id()) {
                    need_retry = true;
                    std::shared_ptr<TxnLazyCommitTask> task = txn_lazy_committer->submit_task(
                            instance_id, version_pb.txn_id(), txn_kv_);
                    task->wait();
                    txn_lazy_committer->remove_task(version_pb.txn_id());
                } else {
                    need_retry = false;
                    response->set_version(version_pb.version());
                }
            }
            { TEST_SYNC_POINT_CALLBACK("get_version_code", &code); }
            return;
        } else if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            msg = "not found";
            code = MetaServiceCode::VERSION_NOT_FOUND;
            return;
        }
        msg = fmt::format("failed to get txn, err={}", err);
        code = cast_as<ErrCategory::READ>(err);
    } while (need_retry);
}
} // namespace doris::cloud