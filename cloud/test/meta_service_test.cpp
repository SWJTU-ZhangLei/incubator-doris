
// clang-format off
#include "meta-service/meta_service.h"

#include "common/config.h"
#include "common/logging.h"
#include "common/sync_point.h"
#include "common/util.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "meta-service/keys.h"
#include "meta-service/mem_txn_kv.h"
#include "resource-manager/resource_manager.h"
#include "mock_resource_manager.h"

#include "brpc/controller.h"
#include "gtest/gtest.h"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <random>
// clang-format on

int main(int argc, char** argv) {
    const std::string conf_file = "selectdb_cloud.conf";
    if (!selectdb::config::init(conf_file.c_str(), true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }

    if (!selectdb::init_glog("meta_service_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

using namespace selectdb;

std::unique_ptr<MetaServiceImpl> get_meta_service() {
    int ret = 0;
    // MemKv
    static auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    if (txn_kv != nullptr) {
        ret = txn_kv->init();
        [&] { ASSERT_EQ(ret, 0); }();
    }
    [&] { ASSERT_NE(txn_kv.get(), nullptr); }();

    // FdbKv
    //     config::fdb_cluster_file_path = "fdb.cluster";
    //     static auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<FdbTxnKv>());
    //     static std::atomic<bool> init {false};
    //     bool tmp = false;
    //     if (init.compare_exchange_strong(tmp, true)) {
    //         int ret = txn_kv->init();
    //         [&] { ASSERT_EQ(ret, 0); ASSERT_NE(txn_kv.get(), nullptr); }();
    //     }

    std::unique_ptr<Transaction> txn;
    txn_kv->create_txn(&txn);
    txn->remove("\x00", "\xfe"); // This is dangerous if the fdb is not correctly set
    txn->commit();

    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs);
    return meta_service;
}

TEST(MetaServiceTest, CreateInstanceTest) {
    auto meta_service = get_meta_service();

    // case: normal create instance
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        req.set_instance_id("test_instance");
        req.set_user_id("test_user");
        req.set_name("test_name");
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        obj.set_provider(ObjectStoreInfoPB::BOS);
        req.mutable_obj_info()->CopyFrom(obj);

        MetaServiceGenericResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // case: request has invalid argument
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        MetaServiceGenericResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }
}

TEST(MetaServiceTest, AlterClusterTest) {
    auto meta_service = get_meta_service();
    ASSERT_NE(meta_service, nullptr);

    // case: normal add cluster
    {
        brpc::Controller cntl;
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_name(mock_cluster_name);
        req.set_op(AlterClusterRequest::ADD_CLUSTER);
        MetaServiceGenericResponse res;
        meta_service->alter_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // case: request has invalid argument
    {
        brpc::Controller cntl;
        AlterClusterRequest req;
        req.set_op(AlterClusterRequest::DROP_CLUSTER);
        MetaServiceGenericResponse res;
        meta_service->alter_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }
}

TEST(MetaServiceTest, GetClusterTest) {
    auto meta_service = get_meta_service();

    // add cluster first
    InstanceKeyInfo key_info {mock_instance};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    InstanceInfoPB instance;
    instance.set_instance_id(mock_instance);
    ClusterPB c1;
    c1.set_cluster_name(mock_cluster_name);
    c1.set_cluster_id(mock_cluster_id);
    c1.add_mysql_user_name()->append("m1");
    instance.add_clusters()->CopyFrom(c1);
    val = instance.SerializeAsString();

    std::unique_ptr<Transaction> txn;
    std::string get_val;
    int ret = meta_service->txn_kv_->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), 0);

    // case: normal get
    {
        brpc::Controller cntl;
        GetClusterRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_cluster_id(mock_cluster_id);
        req.set_cluster_name("test_cluster");
        GetClusterResponse res;
        meta_service->get_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                  &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
}

TEST(MetaServiceTest, GetTabletStatsTest) {
    auto meta_service = get_meta_service();

    // add tablet first
    StatsTabletKeyInfo stat_key_info {mock_instance, 1, 2, 3, 4};
    std::string key, val;
    stats_tablet_key(stat_key_info, &key);

    TabletStatsPB stat;
    stat.mutable_idx()->set_table_id(1);
    stat.mutable_idx()->set_index_id(2);
    stat.mutable_idx()->set_partition_id(3);
    stat.mutable_idx()->set_tablet_id(4);
    val = stat.SerializeAsString();

    std::unique_ptr<Transaction> txn;
    int ret = meta_service->txn_kv_->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), 0);

    // case: normal get
    {
        brpc::Controller cntl;
        GetTabletStatsRequest req;
        req.add_tablet_idx()->CopyFrom(stat.idx());
        GetTabletStatsResponse res;
        meta_service->get_tablet_stats(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
}

TEST(MetaServiceTest, BeginTxnTest) {
    auto meta_service = get_meta_service();

    {
        brpc::Controller cntl;
        BeginTxnRequest req;

        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(666);
        txn_info_pb.set_label("test_label");
        txn_info_pb.add_table_ids(123);
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // case: label already used
    {
        brpc::Controller cntl;
        BeginTxnRequest req;

        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(888);
        txn_info_pb.set_label("test_label_already_in_use");
        txn_info_pb.add_table_ids(456);
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_LABEL_ALREADY_USED);
    }

    // case: dup begin txn request
    {
        brpc::Controller cntl;
        BeginTxnRequest req;

        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(999);
        txn_info_pb.set_label("test_label_dup_request");
        txn_info_pb.add_table_ids(789);
        UniqueIdPB unique_id_pb;
        unique_id_pb.set_hi(100);
        unique_id_pb.set_lo(10);
        txn_info_pb.mutable_request_id()->CopyFrom(unique_id_pb);
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_DUPLICATED_REQ);
    }

    {
        // ===========================================================================
        // threads concurrent execution with sequence in begin_txn with same label:
        //
        //      thread1              thread2
        //         |                    |
        //         |                commit_txn1
        //         |                    |
        //         |                    |
        //         |                    |
        //       commit_txn2            |
        //         |                    |
        //         v                    v
        //

        std::mutex go_mutex;
        std::condition_variable go_cv;
        bool go = false;
        auto sp = selectdb::SyncPoint::get_instance();

        std::atomic<int32_t> count_txn1 = {0};
        std::atomic<int32_t> count_txn2 = {0};
        std::atomic<int32_t> count_txn3 = {0};

        int64_t db_id = 1928354123;
        int64_t table_id = 12131231231;
        std::string test_label = "test_race_with_same_label";

        std::atomic<int32_t> success_txn = {0};

        sp->set_call_back("begin_txn:before:commit_txn:1", [&](void* args) {
            std::string label = *reinterpret_cast<std::string*>(args);
            count_txn1++;
            LOG(INFO) << "count_txn1:" << count_txn1 << " label=" << label;
            if (count_txn1 == 1) {
                {
                    std::unique_lock<std::mutex> _lock(go_mutex);
                    go = false;
                    LOG(INFO) << "count_txn1:" << count_txn1 << " label=" << label << " go=" << go;
                    go_cv.wait(_lock, [&] { return go; });
                }
            }

            if (count_txn1 == 2) {
                {
                    std::unique_lock<std::mutex> _lock(go_mutex);
                    go = true;
                    LOG(INFO) << "count_txn1:" << count_txn1 << " label=" << label << " go=" << go;
                    go_cv.notify_all();
                }
            }
        });

        sp->set_call_back("begin_txn:after:commit_txn:1", [&](void* args) {
            std::string label = *reinterpret_cast<std::string*>(args);
            count_txn2++;
            LOG(INFO) << "count_txn2:" << count_txn2 << " label=" << label;
            if (count_txn2 == 1) {
                {
                    std::unique_lock<std::mutex> _lock(go_mutex);
                    go = false;
                    LOG(INFO) << "count_txn2:" << count_txn2 << " label=" << label << " go=" << go;
                    go_cv.wait(_lock, [&] { return go; });
                }
            }

            if (count_txn2 == 2) {
                {
                    std::unique_lock<std::mutex> _lock(go_mutex);
                    go = true;
                    LOG(INFO) << "count_txn2:" << count_txn2 << " label=" << label << " go=" << go;
                    go_cv.notify_all();
                }
            }
        });

        sp->set_call_back("begin_txn:after:commit_txn:2", [&](void* args) {
            int64_t txn_id = *reinterpret_cast<int64_t*>(args);
            count_txn3++;
            LOG(INFO) << "count_txn3:" << count_txn3 << " txn_id=" << txn_id;
        });

        sp->enable_processing();

        std::thread thread1([&] {
            {
                std::unique_lock<std::mutex> _lock(go_mutex);
                go_cv.wait(_lock, [&] { return go; });
            }
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(test_label);
            txn_info_pb.add_table_ids(table_id);
            UniqueIdPB unique_id_pb;
            unique_id_pb.set_hi(1001);
            unique_id_pb.set_lo(11);
            txn_info_pb.mutable_request_id()->CopyFrom(unique_id_pb);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            if (res.status().code() == MetaServiceCode::OK) {
                success_txn++;
            } else {
                ASSERT_EQ(res.status().code(), MetaServiceCode::KV_TXN_CONFLICT);
            }
        });

        std::thread thread2([&] {
            {
                std::unique_lock<std::mutex> _lock(go_mutex);
                go_cv.wait(_lock, [&] { return go; });
            }
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(test_label);
            txn_info_pb.add_table_ids(table_id);
            UniqueIdPB unique_id_pb;
            unique_id_pb.set_hi(100);
            unique_id_pb.set_lo(10);
            txn_info_pb.mutable_request_id()->CopyFrom(unique_id_pb);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            if (res.status().code() == MetaServiceCode::OK) {
                success_txn++;
            } else {
                ASSERT_EQ(res.status().code(), MetaServiceCode::KV_TXN_CONFLICT);
            }
        });

        std::unique_lock<std::mutex> go_lock(go_mutex);
        go = true;
        go_lock.unlock();
        go_cv.notify_all();

        thread1.join();
        thread2.join();
        sp->clear_all_call_backs();
        sp->clear_trace();
        sp->disable_processing();
        ASSERT_EQ(success_txn.load(), 1);
    }
    {
        // ===========================================================================
        // threads concurrent execution with sequence in begin_txn with different label:
        //
        //      thread1              thread2
        //         |                    |
        //         |                commit_txn1
        //         |                    |
        //         |                    |
        //         |                    |
        //       commit_txn2            |
        //         |                    |
        //         v                    v

        std::mutex go_mutex;
        std::condition_variable go_cv;
        bool go = false;
        auto sp = selectdb::SyncPoint::get_instance();

        std::atomic<int32_t> count_txn1 = {0};
        std::atomic<int32_t> count_txn2 = {0};
        std::mutex flow_mutex_1;
        std::condition_variable flow_cv_1;

        int64_t db_id = 19541231112;
        int64_t table_id = 312312321211;
        std::string test_label1 = "test_race_with_diff_label1";
        std::string test_label2 = "test_race_with_diff_label2";

        std::atomic<int32_t> success_txn = {0};

        sp->set_call_back("begin_txn:before:commit_txn:1", [&](void* args) {
            std::string label = *reinterpret_cast<std::string*>(args);
            if (count_txn1.load() == 1) {
                std::unique_lock<std::mutex> flow_lock_1(flow_mutex_1);
                flow_cv_1.wait(flow_lock_1);
            }
            count_txn1++;
            LOG(INFO) << "count_txn1:" << count_txn1 << " label=" << label;
        });

        sp->set_call_back("begin_txn:after:commit_txn:2", [&](void* args) {
            int64_t txn_id = *reinterpret_cast<int64_t*>(args);
            while (count_txn2.load() == 0 && count_txn1.load() == 1) {
                sleep(1);
                flow_cv_1.notify_all();
            }
            count_txn2++;
            LOG(INFO) << "count_txn2:" << count_txn2 << " txn_id=" << txn_id;
        });
        sp->enable_processing();

        std::thread thread1([&] {
            {
                std::unique_lock<std::mutex> _lock(go_mutex);
                go_cv.wait(_lock, [&] { return go; });
            }
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(test_label1);
            txn_info_pb.add_table_ids(table_id);
            UniqueIdPB unique_id_pb;
            unique_id_pb.set_hi(1001);
            unique_id_pb.set_lo(11);
            txn_info_pb.mutable_request_id()->CopyFrom(unique_id_pb);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            if (res.status().code() == MetaServiceCode::OK) {
                success_txn++;
            } else {
                ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_LABEL_ALREADY_USED);
            }
        });

        std::thread thread2([&] {
            {
                std::unique_lock<std::mutex> _lock(go_mutex);
                go_cv.wait(_lock, [&] { return go; });
            }
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(test_label2);
            txn_info_pb.add_table_ids(table_id);
            UniqueIdPB unique_id_pb;
            unique_id_pb.set_hi(100);
            unique_id_pb.set_lo(10);
            txn_info_pb.mutable_request_id()->CopyFrom(unique_id_pb);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            if (res.status().code() == MetaServiceCode::OK) {
                success_txn++;
            } else {
                ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_LABEL_ALREADY_USED);
            }
        });

        std::unique_lock<std::mutex> go_lock(go_mutex);
        go = true;
        go_lock.unlock();
        go_cv.notify_all();

        thread1.join();
        thread2.join();
        sp->clear_all_call_backs();
        sp->clear_trace();
        sp->disable_processing();
        ASSERT_EQ(success_txn.load(), 2);
    }
    {
        // test reuse label
        // 1. beigin_txn
        // 2. abort_txn
        // 3. begin_txn again can successfully

        std::string cloud_unique_id = "test_cloud_unique_id";
        int64_t db_id = 124343989;
        int64_t table_id = 1231311;
        int64_t txn_id = -1;
        std::string label = "test_reuse_label";
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(table_id);
            UniqueIdPB unique_id_pb;
            unique_id_pb.set_hi(100);
            unique_id_pb.set_lo(10);
            txn_info_pb.mutable_request_id()->CopyFrom(unique_id_pb);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }
        // abort txn
        {
            brpc::Controller cntl;
            AbortTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            ASSERT_GT(txn_id, 0);
            req.set_txn_id(txn_id);
            req.set_reason("test");
            AbortTxnResponse res;
            meta_service->abort_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().status(), TxnStatusPB::TXN_STATUS_ABORTED);
        }
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(table_id);
            UniqueIdPB unique_id_pb;
            unique_id_pb.set_hi(100);
            unique_id_pb.set_lo(10);
            txn_info_pb.mutable_request_id()->CopyFrom(unique_id_pb);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_GT(res.txn_id(), txn_id);
        }
    }
}

TEST(MetaServiceTest, PreCommitTxnTest) {
    auto meta_service = get_meta_service();

    // begin txn first
    brpc::Controller cntl;
    BeginTxnRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    TxnInfoPB txn_info_pb;
    txn_info_pb.set_db_id(666);
    txn_info_pb.set_label("test_label");
    txn_info_pb.add_table_ids(111);
    req.mutable_txn_info()->CopyFrom(txn_info_pb);
    BeginTxnResponse res;
    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                            nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    int64_t txn_id = res.txn_id();

    // case: txn's status should be TXN_STATUS_PRECOMMITTED
    {
        std::unique_ptr<Transaction> txn;
        int ret = meta_service->txn_kv_->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        std::string txn_inf_key;
        std::string txn_inf_val;
        TxnInfoKeyInfo txn_inf_key_info {mock_instance, 666, txn_id};
        txn_info_key(txn_inf_key_info, &txn_inf_key);
        ASSERT_EQ(txn->get(txn_inf_key, &txn_inf_val), 0);
        TxnInfoPB txn_info;
        txn_info.ParseFromString(txn_inf_val);
        // before call precommit_txn, txn's status is TXN_STATUS_PREPARED
        ASSERT_EQ(txn_info.status(), TxnStatusPB::TXN_STATUS_PREPARED);

        brpc::Controller cntl;
        PrecommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(666);
        req.set_txn_id(txn_id);
        PrecommitTxnResponse res;
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        ret = meta_service->txn_kv_->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        ASSERT_EQ(txn->get(txn_inf_key, &txn_inf_val), 0);
        txn_info.ParseFromString(txn_inf_val);
        // after call precommit_txn, txn's status is TXN_STATUS_PRECOMMITTED
        ASSERT_EQ(txn_info.status(), TxnStatusPB::TXN_STATUS_PRECOMMITTED);
    }

    // case: when txn's status is TXN_STATUS_ABORTED/TXN_STATUS_VISIBLE/TXN_STATUS_PRECOMMITTED
    {
        // TXN_STATUS_ABORTED
        std::unique_ptr<Transaction> txn;
        int ret = meta_service->txn_kv_->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        std::string txn_inf_key;
        std::string txn_inf_val;
        TxnInfoKeyInfo txn_inf_key_info {mock_instance, 666, txn_id};
        txn_info_key(txn_inf_key_info, &txn_inf_key);
        ASSERT_EQ(txn->get(txn_inf_key, &txn_inf_val), 0);
        TxnInfoPB txn_info;
        txn_info.ParseFromString(txn_inf_val);
        txn_info.set_status(TxnStatusPB::TXN_STATUS_ABORTED);
        txn_inf_val.clear();
        txn_info.SerializeToString(&txn_inf_val);
        txn->put(txn_inf_key, txn_inf_val);
        ASSERT_EQ(ret = txn->commit(), 0);

        brpc::Controller cntl;
        PrecommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(666);
        req.set_txn_id(txn_id);
        PrecommitTxnResponse res;
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ALREADY_ABORTED);

        // TXN_STATUS_VISIBLE
        txn_info.set_status(TxnStatusPB::TXN_STATUS_VISIBLE);
        txn_inf_val.clear();
        txn_info.SerializeToString(&txn_inf_val);
        txn->put(txn_inf_key, txn_inf_val);
        ASSERT_EQ(ret = txn->commit(), 0);
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ALREADY_VISIBLE);

        // TXN_STATUS_PRECOMMITTED
        txn_info.set_status(TxnStatusPB::TXN_STATUS_PRECOMMITTED);
        txn_inf_val.clear();
        txn_info.SerializeToString(&txn_inf_val);
        txn->put(txn_inf_key, txn_inf_val);
        ASSERT_EQ(ret = txn->commit(), 0);
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ALREADY_PRECOMMITED);
    }
}

static int64_t cnt = 0;
static void create_tmp_rowset_and_meta_tablet(TxnKv* txn_kv, int64_t txn_id, int64_t tablet_id,
                                              int table_id = 1,
                                              std::string instance_id = mock_instance,
                                              int num_segments = 1) {
    ++cnt;

    std::string key;
    std::string val;

    char rowset_id[50];
    snprintf(rowset_id, sizeof(rowset_id), "%048ld", cnt);

    MetaRowsetTmpKeyInfo key_info {instance_id, txn_id, tablet_id};
    meta_rowset_tmp_key(key_info, &key);

    doris::RowsetMetaPB rowset_pb;
    rowset_pb.set_rowset_id(0); // useless but required
    rowset_pb.set_rowset_id_v2(rowset_id);
    rowset_pb.set_tablet_id(tablet_id);
    rowset_pb.set_num_segments(num_segments);
    rowset_pb.set_partition_id(897614);
    rowset_pb.SerializeToString(&val);

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), 0);

    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    std::string key1;
    std::string val1;
    MetaTabletIdxKeyInfo key_info1 {instance_id, tablet_id};
    meta_tablet_idx_key(key_info1, &key1);
    TabletIndexPB tablet_table;
    tablet_table.set_table_id(123456);
    tablet_table.set_index_id(2);
    tablet_table.set_partition_id(3241);
    tablet_table.SerializeToString(&val1);

    txn->put(key1, val1);
    ASSERT_EQ(txn->commit(), 0);
}

TEST(MetaServiceTest, CommitTxnTest) {
    auto meta_service = get_meta_service();

    // case: first version of rowset
    {
        int64_t txn_id = -1;
        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(666);
            txn_info_pb.set_label("test_label");
            txn_info_pb.add_table_ids(1234);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        // mock rowset and tablet
        int64_t tablet_id_base = 1103;
        for (int i = 0; i < 5; ++i) {
            create_tmp_rowset_and_meta_tablet(meta_service->txn_kv_.get(), txn_id,
                                              tablet_id_base + i);
        }

        // precommit txn
        {
            brpc::Controller cntl;
            PrecommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(666);
            req.set_txn_id(txn_id);
            PrecommitTxnResponse res;
            meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // commit txn
        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(666);
            req.set_txn_id(txn_id);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
    }
}

TEST(MetaServiceTest, AbortTxnTest) {
    auto meta_service = get_meta_service();

    // case: abort txn by txn_id
    {
        int64_t db_id = 666;
        int64_t table_id = 12345;
        std::string label = "abort_txn_by_txn_id";
        std::string cloud_unique_id = "test_cloud_unique_id";
        int64_t tablet_id_base = 1104;
        int64_t txn_id = -1;
        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(table_id);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        // mock rowset and tablet
        for (int i = 0; i < 5; ++i) {
            create_tmp_rowset_and_meta_tablet(meta_service->txn_kv_.get(), txn_id,
                                              tablet_id_base + i);
        }

        // abort txn by txn_id
        {
            brpc::Controller cntl;
            AbortTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_txn_id(txn_id);
            req.set_reason("test");
            AbortTxnResponse res;
            meta_service->abort_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().status(), TxnStatusPB::TXN_STATUS_ABORTED);
        }
    }

    // case: abort txn by db_id + label
    {
        int64_t db_id = 66631313131;
        int64_t table_id = 12345;
        std::string label = "abort_txn_by_db_id_and_label";
        std::string cloud_unique_id = "test_cloud_unique_id";
        int64_t tablet_id_base = 1104;
        int64_t txn_id = -1;
        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(table_id);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        // mock rowset and tablet
        for (int i = 0; i < 5; ++i) {
            create_tmp_rowset_and_meta_tablet(meta_service->txn_kv_.get(), txn_id,
                                              tablet_id_base + i);
        }

        // abort txn by db_id and label
        {
            brpc::Controller cntl;
            AbortTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_db_id(db_id);
            req.set_label(label);
            req.set_reason("test");
            AbortTxnResponse res;
            meta_service->abort_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().status(), TxnStatusPB::TXN_STATUS_ABORTED);
        }
    }
}

TEST(MetaServiceTest, GetCurrentMaxTxnIdTest) {
    auto meta_service = get_meta_service();

    const int64_t db_id = 123;
    const std::string label = "test_label123";
    const std::string cloud_unique_id = "test_cloud_unique_id";

    brpc::Controller begin_txn_cntl;
    BeginTxnRequest begin_txn_req;
    BeginTxnResponse begin_txn_res;
    TxnInfoPB txn_info_pb;

    begin_txn_req.set_cloud_unique_id(cloud_unique_id);
    txn_info_pb.set_db_id(db_id);
    txn_info_pb.set_label(label);
    txn_info_pb.add_table_ids(12345);
    begin_txn_req.mutable_txn_info()->CopyFrom(txn_info_pb);

    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl),
                            &begin_txn_req, &begin_txn_res, nullptr);
    ASSERT_EQ(begin_txn_res.status().code(), MetaServiceCode::OK);

    brpc::Controller max_txn_id_cntl;
    GetCurrentMaxTxnRequest max_txn_id_req;
    GetCurrentMaxTxnResponse max_txn_id_res;

    max_txn_id_req.set_cloud_unique_id(cloud_unique_id);

    meta_service->get_current_max_txn_id(
            reinterpret_cast<::google::protobuf::RpcController*>(&max_txn_id_cntl), &max_txn_id_req,
            &max_txn_id_res, nullptr);

    ASSERT_EQ(max_txn_id_res.status().code(), MetaServiceCode::OK);
    ASSERT_GE(max_txn_id_res.current_max_txn_id(), begin_txn_res.txn_id());
}

TEST(MetaServiceTest, CheckTxnConflictTest) {
    auto meta_service = get_meta_service();

    const int64_t db_id = 666;
    const int64_t table_id = 777;
    const std::string label = "test_label";
    const std::string cloud_unique_id = "test_cloud_unique_id";
    int64_t txn_id = -1;

    brpc::Controller begin_txn_cntl;
    BeginTxnRequest begin_txn_req;
    BeginTxnResponse begin_txn_res;
    TxnInfoPB txn_info_pb;

    begin_txn_req.set_cloud_unique_id(cloud_unique_id);
    txn_info_pb.set_db_id(db_id);
    txn_info_pb.set_label(label);
    txn_info_pb.add_table_ids(table_id);
    begin_txn_req.mutable_txn_info()->CopyFrom(txn_info_pb);

    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl),
                            &begin_txn_req, &begin_txn_res, nullptr);
    ASSERT_EQ(begin_txn_res.status().code(), MetaServiceCode::OK);
    txn_id = begin_txn_res.txn_id();
    ASSERT_GT(txn_id, -1);

    brpc::Controller check_txn_conflict_cntl;
    CheckTxnConflictRequest check_txn_conflict_req;
    CheckTxnConflictResponse check_txn_conflict_res;

    check_txn_conflict_req.set_cloud_unique_id(cloud_unique_id);
    check_txn_conflict_req.set_db_id(db_id);
    check_txn_conflict_req.set_end_txn_id(txn_id + 1);
    check_txn_conflict_req.add_table_ids(table_id);

    // first time to check txn conflict
    meta_service->check_txn_conflict(
            reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl),
            &check_txn_conflict_req, &check_txn_conflict_res, nullptr);

    ASSERT_EQ(check_txn_conflict_res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(check_txn_conflict_res.finished(), false);

    // mock rowset and tablet
    int64_t tablet_id_base = 123456;
    for (int i = 0; i < 5; ++i) {
        create_tmp_rowset_and_meta_tablet(meta_service->txn_kv_.get(), txn_id, tablet_id_base + i);
    }

    brpc::Controller commit_txn_cntl;
    CommitTxnRequest commit_txn_req;
    commit_txn_req.set_cloud_unique_id(cloud_unique_id);
    commit_txn_req.set_db_id(db_id);
    commit_txn_req.set_txn_id(txn_id);
    CommitTxnResponse commit_txn_res;
    meta_service->commit_txn(
            reinterpret_cast<::google::protobuf::RpcController*>(&check_txn_conflict_cntl),
            &commit_txn_req, &commit_txn_res, nullptr);
    ASSERT_EQ(commit_txn_res.status().code(), MetaServiceCode::OK);

    // second time to check txn conflict
    meta_service->check_txn_conflict(
            reinterpret_cast<::google::protobuf::RpcController*>(&check_txn_conflict_cntl),
            &check_txn_conflict_req, &check_txn_conflict_res, nullptr);

    ASSERT_EQ(check_txn_conflict_res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(check_txn_conflict_res.finished(), true);
}

TEST(MetaServiceTest, CopyJobTest) {
    auto meta_service = get_meta_service();
    brpc::Controller cntl;
    auto cloud_unique_id = "test_cloud_unique_id";
    auto stage_id = "test_stage_id";
    int64_t table_id = 100;
    std::string instance_id = "copy_job_test_instance_id";
    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    sp->set_call_back("get_instance_id::pred", [](void* p) { *((bool*)p) = true; });
    sp->set_call_back("get_instance_id", [&](void* p) { *((std::string*)p) = instance_id; });
    sp->enable_processing();

    // generate a begin copy request
    BeginCopyRequest begin_copy_request;
    begin_copy_request.set_cloud_unique_id(cloud_unique_id);
    begin_copy_request.set_stage_id(stage_id);
    begin_copy_request.set_stage_type(StagePB::EXTERNAL);
    begin_copy_request.set_table_id(table_id);
    begin_copy_request.set_copy_id("test_copy_id");
    begin_copy_request.set_group_id(0);
    begin_copy_request.set_start_time(200);
    begin_copy_request.set_timeout_time(300);
    for (int i = 0; i < 20; ++i) {
        ObjectFilePB object_file_pb;
        object_file_pb.set_relative_path("obj_" + std::to_string(i));
        object_file_pb.set_etag("obj_" + std::to_string(i) + "_etag");
        begin_copy_request.add_object_files()->CopyFrom(object_file_pb);
    }

    // generate a finish copy request
    FinishCopyRequest finish_copy_request;
    finish_copy_request.set_cloud_unique_id(cloud_unique_id);
    finish_copy_request.set_stage_id(stage_id);
    finish_copy_request.set_stage_type(StagePB::EXTERNAL);
    finish_copy_request.set_table_id(table_id);
    finish_copy_request.set_copy_id("test_copy_id");
    finish_copy_request.set_group_id(0);
    finish_copy_request.set_action(FinishCopyRequest::COMMIT);

    // generate a get copy files request
    GetCopyFilesRequest get_copy_file_req;
    get_copy_file_req.set_cloud_unique_id(cloud_unique_id);
    get_copy_file_req.set_stage_id(stage_id);
    get_copy_file_req.set_table_id(table_id);

    // begin copy
    {
        BeginCopyResponse res;
        meta_service->begin_copy(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                 &begin_copy_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.filtered_object_files_size(), 20);
    }
    // get copy files
    {
        GetCopyFilesResponse res;
        meta_service->get_copy_files(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &get_copy_file_req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.object_files_size(), 20);
    }
    // begin copy with duplicate files
    {
        begin_copy_request.set_copy_id("test_copy_id_1");
        begin_copy_request.clear_object_files();
        for (int i = 15; i < 30; ++i) {
            ObjectFilePB object_file_pb;
            object_file_pb.set_relative_path("obj_" + std::to_string(i));
            object_file_pb.set_etag("obj_" + std::to_string(i) + "_etag");
            begin_copy_request.add_object_files()->CopyFrom(object_file_pb);
        }

        BeginCopyResponse res;
        meta_service->begin_copy(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                 &begin_copy_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.filtered_object_files_size(), 10);
    }
    // get copy files
    {
        GetCopyFilesResponse res;
        meta_service->get_copy_files(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &get_copy_file_req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.object_files_size(), 30);
    }
    // finish the first copy job
    {
        FinishCopyResponse res;
        meta_service->finish_copy(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                  &finish_copy_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
    // get copy files
    {
        GetCopyFilesResponse res;
        meta_service->get_copy_files(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &get_copy_file_req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.object_files_size(), 30);
    }
    // abort the second copy job
    {
        finish_copy_request.set_copy_id("test_copy_id_1");
        finish_copy_request.set_action(FinishCopyRequest::ABORT);

        FinishCopyResponse res;
        meta_service->finish_copy(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                  &finish_copy_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
    // get copy files
    {
        GetCopyFilesResponse res;
        meta_service->get_copy_files(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &get_copy_file_req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.object_files_size(), 20);
    }
    // scan fdb
    {
        std::unique_ptr<Transaction> txn;
        std::string get_val;
        int ret = meta_service->txn_kv_->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        // 0 copy files
        {
            CopyFileKeyInfo key_info0 {instance_id, stage_id, table_id, "", ""};
            CopyFileKeyInfo key_info1 {instance_id, stage_id, table_id + 1, "", ""};
            std::string key0;
            std::string key1;
            copy_file_key(key_info0, &key0);
            copy_file_key(key_info1, &key1);
            std::unique_ptr<RangeGetIterator> it;
            ret = txn->get(key0, key1, &it);
            ASSERT_EQ(ret, 0);
            ASSERT_EQ(it->has_next(), false);
            ASSERT_EQ(it->more(), false);
        }
        // 1 copy job with finish status
        {
            CopyJobKeyInfo key_info0 {instance_id, stage_id, table_id, "", 0};
            CopyJobKeyInfo key_info1 {instance_id, stage_id, table_id + 1, "", 0};
            std::string key0;
            std::string key1;
            copy_job_key(key_info0, &key0);
            copy_job_key(key_info1, &key1);
            std::unique_ptr<RangeGetIterator> it;
            int job_cnt = 0;
            do {
                ret = txn->get(key0, key1, &it);
                ASSERT_EQ(ret, 0);
                while (it->has_next()) {
                    auto [k, v] = it->next();
                    CopyJobPB copy_job;
                    ASSERT_EQ(copy_job.ParseFromArray(v.data(), v.size()), true);
                    ASSERT_EQ(copy_job.object_files_size(), 20);
                    ASSERT_EQ(copy_job.job_status(), CopyJobPB::FINISH);
                    ++job_cnt;
                }
                key0.push_back('\x00');
            } while (it->more());
            ASSERT_EQ(job_cnt, 1);
        }
    }
}

TEST(MetaServiceTest, CalcSyncVersionsTest) {
    using Versions = std::vector<std::pair<int64_t, int64_t>>;
    // * no compaction happened
    // req_cc_cnt == ms_cc_cnt && req_bc_cnt == ms_bc_cnt && req_cp == ms_cp
    // BE  [=][=][=][=][=====][=][=]<.......>
    //                  ^~~~~ req_cp
    // BE  [=][=][=][=][=====][=][=][=][=][=][=]
    //                  ^~~~~ ms_cp
    //                               ^_____^ versions_return: [req_start, req_end]
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 0};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 1};
        auto [req_cp, cp] = std::tuple {5, 5};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = MetaServiceImpl::calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt,
                                                            req_cp, cp, req_start, req_end);
        ASSERT_EQ(versions, (Versions {{8, 12}}));
    }
    // * only one CC happened and CP changed
    // req_cc_cnt == ms_cc_cnt - 1 && req_bc_cnt == ms_bc_cnt && req_cp < ms_cp
    // BE  [=][=][=][=][=====][=][=]<.......>
    //                  ^~~~~ req_cp
    // MS  [=][=][=][=][xxxxxxxxxxxxxx][=======][=][=]
    //                                  ^~~~~~~ ms_cp
    //                  ^__________________^ versions_return: [req_cp, ms_cp - 1] v [req_start, req_end]
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 0};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 2};
        auto [req_cp, cp] = std::tuple {5, 10};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = MetaServiceImpl::calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt,
                                                            req_cp, cp, req_start, req_end);
        ASSERT_EQ(versions, (Versions {{5, 12}})); // [5, 9] v [8, 12]
    }
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 0};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 2};
        auto [req_cp, cp] = std::tuple {5, 15};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = MetaServiceImpl::calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt,
                                                            req_cp, cp, req_start, req_end);
        ASSERT_EQ(versions, (Versions {{5, 14}})); // [5, 14] v [8, 12]
    }
    // * only one CC happened and CP remain unchanged
    // req_cc_cnt == ms_cc_cnt - 1 && req_bc_cnt == ms_bc_cnt && req_cp == ms_cp
    // BE  [=][=][=][=][=====][=][=]<.......>
    //                  ^~~~~ req_cp
    // MS  [=][=][=][=][xxxxxxxxxxxxxx][=][=][=][=][=]
    //                  ^~~~~~~~~~~~~~ ms_cp
    //                  ^__________________^ versions_return: [req_cp, max] v [req_start, req_end]
    //
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 0};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 2};
        auto [req_cp, cp] = std::tuple {5, 5};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = MetaServiceImpl::calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt,
                                                            req_cp, cp, req_start, req_end);
        ASSERT_EQ(versions,
                  (Versions {{5, std::numeric_limits<int64_t>::max() - 1}})); // [5, max] v [8, 12]
    }
    // * more than one CC happened and CP remain unchanged
    // req_cc_cnt < ms_cc_cnt - 1 && req_bc_cnt == ms_bc_cnt && req_cp == ms_cp
    // BE  [=][=][=][=][=====][=][=]<.......>
    //                  ^~~~~ req_cp
    // MS  [=][=][=][=][xxxxxxxxxxxxxx][xxxxxxx][=][=]
    //                  ^~~~~~~~~~~~~~ ms_cp
    //                  ^_____________________^ versions_return: [req_cp, max] v [req_start, req_end]
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 0};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 3};
        auto [req_cp, cp] = std::tuple {5, 5};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = MetaServiceImpl::calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt,
                                                            req_cp, cp, req_start, req_end);
        ASSERT_EQ(versions,
                  (Versions {{5, std::numeric_limits<int64_t>::max() - 1}})); // [5, max] v [8, 12]
    }
    // * more than one CC happened and CP changed
    // req_cc_cnt < ms_cc_cnt - 1 && req_bc_cnt == ms_bc_cnt && req_cp < ms_cp
    // BE  [=][=][=][=][=====][=][=]<.......>
    //                  ^~~~~ req_cp
    // MS  [=][=][=][=][xxxxxxxxxxxxxx][xxxxxxx][=][=]
    //                                           ^~~~~~~ ms_cp
    //                  ^_____________________^ versions_return: [req_cp, ms_cp - 1] v [req_start, req_end]
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 0};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 3};
        auto [req_cp, cp] = std::tuple {5, 15};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = MetaServiceImpl::calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt,
                                                            req_cp, cp, req_start, req_end);
        ASSERT_EQ(versions, (Versions {{5, 14}})); // [5, 14] v [8, 12]
    }
    // * for any BC happended
    // req_bc_cnt < ms_bc_cnt
    // BE  [=][=][=][=][=====][=][=]<.......>
    //                  ^~~~~ req_cp
    // MS  [xxxxxxxxxx][xxxxxxxxxxxxxx][=======][=][=]
    //                                  ^~~~~~~ ms_cp
    //     ^_________________________^ versions_return: [0, ms_cp - 1] v versions_return_in_above_case
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 1};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 1};
        auto [req_cp, cp] = std::tuple {5, 5};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = MetaServiceImpl::calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt,
                                                            req_cp, cp, req_start, req_end);
        ASSERT_EQ(versions, (Versions {{0, 4}, {8, 12}}));
    }
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 1};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 1};
        auto [req_cp, cp] = std::tuple {8, 8};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = MetaServiceImpl::calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt,
                                                            req_cp, cp, req_start, req_end);
        ASSERT_EQ(versions, (Versions {{0, 12}})); // [0, 7] v [8, 12]
    }
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 1};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 2};
        auto [req_cp, cp] = std::tuple {5, 10};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = MetaServiceImpl::calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt,
                                                            req_cp, cp, req_start, req_end);
        ASSERT_EQ(versions, (Versions {{0, 12}})); // [0, 4] v [5, 9] v [8, 12]
    }
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 1};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 2};
        auto [req_cp, cp] = std::tuple {5, 15};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = MetaServiceImpl::calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt,
                                                            req_cp, cp, req_start, req_end);
        ASSERT_EQ(versions, (Versions {{0, 14}})); // [0, 4] v [5, 14] v [8, 12]
    }
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 1};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 2};
        auto [req_cp, cp] = std::tuple {5, 5};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = MetaServiceImpl::calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt,
                                                            req_cp, cp, req_start, req_end);
        // [0, 4] v [5, max] v [8, 12]
        ASSERT_EQ(versions, (Versions {{0, std::numeric_limits<int64_t>::max() - 1}}));
    }
}

TEST(MetaServiceTest, StageTest) {
    auto meta_service = get_meta_service();
    brpc::Controller cntl;
    auto cloud_unique_id = "test_cloud_unique_id";
    std::string instance_id = "stage_test_instance_id";
    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    sp->set_call_back("get_instance_id::pred", [](void* p) { *((bool*)p) = true; });
    sp->set_call_back("get_instance_id", [&](void* p) { *((std::string*)p) = instance_id; });
    sp->enable_processing();

    ObjectStoreInfoPB obj;
    obj.set_ak("123");
    obj.set_sk("321");
    obj.set_bucket("456");
    obj.set_prefix("654");
    obj.set_endpoint("789");
    obj.set_region("987");
    obj.set_provider(ObjectStoreInfoPB::BOS);

    // create instance
    {
        CreateInstanceRequest req;
        req.set_instance_id(instance_id);
        req.set_user_id("test_user");
        req.set_name("test_name");
        req.mutable_obj_info()->CopyFrom(obj);

        MetaServiceGenericResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // test create and get internal stage
    {
        GetStageRequest get_stage_req;
        get_stage_req.set_cloud_unique_id(cloud_unique_id);
        get_stage_req.set_type(StagePB::INTERNAL);
        get_stage_req.set_mysql_user_name("root");

        // get a non-existent internal stage, will create and return
        GetStageResponse res;
        meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                &get_stage_req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(1, res.stage().size());
        ASSERT_FALSE(res.stage().at(0).stage_id().empty());
        auto stage_id = res.stage().at(0).stage_id();

        // get existent internal stage
        GetStageResponse res2;
        meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                &get_stage_req, &res2, nullptr);
        ASSERT_EQ(res2.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(1, res.stage().size());
        ASSERT_EQ(stage_id, res2.stage().at(0).stage_id());
    }

    // test create and get external stage
    {
        // get an external stage with name
        GetStageRequest get_stage_req;
        get_stage_req.set_cloud_unique_id(cloud_unique_id);
        get_stage_req.set_type(StagePB::EXTERNAL);
        get_stage_req.set_stage_name("ex_name_1");

        {
            GetStageResponse res;
            meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &get_stage_req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::STAGE_NOT_FOUND);
        }

        // create 2 stages
        for (auto i = 0; i < 2; ++i) {
            StagePB stage;
            stage.set_type(StagePB::EXTERNAL);
            stage.set_stage_id("ex_id_" + std::to_string(i));
            stage.set_name("ex_name_" + std::to_string(i));
            stage.mutable_obj_info()->CopyFrom(obj);

            CreateStageRequest create_stage_req;
            create_stage_req.set_cloud_unique_id(cloud_unique_id);
            create_stage_req.mutable_stage()->CopyFrom(stage);

            CreateStageResponse create_stage_res;
            meta_service->create_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &create_stage_req, &create_stage_res, nullptr);
            ASSERT_EQ(create_stage_res.status().code(), MetaServiceCode::OK);
        }

        // get an external stage with name
        {
            GetStageResponse res;
            meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &get_stage_req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(1, res.stage().size());
            ASSERT_EQ("ex_id_1", res.stage().at(0).stage_id());
        }

        GetStageRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_type(StagePB::EXTERNAL);
        // get all stages
        {
            GetStageResponse res;
            meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(2, res.stage().size());
            ASSERT_EQ("ex_id_0", res.stage().at(0).stage_id());
            ASSERT_EQ("ex_id_1", res.stage().at(1).stage_id());
        }

        // drop one stage
        {
            DropStageRequest drop_stage_req;
            drop_stage_req.set_cloud_unique_id(cloud_unique_id);
            drop_stage_req.set_type(StagePB::EXTERNAL);
            drop_stage_req.set_stage_name("tmp");
            DropStageResponse res;
            meta_service->drop_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &drop_stage_req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::STAGE_NOT_FOUND);

            drop_stage_req.set_stage_name("ex_name_1");
            meta_service->drop_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &drop_stage_req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

            // get all stage
            GetStageResponse get_stage_res;
            meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &get_stage_res, nullptr);
            ASSERT_EQ(get_stage_res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(1, get_stage_res.stage().size());
            ASSERT_EQ("ex_name_0", get_stage_res.stage().at(0).name());
        }
    }
}

// vim: et tw=100 ts=4 sw=4 cc=80:
