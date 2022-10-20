#pragma once

#include "cloud/meta_mgr.h"

namespace selectdb {
class MetaService_Stub;
} // namespace selectdb

namespace doris::cloud {

class CloudMetaMgr final : public MetaMgr {
public:
    CloudMetaMgr();

    ~CloudMetaMgr() override;

    Status open() override;

    Status get_tablet_meta(int64_t tablet_id, TabletMetaSharedPtr* tablet_meta) override;

    Status sync_tablet_rowsets(Tablet* tablet) override;

    Status write_tablet_meta(const TabletMetaSharedPtr& tablet_meta) override;

    Status prepare_rowset(const RowsetMetaSharedPtr& rs_meta, bool is_tmp,
                          RowsetMetaSharedPtr* existed_rs_meta = nullptr) override;

    Status commit_rowset(const RowsetMetaSharedPtr& rs_meta, bool is_tmp,
                         RowsetMetaSharedPtr* existed_rs_meta = nullptr) override;

    Status commit_txn(StreamLoadContext* ctx, bool is_2pc) override;

    Status abort_txn(StreamLoadContext* ctx) override;

    Status precommit_txn(StreamLoadContext* ctx) override;

    Status get_s3_info(std::vector<std::tuple<std::string, S3Conf>>* s3_infos) override;

    Status prepare_tablet_job(const selectdb::TabletJobInfoPB& job) override;

    Status commit_tablet_job(const selectdb::TabletJobInfoPB& job,
                             selectdb::TabletStatsPB* stats) override;

    Status abort_tablet_job(const selectdb::TabletJobInfoPB& job) override;

private:
    std::unique_ptr<selectdb::MetaService_Stub> _stub;
};

} // namespace doris::cloud
