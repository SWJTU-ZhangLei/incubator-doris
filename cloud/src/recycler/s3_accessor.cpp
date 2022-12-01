#include "recycler/s3_accessor.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "common/logging.h"

namespace selectdb {

class S3Environment {
public:
    S3Environment() { Aws::InitAPI(aws_options_); }

    ~S3Environment() { Aws::ShutdownAPI(aws_options_); }

private:
    Aws::SDKOptions aws_options_;
};

S3Accessor::S3Accessor(S3Conf conf) : conf_(std::move(conf)) {
    path_ = conf_.endpoint + '/' + conf_.bucket + '/' + conf_.prefix;
}

S3Accessor::~S3Accessor() = default;

std::string S3Accessor::get_key(const std::string& relative_path) const {
    return conf_.prefix + '/' + relative_path;
}

int S3Accessor::init() {
    static S3Environment s3_env;
    Aws::Auth::AWSCredentials aws_cred(conf_.ak, conf_.sk);
    Aws::Client::ClientConfiguration aws_config;
    aws_config.endpointOverride = conf_.endpoint;
    aws_config.region = conf_.region;
    s3_client_ = std::make_shared<Aws::S3::S3Client>(
            std::move(aws_cred), std::move(aws_config),
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never);
    return 0;
}

int S3Accessor::delete_objects_by_prefix(const std::string& relative_path) {
    Aws::S3::Model::ListObjectsV2Request request;
    auto prefix = get_key(relative_path);
    request.WithBucket(conf_.bucket).WithPrefix(prefix);

    Aws::S3::Model::DeleteObjectsRequest delete_request;
    delete_request.SetBucket(conf_.bucket);
    bool is_trucated = false;
    do {
        auto outcome = s3_client_->ListObjectsV2(request);
        if (!outcome.IsSuccess()) {
            LOG_WARNING("failed to list objects")
                    .tag("endpoint", conf_.endpoint)
                    .tag("bucket", conf_.bucket)
                    .tag("prefix", prefix)
                    .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                    .tag("error", outcome.GetError().GetMessage());
            return -1;
        }
        const auto& result = outcome.GetResult();
        VLOG_DEBUG << "get " << result.GetContents().size() << " objects";
        Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects;
        objects.reserve(result.GetContents().size());
        for (const auto& obj : result.GetContents()) {
            objects.emplace_back().SetKey(obj.GetKey());
            LOG_INFO("delete object")
                    .tag("endpoint", conf_.endpoint)
                    .tag("bucket", conf_.bucket)
                    .tag("key", obj.GetKey());
        }
        if (!objects.empty()) {
            Aws::S3::Model::Delete del;
            del.WithObjects(std::move(objects)).SetQuiet(true);
            delete_request.SetDelete(std::move(del));
            auto delete_outcome = s3_client_->DeleteObjects(delete_request);
            if (!delete_outcome.IsSuccess()) {
                LOG_WARNING("failed to delete objects")
                        .tag("endpoint", conf_.endpoint)
                        .tag("bucket", conf_.bucket)
                        .tag("prefix", prefix)
                        .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                        .tag("error", outcome.GetError().GetMessage());
                return -2;
            }
            if (!delete_outcome.GetResult().GetErrors().empty()) {
                const auto& e = delete_outcome.GetResult().GetErrors().front();
                LOG_WARNING("failed to delete object")
                        .tag("endpoint", conf_.endpoint)
                        .tag("bucket", conf_.bucket)
                        .tag("key", e.GetKey())
                        .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                        .tag("error", e.GetMessage());
                return -3;
            }
        }
        is_trucated = result.GetIsTruncated();
        request.SetContinuationToken(result.GetNextContinuationToken());
    } while (is_trucated);
    return 0;
}

int S3Accessor::delete_objects(const std::vector<std::string>& relative_paths) {
    if (relative_paths.empty()) {
        return 0;
    }
    std::vector<std::string> keys;
    keys.reserve(relative_paths.size());
    for (auto& path : relative_paths) {
        keys.push_back(get_key(path));
    }
    // `DeleteObjectsRequest` can only contain 1000 keys at most.
    constexpr size_t max_delete_batch = 1000;
    auto key_iter = keys.begin();

    Aws::S3::Model::DeleteObjectsRequest delete_request;
    delete_request.SetBucket(conf_.bucket);
    do {
        Aws::S3::Model::Delete del;
        Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects;
        auto key_begin = key_iter;
        for (; key_iter != keys.end() && (key_iter - key_begin < max_delete_batch); ++key_iter) {
            objects.emplace_back().SetKey(*key_iter);
        }
        del.WithObjects(std::move(objects)).SetQuiet(true);
        delete_request.SetDelete(std::move(del));
        auto delete_outcome = s3_client_->DeleteObjects(delete_request);
        if (!delete_outcome.IsSuccess()) {
            LOG_WARNING("failed to delete objects")
                    .tag("endpoint", conf_.endpoint)
                    .tag("bucket", conf_.bucket)
                    .tag("key[0]", keys[0])
                    .tag("responseCode", static_cast<int>(delete_outcome.GetError().GetResponseCode()))
                    .tag("error", delete_outcome.GetError().GetMessage());
            return -1;
        }
        if (!delete_outcome.GetResult().GetErrors().empty()) {
            const auto& e = delete_outcome.GetResult().GetErrors().front();
            LOG_WARNING("failed to delete object")
                    .tag("endpoint", conf_.endpoint)
                    .tag("bucket", conf_.bucket)
                    .tag("key", e.GetKey())
                    .tag("responseCode", static_cast<int>(delete_outcome.GetError().GetResponseCode()))
                    .tag("error", e.GetMessage());
            return -2;
        }
    } while (key_iter != keys.end());

    return 0;
}

int S3Accessor::delete_object(const std::string& relative_path) {
    // TODO(cyx)
    return 0;
}

int S3Accessor::put_object(const std::string& relative_path, const std::string& content) {
    Aws::S3::Model::PutObjectRequest request;
    auto key = get_key(relative_path);
    request.WithBucket(conf_.bucket).WithKey(key);
    auto input = Aws::MakeShared<Aws::StringStream>("S3Accessor");
    *input << content;
    request.SetBody(input);
    auto outcome = s3_client_->PutObject(request);
    if (!outcome.IsSuccess()) {
        LOG_WARNING("failed to put object")
                .tag("endpoint", conf_.endpoint)
                .tag("bucket", conf_.bucket)
                .tag("key", key)
                .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                .tag("error", outcome.GetError().GetMessage());
        return -1;
    }
    return 0;
}

int S3Accessor::list(const std::string& relative_path, std::vector<std::string>* keys) {
    Aws::S3::Model::ListObjectsV2Request request;
    auto prefix = get_key(relative_path);
    request.WithBucket(conf_.bucket).WithPrefix(prefix);

    bool is_trucated = false;
    do {
        auto outcome = s3_client_->ListObjectsV2(request);
        if (!outcome.IsSuccess()) {
            LOG_WARNING("failed to list objects")
                    .tag("endpoint", conf_.endpoint)
                    .tag("bucket", conf_.bucket)
                    .tag("prefix", prefix)
                    .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                    .tag("error", outcome.GetError().GetMessage());
            return -1;
        }
        const auto& result = outcome.GetResult();
        VLOG_DEBUG << "get " << result.GetContents().size() << " objects";
        for (const auto& obj : result.GetContents()) {
            keys->push_back(obj.GetKey());
        }
        is_trucated = result.GetIsTruncated();
        request.SetContinuationToken(result.GetNextContinuationToken());
    } while (is_trucated);
    return 0;
}

int S3Accessor::exists( const std::string& relative_path, const std::string& etag, bool* exist) {
    Aws::S3::Model::HeadObjectRequest request;
    auto key = get_key(relative_path);
    request.WithBucket(conf_.bucket).WithKey(key);
    auto outcome = s3_client_->HeadObject(request);
    if (outcome.IsSuccess()) {
        *exist = outcome.GetResult().GetETag() == etag;
        return 0;
    } else if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        *exist = false;
        return 0;
    } else {
        LOG_WARNING("failed to head object")
                .tag("endpoint", conf_.endpoint)
                .tag("bucket", conf_.bucket)
                .tag("key", key)
                .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                .tag("error", outcome.GetError().GetMessage());
        return -1;
    }
}

int S3Accessor::get_etag(const std::string& relative_path, std::string* etag) {
    Aws::S3::Model::HeadObjectRequest request;
    auto key = get_key(relative_path);
    request.WithBucket(conf_.bucket).WithKey(key);
    auto outcome = s3_client_->HeadObject(request);
    if (outcome.IsSuccess()) {
        *etag = outcome.GetResult().GetETag();
        return 0;
    } else {
        LOG_WARNING("failed to head object")
                .tag("endpoint", conf_.endpoint)
                .tag("bucket", conf_.bucket)
                .tag("key", key)
                .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                .tag("error", outcome.GetError().GetMessage());
        return -1;
    }
}
} // namespace selectdb
