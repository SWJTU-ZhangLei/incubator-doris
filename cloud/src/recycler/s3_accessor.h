#pragma once

#include <memory>
#include <string>
#include <vector>

namespace Aws::S3 {
class S3Client;
} // namespace Aws::S3

namespace selectdb {

class ObjStoreAccessor {
public:
    ObjStoreAccessor() = default;
    virtual ~ObjStoreAccessor() = default;

    virtual const std::string& path() const = 0;

    // returns 0 for success otherwise error
    virtual int init() = 0;

    // returns 0 for success otherwise error
    virtual int delete_objects_by_prefix(const std::string& relative_path) = 0;

    // returns 0 for success otherwise error
    virtual int delete_objects(const std::vector<std::string>& relative_paths) = 0;

    // returns 0 for success otherwise error
    virtual int delete_object(const std::string& relative_path) = 0;

    // for test
    // returns 0 for success otherwise error
    virtual int put_object(const std::string& relative_path, const std::string& content) = 0;

    // returns 0 for success otherwise error
    virtual int list(const std::string& relative_path, std::vector<std::string>* keys) = 0;
};

struct S3Conf {
    std::string ak;
    std::string sk;
    std::string endpoint;
    std::string region;
    std::string bucket;
    std::string prefix;
};

class S3Accessor : public ObjStoreAccessor {
public:
    explicit S3Accessor(S3Conf conf);
    ~S3Accessor() override;

    const std::string& path() const override { return path_; }

    // returns 0 for success otherwise error
    int init() override;

    // returns 0 for success otherwise error
    int delete_objects_by_prefix(const std::string& relative_path) override;

    // returns 0 for success otherwise error
    int delete_objects(const std::vector<std::string>& relative_paths) override;

    // returns 0 for success otherwise error
    int delete_object(const std::string& relative_path) override;

    // for test
    // returns 0 for success otherwise error
    int put_object(const std::string& relative_path, const std::string& content) override;

    // returns 0 for success otherwise error
    int list(const std::string& relative_path, std::vector<std::string>* keys) override;

private:
    std::string get_key(const std::string& relative_path) const;

private:
    std::shared_ptr<Aws::S3::S3Client> s3_client_;
    S3Conf conf_;
    std::string path_;
};

} // namespace selectdb
