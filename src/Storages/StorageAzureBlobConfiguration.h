#pragma once
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Storages/StorageObjectStorageConfiguration.h>

namespace DB
{

class StorageAzureBlobConfiguration : public StorageObjectStorageConfiguration
{
public:
    Path getPath() const override { return blob_path; }
    const Paths & getPaths() const override { return blobs_paths; }
    Paths & getPaths() override { return blobs_paths; }

    String getDataSourceDescription() override { return fs::path(connection_url) / container; }
    String getNamespace() const override { return container; }

    void check(ContextPtr context) const override;
    ObjectStoragePtr createOrUpdateObjectStorage(ContextPtr context, bool is_readonly = true) override; /// NOLINT

    void fromNamedCollection(const NamedCollection & collection) override;
    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override;

private:
    using AzureClient = Azure::Storage::Blobs::BlobContainerClient;
    using AzureClientPtr = std::unique_ptr<Azure::Storage::Blobs::BlobContainerClient>;

    std::string connection_url;
    bool is_connection_string;

    std::optional<std::string> account_name;
    std::optional<std::string> account_key;

    std::string container;
    std::string blob_path;
    std::vector<String> blobs_paths;

    AzureClientPtr createClient(bool is_read_only);
    AzureObjectStorage::SettingsPtr createSettings(ContextPtr local_context);
};

}
