#include <Storages/IStorage.h>
#include <Storages/IObjectStorageConfiguration.h>
#include <Disks/ObjectStorages/IObjectStorage_fwd.h>
#include <IO/S3/getObjectInfo.h>

namespace DB
{

class StorageObjectStorage : public IStorage
{
    friend class ReadFromObjectStorageStep;
public:
    StorageObjectStorage(
        const StorageID & table_id_,
        const std::string & engine_name_,
        ObjectStoragePtr object_storage_,
        ObjectStorageConfigurationPtr configuration_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment_,
        std::optional<FormatSettings> format_settings_,
        ASTPtr partition_by_ = nullptr);

    String getName() const override { return engine_name; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        bool async_insert) override;

    void truncate(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr local_context,
        TableExclusiveLockHolder &) override;

    NamesAndTypesList getVirtuals() const override;
    static Names getVirtualColumnNames();

    bool supportsPartitionBy() const override;

    static void processNamedCollectionResult(StorageS3::Configuration & configuration, const NamedCollection & collection);

    static SchemaCache & getSchemaCache(const ContextPtr & ctx);

    static StorageS3::Configuration getConfiguration(ASTs & engine_args, ContextPtr local_context, bool get_format_from_file = true);

    static ColumnsDescription getTableStructureFromData(
        const StorageS3::Configuration & configuration,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr ctx);

    using KeysWithInfo = StorageS3Source::KeysWithInfo;

    bool supportsTrivialCountOptimization() const override { return true; }

    bool supportsSubcolumns() const override { return true; }

    bool supportsSubsetOfColumns(const ContextPtr & context) const;

    bool prefersLargeBlocks() const override;

    bool parallelizeOutputAfterReading(ContextPtr context) const override;

    static ColumnsDescription getTableStructureFromDataImpl(
        const ObjectStorageConfigurationPtr & configuration,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context);

protected:
    virtual Configuration updateConfigurationAndGetCopy(ContextPtr local_context);

    virtual void updateConfiguration(ContextPtr local_context);

    void useConfiguration(const Configuration & new_configuration);

    const Configuration & getConfiguration();

private:
    const std::string engine_name;
    const ObjectStoragePtr object_storage;
    const std::optional<FormatSettings> format_settings;
    const NamesAndTypesList virtual_columns;

    ObjectStorageConfigurationPtr configuration;
    Poco::Logger * log;
};
}
