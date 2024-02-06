#pragma once

#include <Storages/IStorage.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/StorageConfiguration.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Storages/StorageObjectStorageConfiguration.h>
#include <Storages/StorageObjectStorageSettings.h>


namespace DB
{

struct SelectQueryInfo;

template <typename StorageSettings>
class StorageObjectStorage : public IStorage
{
public:
    using Configuration = StorageObjectStorageConfiguration;
    using ConfigurationPtr = std::shared_ptr<Configuration>;

    StorageObjectStorage(
        ConfigurationPtr configuration_,
        ObjectStoragePtr object_storage_,
        const String & engine_name_,
        ContextPtr context_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        std::optional<FormatSettings> format_settings_,
        bool distributed_processing_,
        ASTPtr partition_by_);

    String getName() const override { return engine_name; }

    void read(
        QueryPlan & query_plan,
        const Names &,
        const StorageSnapshotPtr &,
        SelectQueryInfo &,
        ContextPtr,
        QueryProcessingStage::Enum,
        size_t,
        size_t) override;

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

    NamesAndTypesList getVirtuals() const override { return virtual_columns; }

    static Names getVirtualColumnNames();

    bool supportsPartitionBy() const override { return true; }

    bool supportsSubcolumns() const override { return true; }

    bool supportsTrivialCountOptimization() const override { return true; }

    bool supportsSubsetOfColumns(const ContextPtr & context) const;

    bool prefersLargeBlocks() const override;

    bool parallelizeOutputAfterReading(ContextPtr context) const override;

    static SchemaCache & getSchemaCache(const ContextPtr & context);

    static ColumnsDescription getTableStructureFromData(
        ObjectStoragePtr object_storage,
        const ConfigurationPtr & configuration,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context);

private:
    const std::string engine_name;
    const NamesAndTypesList virtual_columns;
    std::optional<FormatSettings> format_settings;
    const ASTPtr partition_by;
    const bool distributed_processing;

    ObjectStoragePtr object_storage;
    ConfigurationPtr configuration;
    std::mutex configuration_update_mutex;

    std::pair<ConfigurationPtr, ObjectStoragePtr> updateConfigurationAndGetCopy(ContextPtr local_context);
};

template <typename StorageSettings>
class StorageObjectStorageSource : public ISource, WithContext
{
public:
    using Storage = StorageObjectStorage<StorageSettings>;

    class IIterator : public WithContext
    {
    public:
        explicit IIterator(ContextPtr context_) : WithContext(context_) {}
        virtual ~IIterator() = default;

        virtual RelativePathWithMetadata next() = 0;
        RelativePathWithMetadata operator ()() { return next(); }
    };

    class ReadIterator;
    class GlobIterator;
    class KeysIterator;

    StorageObjectStorageSource(
        String name_,
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration,
        const ReadFromFormatInfo & info,
        std::optional<FormatSettings> format_settings_,
        ContextPtr context_,
        UInt64 max_block_size_,
        std::shared_ptr<IIterator> file_iterator_,
        bool need_only_count_);

    ~StorageObjectStorageSource() override;

    String getName() const override { return name; }

    Chunk generate() override;

    static std::shared_ptr<IIterator> createFileIterator(
        StorageObjectStorageConfigurationPtr configuration,
        ObjectStoragePtr object_storage,
        bool distributed_processing,
        const ContextPtr & context,
        const ActionsDAG::Node * predicate,
        const NamesAndTypesList & virtual_columns,
        RelativePathsWithMetadata * read_keys,
        std::function<void(FileProgress)> file_progress_callback = {});

private:
    void addNumRowsToCache(const String & path, size_t num_rows);
    std::optional<size_t> tryGetNumRowsFromCache(const RelativePathWithMetadata & path_with_metadata);

    const String name;
    ObjectStoragePtr object_storage;
    const StorageObjectStorageConfigurationPtr configuration;
    const std::optional<FormatSettings> format_settings;
    const UInt64 max_block_size;
    const bool need_only_count;
    const ReadFromFormatInfo read_from_format_info;

    ColumnsDescription columns_desc;
    std::shared_ptr<IIterator> file_iterator;
    size_t total_rows_in_file = 0;

    struct ReaderHolder
    {
    public:
        ReaderHolder(
            RelativePathWithMetadata relative_path_with_metadata_,
            std::unique_ptr<ReadBuffer> read_buf_,
            std::shared_ptr<ISource> source_,
            std::unique_ptr<QueryPipeline> pipeline_,
            std::unique_ptr<PullingPipelineExecutor> reader_)
            : relative_path_with_metadata(std::move(relative_path_with_metadata_))
            , read_buf(std::move(read_buf_))
            , source(std::move(source_))
            , pipeline(std::move(pipeline_))
            , reader(std::move(reader_))
        {
        }

        ReaderHolder() = default;
        ReaderHolder(const ReaderHolder & other) = delete;
        ReaderHolder & operator=(const ReaderHolder & other) = delete;
        ReaderHolder(ReaderHolder && other) noexcept { *this = std::move(other); }

        ReaderHolder & operator=(ReaderHolder && other) noexcept
        {
            /// The order of destruction is important.
            /// reader uses pipeline, pipeline uses read_buf.
            reader = std::move(other.reader);
            pipeline = std::move(other.pipeline);
            source = std::move(other.source);
            read_buf = std::move(other.read_buf);
            relative_path_with_metadata = std::move(other.relative_path_with_metadata);
            return *this;
        }

        explicit operator bool() const { return reader != nullptr; }
        PullingPipelineExecutor * operator->() { return reader.get(); }
        const PullingPipelineExecutor * operator->() const { return reader.get(); }
        const String & getRelativePath() const { return relative_path_with_metadata.relative_path; }
        const RelativePathWithMetadata & getRelativePathWithMetadata() const { return relative_path_with_metadata; }
        const IInputFormat * getInputFormat() const { return dynamic_cast<const IInputFormat *>(source.get()); }

    private:
        RelativePathWithMetadata relative_path_with_metadata;
        std::unique_ptr<ReadBuffer> read_buf;
        std::shared_ptr<ISource> source;
        std::unique_ptr<QueryPipeline> pipeline;
        std::unique_ptr<PullingPipelineExecutor> reader;
    };

    ReaderHolder reader;
    LoggerPtr log = getLogger("StorageObjectStorageSource");
    ThreadPool create_reader_pool;
    ThreadPoolCallbackRunner<ReaderHolder> create_reader_scheduler;
    std::future<ReaderHolder> reader_future;

    /// Recreate ReadBuffer and Pipeline for each file.
    ReaderHolder createReader();
    std::future<ReaderHolder> createReaderAsync();

    std::unique_ptr<ReadBuffer> createReadBuffer(const String & key, size_t object_size);
};

template <typename StorageSettings>
class StorageObjectStorageSource<StorageSettings>::ReadIterator : public IIterator
{
public:
    explicit ReadIterator(ContextPtr context_, const ReadTaskCallback & callback_)
        : IIterator(context_), callback(callback_) {}

    RelativePathWithMetadata next() override { return { callback(), {} }; }

private:
    ReadTaskCallback callback;
};

template <typename StorageSettings>
class StorageObjectStorageSource<StorageSettings>::GlobIterator : public IIterator
{
public:
    GlobIterator(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration_,
        const ActionsDAG::Node * predicate,
        const NamesAndTypesList & virtual_columns_,
        ContextPtr context_,
        RelativePathsWithMetadata * outer_blobs_,
        std::function<void(FileProgress)> file_progress_callback_ = {});

    RelativePathWithMetadata next() override;
    ~GlobIterator() override = default;

private:
    ObjectStoragePtr object_storage;
    StorageObjectStorageConfigurationPtr configuration;
    ActionsDAGPtr filter_dag;
    NamesAndTypesList virtual_columns;

    size_t index = 0;

    RelativePathsWithMetadata blobs_with_metadata;
    RelativePathsWithMetadata * outer_blobs;
    ObjectStorageIteratorPtr object_storage_iterator;
    bool recursive{false};

    std::unique_ptr<re2::RE2> matcher;

    void createFilterAST(const String & any_key);
    bool is_finished = false;
    std::mutex next_mutex;

    std::function<void(FileProgress)> file_progress_callback;
};

template <typename StorageSettings>
class StorageObjectStorageSource<StorageSettings>::KeysIterator : public IIterator
{
public:
    KeysIterator(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration_,
        const ActionsDAG::Node * predicate,
        const NamesAndTypesList & virtual_columns_,
        ContextPtr context_,
        RelativePathsWithMetadata * outer_blobs,
        std::function<void(FileProgress)> file_progress_callback = {});

    ~KeysIterator() override = default;

    RelativePathWithMetadata next() override;

private:
    ObjectStoragePtr object_storage;
    const StorageObjectStorageConfigurationPtr configuration;
    const NamesAndTypesList virtual_columns;

    ActionsDAGPtr filter_dag;
    RelativePathsWithMetadata keys;
    std::atomic<size_t> index = 0;
};

// using StorageS3 = StorageObjectStorage<S3StorageSettings>;
using StorageAzureBlobStorage = StorageObjectStorage<AzureStorageSettings>;

}
