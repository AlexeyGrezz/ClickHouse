#include <Storages/StorageObjectStorage.h>

#include <Common/re2.h>
#include <Common/parseGlobs.h>
#include <Disks/ObjectStorages/ObjectStorageIterator.h>

#include <Formats/ReadSchemaUtils.h>
#include <Formats/FormatFactory.h>
#include <Parsers/ASTInsertQuery.h>

#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sources/ConstChunkGenerator.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>

#include <Storages/StorageFactory.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/PartitionedSink.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/StorageObjectStorageSettings.h>
#include <Storages/StorageObjectStorageConfiguration.h>
#include <IO/ReadBufferFromFileBase.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>


namespace ProfileEvents
{
    extern const Event EngineFileLikeReadFiles;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;

}

template <typename StorageSettings>
std::unique_ptr<StorageInMemoryMetadata> getStorageMetadata(
    ObjectStoragePtr object_storage,
    const StorageObjectStorageConfigurationPtr & configuration,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints,
    std::optional<FormatSettings> format_settings,
    const String & comment,
    const std::string & engine_name,
    const ContextPtr & context)
{
    auto storage_metadata = std::make_unique<StorageInMemoryMetadata>();
    if (columns.empty())
    {
        auto fetched_columns = StorageObjectStorage<StorageSettings>::getTableStructureFromData(
            object_storage, configuration, format_settings, context);
        storage_metadata->setColumns(fetched_columns);
    }
    else
    {
        /// We don't allow special columns.
        if (!columns.hasOnlyOrdinary())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Table engine {} doesn't support special columns "
                            "like MATERIALIZED, ALIAS or EPHEMERAL",
                            engine_name);

        storage_metadata->setColumns(columns);
    }

    storage_metadata->setConstraints(constraints);
    storage_metadata->setComment(comment);
    return storage_metadata;
}

template <typename StorageSettings>
StorageObjectStorage<StorageSettings>::StorageObjectStorage(
    ConfigurationPtr configuration_,
    ObjectStoragePtr object_storage_,
    const String & engine_name_,
    ContextPtr context,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    std::optional<FormatSettings> format_settings_,
    bool distributed_processing_,
    ASTPtr partition_by_)
    : IStorage(table_id_, getStorageMetadata<StorageSettings>(
                   object_storage_, configuration_, columns_, constraints_, format_settings_,
                   comment, engine_name, context))
    , engine_name(engine_name_)
    , virtual_columns(VirtualColumnUtils::getPathFileAndSizeVirtualsForStorage(
                          getInMemoryMetadataPtr()->getSampleBlock().getNamesAndTypesList()))
    , format_settings(format_settings_)
    , partition_by(partition_by_)
    , distributed_processing(distributed_processing_)
    , object_storage(object_storage_)
    , configuration(configuration_)
{
    FormatFactory::instance().checkFormatName(configuration->format);
    configuration->check(context);

    StoredObjects objects;
    for (const auto & key : configuration->getPaths())
        objects.emplace_back(key);
}

template <typename StorageSettings>
Names StorageObjectStorage<StorageSettings>::getVirtualColumnNames()
{
    return VirtualColumnUtils::getPathFileAndSizeVirtualsForStorage({}).getNames();
}

template <typename StorageSettings>
bool StorageObjectStorage<StorageSettings>::supportsSubsetOfColumns(const ContextPtr & context) const
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(configuration->format, context, format_settings);
}

template <typename StorageSettings>
bool StorageObjectStorage<StorageSettings>::prefersLargeBlocks() const
{
    return FormatFactory::instance().checkIfOutputFormatPrefersLargeBlocks(configuration->format);
}

template <typename StorageSettings>
bool StorageObjectStorage<StorageSettings>::parallelizeOutputAfterReading(ContextPtr context) const
{
    return FormatFactory::instance().checkParallelizeOutputAfterReading(configuration->format, context);
}

template <typename StorageSettings>
std::pair<StorageObjectStorageConfigurationPtr, ObjectStoragePtr>
StorageObjectStorage<StorageSettings>::updateConfigurationAndGetCopy(ContextPtr local_context)
{
    std::lock_guard lock(configuration_update_mutex);
    auto new_object_storage = configuration->createOrUpdateObjectStorage(local_context);
    if (new_object_storage)
        object_storage = new_object_storage;
    return {configuration, object_storage};
}

template <typename StorageSettings>
void StorageObjectStorage<StorageSettings>::truncate(
    const ASTPtr &,
    const StorageMetadataPtr &,
    ContextPtr,
    TableExclusiveLockHolder &)
{
    if (configuration->withGlobs())
    {
        throw Exception(
            ErrorCodes::DATABASE_ACCESS_DENIED,
            "{} key '{}' contains globs, so the table is in readonly mode and cannot be truncated",
            getName(), configuration->getPath());
    }

    StoredObjects objects;
    for (const auto & key : configuration->getPaths())
        objects.emplace_back(key);

    object_storage->removeObjectsIfExist(objects);
}

namespace
{

class StorageObjectStorageSink : public SinkToStorage
{
public:
    StorageObjectStorageSink(
        ObjectStoragePtr object_storage,
        StorageObjectStorageConfigurationPtr configuration,
        std::optional<FormatSettings> format_settings_,
        const Block & sample_block_,
        ContextPtr context,
        const std::string & blob_path = "")
        : SinkToStorage(sample_block_)
        , sample_block(sample_block_)
        , format_settings(format_settings_)
    {
        const auto & settings = context->getSettingsRef();
        const auto path = blob_path.empty() ? configuration->getPaths().back() : blob_path;
        const auto chosen_compression_method = chooseCompressionMethod(path, configuration->compression_method);

        auto buffer = object_storage->writeObject(
            StoredObject(path), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

        write_buf = wrapWriteBufferWithCompressionMethod(
                        std::move(buffer),
                        chosen_compression_method,
                        static_cast<int>(settings.output_format_compression_level),
                        static_cast<int>(settings.output_format_compression_zstd_window_log));

        writer = FormatFactory::instance().getOutputFormatParallelIfPossible(
            configuration->format, *write_buf, sample_block, context, format_settings);
    }

    String getName() const override { return "StorageObjectStorageSink"; }

    void consume(Chunk chunk) override
    {
        std::lock_guard lock(cancel_mutex);
        if (cancelled)
            return;
        writer->write(getHeader().cloneWithColumns(chunk.detachColumns()));
    }

    void onCancel() override
    {
        std::lock_guard lock(cancel_mutex);
        finalize();
        cancelled = true;
    }

    void onException(std::exception_ptr exception) override
    {
        std::lock_guard lock(cancel_mutex);
        try
        {
            std::rethrow_exception(exception);
        }
        catch (...)
        {
            /// An exception context is needed to proper delete write buffers without finalization.
            release();
        }
    }

    void onFinish() override
    {
        std::lock_guard lock(cancel_mutex);
        finalize();
    }

private:
    const Block sample_block;
    const std::optional<FormatSettings> format_settings;

    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;
    bool cancelled = false;
    std::mutex cancel_mutex;

    void finalize()
    {
        if (!writer)
            return;

        try
        {
            writer->finalize();
            writer->flush();
            write_buf->finalize();
        }
        catch (...)
        {
            /// Stop ParallelFormattingOutputFormat correctly.
            release();
            throw;
        }
    }

    void release()
    {
        writer.reset();
        write_buf->finalize();
    }
};

class PartitionedStorageObjectStorageSink : public PartitionedSink
{
public:
    PartitionedStorageObjectStorageSink(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration_,
        std::optional<FormatSettings> format_settings_,
        const Block & sample_block_,
        ContextPtr context_,
        const ASTPtr & partition_by)
        : PartitionedSink(partition_by, context_, sample_block_)
        , object_storage(object_storage_)
        , configuration(configuration_)
        , format_settings(format_settings_)
        , sample_block(sample_block_)
        , context(context_)
    {
    }

    SinkPtr createSinkForPartition(const String & partition_id) override
    {
        auto blob = configuration->getPaths().back();
        auto partition_key = replaceWildcards(blob, partition_id);
        validatePartitionKey(partition_key, true);
        return std::make_shared<StorageObjectStorageSink>(
            object_storage,
            configuration,
            format_settings,
            sample_block,
            context,
            partition_key
        );
    }

private:
    ObjectStoragePtr object_storage;
    StorageObjectStorageConfigurationPtr configuration;
    const std::optional<FormatSettings> format_settings;
    const Block sample_block;
    const ContextPtr context;
};

}

template <typename StorageSettings>
class ReadFromStorageObejctStorage : public SourceStepWithFilter
{
public:
    using Storage = StorageObjectStorage<StorageSettings>;
    using Source = StorageObjectStorageSource<StorageSettings>;

    ReadFromStorageObejctStorage(
        ObjectStoragePtr object_storage_,
        Storage::ConfigurationPtr configuration_,
        const String & name_,
        const NamesAndTypesList & virtual_columns_,
        const std::optional<DB::FormatSettings> & format_settings_,
        bool distributed_processing_,
        ReadFromFormatInfo info_,
        const bool need_only_count_,
        ContextPtr context_,
        size_t max_block_size_,
        size_t num_streams_)
        : SourceStepWithFilter(DataStream{.header = info_.source_header})
        , object_storage(object_storage_)
        , configuration(configuration_)
        , context(std::move(context_))
        , info(std::move(info_))
        , virtual_columns(virtual_columns_)
        , format_settings(format_settings_)
        , name(name_ + "Source")
        , need_only_count(need_only_count_)
        , max_block_size(max_block_size_)
        , num_streams(num_streams_)
        , distributed_processing(distributed_processing_)
    {
    }

    std::string getName() const override { return name; }

    void applyFilters() override
    {
        auto filter_actions_dag = ActionsDAG::buildFilterActionsDAG(filter_nodes.nodes);
        const ActionsDAG::Node * predicate = nullptr;
        if (filter_actions_dag)
            predicate = filter_actions_dag->getOutputs().at(0);

        createIterator(predicate);
    }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        createIterator(nullptr);

        Pipes pipes;
        for (size_t i = 0; i < num_streams; ++i)
        {
            pipes.emplace_back(std::make_shared<Source>(
                getName(), object_storage, configuration, info, format_settings,
                context, max_block_size, iterator_wrapper, need_only_count));
        }

        auto pipe = Pipe::unitePipes(std::move(pipes));
        if (pipe.empty())
            pipe = Pipe(std::make_shared<NullSource>(info.source_header));

        for (const auto & processor : pipe.getProcessors())
            processors.emplace_back(processor);

        pipeline.init(std::move(pipe));
    }

private:
    ObjectStoragePtr object_storage;
    Storage::ConfigurationPtr configuration;
    ContextPtr context;

    const ReadFromFormatInfo info;
    const NamesAndTypesList virtual_columns;
    const std::optional<DB::FormatSettings> format_settings;
    const String name;
    const bool need_only_count;
    const size_t max_block_size;
    const size_t num_streams;
    const bool distributed_processing;

    std::shared_ptr<typename Source::IIterator> iterator_wrapper;

    void createIterator(const ActionsDAG::Node * predicate)
    {
        if (iterator_wrapper)
            return;

        iterator_wrapper = Source::createFileIterator(
            configuration, object_storage, distributed_processing, context,
            predicate, virtual_columns, nullptr, context->getFileProgressCallback());
    }
};

template <typename StorageSettings>
void StorageObjectStorage<StorageSettings>::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    if (partition_by && configuration->withWildcard())
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Reading from a partitioned {} storage is not implemented yet",
                        getName());
    }

    auto this_ptr = std::static_pointer_cast<StorageObjectStorage>(shared_from_this());
    auto read_from_format_info = prepareReadingFromFormat(column_names,
                                                          storage_snapshot,
                                                          supportsSubsetOfColumns(local_context),
                                                          getVirtuals());
    bool need_only_count = (query_info.optimize_trivial_count
                            || read_from_format_info.requested_columns.empty())
        && local_context->getSettingsRef().optimize_count_from_files;


    auto [query_configuration, query_object_storage] = updateConfigurationAndGetCopy(local_context);
    auto reading = std::make_unique<ReadFromStorageObejctStorage<StorageSettings>>(
        query_object_storage,
        query_configuration,
        getName(),
        virtual_columns,
        format_settings,
        distributed_processing,
        std::move(read_from_format_info),
        need_only_count,
        local_context,
        max_block_size,
        num_streams);

    query_plan.addStep(std::move(reading));
}

template <typename StorageSettings>
std::shared_ptr<typename StorageObjectStorageSource<StorageSettings>::IIterator>
StorageObjectStorageSource<StorageSettings>::createFileIterator(
    StorageObjectStorageConfigurationPtr configuration,
    ObjectStoragePtr object_storage,
    bool distributed_processing,
    const ContextPtr & local_context,
    const ActionsDAG::Node * predicate,
    const NamesAndTypesList & virtual_columns,
    RelativePathsWithMetadata * read_keys,
    std::function<void(FileProgress)> file_progress_callback)
{
    using Source = StorageObjectStorageSource<StorageSettings>;
    if (distributed_processing)
    {
        return std::make_shared<typename Source::ReadIterator>(local_context, local_context->getReadTaskCallback());
    }
    else if (configuration->withGlobs())
    {
        /// Iterate through disclosed globs and make a source for each file
        return std::make_shared<typename Source::GlobIterator>(
            object_storage, configuration,
            predicate, virtual_columns, local_context, read_keys, file_progress_callback);
    }
    else
    {
        return std::make_shared<typename Source::KeysIterator>(
            object_storage, configuration, predicate, virtual_columns,
            local_context, read_keys, file_progress_callback);
    }
}

template <typename StorageSettings>
SinkToStoragePtr StorageObjectStorage<StorageSettings>::write(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    bool /* async_insert */)
{
    auto insert_query = std::dynamic_pointer_cast<ASTInsertQuery>(query);
    auto partition_by_ast = insert_query
        ? (insert_query->partition_by ? insert_query->partition_by : partition_by)
        : nullptr;
    bool is_partitioned_implementation = partition_by_ast && configuration->withWildcard();

    auto sample_block = metadata_snapshot->getSampleBlock();
    auto storage_settings = StorageSettings::create(local_context->getSettingsRef());

    if (is_partitioned_implementation)
    {
        return std::make_shared<PartitionedStorageObjectStorageSink>(
            object_storage, configuration, format_settings, sample_block, local_context, partition_by_ast);
    }
    else
    {
        if (configuration->withGlobs())
        {
            throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED,
                            "{} key '{}' contains globs, so the table is in readonly mode",
                            getName(), configuration->getPath());
        }

        if (!storage_settings.truncate_on_insert
            && object_storage->exists(StoredObject(configuration->getPath())))
        {
            if (storage_settings.create_new_file_on_insert)
            {
                size_t index = configuration->getPaths().size();
                const auto & first_key = configuration->getPaths()[0];
                auto pos = first_key.find_first_of('.');
                String new_key;

                do
                {
                    new_key = first_key.substr(0, pos)
                        + "."
                        + std::to_string(index)
                        + (pos == std::string::npos ? "" : first_key.substr(pos));
                    ++index;
                }
                while (object_storage->exists(StoredObject(new_key)));

                configuration->getPaths().push_back(new_key);
            }
            else
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Object in bucket {} with key {} already exists. "
                    "If you want to overwrite it, enable setting [engine_name]_truncate_on_insert, if you "
                    "want to create a new file on each insert, enable setting [engine_name]_create_new_file_on_insert",
                    configuration->getNamespace(), configuration->getPaths().back());
            }
        }

        return std::make_shared<StorageObjectStorageSink>(
            object_storage, configuration, format_settings, sample_block, local_context);
    }
}

template <typename StorageSettings>
StorageObjectStorageSource<StorageSettings>::GlobIterator::GlobIterator(
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    const ActionsDAG::Node * predicate,
    const NamesAndTypesList & virtual_columns_,
    ContextPtr context_,
    RelativePathsWithMetadata * outer_blobs_,
    std::function<void(FileProgress)> file_progress_callback_)
    : IIterator(context_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , virtual_columns(virtual_columns_)
    , outer_blobs(outer_blobs_)
    , file_progress_callback(file_progress_callback_)
{

    auto blob_path_with_globs = configuration_->getPath();
    const String key_prefix = blob_path_with_globs.substr(0, blob_path_with_globs.find_first_of("*?{"));

    /// We don't have to list bucket, because there is no asterisks.
    if (key_prefix.size() == blob_path_with_globs.size())
    {
        auto object_metadata = object_storage->getObjectMetadata(blob_path_with_globs);
        blobs_with_metadata.emplace_back(blob_path_with_globs, object_metadata);

        if (outer_blobs)
            outer_blobs->emplace_back(blobs_with_metadata.back());

        if (file_progress_callback)
            file_progress_callback(FileProgress(0, object_metadata.size_bytes));

        is_finished = true;
        return;
    }

    object_storage_iterator = object_storage->iterate(key_prefix);

    matcher = std::make_unique<re2::RE2>(makeRegexpPatternFromGlobs(blob_path_with_globs));
    if (!matcher->ok())
    {
        throw Exception(
            ErrorCodes::CANNOT_COMPILE_REGEXP,
            "Cannot compile regex from glob ({}): {}", blob_path_with_globs, matcher->error());
    }

    recursive = blob_path_with_globs == "/**" ? true : false;
    filter_dag = VirtualColumnUtils::createPathAndFileFilterDAG(predicate, virtual_columns);
}

template <typename StorageSettings>
RelativePathWithMetadata StorageObjectStorageSource<StorageSettings>::GlobIterator::next()
{
    std::lock_guard lock(next_mutex);

    if (is_finished && index >= blobs_with_metadata.size())
        return {};

    bool need_new_batch = blobs_with_metadata.empty() || index >= blobs_with_metadata.size();

    if (need_new_batch)
    {
        RelativePathsWithMetadata new_batch;
        while (new_batch.empty())
        {
            auto result = object_storage_iterator->getCurrrentBatchAndScheduleNext();
            if (result.has_value())
            {
                new_batch = result.value();
            }
            else
            {
                is_finished = true;
                return {};
            }

            for (auto it = new_batch.begin(); it != new_batch.end();)
            {
                if (!recursive && !re2::RE2::FullMatch(it->relative_path, *matcher))
                    it = new_batch.erase(it);
                else
                    ++it;
            }
        }

        index = 0;

        if (filter_dag)
        {
            std::vector<String> paths;
            paths.reserve(new_batch.size());
            for (auto & path_with_metadata : new_batch)
                paths.push_back(fs::path(configuration->getNamespace()) / path_with_metadata.relative_path);

            VirtualColumnUtils::filterByPathOrFile(new_batch, paths, filter_dag, virtual_columns, getContext());
        }

        if (outer_blobs)
            outer_blobs->insert(outer_blobs->end(), new_batch.begin(), new_batch.end());

        blobs_with_metadata = std::move(new_batch);
        if (file_progress_callback)
        {
            for (const auto & [relative_path, info] : blobs_with_metadata)
            {
                file_progress_callback(FileProgress(0, info.size_bytes));
            }
        }
    }

    size_t current_index = index++;
    if (current_index >= blobs_with_metadata.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index out of bound for blob metadata");

    return blobs_with_metadata[current_index];
}

template <typename StorageSettings>
StorageObjectStorageSource<StorageSettings>::KeysIterator::KeysIterator(
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    const ActionsDAG::Node * predicate,
    const NamesAndTypesList & virtual_columns_,
    ContextPtr context_,
    RelativePathsWithMetadata * outer_blobs,
    std::function<void(FileProgress)> file_progress_callback)
    : IIterator(context_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , virtual_columns(virtual_columns_)
{
    Strings all_keys = configuration->getPaths();

    ASTPtr filter_ast;
    if (!all_keys.empty())
        filter_dag = VirtualColumnUtils::createPathAndFileFilterDAG(predicate, virtual_columns);

    if (filter_dag)
    {
        Strings paths;
        paths.reserve(all_keys.size());
        for (const auto & key : all_keys)
            paths.push_back(fs::path(configuration->getNamespace()) / key);

        VirtualColumnUtils::filterByPathOrFile(all_keys, paths, filter_dag, virtual_columns, getContext());
    }

    for (auto && key : all_keys)
    {
        ObjectMetadata object_metadata = object_storage->getObjectMetadata(key);
        if (file_progress_callback)
            file_progress_callback(FileProgress(0, object_metadata.size_bytes));
        keys.emplace_back(key, object_metadata);
    }

    if (outer_blobs)
        *outer_blobs = keys;
}

template <typename StorageSettings>
RelativePathWithMetadata StorageObjectStorageSource<StorageSettings>::KeysIterator::next()
{
    size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
    if (current_index >= keys.size())
        return {};
    else
        return keys[current_index];
}

template <typename StorageSettings>
Chunk StorageObjectStorageSource<StorageSettings>::generate()
{
    while (true)
    {
        if (isCancelled() || !reader)
        {
            if (reader)
                reader->cancel();
            break;
        }

        Chunk chunk;
        if (reader->pull(chunk))
        {
            UInt64 num_rows = chunk.getNumRows();
            total_rows_in_file += num_rows;
            size_t chunk_size = 0;
            if (const auto * input_format = reader.getInputFormat())
                chunk_size = input_format->getApproxBytesReadForChunk();
            progress(num_rows, chunk_size ? chunk_size : chunk.bytes());

            VirtualColumnUtils::addRequestedPathFileAndSizeVirtualsToChunk(
                chunk,
                read_from_format_info.requested_virtual_columns,
                fs::path(configuration->getNamespace()) / reader.getRelativePath(),
                reader.getRelativePathWithMetadata().metadata.size_bytes);

            return chunk;
        }

        if (reader.getInputFormat() && getContext()->getSettingsRef().use_cache_for_count_from_files)
            addNumRowsToCache(reader.getRelativePath(), total_rows_in_file);

        total_rows_in_file = 0;

        assert(reader_future.valid());
        reader = reader_future.get();

        if (!reader)
            break;

        /// Even if task is finished the thread may be not freed in pool.
        /// So wait until it will be freed before scheduling a new task.
        create_reader_pool.wait();
        reader_future = createReaderAsync();
    }

    return {};
}

template <typename StorageSettings>
void StorageObjectStorageSource<StorageSettings>::addNumRowsToCache(const String & path, size_t num_rows)
{
    String source = fs::path(configuration->getDataSourceDescription()) / path;
    auto cache_key = getKeyForSchemaCache(source, configuration->format, format_settings, getContext());
    Storage::getSchemaCache(getContext()).addNumRows(cache_key, num_rows);
}

template <typename StorageSettings>
std::optional<size_t> StorageObjectStorageSource<StorageSettings>::tryGetNumRowsFromCache(DB::RelativePathWithMetadata & path_with_metadata)
{
    String source = fs::path(configuration->getDataSourceDescription()) / path_with_metadata.relative_path;
    auto cache_key = getKeyForSchemaCache(source, configuration->format, format_settings, getContext());
    auto get_last_mod_time = [&]() -> std::optional<time_t>
    {
        // auto last_mod = path_with_metadata.metadata.last_modified;
        // if (last_mod)
        //     return last_mod->epochTime();
        // else
        {
            path_with_metadata.metadata = object_storage->getObjectMetadata(path_with_metadata.relative_path);
            return path_with_metadata.metadata.last_modified->epochMicroseconds();
        }
    };
    return Storage::getSchemaCache(getContext()).tryGetNumRows(cache_key, get_last_mod_time);
}

template <typename StorageSettings>
StorageObjectStorageSource<StorageSettings>::StorageObjectStorageSource(
    String name_,
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    const ReadFromFormatInfo & info,
    std::optional<FormatSettings> format_settings_,
    ContextPtr context_,
    UInt64 max_block_size_,
    std::shared_ptr<IIterator> file_iterator_,
    bool need_only_count_)
    :ISource(info.source_header, false)
    , WithContext(context_)
    , name(std::move(name_))
    , object_storage(object_storage_)
    , configuration(configuration_)
    , format_settings(format_settings_)
    , max_block_size(max_block_size_)
    , need_only_count(need_only_count_)
    , read_from_format_info(info)
    , columns_desc(info.columns_description)
    , file_iterator(file_iterator_)
    , create_reader_pool(StorageSettings::ObjectStorageThreads(),
                         StorageSettings::ObjectStorageThreadsActive(),
                         StorageSettings::ObjectStorageThreadsScheduled(), 1)
    , create_reader_scheduler(threadPoolCallbackRunner<ReaderHolder>(create_reader_pool, "Reader"))
{
    reader = createReader();
    if (reader)
        reader_future = createReaderAsync();
}

template <typename StorageSettings>
StorageObjectStorageSource<StorageSettings>::~StorageObjectStorageSource()
{
    create_reader_pool.wait();
}

template <typename StorageSettings>
StorageObjectStorageSource<StorageSettings>::ReaderHolder
StorageObjectStorageSource<StorageSettings>::createReader()
{
    auto path_with_metadata = file_iterator->next();
    if (path_with_metadata.relative_path.empty())
        return {};

    if (path_with_metadata.metadata.size_bytes == 0)
        path_with_metadata.metadata = object_storage->getObjectMetadata(path_with_metadata.relative_path);

    QueryPipelineBuilder builder;
    std::shared_ptr<ISource> source;
    std::unique_ptr<ReadBuffer> read_buf;
    std::optional<size_t> num_rows_from_cache = need_only_count
        && getContext()->getSettingsRef().use_cache_for_count_from_files
        ? tryGetNumRowsFromCache(path_with_metadata)
        : std::nullopt;

    if (num_rows_from_cache)
    {
        /// We should not return single chunk with all number of rows,
        /// because there is a chance that this chunk will be materialized later
        /// (it can cause memory problems even with default values in columns or when virtual columns are requested).
        /// Instead, we use special ConstChunkGenerator that will generate chunks
        /// with max_block_size rows until total number of rows is reached.
        source = std::make_shared<ConstChunkGenerator>(
            read_from_format_info.format_header, *num_rows_from_cache, max_block_size);
        builder.init(Pipe(source));
    }
    else
    {
        std::optional<size_t> max_parsing_threads;
        if (need_only_count)
            max_parsing_threads = 1;

        auto compression_method = chooseCompressionMethod(
            path_with_metadata.relative_path, configuration->compression_method);

        read_buf = createReadBuffer(path_with_metadata.relative_path, path_with_metadata.metadata.size_bytes);

        auto input_format = FormatFactory::instance().getInput(
            configuration->format, *read_buf, read_from_format_info.format_header,
            getContext(), max_block_size, format_settings, max_parsing_threads,
            std::nullopt, /* is_remote_fs */ true, compression_method);

        if (need_only_count)
            input_format->needOnlyCount();

        builder.init(Pipe(input_format));

        if (columns_desc.hasDefaults())
        {
            builder.addSimpleTransform(
                [&](const Block & header)
                {
                    return std::make_shared<AddingDefaultsTransform>(header, columns_desc, *input_format, getContext());
                });
        }

        source = input_format;
    }

    /// Add ExtractColumnsTransform to extract requested columns/subcolumns
    /// from chunk read by IInputFormat.
    builder.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ExtractColumnsTransform>(header, read_from_format_info.requested_columns);
    });

    auto pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
    auto current_reader = std::make_unique<PullingPipelineExecutor>(*pipeline);

    ProfileEvents::increment(ProfileEvents::EngineFileLikeReadFiles);

    return ReaderHolder{path_with_metadata, std::move(read_buf),
                        std::move(source), std::move(pipeline), std::move(current_reader)};
}

template <typename StorageSettings>
std::future<typename StorageObjectStorageSource<StorageSettings>::ReaderHolder>
StorageObjectStorageSource<StorageSettings>::createReaderAsync()
{
    return create_reader_scheduler([this] { return createReader(); }, Priority{});
}

template <typename StorageSettings>
std::unique_ptr<ReadBuffer> StorageObjectStorageSource<StorageSettings>::createReadBuffer(const String & key, size_t object_size)
{
    auto read_settings = getContext()->getReadSettings().adjustBufferSize(object_size);
    read_settings.enable_filesystem_cache = false;
    read_settings.remote_read_min_bytes_for_seek = read_settings.remote_fs_buffer_size;

    // auto download_buffer_size = getContext()->getSettings().max_download_buffer_size;
    // const bool object_too_small = object_size <= 2 * download_buffer_size;

    // Create a read buffer that will prefetch the first ~1 MB of the file.
    // When reading lots of tiny files, this prefetching almost doubles the throughput.
    // For bigger files, parallel reading is more useful.
    // if (object_too_small && read_settings.remote_fs_method == RemoteFSReadMethod::threadpool)
    // {
    //     LOG_TRACE(log, "Downloading object of size {} with initial prefetch", object_size);

    //     auto async_reader = object_storage->readObjects(
    //         StoredObjects{StoredObject{key, /* local_path */ "", object_size}}, read_settings);

    //     async_reader->setReadUntilEnd();
    //     if (read_settings.remote_fs_prefetch)
    //         async_reader->prefetch(DEFAULT_PREFETCH_PRIORITY);

    //     return async_reader;
    // }
    // else
    return object_storage->readObject(StoredObject(key), read_settings);
}

namespace
{
template <typename StorageSettings>
class ReadBufferIterator : public IReadBufferIterator, WithContext
{
public:
    using Storage = StorageObjectStorage<StorageSettings>;
    using Source = StorageObjectStorageSource<StorageSettings>;
    using FileIterator = std::shared_ptr<typename Source::IIterator>;

    ReadBufferIterator(
        ObjectStoragePtr object_storage_,
        Storage::ConfigurationPtr configuration_,
        const FileIterator & file_iterator_,
        const std::optional<FormatSettings> & format_settings_,
        RelativePathsWithMetadata & read_keys_,
        const ContextPtr & context_)
        : WithContext(context_)
        , object_storage(object_storage_)
        , configuration(configuration_)
        , file_iterator(file_iterator_)
        , format_settings(format_settings_)
        , storage_settings(StorageSettings::create(context_->getSettingsRef()))
        , read_keys(read_keys_)
        , prev_read_keys_size(read_keys_.size())
    {
    }

    std::pair<std::unique_ptr<ReadBuffer>, std::optional<ColumnsDescription>> next() override
    {
        /// For default mode check cached columns for currently read keys on first iteration.
        if (first && storage_settings.schema_inference_mode == SchemaInferenceMode::DEFAULT)
        {
            if (auto cached_columns = tryGetColumnsFromCache(read_keys.begin(), read_keys.end()))
                return {nullptr, cached_columns};
        }

        current_path_with_metadata = file_iterator->next();
        if (current_path_with_metadata.relative_path.empty())
        {
            if (first)
            {
                throw Exception(
                    ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                    "Cannot extract table structure from {} format file, "
                    "because there are no files with provided path. "
                    "You must specify table structure manually",
                    configuration->format);
            }
            return {nullptr, std::nullopt};
        }

        first = false;

        /// File iterator could get new keys after new iteration,
        /// check them in schema cache if schema inference mode is default.
        if (getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::DEFAULT
            && read_keys.size() > prev_read_keys_size)
        {
            auto columns_from_cache = tryGetColumnsFromCache(read_keys.begin() + prev_read_keys_size, read_keys.end());
            prev_read_keys_size = read_keys.size();
            if (columns_from_cache)
                return {nullptr, columns_from_cache};
        }
        else if (getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::UNION)
        {
            RelativePathsWithMetadata paths = {current_path_with_metadata};
            if (auto columns_from_cache = tryGetColumnsFromCache(paths.begin(), paths.end()))
                return {nullptr, columns_from_cache};
        }

        first = false;

        std::unique_ptr<ReadBuffer> read_buffer = object_storage->readObject(
            StoredObject(current_path_with_metadata.relative_path),
            getContext()->getReadSettings(),
            {},
            current_path_with_metadata.metadata.size_bytes);

        read_buffer = wrapReadBufferWithCompressionMethod(
            std::move(read_buffer),
            chooseCompressionMethod(current_path_with_metadata.relative_path, configuration->compression_method),
            static_cast<int>(getContext()->getSettingsRef().zstd_window_log_max));

        return {std::move(read_buffer), std::nullopt};
    }

    void setNumRowsToLastFile(size_t num_rows) override
    {
        if (storage_settings.schema_inference_use_cache)
        {
            Storage::getSchemaCache(getContext()).addNumRows(
                getKeyForSchemaCache(current_path_with_metadata.relative_path), num_rows);
        }
    }

    void setSchemaToLastFile(const ColumnsDescription & columns) override
    {
        if (storage_settings.schema_inference_use_cache
            && storage_settings.schema_inference_mode == SchemaInferenceMode::UNION)
        {
            Storage::getSchemaCache(getContext()).addColumns(
                getKeyForSchemaCache(current_path_with_metadata.relative_path), columns);
        }
    }

    void setResultingSchema(const ColumnsDescription & columns) override
    {
        if (storage_settings.schema_inference_use_cache
            && storage_settings.schema_inference_mode == SchemaInferenceMode::DEFAULT)
        {
            Storage::getSchemaCache(getContext()).addManyColumns(getPathsForSchemaCache(), columns);
        }
    }

    String getLastFileName() const override { return current_path_with_metadata.relative_path; }

private:
    SchemaCache::Key getKeyForSchemaCache(const String & path) const
    {
        auto source = fs::path(configuration->getDataSourceDescription()) / path;
        return DB::getKeyForSchemaCache(source, configuration->format, format_settings, getContext());
    }

    SchemaCache::Keys getPathsForSchemaCache() const
    {
        Strings sources;
        sources.reserve(read_keys.size());
        std::transform(
            read_keys.begin(), read_keys.end(),
            std::back_inserter(sources),
            [&](const auto & elem)
            {
                return fs::path(configuration->getDataSourceDescription()) / elem.relative_path;
            });
        return DB::getKeysForSchemaCache(sources, configuration->format, format_settings, getContext());
    }

    std::optional<ColumnsDescription> tryGetColumnsFromCache(
        const RelativePathsWithMetadata::iterator & begin,
        const RelativePathsWithMetadata::iterator & end)
    {
        if (!storage_settings.schema_inference_use_cache)
            return std::nullopt;

        auto & schema_cache = Storage::getSchemaCache(getContext());
        for (auto it = begin; it < end; ++it)
        {
            auto get_last_mod_time = [&] -> std::optional<time_t>
            {
                if (it->metadata.last_modified)
                    return it->metadata.last_modified->epochMicroseconds();
                else
                {
                    it->metadata = object_storage->getObjectMetadata(it->relative_path);
                    return it->metadata.last_modified->epochMicroseconds();
                }
            };

            auto cache_key = getKeyForSchemaCache(it->relative_path);
            auto columns = schema_cache.tryGetColumns(cache_key, get_last_mod_time);
            if (columns)
                return columns;
        }

        return std::nullopt;
    }

    ObjectStoragePtr object_storage;
    const Storage::ConfigurationPtr configuration;
    const FileIterator file_iterator;
    const std::optional<FormatSettings> & format_settings;
    const StorageObjectStorageSettings storage_settings;
    RelativePathsWithMetadata & read_keys;

    size_t prev_read_keys_size;
    RelativePathWithMetadata current_path_with_metadata;
    bool first = true;
};
}

template <typename StorageSettings>
ColumnsDescription StorageObjectStorage<StorageSettings>::getTableStructureFromData(
    ObjectStoragePtr object_storage,
    const ConfigurationPtr & configuration,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context)
{
    using Source = StorageObjectStorageSource<StorageSettings>;

    RelativePathsWithMetadata read_keys;
    auto file_iterator = Source::createFileIterator(
        configuration, object_storage, /* distributed_processing */false,
        context, /* predicate */{}, /* virtual_columns */{}, &read_keys);

    ReadBufferIterator<StorageSettings> read_buffer_iterator(
        object_storage, configuration, file_iterator,
        format_settings, read_keys, context);

    return readSchemaFromFormat(
        configuration->format, format_settings,
        read_buffer_iterator, configuration->withGlobs(), context);
}

template <typename StorageSettings>
SchemaCache & StorageObjectStorage<StorageSettings>::getSchemaCache(const ContextPtr & context)
{
    static SchemaCache schema_cache(
        context->getConfigRef().getUInt(
            StorageSettings::SCHEMA_CACHE_MAX_ELEMENTS_CONFIG_SETTING,
            DEFAULT_SCHEMA_CACHE_ELEMENTS));
    return schema_cache;
}

template class StorageObjectStorage<S3StorageSettings>;
template class StorageObjectStorage<AzureStorageSettings>;

template class StorageObjectStorageSource<S3StorageSettings>;
template class StorageObjectStorageSource<AzureStorageSettings>;

}
