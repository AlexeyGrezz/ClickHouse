#include "Storages/StorageAzureBlobCluster.h"

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/IStorage.h>
#include <Storages/StorageURL.h>
#include <Storages/StorageDictionary.h>
#include <Storages/extractTableFunctionArgumentsFromSelectQuery.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/Exception.h>
#include <Parsers/queryToString.h>
#include <TableFunctions/TableFunctionAzureBlobStorageCluster.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename StorageSettings>
StorageAzureBlobCluster<StorageSettings>::StorageAzureBlobCluster(
    const String & cluster_name_,
    const StorageObjectStorageConfigurationPtr & configuration_,
    ObjectStoragePtr object_storage_,
    const String & engine_name_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ContextPtr context_,
    bool structure_argument_was_provided_)
    : IStorageCluster(cluster_name_,
                      table_id_,
                      getLogger(fmt::format("{}({})", engine_name_, table_id_.table_name)),
                      structure_argument_was_provided_)
    , engine_name(engine_name_)
    , configuration{configuration_}
    , object_storage(object_storage_)
{
    configuration->check(context_);
    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        /// `format_settings` is set to std::nullopt, because StorageAzureBlobCluster is used only as table function
        auto columns = StorageObjectStorage<StorageSettings>::getTableStructureFromData(
            object_storage, configuration, /*format_settings=*/std::nullopt, context_);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);

    virtual_columns = VirtualColumnUtils::getPathFileAndSizeVirtualsForStorage(
        storage_metadata.getSampleBlock().getNamesAndTypesList());
}

template <typename StorageSettings>
void StorageAzureBlobCluster<StorageSettings>::addColumnsStructureToQuery(
    ASTPtr & query,
    const String & structure,
    const ContextPtr & context)
{
    ASTExpressionList * expression_list = extractTableFunctionArgumentsFromSelectQuery(query);
    if (!expression_list)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Expected SELECT query from table function s3Cluster, got '{}'",
                        queryToString(query));
    }

    TableFunctionAzureBlobStorageCluster<StorageSettings>::addColumnsStructureToArguments(
        expression_list->children, structure, context);
}

template <typename StorageSettings>
RemoteQueryExecutor::Extension StorageAzureBlobCluster<StorageSettings>::getTaskIteratorExtension(
    const ActionsDAG::Node * predicate,
    const ContextPtr & context) const
{
    using Source = StorageObjectStorageSource<StorageSettings>;

    auto iterator = std::make_shared<typename Source::GlobIterator>(
        object_storage, configuration, predicate, virtual_columns, context, nullptr);

    auto callback = std::make_shared<std::function<String()>>(
        [iterator]() mutable -> String{ return iterator->next().relative_path; });
    return RemoteQueryExecutor::Extension{ .task_iterator = std::move(callback) };
}

template class StorageAzureBlobCluster<AzureStorageSettings>;

}

#endif
