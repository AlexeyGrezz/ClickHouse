#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <TableFunctions/TableFunctionAzureBlobStorageCluster.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Storages/StorageObjectStorage.h>

#include "registerTableFunctions.h"


namespace DB
{

template <typename StorageSettings>
StoragePtr TableFunctionAzureBlobStorageCluster<StorageSettings>::executeImpl(
    const ASTPtr & /*function*/, ContextPtr context,
    const std::string & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    using Base = TableFunctionAzureBlobStorage<StorageSettings>;
    StoragePtr storage;
    ColumnsDescription columns;
    bool structure_argument_was_provided = Base::configuration->structure != "auto";

    if (structure_argument_was_provided)
    {
        columns = parseColumnsListFromString(TableFunctionAzureBlobStorage<StorageSettings>::configuration->structure, context);
    }
    else if (!Base::structure_hint.empty())
    {
        columns = Base::structure_hint;
    }

    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        /// On worker node this filename won't contains globs
        storage = std::make_shared<StorageObjectStorage<StorageSettings>>(
            Base::configuration,
            Base::configuration->createOrUpdateObjectStorage(context, !is_insert_query),
            "AzureBlobStorage",
            context,
            StorageID(Base::getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{},
            /* comment */String{},
            /* format_settings */std::nullopt, /// No format_settings
            /* distributed_processing */ true,
            /*partition_by_=*/nullptr);
    }
    else
    {
        storage = std::make_shared<StorageAzureBlobCluster<StorageSettings>>(
            ITableFunctionCluster<Base>::cluster_name,
            Base::configuration,
            Base::configuration->createOrUpdateObjectStorage(context, !is_insert_query),
            "AzureBlobStorageCluster",
            StorageID(Base::getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{},
            context,
            structure_argument_was_provided);
    }

    storage->startup();
    return storage;
}


void registerTableFunctionAzureBlobStorageCluster(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionAzureBlobStorageCluster<AzureStorageSettings>>(
    {
        .documentation = {
            .description=R"(The table function can be used to read the data stored on Azure Blob Storage in parallel for many nodes in a specified cluster.)",
            .examples{{"azureBlobStorageCluster", "SELECT * FROM  azureBlobStorageCluster(cluster, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])", ""}}},
            .allow_readonly = false
        }
    );
}

template class TableFunctionAzureBlobStorageCluster<AzureStorageSettings>;

}

#endif
