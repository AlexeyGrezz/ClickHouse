#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#include <Interpreters/Cluster.h>
#include <Storages/IStorageCluster.h>
#include <Storages/StorageObjectStorage.h>

namespace DB
{

class Context;

template <typename StorageSettings>
class StorageAzureBlobCluster : public IStorageCluster
{
public:
    StorageAzureBlobCluster(
        const String & cluster_name_,
        const StorageObjectStorageConfigurationPtr & configuration_,
        ObjectStoragePtr object_storage_,
        const String & engine_name_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        ContextPtr context_,
        bool structure_argument_was_provided_);

    std::string getName() const override { return engine_name; }

    NamesAndTypesList getVirtuals() const override { return virtual_columns; }

    RemoteQueryExecutor::Extension
    getTaskIteratorExtension(
        const ActionsDAG::Node * predicate,
        const ContextPtr & context) const override;

    bool supportsSubcolumns() const override { return true; }

    bool supportsTrivialCountOptimization() const override { return true; }

private:
    void updateBeforeRead(const ContextPtr & /*context*/) override {}

    void addColumnsStructureToQuery(
        ASTPtr & query,
        const String & structure,
        const ContextPtr & context) override;

    const String & engine_name;
    const StorageObjectStorageConfigurationPtr configuration;
    const ObjectStoragePtr object_storage;
    NamesAndTypesList virtual_columns;
};


}

#endif
