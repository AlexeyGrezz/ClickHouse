#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionS3.h>
#include <TableFunctions/TableFunctionAzureBlobStorage.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Access/Common/AccessFlags.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/StorageObjectStorage.h>
#include <Storages/StorageAzureBlobConfiguration.h>
#include <Storages/StorageURL.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Formats/FormatFactory.h>
#include "registerTableFunctions.h"
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <boost/algorithm/string.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

static void initializeConfiguration(
    StorageObjectStorageConfiguration & configuration,
    ASTs & engine_args,
    ContextPtr local_context,
    bool with_table_structure)
{
    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, local_context))
        configuration.fromNamedCollection(*named_collection);
    else
        configuration.fromAST(engine_args, local_context, with_table_structure);
}

template <typename StorageSettings>
void TableFunctionAzureBlobStorage<StorageSettings>::parseArgumentsImpl(ASTs & engine_args, const ContextPtr & local_context)
{
    configuration = std::make_shared<StorageAzureBlobConfiguration>();
    initializeConfiguration(*configuration, engine_args, local_context, true);
}

template <typename StorageSettings>
void TableFunctionAzureBlobStorage<StorageSettings>::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Clone ast function, because we can modify its arguments like removing headers.
    auto ast_copy = ast_function->clone();
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

    auto & args = args_func.at(0)->children;

    parseArgumentsImpl(args, context);
}

template <typename StorageSettings>
void TableFunctionAzureBlobStorage<StorageSettings>::addColumnsStructureToArguments(
    ASTs & args,
    const String & structure,
    const ContextPtr & context)
{
    if (tryGetNamedCollectionWithOverrides(args, context))
    {
        /// In case of named collection, just add key-value pair "structure='...'"
        /// at the end of arguments to override existed structure.
        ASTs equal_func_args = {std::make_shared<ASTIdentifier>("structure"), std::make_shared<ASTLiteral>(structure)};
        auto equal_func = makeASTFunction("equals", std::move(equal_func_args));
        args.push_back(equal_func);
    }
    else
    {
        if (args.size() < 3 || args.size() > 8)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage Azure requires 3 to 7 arguments: "
                            "AzureBlobStorage(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])");

        auto structure_literal = std::make_shared<ASTLiteral>(structure);

        auto is_format_arg
            = [](const std::string & s) -> bool { return s == "auto" || FormatFactory::instance().getAllFormats().contains(s); };


        if (args.size() == 3)
        {
            /// Add format=auto & compression=auto before structure argument.
            args.push_back(std::make_shared<ASTLiteral>("auto"));
            args.push_back(std::make_shared<ASTLiteral>("auto"));
            args.push_back(structure_literal);
        }
        else if (args.size() == 4)
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/account_name/structure");
            if (is_format_arg(fourth_arg))
            {
                /// Add compression=auto before structure argument.
                args.push_back(std::make_shared<ASTLiteral>("auto"));
                args.push_back(structure_literal);
            }
            else
            {
                args.back() = structure_literal;
            }
        }
        else if (args.size() == 5)
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/account_name");
            if (!is_format_arg(fourth_arg))
            {
                /// Add format=auto & compression=auto before structure argument.
                args.push_back(std::make_shared<ASTLiteral>("auto"));
                args.push_back(std::make_shared<ASTLiteral>("auto"));
            }
            args.push_back(structure_literal);
        }
        else if (args.size() == 6)
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/account_name");
            if (!is_format_arg(fourth_arg))
            {
                /// Add compression=auto before structure argument.
                args.push_back(std::make_shared<ASTLiteral>("auto"));
                args.push_back(structure_literal);
            }
            else
            {
                args.back() = structure_literal;
            }
        }
        else if (args.size() == 7)
        {
            args.push_back(structure_literal);
        }
        else if (args.size() == 8)
        {
            args.back() = structure_literal;
        }
    }
}

template <typename StorageSettings>
ColumnsDescription TableFunctionAzureBlobStorage<StorageSettings>::getActualTableStructure(ContextPtr context, bool is_insert_query) const
{
    if (configuration->structure == "auto")
    {
        context->checkAccess(getSourceAccessType());
        auto storage = getObjectStorage(context, !is_insert_query);
        return StorageObjectStorage<StorageSettings>::getTableStructureFromData(storage, configuration, std::nullopt, context);
    }

    return parseColumnsListFromString(configuration->structure, context);
}

template <typename StorageSettings>
ObjectStoragePtr TableFunctionAzureBlobStorage<StorageSettings>::getObjectStorage(const ContextPtr & context, bool create_readonly) const
{
    if (!object_storage)
        object_storage = configuration->createOrUpdateObjectStorage(context, create_readonly);
    return object_storage;
}

template <typename StorageSettings>
bool TableFunctionAzureBlobStorage<StorageSettings>::supportsReadingSubsetOfColumns(const ContextPtr & context)
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(configuration->format, context);
}

template <typename StorageSettings>
std::unordered_set<String> TableFunctionAzureBlobStorage<StorageSettings>::getVirtualsToCheckBeforeUsingStructureHint() const
{
    auto virtual_column_names = StorageObjectStorage<StorageSettings>::getVirtualColumnNames();
    return {virtual_column_names.begin(), virtual_column_names.end()};
}

template <typename StorageSettings>
StoragePtr TableFunctionAzureBlobStorage<StorageSettings>::executeImpl(
    const ASTPtr & /* ast_function */,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /* cached_columns */,
    bool is_insert_query) const
{
    ColumnsDescription columns;
    if (configuration->structure != "auto")
        columns = parseColumnsListFromString(configuration->structure, context);
    else if (!structure_hint.empty())
        columns = structure_hint;

    StoragePtr storage = std::make_shared<StorageObjectStorage<StorageSettings>>(
        configuration,
        getObjectStorage(context, !is_insert_query),
        "AzureBlobStorage",
        context,
        StorageID(getDatabaseName(), table_name),
        columns,
        ConstraintsDescription{},
        String{},
        /// No format_settings for table function Azure
        std::nullopt,
        /* distributed_processing */ false,
        nullptr);

    storage->startup();

    return storage;
}

void registerTableFunctionAzureBlobStorage(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionAzureBlobStorage<AzureStorageSettings>>(
        {.documentation
         = {.description=R"(The table function can be used to read the data stored on Azure Blob Storage.)",
            .examples{{"azureBlobStorage", "SELECT * FROM  azureBlobStorage(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])", ""}}},
         .allow_readonly = false});
}

template class TableFunctionAzureBlobStorage<AzureStorageSettings>;

}

#endif
