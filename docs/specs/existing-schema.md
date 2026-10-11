# Existing schema storage handles

An optional native backend factory returns handles that do not declare, migrate or repair schema. The policy survives named database selection and native fenced constructors. Unsupported implementations refuse; existing unrestricted callers retain their behavior.

Document validation checks actual persisted declaration presence and compatibility (stored.merge(required) equals stored). Absence is NotFound, malformed or incompatible requests InvalidInput, corrupt metadata Serialization. Native errors retain sources. Restricted ensure refuses; scoped drop_collection remains fenced data deletion.

Internal stream/blob records and data files are allowed data mutations. Native index/base-table preparation is trusted bootstrap. Mongo restricted operations inspect existing native schema rather than create indexes, including reads. External concurrent schema administration is unsupported. Reopened handles require persisted evidence rather than driver names or caller flags.

Every supported native write retains its captured fence and atomic guard check. Mongo GridFS fencing remains unsupported. Driver defaults are Unsupported; no workspace fallback is provided.

## Public API and native limits

StorageBackend::prepare_schema is optional trusted async bootstrap; its default refuses. It prepares internal storage only, never application document declarations. Memory and File require no extra internal schema initialization. SQLite prepares its base tables; Mongo prepares document-validation metadata, stream heads/segments and GridFS files/chunks/indexes. Restricted document writes require the validation collection to exist; scope-registration records remain internal data mutations. Checked batch read dependencies also require persisted declarations. Existing-schema backends refuse this method, even for matching schema.

StorageBackend::existing_schema is a synchronous optional native factory with default Unsupported(ExistingSchema). DocumentStore::validate_collection_spec defaults to Unsupported(CollectionValidation). The native factory does not accept a caller-supplied preparation boolean or change a fence. Classic opening/constructors retain their behavior: the restriction starts when existing_schema is called, so this does not retroactively remove trusted initialization performed before that call.

SQLite named and guard database selection opens existing files without create flags or journal-mode writes and validates the driver's table layout. File named database selection requires an existing directory. Mongo uses fresh declaration and index-catalog reads rather than the classic declaration/preparation caches. Memory preserves the actual shared native database maps.

Restricted Mongo GridFS uploads refuse Unsupported(ExistingSchema): the SDK may issue create_collection before checking existing indexes. Its native fenced writes retain their existing Unsupported(Fencing). Existing blob reads and unfenced deletion use read-only schema checks. No presigned-upload or cross-driver purge authority is introduced.

An administrator changing or deleting schema concurrently is outside this contract. The owner bootstrap and restricted data handles must address the same real native database; inspection is not authority to repair missing schema. Generation/purge and validated-batch APIs are independent publication slices.
