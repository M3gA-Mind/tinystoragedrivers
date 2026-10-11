# Existing schema storage handles

An optional native backend factory returns handles that do not declare, migrate or repair schema. The policy survives named database selection and native fenced constructors. Unsupported implementations refuse; existing unrestricted callers retain their behavior.

Document validation checks actual persisted declaration presence and compatibility (stored.merge(required) equals stored). Absence is NotFound, malformed or incompatible requests InvalidInput, corrupt metadata Serialization. Native errors retain sources. Restricted ensure refuses; scoped drop_collection remains fenced data deletion.

Internal stream/blob records and data files are allowed data mutations. Native index/base-table preparation is trusted bootstrap. Mongo restricted operations inspect existing native schema rather than create indexes, including reads. External concurrent schema administration is unsupported. Reopened handles require persisted evidence rather than driver names or caller flags.

Every supported native write retains its captured fence and atomic guard check. Mongo GridFS fencing remains unsupported. Driver defaults are Unsupported; no workspace fallback is provided.
