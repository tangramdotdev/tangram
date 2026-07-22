#ifndef TANGRAM_PROVIDER_H
#define TANGRAM_PROVIDER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

// The C ABI for the Tangram VFS provider. Every function returns an int32_t: zero on success, a small positive value for a call-level failure, or -errno for a filesystem error. Rust owns every returned TgResponse and TgResponseBatch; the caller frees them with tg_response_free and tg_response_batch_free.

typedef void *TgVfsProvider;
typedef void *TgResponse;
typedef void *TgResponseBatch;

// Invoked with the responses from an asynchronous operation on a runtime worker thread. It takes ownership of the batch.
typedef void (*TgBatchCallback)(void *user_data, TgResponseBatch batch);

typedef enum {
	TgRequestKindClose = 0,
	TgRequestKindForget = 1,
	TgRequestKindGetAttr = 2,
	TgRequestKindGetXattr = 3,
	TgRequestKindListXattrs = 4,
	TgRequestKindLookup = 5,
	TgRequestKindLookupParent = 6,
	TgRequestKindOpen = 7,
	TgRequestKindOpenDir = 8,
	TgRequestKindRead = 9,
	TgRequestKindReadDir = 10,
	TgRequestKindReadDirPlus = 11,
	TgRequestKindReadLink = 12,
	TgRequestKindRemember = 13,
} TgRequestKind;

typedef enum {
	TgResponseKindGetAttr = 0,
	TgResponseKindGetXattr = 1,
	TgResponseKindListXattrs = 2,
	TgResponseKindLookup = 3,
	TgResponseKindLookupParent = 4,
	TgResponseKindOpen = 5,
	TgResponseKindOpenDir = 6,
	TgResponseKindRead = 7,
	TgResponseKindReadDir = 8,
	TgResponseKindReadDirPlus = 9,
	TgResponseKindReadLink = 10,
	TgResponseKindUnit = 11,
} TgResponseKind;

typedef enum {
	TgEntryKindDirectory = 1,
	TgEntryKindFile = 0,
	TgEntryKindSymlink = 2,
} TgEntryKind;

typedef enum {
	TgAttrsKindDirectory = 1,
	TgAttrsKindFile = 0,
	TgAttrsKindSymlink = 2,
} TgAttrsKind;

typedef struct {
	uint32_t nanos;
	uint64_t secs;
} TgTimestamp;

typedef struct {
	TgTimestamp atime;
	TgTimestamp ctime;
	// Valid only when the kind is File.
	bool executable;
	uint32_t gid;
	TgAttrsKind kind;
	TgTimestamp mtime;
	uint64_t size;
	uint32_t uid;
} TgAttrs;

typedef struct {
	uint64_t handle;
	uint64_t id;
	TgRequestKind kind;
	uint64_t length;
	// Valid for GetXattr and Lookup.
	const uint8_t *name;
	size_t name_len;
	uint64_t nlookup;
	uint64_t offset;
	uint64_t position;
} TgRequest;

typedef struct {
	uint64_t id;
	TgEntryKind kind;
	// Borrowed from the owning response.
	const uint8_t *name;
	size_t name_len;
} TgDirEntry;

typedef struct {
	TgAttrs attrs;
	uint64_t id;
	// Borrowed from the owning response.
	const uint8_t *name;
	size_t name_len;
} TgDirEntryPlus;

// The configuration for a provider. A zero field selects the default.
typedef struct {
	// The server's data directory, whose object store and cache directory the fast path reads directly. NULL disables the fast path.
	const char *data_directory;
	// The sweep interval for expired cache-only nodes.
	uint64_t node_eviction_interval_secs;
	// The retention of an unreferenced cache-only node after its last access.
	uint64_t node_ttl_secs;
	// The map size with which to open the object store. It must be at least the server's.
	uint64_t object_store_map_size;
	// The object store's path within the data directory. NULL or empty selects the default.
	const char *object_store_path;
	// The prefix for the object store's POSIX lock semaphores. It must match the server's. NULL or empty selects the default hash-derived names.
	const char *object_store_posix_sem_prefix;
} TgConfig;

// Manage provider lifecycle; the optional config pointer selects the provider configuration, and NULL selects the defaults.
int32_t tg_provider_new(const char *uri, const TgConfig *config, TgVfsProvider *out_provider);
int32_t tg_provider_drop(TgVfsProvider provider);

// Handle batches.
int32_t tg_provider_handle_batch_sync(TgVfsProvider provider, const TgRequest *requests, size_t requests_len, TgResponseBatch *out_responses);
int32_t tg_provider_submit_batch_async(TgVfsProvider provider, const TgRequest *requests, size_t requests_len, TgBatchCallback callback, void *user_data);

// Handle synchronous operations.
int32_t tg_provider_close_sync(TgVfsProvider provider, uint64_t handle);
int32_t tg_provider_remember_sync(TgVfsProvider provider, uint64_t id);
int32_t tg_provider_forget_sync(TgVfsProvider provider, uint64_t id, uint64_t nlookup);
int32_t tg_provider_getattr_sync(TgVfsProvider provider, uint64_t id, TgResponse *out_response);
int32_t tg_provider_getxattr_sync(TgVfsProvider provider, uint64_t id, const uint8_t *name, size_t name_len, TgResponse *out_response);
int32_t tg_provider_listxattrs_sync(TgVfsProvider provider, uint64_t id, TgResponse *out_response);
int32_t tg_provider_lookup_sync(TgVfsProvider provider, uint64_t id, const uint8_t *name, size_t name_len, TgResponse *out_response);
int32_t tg_provider_lookup_parent_sync(TgVfsProvider provider, uint64_t id, TgResponse *out_response);
int32_t tg_provider_open_sync(TgVfsProvider provider, uint64_t id, TgResponse *out_response);
int32_t tg_provider_opendir_sync(TgVfsProvider provider, uint64_t id, TgResponse *out_response);
int32_t tg_provider_read_sync(TgVfsProvider provider, uint64_t handle, uint64_t position, uint64_t length, TgResponse *out_response);
int32_t tg_provider_readdir_sync(TgVfsProvider provider, uint64_t handle, uint64_t offset, uint64_t length, TgResponse *out_response);
int32_t tg_provider_readdirplus_sync(TgVfsProvider provider, uint64_t handle, uint64_t offset, uint64_t length, TgResponse *out_response);
int32_t tg_provider_readlink_sync(TgVfsProvider provider, uint64_t id, TgResponse *out_response);

// Handle asynchronous operations.
int32_t tg_provider_close_async(TgVfsProvider provider, uint64_t handle, TgBatchCallback callback, void *user_data);
int32_t tg_provider_getattr_async(TgVfsProvider provider, uint64_t id, TgBatchCallback callback, void *user_data);
int32_t tg_provider_getxattr_async(TgVfsProvider provider, uint64_t id, const uint8_t *name, size_t name_len, TgBatchCallback callback, void *user_data);
int32_t tg_provider_listxattrs_async(TgVfsProvider provider, uint64_t id, TgBatchCallback callback, void *user_data);
int32_t tg_provider_lookup_async(TgVfsProvider provider, uint64_t id, const uint8_t *name, size_t name_len, TgBatchCallback callback, void *user_data);
int32_t tg_provider_lookup_parent_async(TgVfsProvider provider, uint64_t id, TgBatchCallback callback, void *user_data);
int32_t tg_provider_open_async(TgVfsProvider provider, uint64_t id, TgBatchCallback callback, void *user_data);
int32_t tg_provider_opendir_async(TgVfsProvider provider, uint64_t id, TgBatchCallback callback, void *user_data);
int32_t tg_provider_read_async(TgVfsProvider provider, uint64_t handle, uint64_t position, uint64_t length, TgBatchCallback callback, void *user_data);
int32_t tg_provider_readdir_async(TgVfsProvider provider, uint64_t handle, uint64_t offset, uint64_t length, TgBatchCallback callback, void *user_data);
int32_t tg_provider_readdirplus_async(TgVfsProvider provider, uint64_t handle, uint64_t offset, uint64_t length, TgBatchCallback callback, void *user_data);
int32_t tg_provider_readlink_async(TgVfsProvider provider, uint64_t id, TgBatchCallback callback, void *user_data);

// Access responses.
int32_t tg_response_kind(TgResponse response, TgResponseKind *out_kind);
int32_t tg_response_attrs(TgResponse response, TgAttrs *out_attrs);
int32_t tg_response_lookup(TgResponse response, bool *out_present, uint64_t *out_id);
int32_t tg_response_lookup_parent(TgResponse response, uint64_t *out_id);
int32_t tg_response_open(TgResponse response, uint64_t *out_handle, int32_t *out_backing_fd);
int32_t tg_response_opendir(TgResponse response, uint64_t *out_handle);
int32_t tg_response_bytes(TgResponse response, const uint8_t **out_ptr, size_t *out_len);
int32_t tg_response_xattr(TgResponse response, bool *out_present, const uint8_t **out_ptr, size_t *out_len);
int32_t tg_response_xattr_names_len(TgResponse response, size_t *out_len);
int32_t tg_response_xattr_names_get(TgResponse response, size_t index, const uint8_t **out_ptr, size_t *out_len);
int32_t tg_response_readdir_len(TgResponse response, size_t *out_len);
int32_t tg_response_readdir_get(TgResponse response, size_t index, TgDirEntry *out_entry);
int32_t tg_response_readdirplus_len(TgResponse response, size_t *out_len);
int32_t tg_response_readdirplus_get(TgResponse response, size_t index, TgDirEntryPlus *out_entry);

// Access batches.
int32_t tg_response_batch_len(TgResponseBatch batch, size_t *out_len);
int32_t tg_response_batch_get(TgResponseBatch batch, size_t index, TgResponse *out_response);

// Free responses and batches.
int32_t tg_response_free(TgResponse response);
int32_t tg_response_batch_free(TgResponseBatch batch);

#endif // TANGRAM_PROVIDER_H
