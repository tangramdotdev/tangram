use ../../util/sync_service.nu

# The sync service falls back to a local candidate when no remote candidate is available.

sync_service test candidate_local_fallback
