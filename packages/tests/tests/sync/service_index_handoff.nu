use ../../lib/sync_service.nu

# Interrupted transfers keep the service alive until permissions for the transferred objects have been enqueued.
sync_service test index_handoff
