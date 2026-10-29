use ../../lib/sync_service.nu

# Concurrent and sequential requests within a sync reuse the same heartbeat task.
sync_service test shared_heartbeat
