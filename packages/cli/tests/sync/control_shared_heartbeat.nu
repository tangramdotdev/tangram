use ../lib/sync_control.nu

# Concurrent and sequential requests within a sync reuse the same heartbeat task.
sync_control test shared_heartbeat
