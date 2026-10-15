use ../lib/sync_service.nu

# Delayed and replayed heartbeat responses cannot roll back an attempt or keep a dead peer alive.
sync_service test stale_heartbeats
