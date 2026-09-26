use ../lib/sync_control.nu

# Delayed and replayed heartbeat responses cannot roll back an attempt or keep a dead peer alive.
sync_control test stale_heartbeats
