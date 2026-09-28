use ../lib/sync_control.nu

# A pending sync request completes when its source becomes available.

sync_control test pending_late_source
