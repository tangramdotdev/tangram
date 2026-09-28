use ../lib/sync_control.nu

# A pending sync request reports an error when its required object is missing.

sync_control test pending_missing
