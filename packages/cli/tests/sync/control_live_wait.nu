use ../lib/sync_control.nu

# A live sync control request remains available until its work completes.

sync_control test live_wait
