use ../lib/sync_control.nu

# Waiting on a failed sync control request returns its error.

sync_control test failed_wait
