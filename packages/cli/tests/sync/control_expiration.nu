use ../lib/sync_control.nu

# Heartbeat retries do not renew attempts, and expired node requests cannot recreate them.
sync_control test expiration
