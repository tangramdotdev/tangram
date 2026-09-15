use ../lib/sync_control.nu

# Heartbeat retries do not renew leases, and expired node requests cannot recreate them.
sync_control test expiration
