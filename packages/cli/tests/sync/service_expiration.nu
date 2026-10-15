use ../lib/sync_service.nu

# Heartbeat retries do not renew attempts, and expired node requests cannot recreate them.
sync_service test expiration
