use ../lib/sync_control.nu

# Authorization asks syncs for permissions independently of byte availability.
sync_control test authorization_storage
