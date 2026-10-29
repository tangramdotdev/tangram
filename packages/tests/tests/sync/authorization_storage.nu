use ../../util/sync_service.nu

# Authorization asks syncs for permissions independently of byte availability.
sync_service test authorization_storage
