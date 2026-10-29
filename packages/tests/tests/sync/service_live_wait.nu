use ../../util/sync_service.nu

# A live sync service request remains available until its work completes.

sync_service test live_wait
