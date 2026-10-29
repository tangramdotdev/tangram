use ../../lib/sync_service.nu

# Waiting on a failed sync service request returns its error.

sync_service test failed_wait
