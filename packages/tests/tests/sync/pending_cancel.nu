use ../../util/sync_service.nu

# Canceling a pending sync request removes its pending transfer.

sync_service test pending_cancel
