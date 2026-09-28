use ../lib/sync_control.nu

# Canceling a pending sync request removes its pending transfer.

sync_control test pending_cancel
