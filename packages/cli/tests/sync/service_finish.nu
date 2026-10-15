use ../lib/sync_service.nu

# Finished and cancelled transfers retain their responses until acknowledged or their attempts expire.
sync_service test finish
