use ../lib/sync_control.nu

# Finished and cancelled transfers retain their responses until acknowledged or their leases expire.
sync_control test finish
