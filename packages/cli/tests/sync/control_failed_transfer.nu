use ../lib/sync_control.nu

# A failed transfer still answers requests for stored nodes and reports errors for missing nodes.
sync_control test failed_transfer
