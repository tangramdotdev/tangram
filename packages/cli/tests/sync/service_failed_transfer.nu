use ../lib/sync_service.nu

# A failed transfer still answers requests for stored nodes and reports errors for missing nodes.
sync_service test failed_transfer
