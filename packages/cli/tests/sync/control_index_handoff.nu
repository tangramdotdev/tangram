use ../lib/sync_control.nu

# Interrupted transfers retain control until permissions for the transferred objects have been enqueued.
sync_control test index_handoff
