use ../lib/sync_control.nu

# Interrupted transfers retain control until their partial grants have been enqueued.
sync_control test index_handoff
