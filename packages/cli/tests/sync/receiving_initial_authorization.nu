use ../lib/sync_control.nu

# A receiver requests bytes when local reuse would await another incoming sync.
sync_control test receiving_initial_authorization
