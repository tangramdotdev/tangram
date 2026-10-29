use ../../lib/sync_service.nu

# A receiver requests bytes when local reuse would await another incoming sync.
sync_service test receiving_initial_authorization
