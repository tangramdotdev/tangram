use ../lib/sync_control.nu

# Local polling continues while a control request is acknowledged but unanswered.
sync_control test final_read
