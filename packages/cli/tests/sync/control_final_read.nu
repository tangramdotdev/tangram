use ../lib/sync_control.nu

# A terminal control failure still permits a final read of a locally stored object.
sync_control test final_read
