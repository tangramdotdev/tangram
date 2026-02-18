use ../../test.nu *

let remote = spawn -n remote
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
	tag: { cache_ttl: 100 }
}

# Tag the old version of checkin/a on the remote.
let checkin_old_path = artifact {
	tangram.ts: '
		export default () => "checkin/a/1.0.0";
	'
}
tg -u $remote.url tag checkin/a/1.0.0 $checkin_old_path

# Create a package that depends on checkin/a/^1.
let checkin_package_path = artifact {
	tangram.ts: '
		import a from "checkin/a/^1";
		export default () => tg.run(a);
	'
}

# Check in once to create the lock and warm the remote tag list cache.
tg -u $local.url checkin $checkin_package_path

let checkin_lockfile_path = $checkin_package_path | path join 'tangram.lock'
let checkin_initial_lock = open $checkin_lockfile_path | from json
let checkin_initial_tag = $checkin_initial_lock.nodes.1.dependencies."checkin/a/^1".options.tag
assert ($checkin_initial_tag == "checkin/a/1.0.0") "the initial lock should resolve to checkin/a/1.0.0"

# Tag a new version on the remote.
let checkin_new_path = artifact {
	tangram.ts: '
		export default () => "checkin/a/1.1.0";
	'
}
tg -u $remote.url tag checkin/a/1.1.0 $checkin_new_path

# Run checkin --update with the default TTL. The cached tag list should keep checkin/a/1.0.0.
tg -u $local.url checkin $checkin_package_path --update checkin/a
let checkin_stale_lock = open $checkin_lockfile_path | from json
let checkin_stale_tag = $checkin_stale_lock.nodes.1.dependencies."checkin/a/^1".options.tag
assert ($checkin_stale_tag == "checkin/a/1.0.0") "without a ttl override, checkin should use the cached tag list"

# Run checkin --update with --ttl 0. The fresh tag list should now use checkin/a/1.1.0.
tg -u $local.url checkin $checkin_package_path --update checkin/a --ttl 0
let checkin_fresh_lock = open $checkin_lockfile_path | from json
let checkin_fresh_tag = $checkin_fresh_lock.nodes.1.dependencies."checkin/a/^1".options.tag
assert ($checkin_fresh_tag == "checkin/a/1.1.0") "with ttl 0, checkin should bypass the cache"

# Tag the old version of update/a on the remote.
let update_old_path = artifact {
	tangram.ts: '
		export default () => "update/a/1.0.0";
	'
}
tg -u $remote.url tag update/a/1.0.0 $update_old_path

# Create a package that depends on update/a/^1.
let update_package_path = artifact {
	tangram.ts: '
		import a from "update/a/^1";
		export default () => tg.run(a);
	'
}

# Check in once to create the lock and warm the remote tag list cache.
tg -u $local.url checkin $update_package_path

let update_lockfile_path = $update_package_path | path join 'tangram.lock'
let update_initial_lock = open $update_lockfile_path | from json
let update_initial_tag = $update_initial_lock.nodes.1.dependencies."update/a/^1".options.tag
assert ($update_initial_tag == "update/a/1.0.0") "the initial lock should resolve to update/a/1.0.0"

# Tag a new version on the remote.
let update_new_path = artifact {
	tangram.ts: '
		export default () => "update/a/1.1.0";
	'
}
tg -u $remote.url tag update/a/1.1.0 $update_new_path

# Run update with the default TTL. The cached tag list should keep update/a/1.0.0.
tg -u $local.url update $update_package_path
let update_stale_lock = open $update_lockfile_path | from json
let update_stale_tag = $update_stale_lock.nodes.1.dependencies."update/a/^1".options.tag
assert ($update_stale_tag == "update/a/1.0.0") "without a ttl override, update should use the cached tag list"

# Run update with --ttl 0. The fresh tag list should now use update/a/1.1.0.
tg -u $local.url update $update_package_path --ttl 0
let update_fresh_lock = open $update_lockfile_path | from json
let update_fresh_tag = $update_fresh_lock.nodes.1.dependencies."update/a/^1".options.tag
assert ($update_fresh_tag == "update/a/1.1.0") "with ttl 0, update should bypass the cache"
