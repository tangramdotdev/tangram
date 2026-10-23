# Shared helpers for CLI tests. This module must not import the test runner.

use ../../../../scripts/macos/identifiers.nu

export use std/assert

const repository_path = path self '../../../../'
const supervisors_name = 'supervisors'
export const vfs_cleanup_marker_name = '.tangram_test_vfs_cleanup'

def acquire_database_instance [pool_path: string] {
	let postgres_schema_path = $repository_path | path join packages/server/src/database/postgres.sql
	let scylla_schema_path = $repository_path | path join packages/cache/scylla/schema.cql
	let result = (^bash -c (database_pool_acquire) _ $pool_path $postgres_schema_path $scylla_schema_path | complete)
	if $result.exit_code != 0 {
		error make {
			msg: 'failed to acquire a database pool instance'
			help: ($result.stderr | str trim)
		}
	}
	let instance = $result.stdout | str trim
	if ($instance | is-empty) {
		error make { msg: 'the acquired database pool instance is empty' }
	}

	$instance
}

export def database_pool_acquire [] {
	r#'
set -euo pipefail

pool_path=$1
postgres_schema_path=$2
scylla_schema_path=$3
operation_timeout=30
mkdir -p -- "$pool_path"

owner_pid=$PPID
owner_start=$(ps -o lstart= -p "$owner_pid" 2>/dev/null | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' || true)
if [ -z "$owner_start" ]; then
	echo "failed to identify the database lease owner process $owner_pid" >&2
	exit 1
fi

try_lease_existing() {
	for slot_path in "$pool_path"/pool[0-9]*; do
		if [ ! -d "$slot_path" ]; then
			continue
		fi
		temporary_lease_path=$(mktemp "$slot_path/.lease.XXXXXX")
		printf '%s\n%s\n' "$owner_pid" "$owner_start" > "$temporary_lease_path"
		if ln "$temporary_lease_path" "$slot_path/lease" 2>/dev/null; then
			rm -f -- "$temporary_lease_path"
			basename "$slot_path"
			return 0
		fi
		rm -f -- "$temporary_lease_path"
	done
	return 1
}

if instance=$(try_lease_existing); then
	echo "$instance"
	exit 0
fi

# Schema changes are coordinated globally by Scylla. Serialize only on-demand provisioning so a cold expansion cannot overwhelm it; the database operations themselves remain unrestricted.
if [ "${TANGRAM_TEST_DATABASE_PROVISION_CONCURRENT:-}" != 1 ]; then
	exec 9>"$pool_path/provision.lock"
	if [ "$(uname -s)" = Darwin ]; then
		lockf 9
	else
		flock 9
	fi
	if instance=$(try_lease_existing); then
		echo "$instance"
		exit 0
	fi
fi

index=0
while true; do
	instance=$(printf 'pool%04d' "$index")
	slot_path="$pool_path/$instance"
	temporary_slot_path="$pool_path/.provisioning.$instance"
	if [ ! -e "$slot_path" ] && mkdir "$temporary_slot_path" 2>/dev/null; then
		if [ ! -e "$slot_path" ]; then
			break
		fi
		rmdir "$temporary_slot_path"
	fi
	index=$((index + 1))
done
postgres_created=false
scylla_created=false

cleanup_provision() {
	status=$?
	trap - EXIT
	rm -rf -- "$temporary_slot_path"
	if $scylla_created; then
		timeout --kill-after=2s 10 tangram_scylla_client 127.0.0.1 9042 -e "drop keyspace if exists \"cache_$instance\";" >/dev/null 2>&1 || true
	fi
	if $postgres_created; then
		timeout --kill-after=2s 10 dropdb --host=127.0.0.1 --username=postgres --if-exists --force "database_$instance" >/dev/null 2>&1 || true
	fi
	exit "$status"
}
trap cleanup_provision EXIT

printf '%s\n%s\n' "$owner_pid" "$owner_start" > "$temporary_slot_path/lease"
run_phase() {
	phase=$1
	shift
	"$@" || {
		status=$?
		echo "$phase failed with exit code $status" >&2
		return "$status"
	}
}

run_phase "creating PostgreSQL database $instance" timeout --kill-after=2s "$operation_timeout" createdb --host=127.0.0.1 --username=postgres "database_$instance"
postgres_created=true
run_phase "initializing PostgreSQL database $instance" timeout --kill-after=2s "$operation_timeout" psql --host=127.0.0.1 --username=postgres --dbname="database_$instance" --set=ON_ERROR_STOP=1 --single-transaction --file="$postgres_schema_path" >/dev/null

run_phase "creating ScyllaDB keyspace $instance" timeout --kill-after=2s "$operation_timeout" tangram_scylla_client 127.0.0.1 9042 -e "create keyspace \"cache_$instance\" with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 };" >/dev/null
scylla_created=true
run_phase "initializing ScyllaDB keyspace $instance" timeout --kill-after=2s "$operation_timeout" tangram_scylla_client 127.0.0.1 9042 -k "cache_$instance" -f "$scylla_schema_path" >/dev/null

mv -- "$temporary_slot_path" "$slot_path"
trap - EXIT
echo "$instance"
'#
}

export def foundationdb_cluster_description [] {
	if $nu.os-info.name == 'linux' { 'local:local@127.0.0.1:4500' } else { 'docker:docker@127.0.0.1:4500' }
}

export def container_cgroup_parent [controllers: list<string>] {
	let current = ^awk -F: '$1 == "0" { print $3 }' /proc/self/cgroup | str trim
	let current = $'/sys/fs/cgroup($current)'
	for parent in [$current ($current | path dirname)] {
		let path = $parent | path join cgroup.subtree_control
		if not ($path | path exists) { continue }
		let enabled = open --raw $path | str trim | split row ' '
		if ($controllers | all { $in in $enabled }) {
			let writable = ^sh -c 'test -w "$1" && test -w "$1/cgroup.procs"' _ $parent | complete
			if $writable.exit_code == 0 { return $parent }
		}
	}
	skip_test $'this test requires a delegated cgroup parent with ($controllers | str join ", ") enabled'
}

export def container_id_maps [] {
	if (id -u | into int) == 0 {
		return {
			gid_map: { count: 65_536, host: 100_000 },
			uid_map: { count: 65_536, host: 100_000 },
		}
	}
	let user = id -un | str trim
	let uid_ranges = open --raw /etc/subuid | lines | parse '{name}:{host}:{count}' | where name == $user
	let gid_ranges = open --raw /etc/subgid | lines | parse '{name}:{host}:{count}' | where name == $user
	if ($uid_ranges | is-empty) or ($gid_ranges | is-empty) {
		skip_test 'this test requires subordinate uid and gid ranges'
	}
	let uid_helpers = which newuidmap
	let gid_helpers = which newgidmap
	if ($uid_helpers | is-empty) or ($gid_helpers | is-empty) {
		skip_test 'this test requires newuidmap and newgidmap'
	}
	let uid_range = $uid_ranges | first
	let gid_range = $gid_ranges | first

	{
		gid_map: {
			count: ($gid_range.count | into int),
			helper: ($gid_helpers | first | get path),
			host: ($gid_range.host | into int),
		},
		uid_map: {
			count: ($uid_range.count | into int),
			helper: ($uid_helpers | first | get path),
			host: ($uid_range.host | into int),
		},
	}
}

export def artifact [artifact] {
	def inner [artifact: any, path: string] {
		let artifact = if ($artifact | describe) == 'string' {
			{ kind: 'file', contents: (doc $artifact), executable: false }
		} else if (($artifact | describe) | str starts-with 'record') {
			if $artifact.kind? != null {
				$artifact
			} else {
				{ kind: 'directory', entries: $artifact }
			}
		} else {
			$artifact
		}
		match $artifact.kind {
			'directory' => {
				try { mkdir $path }
				for entry in ($artifact.entries | transpose name value) {
					inner $entry.value ($path | path join $entry.name)
				}
			}
			'file' => {
				$artifact.contents | save $path
				if $artifact.executable {
					chmod +x $path
				}
				for pair in (($artifact.xattrs? | default {}) | transpose key value) {
					xattr_write $pair.key $pair.value $path
				}
			}
			'symlink' => {
				ln -s $artifact.path $path
			}
		}
	}
	let path = mktemp -d | path join 'artifact'
	inner $artifact $path
	$path
}

export def directory [entries: record] {
	{ kind: 'directory', entries: $entries }
}

export def file [
	--executable (-x)
	--xattrs: record
	contents?: string
] {
	{ kind: 'file', contents: (doc ($contents | default '')), executable: $executable, xattrs: $xattrs }
}

export def symlink [path: string] {
	{ kind: 'symlink', path: $path }
}

export def doc [string: string] {
	# Split the lines.
	mut lines = $string | split row "\n"

	# Remove the first line if it is empty or contains only whitespace.
	if ($lines | length) > 0 and (($lines | first | str trim | str length) == 0) {
		$lines = $lines | skip 1
	}
	if ($lines | length) > 0 {
		let last = $lines | last
		if ($last | str trim | str length) == 0 {
			$lines = $lines | drop
		}
	}

	# Get the common leading whitespace prefix. Filter out lines that are empty or contain only whitespace.
	let non_whitespace_lines = $lines | where { |line|
		let trimmed = $line | str trim
		($trimmed | str length) > 0
	}

	# Extract leading whitespace from each non-empty line.
	let leading_whitespace = $non_whitespace_lines | each { |line|
		$line | split chars | take while { |char| $char == "\t" or $char == " " } | str join
	}

	# Find the common prefix of all leading whitespace strings.
	let common_prefix = if ($leading_whitespace | length) > 0 {
		$leading_whitespace | reduce { |it, acc|
			let acc_len = $acc | str length
			let it_len = $it | str length
			let min_len = if $acc_len < $it_len { $acc_len } else { $it_len }
			mut prefix_len = 0
			let acc_chars = $acc | split chars
			let it_chars = $it | split chars
			for i in 0..<$min_len {
				if ($acc_chars | get $i) == ($it_chars | get $i) {
					$prefix_len = $prefix_len + 1
				} else {
					break
				}
			}
			$acc | str substring ..<$prefix_len
		}
	} else {
		""
	}

	let prefix_len = $common_prefix | str length

	# Remove the common prefix from each line and combine them with newlines.
	let result = $lines
		| each { |line|
			if ($line | str length) >= $prefix_len {
				$line | str substring $prefix_len..
			} else {
				$line
			}
		}
		| str join "\n"

	$result
}

# Extract the identity when a test compares IDs or calls an endpoint that accepts only an ID.
export def "referent node" [value?: string] {
	let input = $in
	let value = if $value == null { $input } else { $value }
	$value | str trim | split row '?' | first
}

# Extract the authorization tokens for a location from a printed referent.
export def "referent tokens" [location: string, value?: string] {
	let input = $in
	let value = if $value == null { $input } else { $value }
	$'http://localhost/($value | str trim)' | url parse | get params
	| where {|param| $param.key starts-with $'tokens[($location)][' }
	| get value
}

# Read the signed claims when a test verifies the permissions and expiration of a capability.
export def "token body" [value?: string] {
	let input = $in
	let value = if $value == null { $input } else { $value }
	$value | split row '.' | get 1 | decode base64 | decode utf-8 | from json
}

# Decode the length-prefixed JSON header before reading the SSE stream.
export def "stream header" [] {
	let bytes = $in | into binary
	mut length = 0
	mut offset = 0
	loop {
		let byte = $bytes | bytes at $offset..$offset | into int
		$length = $length + (($byte | bits and 127) | bits shl (7 * $offset))
		$offset = $offset + 1
		if $byte < 128 { break }
		if $offset >= 10 { error make { msg: 'invalid stream header length' } }
	}
	let header = $bytes | bytes at $offset..<($offset + $length) | decode utf-8 | from json
	let stream = $bytes | bytes at ($offset + $length).. | decode utf-8
	{ header: $header, stream: $stream }
}

export def --env snapshot [
	--entries: list<string>
	--name: string
	--normalize (-n)
	--normalize-ids
	--path (-p)
	--redact (-r): oneof<list<string>, string>
	value: any
	inline?: string
] {
	let value = if $path {
		snapshot_path --entries $entries $value | to json -i 2
	} else {
		$value | to text
	}
	let redactions = if $redact == null {
		null
	} else if ($redact | describe) == 'string' {
		[$redact]
	} else {
		$redact
	}
	let value = if $redactions == null { $value } else { $value | redact ...$redactions }
	let value = if $normalize_ids {
		$value | normalize --normalize-ids
	} else if $normalize {
		$value | normalize
	} else {
		$value
	}
	let value = $value | normalize_tokens

	if $inline != null {
		snapshot_inline --span=(metadata $inline).span $value $inline
	} else {
		snapshot_file --name=$name $value
	}
}

def --env snapshot_inline [
	--span: record
	value: string
	inline: string
] {
	# Get the expected value by processing the snapshot with doc.
	let expected_value = doc $inline

	# If the values match, return early.
	if $value == $expected_value {
		return
	}

	# Save the inline snapshot.
	let test_path = $env.CURRENT_FILE
	let test_name = $test_path | path parse | get stem
	let test_directory_path = $test_path | path dirname
	let inline_path = $test_directory_path | path join $'($test_name).inline'

	# Read existing inline data or start fresh.
	mut inline_entries = if ($inline_path | path exists) {
		open $inline_path | from json
	} else {
		[]
	}

	# Get the exact file position using view files.
	let files = view files
	let file = $files | where { |f| $span.start >= $f.start and $span.start < $f.end } | first
	let position = $span.start - $file.start
	let length = $span.end - $span.start

	# Add this entry.
	$inline_entries = $inline_entries | append {
		position: $position,
		length: $length,
		old: $expected_value,
		new: $value,
	}

	$inline_entries | to json | save -f $inline_path

	error make {
		msg: 'the snapshot does not match',
		help: (diff $expected_value $value),
		label: {
			span: $span,
			text: 'the snapshot',
		},
	}
}

def --env snapshot_file [
	--name (-n): string
	value: string
] {
	# Get the snapshot path.
	let test_path = $env.CURRENT_FILE
	let test_name = $test_path | path parse | get stem
	let test_directory_path = $test_path | path dirname
	let snapshot_directory_path = $test_directory_path | path join $test_name
	if $name != null {
		try { mkdir -v $snapshot_directory_path }
	}
	let snapshot_path = if $name == null {
		$test_directory_path | path join $'($test_name).snapshot'
	} else {
		$snapshot_directory_path | path join $'($name).snapshot'
	}
	let pending_path = $snapshot_path | str replace '.snapshot' '.pending'
	let touched_path = $snapshot_path | str replace '.snapshot' '.touched'

	# Touch the snapshot.
	touch $touched_path

	# Error if the snapshot does not exist.
	if not ($snapshot_path | path exists) {
		$value | save -f $pending_path
		error make {
			msg: 'the snapshot does not exist',
			label: {
				span: (metadata $value).span,
				text: 'the value',
			}
		}
	}

	# Read the snapshot.
	let old_value = open $snapshot_path

	# Error if the new value does not match the old value.
	if $value != $old_value {
		$value | save -f $pending_path
		error make {
			msg: 'the snapshot does not match',
			help: (diff $snapshot_path $pending_path --path),
			label: {
				span: (metadata $value).span,
				text: 'the value',
			},
		}
	}
}

def snapshot_path [path: string, --entries: list<string>] {
	let $type = $path | path type
	if $type == 'dir' {
		let entries = ls -a $path
			| where name != ($path | path join '.') and name != ($path | path join '..')
			| where { |entry| $entries == null or ($entry.name | path basename) in $entries }
			| each { |entry|
					let name = $entry.name | path basename
					let artifact = snapshot_path $entry.name
					{ name: $name, artifact: $artifact }
				}
			| reduce -f {} { |entry, acc|
					$acc | insert $entry.name $entry.artifact
				}
		{ kind: 'directory', entries: $entries }
	} else if $type == 'file' {
		let contents = open $path
		let executable = ls -l $path | first | get mode | str contains 'x'
		let names = xattr_list $path | where { |name| $name starts-with 'user.tangram' }
		let xattrs = $names | reduce -f {} { |name, acc|
			let value = xattr_read $name $path | normalize
			$acc | insert $name $value
		}
		mut output = { kind: 'file', contents: $contents }
		if $executable {
			$output.executable = true
		}
		if not ($xattrs | is-empty) {
			$output.xattrs = $xattrs
		}
		$output
	} else if $type == 'symlink' {
		mut target = do -i { ls -l $path | first | get target }
		if $target == null {
			$target = (readlink $path | str trim)
		}
		{ kind: 'symlink', path: $target }
	}
}

export def --env "server spawn" [
	--busybox
	--cloud # Create a cloud-backed instance for the server.
	--config (-c): record
	--directory (-d): string
	--instance: record # Spawn the server in this instance.
	--name (-n): string
	--now: string # Set the server's simulated wall clock to an RFC 3339 timestamp.
	--preserve-keys
	--quickjs # Use QuickJS as the JavaScript engine.
	--region: string # Set the server's region.
	--url (-u): string
] {
	let use_fskit = (($env.TANGRAM_TEST_FSKIT? | default "") | str length) > 0
	let server_config = $config | default {}
	let topology_keys = [instance primary_region region regions]
	let invalid_topology_keys = $server_config | columns | where { |key| $key in $topology_keys }
	if not ($invalid_topology_keys | is-empty) {
		error make {
			msg: $'server config contains topology fields: ($invalid_topology_keys | str join ", ")'
			help: 'set topology with instance and server spawn arguments'
		}
	}

	# Use unique semaphore names in the namespace FSKit can access.
	let cache_posix_sem_prefix = if $use_fskit {
		let app_group_identifier = (identifiers).app_group_identifier
		$'($app_group_identifier)/((random chars) | str lowercase | str substring 0..5)'
	} else {
		$'/tg-((random chars) | str lowercase | str substring 0..7)'
	}

	mut default_config: any = {
		accounts: true,
		runners: true,
		advanced: {
			disable_version_check: true
			internal_error_locations: false
		},
		index: {
			kind: 'lmdb',
			map_size: 10_485_760,
		},
		cache: {
			kind: 'lmdb',
			map_size: 10_485_760,
			posix_sem_prefix: $cache_posix_sem_prefix,
		},
		remotes: {},
		tokio_single_threaded: true,
		v8_thread_pool_size: 1,
	}

	let use_quickjs = $quickjs or (($env.TANGRAM_TEST_QUICKJS? | default "") | str length) > 0
	if $use_quickjs {
		$default_config = $default_config | merge deep {
			runner: {
				javascript: {
					engine: 'quickjs',
				},
			},
		}
	}

	let use_turso = (($env.TANGRAM_TEST_TURSO? | default "") | str length) > 0
	if $use_turso {
		$default_config = $default_config | merge deep {
			database: {
				kind: 'turso',
				path: 'database.sqlite3',
			},
		}
	}

	if $use_fskit {
		$default_config = $default_config | merge deep {
			vfs: {
				kind: 'fskit',
			},
		}
	}

	let use_vfs = (($env.TANGRAM_TEST_VFS? | default "") | str length) > 0

	let use_vm = (($env.TANGRAM_TEST_VM? | default "") | str length) > 0
	if $use_vm {
		let kernel_path = $env.TANGRAM_TEST_KERNEL_PATH? | default ""
		if ($kernel_path | str length) == 0 {
			error make { msg: 'TANGRAM_TEST_VM is set but TANGRAM_TEST_KERNEL_PATH is empty' }
		}
		$default_config = $default_config | merge deep {
			runner: { backend: { kind: 'cloud-hypervisor', kernel_path: $kernel_path } },
		}
	}

	if $cloud and $instance != null {
		error make { msg: '--cloud may not be combined with --instance' }
	}
	let instance = if $instance != null {
		$instance
	} else if $cloud {
		instance --cloud
	} else {
		instance
	}
	let instance_kind = $instance.kind
	let use_cloud = match $instance_kind {
		'cloud' => true,
		'local' => false,
		_ => {
			error make { msg: $'invalid instance kind: ($instance_kind)' }
		},
	}

	# Create the server directory. Local instances own one directory, while every cloud server owns its own directory.
	let directory_path = if $directory != null {
		$directory
	} else if $instance_kind == 'local' {
		$instance.directory
	} else {
		mktemp -d
	}
	try { mkdir $directory_path }

	let cloud_instance = if $use_cloud { $instance.id } else { null }
	let region_names = validate_instance_config $instance.config
	if not ($region_names | is-empty) and $region == null {
		error make { msg: 'a region is required when spawning a server in a regional instance' }
	}
	if $region != null and $region not-in $region_names {
		error make { msg: $'the server region is not in the instance regions list: ($region)' }
	}
	mut storage_instance: any = null
	if $use_cloud {
		if ($cloud_instance | is-empty) {
			error make { msg: 'the Tangram cloud instance is empty' }
		}
		let pool_path = $env.TANGRAM_TEST_DATABASE_POOL? | default ''
		if ($pool_path | is-empty) {
			error make { msg: 'TANGRAM_TEST_DATABASE_POOL is not set' }
		}
		let cluster = fdb_cluster
		$storage_instance = cloud_region_storage $cloud_instance $region $pool_path
		if ($storage_instance | is-empty) {
			error make { msg: 'the Tangram region storage instance is empty' }
		}
		track_database_pool_instance $storage_instance
		let partition_offset = $storage_instance | str replace 'pool' '' | into int

		let advanced = $default_config.advanced | merge { multi_server: true }
		let config = {
			advanced: $advanced,
			database: {
				kind: 'postgres',
				read: {
					pool: {
						max: 1,
					},
					url: $'postgres://postgres@127.0.0.1:5432/database_($cloud_instance)?sslmode=disable',
				},
				write: {
					pool: {
						max: 1,
					},
					url: $'postgres://postgres@127.0.0.1:5432/database_($cloud_instance)?sslmode=disable',
				},
			},
			instance: $cloud_instance,
			index: {
				cluster: $cluster,
				instance: $storage_instance,
				kind: 'fdb',
			},
			messenger: {
				kind: 'nats',
				url: 'nats://127.0.0.1:4222',
			},
			cache: {
				addr: '127.0.0.1:9042',
				connections: 1,
				keepalive: false,
				keyspace: $'cache_($storage_instance)',
				kind: 'scylla',
				partition_offset: $partition_offset,
			},
			remotes: {},
		}
		$default_config = $default_config | merge $config
	}

	# Replace the backend defaults when selecting another cache backend.
	let cache_kind = $server_config.cache?.kind? | default $instance.config.cache?.kind?
	if $cache_kind != null and $cache_kind != $default_config.cache.kind {
		$default_config = $default_config | upsert cache { kind: $cache_kind }
	}

	# Replace the backend defaults when selecting another index backend.
	let index_kind = $server_config.index?.kind? | default $instance.config.index?.kind?
	if $index_kind != null and $index_kind != $default_config.index.kind {
		$default_config = $default_config | upsert index { kind: $index_kind }
	}

	# Write the config.
	let config = $default_config
		| merge deep --strategy append $instance.config
		| merge deep --strategy append $server_config
	let config = if $instance.config.primary_region? == null {
		$config
	} else {
		$config | upsert primary_region $instance.config.primary_region
	}
	let config = if $instance.config.regions? == null {
		$config
	} else {
		$config | upsert regions $instance.config.regions
	}
	let config = if $use_cloud { $config | upsert instance $cloud_instance } else { $config }
	let config = if $region != null { $config | upsert region $region } else { $config }
	let config = if $use_cloud and $config.roles? == null {
		$config | upsert roles [api indexer scheduler]
	} else {
		$config
	}
	let roles = $config.roles? | default [api indexer runner scheduler]
	let multi_server = $config.advanced.multi_server? | default false
	let config = if $multi_server and ('indexer' in $roles) and ($config.indexer?.id? == null) {
		let id = bytes build 0x[00 00 10 00] (random binary 16) | tg id | into string
		$config | upsert indexer.id $id
	} else {
		$config
	}

	# Pin token keys to the server directory so restarts can verify existing tokens.
	let config = if $preserve_keys {
		let private_key_path = $directory_path | path join 'private_key'
		let public_key_path = $directory_path | path join 'public_key'
		if not ($private_key_path | path exists) {
			'U9ZBC697GDA0dlUBF/VVM4eqoJUVfQqwRNr6L2z8Ajg=' | decode base64 | save -f $private_key_path
			'MKmfiiYtaN4W/pP+V2hmmjtT2/+ILjYfiMJ9y4EsG1U=' | decode base64 | save -f $public_key_path
		}
		let keys = {
			private_key: {
				algorithm: 'ed25519',
				name: 'default',
				path: $private_key_path,
			},
			public_keys: [{
				algorithm: 'ed25519',
				name: 'default',
				path: $public_key_path,
			}],
		}
		$config | merge deep {
			authentication: {
				tokens: $keys,
			},
			verification: {
				tokens: $keys
			},
		}
	} else {
		$config
	}

	# Give a shared index the same semaphore names across servers and restarts.
	let config = if $config.index.kind? == 'lmdb' and $config.index.posix_sem_prefix? == null {
		let path = $directory_path | path join ($config.index.path? | default 'index.lmdb') | path expand
		let prefix = $'/tgi-($path | hash sha256 | str substring 0..15)'
		$config | upsert index.posix_sem_prefix $prefix
	} else {
		$config
	}

	# Force the selected VFS unless the test disables it.
	let forced_vfs_kind = if $use_fskit { 'fskit' } else if $use_vfs { 'fuse' } else { null }
	let config = if $forced_vfs_kind == null {
		$config
	} else if ($config | get --optional vfs) == false {
		$config
	} else if (($config | get --optional vfs | describe) | str starts-with 'record') {
		$config | upsert vfs ($config | get vfs | upsert kind $forced_vfs_kind)
	} else {
		$config | upsert vfs { kind: $forced_vfs_kind }
	}
	# Start the server's virtual clock at the requested time.
	let config = if $now == null { $config } else { $config | upsert advanced (($config.advanced? | default {}) | upsert clock { start: $now }) }
	let config_path = mktemp -d
	let config_path = $config_path | path join 'config.json'
	$config | to json | save -f $config_path

	# Determine the url.
	let url = $url | default (unix_socket_url $directory_path)
	$env.TANGRAM_URL = $url

	# Create a path for the server's captured output.
	let log_path = ($config_path | path dirname | path join 'log')
	touch $log_path
	# Record the virtual clock's time, so that a restarted server resumes from it.
	let clock_path = if $now == null {
		null
	} else {
		let clock_path = $config_path | path dirname | path join 'clock'
		$now | save -f $clock_path
		$clock_path
	}

	# Create and start the server.
	let vfs = $config | get --optional vfs
	let checkout_directory_name = if $vfs == null or $vfs == false { 'store' } else { 'checkouts' }
	let checkout_directory = $directory_path | path join $checkout_directory_name
	let name = $name | default 'server'
	let server = {
		checkout_directory: $checkout_directory,
		clock: $clock_path,
		config: $config,
		config_path: $config_path,
		directory: $directory_path,
		exit: ($config_path | path dirname | path join 'exit'),
		instance: $instance,
		log: $log_path,
		macos_app_group_socket: null,
		name: $name,
		pid: null,
		supervisor: ($config_path | path dirname),
		url: $url,
	}
	let server = server start $server

	# Tag busybox if requested.
	if $busybox {
		skip_if_offline
		let path = mktemp -d
		let source = '
			const SOURCES: Record<string, { url: string, checksum: tg.Checksum }> = {
				"aarch64-darwin": {
					url: "https://github.com/tangramdotdev/bootstrap/releases/download/v2026.09.16/utils_aarch64_darwin.tar.zst",
					checksum: "sha256:164b27c527541c1695d0ad9ca5ccb65b495315770ca2916c9c2b7f691d002dc4",
				},
				"x86_64-darwin": {
					url: "https://github.com/tangramdotdev/bootstrap/releases/download/v2026.09.16/utils_x86_64_darwin.tar.zst",
					checksum: "sha256:10b0cf8ca64429f4362f8aaded78326d418db8a1d4d7f2fa91a5f8019dbb49de",
				},
				"aarch64-linux": {
					url: "https://github.com/tangramdotdev/bootstrap/releases/download/v2026.09.16/utils_aarch64_linux.tar.zst",
					checksum: "sha256:316b1b5d96bbf7b6e411f507a41c87e2dc8b43cd6c98960c20d64d3a5390b3a4",
				},
				"x86_64-linux": {
					url: "https://github.com/tangramdotdev/bootstrap/releases/download/v2026.09.16/utils_x86_64_linux.tar.zst",
					checksum: "sha256:c60c8c54913fd8be2b8614c8abe0b2bb9752fc4ebf5d3a0a13e81701020078cf",
				},
			};

			export const env = (host?: string) => {
				const host_ = host ?? tg.host.current;
				tg.assert(typeof host_ === "string");
				const kv = Object.entries(SOURCES).find(([k, _]) => k === host_);
				tg.assert(kv, `unknown host: ${host_}`);
				const { url, checksum } = kv[1];
				const dir = tg.download(url, checksum, { mode: "extract" }).then(tg.Directory.expect);
				return { PATH: tg.Mutation.suffix(tg`${dir}/bin`, ":") };
			};

			export default env;
		';
		$source | save ($path | path join 'tangram.ts')
		if ($config.authentication?.users?.providers?.insecure? | default false) {
			let alice = tg -c ($config_path) login --verbose --name alice | from json
			tg -c ($config_path) --token ($alice.token) tag 'busybox' $path
			tg -c ($config_path) --token ($alice.token) grant public tag_read 'busybox'
		} else {
			tg check $path
			tg -c ($config_path) tag 'busybox' $path
		}
		rm -rf $path
	}

	$server
}

# Check in a path on a temporary single server and push it to a server in multi-server mode, which does not support check-in.
export def "checkin push" [server: record, path: string, --token: string] {
	let remote = if $token == null { { url: $server.url } } else { { token: $token, url: $server.url } }
	let checkin = server spawn --name checkin --config { remotes: { default: $remote } }
	let id = tg --url $checkin.url checkin --no-tokens $path | referent node
	tg --url $checkin.url push $id | ignore
	server stop $checkin
	$id
}

export def --env "server start" [server: record] {
	# Wait for the previous supervisor, which finishes shortly after its server exits.
	if (server_is_running $server) {
		let output = ^tangram_cli_test_supervise wait --directory $server.supervisor --timeout 10 | complete
		if $output.exit_code != 0 {
			error make { msg: 'the server is already running' }
		}
	}

	# Record every semaphore name before startup can create it.
	let config = open $server.config_path
	let prefixes = [$config.index?.posix_sem_prefix? $config.cache?.posix_sem_prefix?] | compact | uniq
	let names = $prefixes | each {|prefix| [$'($prefix)r' $'($prefix)w'] } | flatten
	if not ($names | is-empty) {
		let root = $env.TMPDIR? | default ($server.config_path | path dirname)
		(($names | str join "\n") + "\n") | save --append ($root | path join 'posix_semaphores')
	}

	# Resume the virtual clock from its latest recorded time.
	if $server.clock? != null {
		let now = open --raw $server.clock | str trim
		$config | upsert advanced.clock.start $now | to json | save -f $server.config_path
	}

	# Record VFS intent before launch so cleanup still finds it after a crash.
	mark_vfs_cleanup $server

	# Create the environment.
	mut environment = {}
	let macos_app_group_socket = create_macos_app_group_socket_path $server
	if $macos_app_group_socket != null {
		$environment = $environment | upsert TANGRAM_MACOS_APP_GROUP_SOCKET $macos_app_group_socket
	}
	let environment = $environment

	# Start the server under the supervisor, which waits for it to signal readiness and removes the socket after it exits.
	let supervisor = $server.config_path | path dirname
	track_supervisor $supervisor
	rm -f ($supervisor | path join 'error')
	let remove = if $macos_app_group_socket == null { [] } else { [--remove $macos_app_group_socket] }
	let output = try {
		with-env $environment {
			^tangram_cli_test_supervise start --description server --directory $supervisor --name $server.name --ready-fd 3 ...$remove -- tangram -c $server.config_path -d $server.directory -u $server.url serve --ready-fd 3
		} | from json
	} catch {
		let error_path = $supervisor | path join 'error'
		let message = if ($error_path | path exists) { open --raw $error_path } else { $'($server.name) did not start' }
		error make { msg: $message }
	}
	$env.TANGRAM_URL = $server.url
	let server = $server
		| upsert exit ($supervisor | path join 'exit')
		| upsert macos_app_group_socket $macos_app_group_socket
		| upsert pid $output.server
		| upsert supervisor $supervisor

	$server
}

export def "server stop" [server: record] {
	let supervisor = $server.supervisor?
	if $supervisor != null {
		^tangram_cli_test_supervise stop --directory $supervisor
	}
	remove_macos_app_group_socket $server.macos_app_group_socket?
}

export def --env "server restart" [server: record] {
	server stop $server
	let server = server start $server

	$server
}

def server_is_running [server: record] {
	let supervisor = $server.supervisor?
	if $supervisor == null {
		return false
	}

	(^tangram_cli_test_supervise status --directory $supervisor | complete).exit_code == 0
}

# Record a supervisor directory so the test cleanup stops its process.
export def track_supervisor [directory: string] {
	let path = $env.TMPDIR? | default ($nu.temp-dir? | default $nu.temp-path?) | path join $supervisors_name
	let tracked = if ($path | path exists) { open --raw $path | lines } else { [] }
	if $directory not-in $tracked {
		$directory ++ "\n" | save --append $path
	}
}

def create_macos_app_group_socket_path [server: record] {
	let vfs = $server.config.vfs?
	let kind = if ($vfs | describe) =~ '^record' { $vfs.kind? } else { null }
	if $nu.os-info.name != 'macos' or $kind != 'fskit' {
		return null
	}
	let group_id = (identifiers).app_group_identifier
	let group_container = $env.HOME | path join 'Library/Group Containers' $group_id
	try { mkdir $group_container }
	let socket_name = $'socket-((random chars) | str lowercase)'

	$group_container | path join $socket_name
}

def remove_macos_app_group_socket [path] {
	if $path != null {
		try { rm -f $path }
	}
}

# Set a server's virtual clock.
export def set_time [server: record, now: string] {
	clock $server set $now
}

# Advance a server's virtual clock.
export def advance_time [server: record, duration: duration] {
	let milliseconds = $duration / 1ms | math round
	clock $server advance $'($milliseconds)ms'
}

# Advance the virtual clocks of several servers by the same duration.
export def advance_times [servers: list<record>, duration: duration] {
	for server in $servers {
		advance_time $server $duration
	}
}

# Run a clock command against a server with its root token, and record the resulting time so that a restart resumes from it.
def clock [server: record, ...args: string] {
	if $server.clock? == null {
		error make { msg: 'the server does not have a virtual clock' }
	}
	let token = $server.config.authentication?.root?.token?
	let token_args = if $token == null { [] } else { [--token $token] }
	let output = tg --url $server.url ...$token_args clock ...$args | from json
	$output.now | save -f $server.clock
}

# Stop a server, so that its output is complete, and return the distinct errors
# it logged as '<target> <message>'. The server must have been spawned with
# `--config { tracing: { stderr_format: 'json' } }`.
export def server_errors [server: record] {
	server stop $server
	open --raw $server.log
		| lines
		| each { try { from json } catch { null } }
		| compact
		| where { |event| try { $event.level? == 'ERROR' } catch { false } }
		| each { |event| $"($event.target) ($event.fields.message)" }
		| uniq
		| sort
}

export def instance [
	--cloud
	--config: record
	--primary-region: string
	--regions: list
] {
	let config = $config | default {}
	let config = if $primary_region == null { $config } else { $config | upsert primary_region $primary_region }
	let config = if $regions == null { $config } else { $config | upsert regions $regions }
	let config = if $config.regions? == null {
		$config
	} else {
		$config | upsert regions (allocate_region_urls $config.regions)
	}
	validate_instance_config $config | ignore
	let use_cloud = $cloud and (($env.TANGRAM_TEST_CLOUD? | default '') | str length) > 0
	if not $use_cloud {
		let directory = mktemp -d

		return { config: $config, directory: $directory, kind: 'local' }
	}
	let pool_path = $env.TANGRAM_TEST_DATABASE_POOL? | default ''
	if ($pool_path | is-empty) {
		error make { msg: 'TANGRAM_TEST_DATABASE_POOL is not set' }
	}
	let id = acquire_database_instance $pool_path
	track_database_pool_instance $id

	{ config: $config, id: $id, kind: 'cloud' }
}

export def "instance region url" [instance: record, region: string] {
	let matches = $instance.config.regions? | default [] | where name == $region
	if ($matches | is-empty) {
		error make { msg: $'the instance does not contain region ($region)' }
	}

	$matches | first | get url
}

def allocate_region_urls [regions: list] {
	$regions | each { |region|
		if $region.url? != null {
			$region
		} else {
			let socket_directory = mktemp -d
			$region | upsert url (unix_socket_url $socket_directory)
		}
	}
}

def unix_socket_url [directory: string] {
	$'http+unix://($directory | url encode --all)%2Fsocket'
}

def validate_instance_config [config: record] {
	let invalid_topology_keys = $config | columns | where { |key| $key in [instance region] }
	if not ($invalid_topology_keys | is-empty) {
		error make {
			msg: $'instance config contains server topology fields: ($invalid_topology_keys | str join ", ")'
			help: 'set the server region with --region'
		}
	}
	let primary_region = $config.primary_region?
	let regions = $config.regions?
	if $primary_region != null and $regions == null {
		error make { msg: 'regions are required when a primary region is set' }
	}
	let region_names = $regions | default [] | get name
	if ($region_names | uniq | length) != ($region_names | length) {
		error make { msg: 'the instance regions list contains duplicate names' }
	}
	if $primary_region != null and $primary_region not-in $region_names {
		error make { msg: $'the primary region is not in the regions list: ($primary_region)' }
	}

	$region_names
}

def track_database_pool_instance [instance: string] {
	$instance ++ "\n" | save --append (($nu.temp-dir? | default $nu.temp-path?) | path join 'instances')
}

def cloud_region_storage [instance: string, region: any, pool_path: string] {
	let region = $region | default '' | into string
	let key = $'($instance)\n($region)' | hash sha256
	let storages_path = ($nu.temp-dir? | default $nu.temp-path?) | path join 'region_storages'
	mkdir $storages_path
	let storage_path = $storages_path | path join $key
	if ($storage_path | path exists) {
		return (open --raw $storage_path | str trim)
	}
	let lock_path = $storage_path + '.lock'
	let lock_output = ^mkdir $lock_path | complete
	if $lock_output.exit_code != 0 {
		let output = (open /dev/null | timeout 10 bash -c 'while [ ! -s "$1" ]; do sleep 0.05; done' _ $storage_path | complete)
		if $output.exit_code != 0 {
			error make { msg: $'timed out waiting for the Tangram region storage: ($region)' }
		}

		return (open --raw $storage_path | str trim)
	}
	let storage = if (try_claim_cloud_instance_storage $instance) {
		$instance
	} else {
		acquire_database_instance $pool_path
	}
	$storage | save -f $storage_path
	rm $lock_path

	$storage
}

def try_claim_cloud_instance_storage [instance: string] {
	let claims_path = ($nu.temp-dir? | default $nu.temp-path?) | path join 'instance_storage_claims'
	mkdir $claims_path
	let claim_path = $claims_path | path join $instance
	let output = ^mkdir $claim_path | complete

	$output.exit_code == 0
}

export def diff [old: string, new: string, --path] {
	let old_path = if $path { $old } else { let t = mktemp; $old | save -f $t; $t }
	let new_path = if $path { $new } else { let t = mktemp; $new | save -f $t; $t }
	let result = delta --file-style=omit --hunk-header-style=omit --no-gitconfig $old_path $new_path | complete | get stdout
	if not $path { rm $old_path $new_path }
	$result
}

export def --env success [
	output: record
	message?: string
] {
	if $output.exit_code != 0 {
		error make {
			msg: ($message | default 'the process failed'),
			label: {
				span: (metadata $output).span,
				text: 'the output',
			},
			help: $output.stderr,
		}
	}
}

# Skip the test, reporting the reason. The runner treats exit code 77 as skipped rather than passed or failed. This is named skip_test because skip is a Nushell builtin.
export def skip_test [reason: string] {
	print --stderr $reason
	exit 77
}

# Skip the test when the runner was invoked with --offline. Call this at the top of tests which require network access.
export def skip_if_offline [] {
	if (($env.TANGRAM_TEST_OFFLINE? | default '') | str length) > 0 {
		skip_test 'this test requires network access'
	}
}

export def skip_if_no_cloud [] {
	let cloud = (($env.TANGRAM_TEST_CLOUD? | default '') | str length) > 0
	if not $cloud {
		skip_test 'this test requires cloud databases'
	}
}

# Determine whether the running kernel has enabled the FUSE io_uring transport.
export def fuse_io_uring_available [] {
	if $nu.os-info.name != 'linux' {
		return false
	}
	let path = '/sys/module/fuse/parameters/enable_uring'
	if not ($path | path exists) {
		return false
	}
	try {
		(open --raw $path | str trim | str lowercase) in ['1' 'y']
	} catch {
		false
	}
}

# Poll a condition until it returns true, erroring if the timeout elapses. Prefer this over a bare sleep, so the test runs as soon as the condition holds and tolerates slow machines.
export def wait_until [
	condition: closure
	message?: string
	--timeout: duration = 10sec
	--interval: duration = 50ms
] {
	let start = date now
	loop {
		if (do $condition) {
			return
		}
		if ((date now) - $start) > $timeout {
			error make {
				msg: ($message | default 'the condition was not met within the timeout'),
				label: {
					span: (metadata $condition).span,
					text: 'the condition',
				},
			}
		}
		sleep $interval
	}
}

# Watch a checkpoint and return a handle for the other checkpoint helpers. Without --url, the watch is on the server in $env.TANGRAM_URL.
export def "checkpoint watch" [
	name: string
	--params: record
	--token: string
	--url: string
] {
	let connection = [
		(if $url == null { [] } else { [--url $url] })
		(if $token == null { [] } else { [--token $token] })
	] | flatten
	let params = $params | default {} | to json --raw
	let watch = tg ...$connection checkpoint watch $name --params $params | from json | get watch

	{ connection: $connection, name: $name, watch: $watch }
}

# Wait for a hit of a watched checkpoint and return it.
export def "checkpoint wait" [
	checkpoint: record
	hit: int = 0
	--timeout: duration = 30sec
] {
	let seconds = $timeout / 1sec | math ceil
	let output = tg ...$checkpoint.connection checkpoint wait --timeout $'($seconds)s' $checkpoint.name $checkpoint.watch $hit | complete
	if $output.exit_code != 0 {
		error make {
			msg: $'the checkpoint ($checkpoint.name) did not reach hit ($hit) within ($timeout)'
			help: $output.stderr
		}
	}

	$output.stdout | from json
}

# Let a held hit of a watched checkpoint continue.
export def "checkpoint continue" [checkpoint: record, hit: int = 0] {
	tg ...$checkpoint.connection checkpoint continue $checkpoint.name $checkpoint.watch $hit
}

# Wait for a hit of a watched checkpoint, let it continue, and return it.
export def "checkpoint pass" [
	checkpoint: record
	hit: int = 0
	--timeout: duration = 30sec
] {
	let result = checkpoint wait $checkpoint $hit --timeout $timeout
	checkpoint continue $checkpoint $hit

	$result
}

# Stop watching a checkpoint, which lets every held hit continue.
export def "checkpoint unwatch" [checkpoint: record] {
	tg ...$checkpoint.connection checkpoint unwatch $checkpoint.name $checkpoint.watch
}

# Index a server and wait for usage aggregation to apply every usage change that indexing recorded. The server must run a single usage aggregation partition.
export def aggregate_usage [
	--token: string
	--url: string
] {
	let connection = [
		(if $url == null { [] } else { [--url $url] })
		(if $token == null { [] } else { [--token $token] })
	] | flatten
	tg ...$connection index

	# Pass two batches, so that a complete batch starts after indexing finishes.
	let checkpoint = checkpoint watch indexer.usage.aggregation.batch --token $token --url $url
	checkpoint pass $checkpoint 0 | ignore
	checkpoint pass $checkpoint 1 | ignore
	checkpoint unwatch $checkpoint
}

# Wait for a process to exit. Unlike polling `ps`, this returns as soon as the process exits.
export def wait_for_exit [
	pid: int
	message?: string
	--timeout: duration = 60sec
] {
	let seconds = $timeout / 1sec | math ceil
	let output = ^tangram_cli_test_supervise exit --pid $pid --timeout $seconds | complete
	if $output.exit_code != 0 {
		error make { msg: ($message | default $'the process ($pid) did not exit within ($timeout)') }
	}
}

# Wait until a server's log contains some text, following the log rather than polling it.
export def wait_for_log [
	server: record
	text: string
	--timeout: duration = 10sec
] {
	let seconds = $timeout / 1sec | math ceil
	let fifo = mktemp --directory | path join fifo
	let script = 'mkfifo "$3"; tail -n +1 -f "$1" > "$3" & pid=$!; grep -q -m1 -F -- "$2" < "$3"; status=$?; kill $pid; wait $pid 2> /dev/null; rm -f "$3"; exit $status'
	let output = ^timeout $seconds bash -c $script _ $server.log $text $fifo | complete
	if $output.exit_code != 0 {
		error make { msg: $'the server log did not contain "($text)" within ($timeout)' }
	}
}

# Wait until a file is larger than a size, following the file rather than polling it. Return false if the timeout passes first.
export def wait_for_growth [
	path: string
	size: int
	--timeout: duration = 10sec
] {
	let seconds = $timeout / 1sec | math ceil
	let fifo = mktemp --directory | path join fifo
	let script = 'mkfifo "$3"; tail -c +"$(( $2 + 1 ))" -f "$1" > "$3" & pid=$!; head -c 1 < "$3" > /dev/null; status=$?; kill $pid; wait $pid 2> /dev/null; rm -f "$3"; exit $status'
	let output = ^timeout $seconds bash -c $script _ $path $size $fifo | complete

	$output.exit_code == 0
}

# Redact literal strings in the input for snapshotting.
export def redact [...redactions: string] {
	mut output = $in
	for redaction in ($redactions | sort-by { |redaction| $redaction | str length } --reverse) {
		$output = $output | str replace --all $redaction '<redacted>'
	}
	$output
}

export def --env failure [
	output: record
	message?: string
] {
	if $output.exit_code == 0 {
		error make {
			msg: ($message | default 'the process succeeded'),
			label: {
				span: (metadata $output).span,
				text: 'the output',
			},
			help: $output.stderr,
		}
	}
}

export def xattr_list [path: string] {
	match $nu.os-info.name {
		'macos' => { xattr $path | lines }
		'linux' => { getfattr -m '.' $path | complete | get stdout | lines | where { |l| not ($l starts-with '#') and $l != '' } }
	}
}

export def xattr_read [name: string, path: string] {
	match $nu.os-info.name {
		'macos' => { xattr -p $name $path | str trim }
		'linux' => { getfattr -n $name --only-values $path | str trim }
	}
}

export def xattr_write [name: string, value: string, path: string] {
	match $nu.os-info.name {
		'macos' => { xattr -w $name $value $path }
		'linux' => { setfattr -n $name -v $value $path }
	}
}

# Normalize runtime IDs and tokens in a string for snapshotting. With --normalize-ids, include content-addressed IDs. The length floor keeps identifiers such as `pcs_id` from being normalized.
export def normalize [value?: string, --normalize-ids] {
	let input = $in
	let value = ($value | default $input)

	mut output = $value
	mut numeric_process_index = 0
	for id in ($output | parse --regex 'id = (?<id>[0-9]+)' | get id | uniq) {
		if $numeric_process_index > 9 {
			error make { msg: 'too many IDs to normalize for the prefix' }
		}
		let digit = $numeric_process_index | into string
		let replacement = 'pcs_00' + (0..<26 | each { $digit } | str join)
		$numeric_process_index += 1
		$output = $output | str replace --all $'id = ($id)' $'id = ($replacement)'
	}

	let prefixes = if $normalize_ids {
		[blb cmd dir err fil gph grp org pcs sbx sym tag usr]
	} else {
		[err grp org pcs sbx tag usr]
	}
	let prefixes_pattern = $prefixes | str join '|'
	let pattern = '(?<id>(' + $prefixes_pattern + ')_[a-z0-9]{20,})'
	mut counters = {}
	for id in ($output | parse --regex $pattern | get id | uniq) {
		let prefix = ($id | split row '_' | first)
		let suffix = ($id | split row '_' | last)
		let index = ($counters | get --optional $prefix | default 0)
		if $index > 9 {
			error make { msg: 'too many IDs to normalize for the prefix' }
		}
		let header_length = if ($suffix | str length) < 2 { $suffix | str length } else { 2 }
		let header = ($suffix | str substring 0..<$header_length)
		let digit = ($index | into string)
		let replacement_length = ($suffix | str length) - $header_length
		let replacement_suffix = if $replacement_length <= 0 {
			''
		} else {
			0..<$replacement_length | each { $digit } | str join
		}
		let replacement = $'($prefix)_($header)($replacement_suffix)'
		$counters = ($counters | upsert $prefix ($index + 1))
		$output = $output | str replace --all $id $replacement
	}

	$output | normalize_tokens
}

# Normalize authorization tokens in a string for snapshotting. Tokens are never stable.
export def normalize_tokens [value?: string] {
	let input = $in
	mut output = ($value | default $input)
	$output = $output | str replace --all --regex '([?&]token=|"token":\s*")[A-Za-z0-9._~%+/=-]+' '${1}<token>'
	$output = $output | str replace --all --regex '0\.[A-Za-z0-9_~%+/=-]+\.[A-Za-z0-9_~%+/=-]+\.[A-Za-z0-9_~%+/=-]+' '<token>'

	$output
}

export def fdb_cluster [] {
	let env_cluster = $env.TANGRAM_TEST_FDB_CLUSTER? | default ''
	if ($env_cluster | str length) > 0 {
		return $env_cluster
	}

	let cluster = mktemp -t
	foundationdb_cluster_description | save -f $cluster
	$cluster
}

# Remove this test's semaphore names after its processes have stopped.
export def cleanup_posix_semaphores [temp_path: string] {
	^tangram_cli_test semaphore cleanup $temp_path
}

export def cleanup_background_jobs [temp_path: string] {
	# Stop the supervised servers in the reverse order of their starts, so they can tear down their sandboxes and mounts.
	let path = $temp_path | path join $supervisors_name
	let supervisors = if ($path | path exists) { open --raw $path | lines | where { $in != '' } } else { [] }
	for supervisor in ($supervisors | reverse) {
		try { ^tangram_cli_test_supervise stop --directory $supervisor }
	}

	# Kill every remaining test job and its process group, including unlabelled helpers and mock services.
	for job in (job list) {
		let pids = $job.pids? | default []
		if not ($pids | is-empty) {
			let targets = $pids | each { |pid| [$'-($pid)' $'($pid)'] } | flatten
			try { ^kill -TERM -- ...$targets e> /dev/null }
		}
		try { job kill $job.id }
	}
}

def mark_vfs_cleanup [server: record] {
	let config = $server.config? | default {}
	let file_config = try { open $server.config_path } catch { {} }
	let enabled = (config_uses_vfs $config) or (config_uses_vfs $file_config)
	if not $enabled {
		return
	}
	let root = $env.TMPDIR? | default ($server.config_path | path dirname)
	'' | save --force ($root | path join $vfs_cleanup_marker_name)
}

def config_uses_vfs [config: record] {
	let vfs = $config | get --optional vfs

	$vfs == true or (($vfs | describe) | str starts-with 'record')
}
