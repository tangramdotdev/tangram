#!/usr/bin/env nu

use ./common.nu *

# The shared libraries that a Linux binary may require.
const linux_libraries = [
	'^ld-linux-.*\.so\.\d+$'
	'^libc\.so\.6$'
	'^libdl\.so\.2$'
	'^libgcc_s\.so\.1$'
	'^libm\.so\.6$'
	'^libpthread\.so\.0$'
	'^librt\.so\.1$'
	'^libutil\.so\.1$'
]

# Verify a release archive and print a report.
def main [
	archive: path # The archive, such as release/tangram_aarch64-darwin.tar.gz.
	--skip-signing # On macOS, do not require the Developer ID signature and the notarization.
	--skip-smoke # Do not run the binary.
] {
	# Resolve the target from the name of the archive.
	let archive = $archive | path expand
	let name = $archive | path basename
	let entry = $targets | values | where { |entry| $name == $'tangram_($entry.host).tar.gz' } | get --optional 0
	if $entry == null {
		error make { msg: $'the archive is not named for a release target: ($name)' }
	}
	let version = version
	let directory = mktemp --directory --tmpdir tangram-release-verify.XXXXXX
	^tar -xzf $archive -C $directory
	let binary_path = $directory | path join tangram
	mut checks = []

	# Check the contents of the archive.
	let members = ^tar -tzf $archive | lines | sort
	$checks ++= [(check 'the archive contains tangram, tg, and tgx' ($members == [tangram tg tgx]) ($members | str join ' '))]
	let link = ^readlink ($directory | path join tg) | complete | get stdout | str trim
	$checks ++= [(check 'tg is a symlink to tangram' ($link == 'tangram') $link)]
	let tgx = $directory | path join tgx
	let tgx_executable = ($tgx | path exists) and ((ls -l $tgx | get mode | first) | str contains 'x')
	$checks ++= [(check 'tgx is executable' $tgx_executable '')]

	# Check the format and the architecture of the binary.
	let description = ^file -b $binary_path | str trim
	let format = if $entry.os == 'darwin' { 'Mach-O' } else { 'ELF' }
	let arch_names = if $entry.arch == 'aarch64' { ['arm64' 'aarch64'] } else { ['x86_64' 'x86-64'] }
	let matches = ($description | str contains $format) and ($arch_names | any { |arch| $description | str contains $arch })
	$checks ++= [(check $'the binary is ($format) for ($entry.arch)' $matches $description)]

	# Check the platform requirements of the binary.
	if $entry.os == 'darwin' and (host_os) == 'darwin' {
		$checks ++= (check_darwin $binary_path $skip_signing)
	}
	if $entry.os == 'linux' and (which readelf | is-not-empty) {
		$checks ++= (check_linux $binary_path)
	}
	if $entry.os == 'linux' and (which readelf | is-empty) {
		$checks ++= [(check 'readelf is available to inspect the binary' false 'install binutils')]
	}

	# Run the binary if this machine can.
	let runnable = $entry.os == (host_os) and ($entry.arch == (host_arch) or ($entry.os == 'darwin' and (rosetta)))
	if $runnable and not $skip_smoke {
		$checks ++= (check_smoke $directory $version)
	}
	rm -rf $directory

	# Print the report.
	let size = ls $archive | get size | first
	let digest = open --raw $archive | hash sha256
	print $'archive  ($name)'
	print $'size     ($size)'
	print $'sha256   ($digest)'
	print $'machine  (host_arch)-(host_os)'
	if not $runnable {
		print $'note     this machine cannot run the binary, so it was not run'
	}
	print ''
	for check in $checks {
		let status = if $check.pass { 'pass' } else { 'FAIL' }
		let detail = if ($check.detail | is-empty) { '' } else { $'  [($check.detail)]' }
		print $'($status)  ($check.name)($detail)'
	}
	let failures = $checks | where { |check| not $check.pass } | length
	print ''
	if $failures > 0 {
		print $'($failures) of ($checks | length) checks failed'
		exit 1
	}
	print $'all ($checks | length) checks passed'
}

# Create a check.
def check [name: string, pass: bool, detail: string]: nothing -> record {
	{ name: $name, pass: $pass, detail: $detail }
}

# Determine whether this machine can run x86_64 binaries with Rosetta.
def rosetta []: nothing -> bool {
	(^arch -x86_64 /usr/bin/true | complete).exit_code == 0
}

# Check the signature, the deployment target, and the libraries of a macOS binary.
def check_darwin [binary_path: path, skip_signing: bool]: nothing -> list<record> {
	mut checks = []

	# Check the signature.
	if not $skip_signing {
		let verify = ^codesign --verify --strict $binary_path | complete
		$checks ++= [(check 'the signature is valid' ($verify.exit_code == 0) ($verify.stderr | str trim | lines | first 1 | str join))]
		let signature = ^codesign -dv --verbose=4 $binary_path | complete
		let details = $signature.stdout + $signature.stderr
		let authority = $details | lines | where { |line| $line starts-with 'Authority=' } | get --optional 0 | default 'no authority'
		$checks ++= [(check 'the binary is signed with a Developer ID Application identity' ($authority | str contains 'Developer ID Application') $authority)]
		$checks ++= [(check 'the signature uses the hardened runtime' ($details =~ 'flags=.*runtime') '')]
		$checks ++= [(check 'the signature has a secure timestamp' ($details | str contains 'Timestamp=') '')]
		let notarized = ^codesign --verify --strict -R=notarized --check-notarization $binary_path | complete
		$checks ++= [(check 'the binary is notarized' ($notarized.exit_code == 0) ($notarized.stderr | str trim))]
	}

	# Check the deployment target.
	let minos = ^otool -l $binary_path | lines | where { |line| $line =~ '^\s*minos ' } | get --optional 0 | default '' | str trim | str replace 'minos ' ''
	$checks ++= [(check $'the deployment target is (deployment_target)' ($minos == (deployment_target)) $minos)]

	# Check that every library is a system library.
	let libraries = ^otool -L $binary_path | lines | skip 1 | each { |line| $line | str trim | split row ' ' | first }
	let foreign = $libraries | where { |library| not (($library starts-with '/usr/lib/') or ($library starts-with '/System/Library/')) }
	$checks ++= [(check 'the binary links only system libraries' ($foreign | is-empty) ($foreign | str join ' '))]

	$checks
}

# Check the libraries and the glibc requirement of a Linux binary.
def check_linux [binary_path: path]: nothing -> list<record> {
	mut checks = []

	# Check that every library is expected.
	let libraries = ^readelf -d $binary_path | lines | parse --regex 'NEEDED.*\[(?<library>.*)\]' | get library
	let foreign = $libraries | where { |library| not ($linux_libraries | any { |pattern| $library =~ $pattern }) }
	$checks ++= [(check 'the binary requires only glibc libraries' ($foreign | is-empty) ($libraries | str join ' '))]

	# Check the newest glibc symbol version.
	let versions = ^readelf -V $binary_path
		| parse --regex 'GLIBC_(?<major>\d+)\.(?<minor>\d+)'
		| each { |row| [($row.major | into int) ($row.minor | into int)] }
		| uniq
	let newest = $versions | sort-by { |version| $version.0 * 1000 + $version.1 } | last
	let pass = ($newest.0 * 1000 + $newest.1) <= ($glibc_floor.0 * 1000 + $glibc_floor.1)
	$checks ++= [(check $'the binary requires glibc ($glibc_floor.0).($glibc_floor.1) or older' $pass $'($newest.0).($newest.1)')]

	$checks
}

# Run the binary against a scratch server.
def check_smoke [directory: path, version: string]: nothing -> list<record> {
	mut checks = []
	let binary_path = $directory | path join tangram

	# Check the version.
	let output = ^$binary_path --version | complete
	let printed = $output.stdout | str trim
	let pass = $output.exit_code == 0 and ($printed | parse --regex ('^tangram ' + ($version | str replace --all '.' '\.') + '\+[0-9a-f]{40}$') | is-not-empty)
	$checks ++= [(check $'the version is ($version) with a commit' $pass $printed)]

	# Create a package and a scratch server.
	let package_path = $directory | path join package
	mkdir $package_path
	'
		export default () => "hello from javascript";

		export const command = async () => {
			const file = await tg.build`printf "hello from the sandbox" > "${tg.output}"`;
			return await file.text();
		};
	' | save ($package_path | path join tangram.ts)
	let config_path = $directory | path join config.json
	'{}' | save $config_path
	let args = ['--config' $config_path '--directory' ($directory | path join .tangram) '--quiet']

	# Check that the server starts.
	let health = ^$binary_path ...$args health | complete
	$checks ++= [(check 'the server starts and reports its health' ($health.exit_code == 0) ($health.stderr | str trim | lines | last 1 | str join))]

	# Check that a JavaScript build runs.
	let build = ^$binary_path ...$args build $package_path | complete
	let pass = $build.exit_code == 0 and ($build.stdout | str trim) == '"hello from javascript"'
	$checks ++= [(check 'a JavaScript build succeeds' $pass (failure_detail $build))]

	# Check that a command runs in a sandbox.
	let command = ^$binary_path ...$args build $'($package_path)#command' | complete
	let pass = $command.exit_code == 0 and ($command.stdout | str trim) == '"hello from the sandbox"'
	$checks ++= [(check 'a sandboxed command succeeds' $pass (failure_detail $command))]

	# Stop the server.
	let stop = ^$binary_path ...$args server stop | complete
	$checks ++= [(check 'the server stops' ($stop.exit_code == 0) '')]

	$checks
}

# Summarize a failed command.
def failure_detail [output: record]: nothing -> string {
	if $output.exit_code == 0 {
		$output.stdout | str trim
	} else {
		$output.stderr | str trim | lines | where { |line| $line starts-with '->' } | str join ' '
	}
}
