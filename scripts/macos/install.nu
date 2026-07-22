#!/usr/bin/env nu

use ./identifiers.nu

# Builds the app, installs it to ~/Applications, and registers and enables its file system extension. Idempotent, and does not launch the app.
def main [--release, --no-build] {
	let configuration = if $release { "Release" } else { "Debug" }
	let app_name = if $release { "Tangram" } else { "Tangram Dev" }
	let identifiers = (identifiers)
	let fskit_bundle_id = if $release { $identifiers.fskit_bundle_id.release } else { $identifiers.fskit_bundle_id.debug }

	let root = ($env.FILE_PWD | path dirname | path dirname)
	let built_app = ($root | path join "packages/macos/build" $configuration $"($app_name).app")
	let built_appex = ($root | path join "packages/macos/build" $configuration "TangramFSKit.appex")
	let installed_app = ($env.HOME | path join "Applications" $"($app_name).app")
	let build_args = if $release { ["--release"] } else { [] }

	cd $root
	if not $no_build {
		^nu ($root | path join "scripts/macos/build.nu") ...$build_args
	}
	if not ($built_app | path exists) {
		error make { msg: $"the app was not built at ($built_app)" }
	}

	# Unregister the extension from the build directory before the copy, so that
	# the only registration that survives is the installed one.
	^pluginkit -r ($built_app | path join "Contents/Extensions/TangramFSKit.appex") | complete | ignore
	^pluginkit -r $built_appex | complete | ignore

	mkdir ($installed_app | path dirname)
	rm -rf $installed_app
	^ditto $built_app $installed_app
	rm -rf $built_app
	rm -rf $built_appex

	^/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister -f -R -trusted $installed_app
	let installed_appex = ($installed_app | path join "Contents/Extensions/TangramFSKit.appex")
	^pluginkit -r $installed_appex | complete | ignore
	^pluginkit -a $installed_appex

	# Enable the file system extension; registration alone does not make it mountable.
	^pluginkit -e use -i $fskit_bundle_id | complete | ignore

	wait_until_enabled $fskit_bundle_id
	print $"installed and enabled ($fskit_bundle_id) at ($installed_app)"
}

# Polls until pluginkit reports the extension as enabled, which it marks with a leading '+'.
def wait_until_enabled [bundle_id: string] {
	for _ in 0..30 {
		let output = (^pluginkit -m -i $bundle_id | complete)
		if $output.exit_code == 0 and (($output.stdout | str trim) | str starts-with '+') {
			return
		}
		sleep 1sec
	}
	let output = (^pluginkit -m -i $bundle_id | complete)
	error make {
		msg: $"the file system extension ($bundle_id) is not enabled: ($output.stdout | str trim)
approve it in System Settings > General > Login Items & Extensions > File System Extensions"
	}
}
