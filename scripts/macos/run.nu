#!/usr/bin/env nu

def main [
	--helper: path # A prebuilt tangram executable to embed instead of building one with Cargo.
	--provider: path # A prebuilt libtangram_vfs_provider.a to link instead of building one with Cargo.
	--release
] {
	let app_name = if $release { "Tangram" } else { "Tangram Dev" }
	let installed_app = ($env.HOME | path join "Applications" $"($app_name).app")
	let release_args = if $release { ["--release"] } else { [] }
	let helper_args = if $helper != null { ["--helper" ($helper | path expand)] } else { [] }
	let provider_args = if $provider != null { ["--provider" ($provider | path expand)] } else { [] }
	let install_args = [...$release_args ...$helper_args ...$provider_args]

	^nu scripts/macos/install.nu ...$install_args

	try {
		osascript -e $"tell application \"($app_name)\" to quit"
	}
	^open -n $installed_app
	# Stream the extension's log so its os_log output is visible (Ctrl-C to stop).
	^log stream --predicate 'subsystem == "tangram.fskit"' --level info
}
