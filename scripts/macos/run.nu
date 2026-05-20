#!/usr/bin/env nu

def main [--release] {
	let app_name = if $release { "Tangram" } else { "Tangram Dev" }
	let installed_app = ($env.HOME | path join "Applications" $"($app_name).app")
	let install_args = if $release { ["--release"] } else { [] }

	^nu scripts/macos/install.nu ...$install_args

	try {
		osascript -e $"tell application \"($app_name)\" to quit"
	}
	^open -n $installed_app
	# Stream the extension's log so its os_log output is visible (Ctrl-C to stop).
	^log stream --predicate 'subsystem == "tangram.fskit"' --level info
}
