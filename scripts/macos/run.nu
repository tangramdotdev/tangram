#!/usr/bin/env nu

def main [--release] {
	let configuration = if $release { "Release" } else { "Debug" }
	let app_name = if $release { "Tangram" } else { "Tangram Dev" }
	let app = $"packages/macos/build/($configuration)/($app_name).app"
	let build_args = if $release { ["--release"] } else { [] }
	nu scripts/macos/build.nu ...$build_args
	try {
		osascript -e $"tell application \"($app_name)\" to quit"
	}
	^open -n $app
}
