#!/usr/bin/env nu

def main [--release] {
	let configuration = if $release { "Release" } else { "Debug" }
	let profile = if $release { "release" } else { "debug" }
	let app_name = if $release { "Tangram" } else { "Tangram Dev" }

	let root = ($env.FILE_PWD | path dirname | path dirname)
	let path = ($root | path join "packages/macos")
	let app_path = ($path | path join "build" $configuration $"($app_name).app")
	let derived_data_path = ($path | path join "build/DerivedData")
	let project = ($path | path join "Tangram.xcodeproj")
	let result_bundle_path = ($path | path join "build/Tangram.xcresult")
	let source_path = ($root | path join "target" $profile "tangram")
	let destination_path = ($app_path | path join "Contents/Helpers/tangram")

	if $release {
		cargo build --release --all-features -p tangram_cli
	} else {
		cargo build --all-features -p tangram_cli
	}

	let arch = (^uname -m | str trim)
	xcodegen generate --spec ($path | path join "project.yml")
	(
		xcodebuild
			-configuration $configuration
			-derivedDataPath $derived_data_path
			-destination $"platform=macOS,arch=($arch)"
			-project $project
			-quiet
			-resultBundlePath $result_bundle_path
			-scheme Tangram
			-showBuildTimingSummary
			SYMROOT=($path | path join "build")
			build
	)

	mkdir ($destination_path | path dirname)
	cp $source_path $destination_path
	chmod +x $destination_path
	let result = codesign --force --sign - $destination_path | complete
	if $result.exit_code != 0 {
		print --stderr $result.stderr
		exit $result.exit_code
	}
	let result = codesign --force --deep --sign - $app_path | complete
	if $result.exit_code != 0 {
		print --stderr $result.stderr
		exit $result.exit_code
	}
}
