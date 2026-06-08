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
	let app_entitlements_path = ($path | path join "Tangram/Resources/Tangram.entitlements")
	let helper_entitlements_path = ($path | path join "Tangram/Resources/TangramHelper.entitlements")
	let signing_identity = "Apple Development: David Yamnitsky (933H7Z8N99)"

	if $release {
		cargo build --release -p tangram_cli
	} else {
		cargo build -p tangram_cli
	}

	let arch = (^uname -m | str trim)
	xcodegen generate --spec ($path | path join "project.yml")
	rm -rf $result_bundle_path
	rm -rf $app_path
	rm -rf ($path | path join "build" $configuration "TangramFSKit.appex")
	(
			xcodebuild
			-allowProvisioningDeviceRegistration
			-allowProvisioningUpdates
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
	let result = codesign --force --entitlements $helper_entitlements_path --sign $signing_identity $destination_path | complete
	if $result.exit_code != 0 {
		print --stderr $result.stderr
		exit $result.exit_code
	}
	let result = codesign --force --options runtime --entitlements $app_entitlements_path --sign $signing_identity $app_path | complete
	if $result.exit_code != 0 {
		print --stderr $result.stderr
		exit $result.exit_code
	}
}
