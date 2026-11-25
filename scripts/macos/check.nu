#!/usr/bin/env nu

use ./identifiers.nu

def main [] {
	let root = ($env.FILE_PWD | path dirname | path dirname)
	let path = ($root | path join "packages/macos")
	let app_path = ($path | path join "build/Check/Debug/Tangram Dev.app")
	let derived_data_path = ($path | path join "build/CheckDerivedData")
	let project = ($path | path join "Tangram.xcodeproj")
	let result_bundle_path = ($path | path join "build/TangramCheck.xcresult")
	let source_path = ($root | path join "target/debug/tangram")
	let doctor = ($root | path join "scripts/macos/doctor.nu")

	cd $root
	^nu $doctor

	$env.TANGRAM_DEVELOPMENT_TEAM = (identifiers).development_team

	^cargo build -p tangram_cli -p tangram_vfs_provider
	^xcodegen generate --spec ($path | path join "project.yml")

	let arch = (^uname -m | str trim)
	rm -rf $result_bundle_path
	rm -rf $app_path
	(
		xcodebuild
			-configuration Debug
			-derivedDataPath $derived_data_path
			-destination $"platform=macOS,arch=($arch)"
			-project $project
			-quiet
			-resultBundlePath $result_bundle_path
			-scheme Tangram
			-showBuildTimingSummary
			$"ARCHS=($arch)"
			CODE_SIGNING_ALLOWED=NO
			ONLY_ACTIVE_ARCH=YES
			SYMROOT=($path | path join "build/Check")
			$"TANGRAM_HELPER_PATH=($source_path)"
			build
	)

	let helper_path = ($app_path | path join "Contents/Helpers/tangram")
	if not ($helper_path | path exists) {
		error make { msg: "the unsigned app does not contain the Tangram helper" }
	}
	print $"the unsigned macOS app compiled successfully at ($app_path)"
}
