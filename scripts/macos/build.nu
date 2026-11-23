#!/usr/bin/env nu

use ./identifiers.nu

def main [
	--cargo-target-dir: path
	--helper: path # A prebuilt tangram executable for this Mac's architecture to embed instead of building one with Cargo.
	--provider: path # A prebuilt libtangram_vfs_provider.a for this Mac's architecture to link instead of building one with Cargo.
	--release
] {
	let configuration = if $release { "Release" } else { "Debug" }
	let profile = if $release { "release" } else { "debug" }
	let app_name = if $release { "Tangram" } else { "Tangram Dev" }

	let root = ($env.FILE_PWD | path dirname | path dirname)
	let cargo_target_dir = ($cargo_target_dir | default ($root | path join "target") | path expand)
	let path = ($root | path join "packages/macos")
	let app_path = ($path | path join "build" $configuration $"($app_name).app")
	let derived_data_path = ($path | path join "build/DerivedData")
	let prebuilt = $helper != null or $provider != null
	if $prebuilt and ($helper == null or $provider == null) {
		error make {msg: '--helper and --provider must be passed together'}
	}
	let library_search_path = if $prebuilt { $path | path join "build/Libraries" } else { $cargo_target_dir | path join $profile }
	let project = ($path | path join "Tangram.xcodeproj")
	let project_spec = ($path | path join "project.yml")
	let result_bundle_path = ($path | path join "build/Tangram.xcresult")
	let source_path = if $prebuilt { $helper | path expand } else { $cargo_target_dir | path join $profile "tangram" }
	let provider_path = if $prebuilt { $provider | path expand } else { null }
	let doctor = ($root | path join "scripts/macos/doctor.nu")

	cd $root
	if $prebuilt {
		^nu $doctor --prebuilt --signing
	} else {
		^nu $doctor --signing
	}

	$env.CARGO_TARGET_DIR = $cargo_target_dir

	# xcodegen does not follow the xcconfig's optional include of Local.xcconfig, so the resolved team is passed to it here. Otherwise the generated project provisions against the committed default team while the build settings use the local one.
	$env.TANGRAM_DEVELOPMENT_TEAM = (identifiers).development_team

	let arch = (^uname -m | str trim)
	if $prebuilt {
		for file in [$source_path $provider_path] {
			if ($file | path type) != file {
				error make {msg: $'the prebuilt file was not found at ($file)'}
			}
			if $arch not-in (^lipo -archs $file | str trim | split row ' ') {
				error make {msg: $'($file) is not built for ($arch)'}
			}
		}

		# Give the prebuilt provider library the name that the extension links.
		mkdir $library_search_path
		^install -m 644 $provider_path ($library_search_path | path join libtangram_vfs_provider.a)
	} else if $release {
		cargo build --release -p tangram_cli -p tangram_vfs_provider
	} else {
		cargo build -p tangram_cli -p tangram_vfs_provider
	}

	xcodegen generate --spec $project_spec
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
			$"ARCHS=($arch)"
			$"LIBRARY_SEARCH_PATHS=($library_search_path)"
			SYMROOT=($path | path join "build")
			$"TANGRAM_HELPER_PATH=($source_path)"
			build
	)
	codesign --verify --deep --strict --verbose=2 $app_path
}
