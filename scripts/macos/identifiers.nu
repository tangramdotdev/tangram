#!/usr/bin/env nu

const macos_path = path self '../../packages/macos'

# Resolves the macOS identifiers from Configuration.xcconfig, applying the optional Local.xcconfig override.
export def main []: nothing -> record {
	let values = (
		parse_xcconfig ($macos_path | path join 'Configuration.xcconfig')
		| merge (parse_xcconfig ($macos_path | path join 'Local.xcconfig'))
	)
	let bundle_prefix = ($values | get --optional TANGRAM_BUNDLE_PREFIX | default 'dev.tangram')
	let app_group_name = ($values | get --optional TANGRAM_APP_GROUP_NAME | default 'dev.tangram')
	let development_team = ($values | get --optional DEVELOPMENT_TEAM | default '')
	{
		app_group_identifier: $'($development_team).($app_group_name)',
		app_group_name: $app_group_name,
		bundle_prefix: $bundle_prefix,
		development_team: $development_team,
		app_bundle_id: {
			debug: $'($bundle_prefix).Tangram.debug',
			release: $'($bundle_prefix).Tangram',
		},
		fskit_bundle_id: {
			debug: $'($bundle_prefix).Tangram.debug.filesystem',
			release: $'($bundle_prefix).Tangram.fskit',
		},
	}
}

def parse_xcconfig [path: path]: nothing -> record {
	if not ($path | path exists) {
		return {}
	}
	open --raw $path
	| lines
	| each { |line| $line | str trim }
	| where { |line|
		($line | str contains '=') and (not ($line | str starts-with '//')) and (not ($line | str starts-with '#'))
	}
	| reduce --fold {} { |line, acc|
		let parts = ($line | split row '=')
		$acc | upsert ($parts | first | str trim) ($parts | skip 1 | str join '=' | str trim)
	}
}
