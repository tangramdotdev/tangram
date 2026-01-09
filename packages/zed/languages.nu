#!/usr/bin/env nu

def main [] {
	let cwd = $env.PWD
	let zed_commit = "ddf70bc3217eb2f55f1bdf1d44d1f3d8719502e6"
	let languages = [
		{ zed: 'javascript', tangram: 'tangram-javascript', name: 'Tangram JavaScript', grammar: 'javascript', suffixes: ['tangram.js', 'tg.js'] }
		{ zed: 'typescript', tangram: 'tangram-typescript', name: 'Tangram TypeScript', grammar: 'typescript', suffixes: ['tangram.ts', 'tg.ts'] }
	]
	for language in $languages {
		let api_url = $'https://api.github.com/repos/zed-industries/zed/contents/crates/languages/src/($language.zed)?ref=($zed_commit)'
		let language_path = $'($cwd)/languages/($language.tangram)'
		rm -rf $language_path
		mkdir $language_path
		let files = http get $api_url | where type == 'file'
		for file in $files {
			let output_path = $'($language_path)/($file.name)'
			http get $file.download_url | save -f $output_path
			print -e $'downloaded ($file.download_url)'
		}

		# Modify the config.toml to use Tangram-specific settings.
		let config_path = $'($language_path)/config.toml'
		let config = open --raw $config_path
		let config = $config | str replace --regex '(?m)^name = ".*"' $'name = "($language.name)"'
		let config = $config | str replace --regex '(?m)^grammar = ".*"' $'grammar = "($language.grammar)"'
		let suffixes = $language.suffixes | each { |s| $'    "($s)",' } | str join "\n"
		let config = $config | str replace --regex '(?s)path_suffixes = \[.*?\]' $"path_suffixes = [\n($suffixes)\n]"
		$config | save -f $config_path
		print -e $'modified ($config_path)'

		# Replace imports.scm with minimal query since the downloaded version uses Zed-specific node names.
		"(import_statement) @import\n" | save -f $'($language_path)/imports.scm'
	}
}
