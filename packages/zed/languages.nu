#!/usr/bin/env nu

def main [name?: string] {
	let cwd = $env.PWD
	let zed = { repository: 'zed-industries/zed', commit: 'ddf70bc3217eb2f55f1bdf1d44d1f3d8719502e6' }
	let toml = { repository: 'zed-extensions/toml', commit: '8e18e5045841f0f265f9c8b89f138c7f7dd09e2e' }
	let languages = [
		{ source: ($zed | merge { path: 'crates/languages/src/javascript' }), tangram: 'tangram-javascript', name: 'Tangram JavaScript', grammar: 'javascript', suffixes: ['tangram.js', 'tg.js'] }
		{ source: ($zed | merge { path: 'crates/languages/src/json' }), tangram: 'tangram-interface-json', name: 'Tangram Interface JSON', grammar: 'json', suffixes: ['tgi.json'] }
		{ source: ($zed | merge { path: 'crates/languages/src/python' }), tangram: 'tangram-interface-python', name: 'Tangram Interface Python', grammar: 'python', suffixes: ['tgi.py'] }
		{ source: ($zed | merge { path: 'crates/languages/src/typescript' }), tangram: 'tangram-interface-typescript', name: 'Tangram Interface TypeScript', grammar: 'typescript', suffixes: ['tgi.ts'] }
		{ source: ($zed | merge { path: 'crates/languages/src/json' }), tangram: 'tangram-json', name: 'Tangram JSON', grammar: 'json', suffixes: ['tangram.json', 'tg.json'] }
		{ source: ($zed | merge { path: 'crates/languages/src/python' }), tangram: 'tangram-python', name: 'Tangram Python', grammar: 'python', suffixes: ['tangram.py', 'tg.py'] }
		{ source: ($toml | merge { path: 'languages/toml' }), tangram: 'tangram-toml', name: 'Tangram TOML', grammar: 'toml', suffixes: ['tangram.toml', 'tg.toml'] }
		{ source: ($zed | merge { path: 'crates/languages/src/typescript' }), tangram: 'tangram-typescript', name: 'Tangram TypeScript', grammar: 'typescript', suffixes: ['tangram.ts', 'tg.ts'] }
		{ source: ($zed | merge { path: 'crates/languages/src/yaml' }), tangram: 'tangram-yaml', name: 'Tangram YAML', grammar: 'yaml', suffixes: ['tangram.yaml', 'tg.yaml'] }
	]
	for language in ($languages | where { |language| $name == null or $language.tangram == $name }) {
		let source = $language.source
		let api_url = $'https://api.github.com/repos/($source.repository)/contents/($source.path)?ref=($source.commit)'
		let language_path = $'($cwd)/languages/($language.tangram)'
		rm -rf $language_path
		mkdir $language_path
		let files = http get $api_url | where type == 'file'
		for file in $files {
			# Tangram modules run through Tangram, not Python or pytest launchers, and interface files and configs do not run at all.
			if $language.grammar == 'python' and $file.name in ['debugger.scm', 'runnables.scm'] { continue }
			if ($language.tangram | str starts-with 'tangram-interface') and $file.name in ['debugger.scm', 'runnables.scm'] { continue }
			if $language.grammar in ['json', 'toml', 'yaml'] and $file.name in ['runnables.scm'] { continue }

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
		let config = if $language.grammar == 'python' {
			let config = $config
			| str replace --regex '(?m)^first_line_pattern = .*\n' ''
			| str replace --regex '(?m)^debuggers = .*\n' ''
			| str replace --regex '(?m)^import_path_strip_regex = .*\n' ''
			$"tab_size = 4\n($config)"
		} else { $config }
		let config = if ($language.tangram | str starts-with 'tangram-interface') {
			$config
			| str replace --regex '(?m)^first_line_pattern = .*\n' ''
			| str replace --regex '(?m)^debuggers = .*\n' ''
		} else { $config }
		$config | save -f $config_path
		print -e $'modified ($config_path)'

		# Replace imports.scm with minimal query since the downloaded version uses Zed-specific node names.
		if $language.grammar in ['javascript', 'typescript'] {
			"(import_statement) @import\n" | save -f $'($language_path)/imports.scm'
		}
	}
}
