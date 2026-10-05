#!/usr/bin/env nu

def main [name?: string] {
	let cwd = $env.PWD
	let zed_commit = "ddf70bc3217eb2f55f1bdf1d44d1f3d8719502e6"
	let languages = [
		{ zed: 'javascript', tangram: 'tangram-javascript', name: 'Tangram JavaScript', grammar: 'javascript', suffixes: ['tangram.js', 'tg.js'] }
		{ zed: 'python', tangram: 'tangram-python', name: 'Tangram Python', grammar: 'python', suffixes: ['tangram.py', 'tg.py'] }
		{ zed: 'typescript', tangram: 'tangram-typescript', name: 'Tangram TypeScript', grammar: 'typescript', suffixes: ['tangram.ts', 'tg.ts'] }
	]
	for language in ($languages | where { |language| $name == null or $language.tangram == $name }) {
		let api_url = $'https://api.github.com/repos/zed-industries/zed/contents/crates/languages/src/($language.zed)?ref=($zed_commit)'
		let language_path = $'($cwd)/languages/($language.tangram)'
		rm -rf $language_path
		mkdir $language_path
		let files = http get $api_url | where type == 'file'
		for file in $files {
			# Tangram modules run through Tangram, not Python or pytest launchers.
			if $language.grammar == 'python' and $file.name in ['debugger.scm', 'runnables.scm'] { continue }

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
		$config | save -f $config_path
		print -e $'modified ($config_path)'

		# Replace imports.scm with minimal query since the downloaded version uses Zed-specific node names.
		if $language.grammar in ['javascript', 'typescript'] {
			"(import_statement) @import\n" | save -f $'($language_path)/imports.scm'
		}
	}
}
