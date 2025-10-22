#!/usr/bin/env nu

def main [] {
    let cwd = $env.PWD
    let files = [
        "highlights.scm"
        "injections.scm"
        "indents.scm"
        "brackets.scm"
        "outline.scm"
        "embedding.scm"
        "imports.scm"
        "overrides.scm"
        "runnables.scm"
        "textobjects.scm"
        "contexts.scm"
    ]
    let languages = {
        javascript: "tangram-javascript"
        typescript: "tangram-typescript"
    }
    for lang in ($languages | transpose zed tangram) {
        let zed_name = $lang.zed
        let tangram_name = $lang.tangram
        let url = $"https://raw.githubusercontent.com/zed-industries/zed/main/crates/languages/src/($zed_name)"
        let language_path = $"($cwd)/languages/($tangram_name)"
        mkdir $language_path
        for file in $files {
            let url = $"($url)/($file)"
            let output_path = $"($language_path)/($file)"
            try {
                http get $url | save -f $output_path
                print $"downloaded ($url)"
            } catch {
                print $"failed to download ($url)"
            }
        }
    }
}
