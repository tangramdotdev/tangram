use ../lib/test.nu *

# Downloading a URL with a wildcard checksum returns a blob with the downloaded contents.

skip_if_offline

let server = server spawn

let output = tg download "http://www.example.com" --checksum sha256:any | complete
success $output
assert ($output.stdout | str trim | str starts-with "blb_") "the download should return a blob id"

let contents = tg read ($output.stdout | str trim)
snapshot --normalize $contents '<!doctype html><html lang=en><head><meta charset=utf-8><link rel=icon href=data:,><meta name=viewport content="width=device-width,initial-scale=1"><title>Example Domain</title><style>html{color-scheme:light dark;background:light-dark(#eee,#222)}body{font:16px/1.6 system-ui,sans-serif;max-width:30em;min-height:100vh;margin:auto;padding:4.75em 2em 20vh;box-sizing:border-box;display:grid;place-content:center;text-align:center}</style></head><body><p>This domain is for use in documentation examples without needing permission. This is not a service, avoid relying on it for testing and monitoring purposes.</p><a href=https://iana.org/help/example-domains>Learn more</a><script src=/s.js></script></body></html>'
