use ../../util/test.nu *

# Creating commands that reference a loaded directory must take time linear in the directory's entries, even when its entries hold more distinct tokens than fit in a cache.

def files [prefix: string, count: int] {
    0..<$count | each {|index| { $'f($index)': $'($prefix) ($index)' } } | into record
}

let local = server spawn

let small = tg checkin --no-tokens (artifact (files small 500)) | referent node
let large = tg checkin --no-tokens (artifact (files large 2000)) | referent node

let path = artifact {
    'tangram.py': '
        import time

        import tangram as tg


        async def default(small: tg.Directory, large: tg.Directory):
            async def measure(directory: tg.Directory, count: int) -> float:
                start = time.monotonic()
                for index in range(count):
                    template = await tg.template(t"{directory}/f{str(index)}")
                    command = await tg.command(
                        args=[template], executable="sh", host=tg.host.current
                    )
                    command.id
                return time.monotonic() - start

            # Load both directories, so that each entry holds its own token.
            await small.entries
            await large.entries

            # Warm up, then time creating the commands in alternating rounds, keeping the fastest round for each directory.
            await measure(small, 20)
            small_elapsed = float("inf")
            large_elapsed = float("inf")
            for _ in range(3):
                small_elapsed = min(small_elapsed, await measure(small, 100))
                large_elapsed = min(large_elapsed, await measure(large, 100))
            return {"large_elapsed": large_elapsed, "small_elapsed": small_elapsed}
    '
}

let output = tg build -A $small -A $large ($path | path join tangram.py) | from json
print $output
print $'ratio: ($output.large_elapsed / $output.small_elapsed)'

# The large directory has four times as many entries, so its commands should take about four times as long.
assert ($output.large_elapsed < ($output.small_elapsed * 8))
