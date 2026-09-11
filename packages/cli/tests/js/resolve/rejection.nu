use ../../../test.nu *

# Rejections handled within the microtask checkpoint do not fail the process.
let server = server spawn
let path = artifact {
    tangram.ts: '
        export default async function () {
            await Promise.reject(new Error("handled")).catch(() => {});
            await Promise.resolve().then(async () => {
                throw new Error("adopted");
            }).catch(() => {});
            await tg.sleep(0.01);
            return "ok";
        }
    '
}
let output = tg run --sandbox $path | from json
assert equal $output "ok"

# Handling another rejection must not hide an unhandled rejection.
let path = artifact {
    tangram.ts: '
        export default async function () {
            void Promise.reject(new Error("unhandled rejection"));
            await Promise.reject(new Error("handled")).catch(() => {});
            await tg.sleep(0.01);
        }
    '
}
let output = tg run --sandbox $path | complete
failure $output
assert ($output.stderr | str contains "unhandled rejection")
