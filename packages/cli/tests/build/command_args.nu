use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'tangram.ts': '
        export const child = (...args: Array<tg.Value>) => args;
        export default async function () {
            for (const flag of ["-a", "-A"]) {
                const value = flag === "-a" ? tg.Command.Value.string("raw") : tg.Command.Value.value(42);
                const expected = [flag, flag === "-a" ? "raw" : 42];
                const initial = await tg.command(child).arg(tg.Command.Value.string(flag), value).build();
                const fluent = await tg.command(child).build().arg(tg.Command.Value.string(flag), value);
                const handoff = await tg.command(child).arg(tg.Command.Value.string(flag), value).build().run();
                tg.assert(JSON.stringify(initial) === JSON.stringify(expected));
                tg.assert(JSON.stringify(fluent) === JSON.stringify(expected));
                tg.assert(JSON.stringify(handoff) === JSON.stringify(expected));
            }
            return "command arguments preserved";
        }
    '
}
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'command arguments preserved'
