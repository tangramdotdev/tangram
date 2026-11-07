use ../../util/test.nu *

const interrupt_path = path self ../../util/interrupt.py

# The Node.js client also waits for Ctrl-C cleanup and restores the parent handler.
const javascript_path = path self '../../../javascript'
let local = server spawn
cd $javascript_path
let source = r#'
import * as tg from "@tangramdotdev/client";
const env = Object.fromEntries(Object.entries(process.env).filter(([, value]) => value !== undefined));
tg.setProcess({ args: [], cwd: process.cwd(), env, executable: process.execPath });
await tg.run({ executable: "sh", args: ["-c", "trap 'trap \"\" INT TERM; printf \"stopping one\\n\"; sleep 0.2; printf \"stopping two\\n\"; exit 0' INT\nprintf \"child ready\\n\"\nwhile :; do sleep 0.1; done"] });
console.log("parent ready");
await new Promise((resolve) => setTimeout(resolve, 30000));
'#

for setup in ['' 'process.on("SIGINT", () => process.exit(130));'] {
    python3 $interrupt_path node --input-type=module -e ($setup + $source)
}
