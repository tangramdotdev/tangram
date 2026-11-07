use ../../util/test.nu *

const interrupt_path = path self ../../util/interrupt.py

# Ctrl-C lets the child finish cleanup, then restores the parent interrupt handler.
let local = server spawn
let path = artifact {
    'tangram.ts': r#'
export default async () => {
    await tg.run({ executable: "sh", args: ["-c", "trap 'trap \"\" INT TERM; printf \"stopping one\\n\"; sleep 0.2; printf \"stopping two\\n\"; exit 0' INT\nprintf \"child ready\\n\"\nwhile :; do sleep 0.1; done"] });
    console.log("parent ready");
    await new Promise((resolve) => setTimeout(resolve, 30000));
};
'#
}
python3 $interrupt_path tg --quiet run $path
