use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export const outer = async () => {
            return tg.build(inner);
        }

        export const inner = async () => {
            tg.run(tg`
                echo "hello stdout"
                echo "" > ${tg.output}
            `)
        }
	'
}

cd $path
let output = tg build '.#outer' | complete
success $output
