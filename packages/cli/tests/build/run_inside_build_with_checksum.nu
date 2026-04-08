use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			return await inner();
		}

		export const throwError = async () => {
			await inner();
			throw new Error("broken");
		}

		export const inner = async () => {
			return await tg.run(tg`
				echo "hello stdout"
				echo "" > ${tg.output}
			`, { checksum: "sha256:3743be7f70d041f1f049134d69ac50e0881627ba6176907ff043c33941ce80eb" })
			.sandbox()
		}
	'
}

cd $path

let output = tg build '.#throwError' | complete
failure $output

let output = tg build . | complete
success $output
