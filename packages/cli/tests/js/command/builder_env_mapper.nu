use ../../../test.nu *

# The builder's env mapper expands the types accepted by the env method.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.directory({ bin: {} });
			type Env = tg.Command.Arg.Env | tg.Directory;
			let command = await tg
				.command({ host: tg.host.current, executable: "echo" })
				.envMapper<Env>((env) =>
					env instanceof tg.Directory
						? { PATH: tg.template(env, "/bin") }
						: env
				)
				.env({ FOO: "bar" })
				.env(directory);
			let env = await command.env;
			let path = env.PATH;
			if (path === undefined || path.kind !== "string") {
				return false;
			}
			let value = path.value;
			return (
				env.FOO?.value === "bar" &&
				value instanceof tg.Template &&
				value.components[0]?.id === directory.id &&
				value.components[1] === "/bin"
			);
		}
	'
}

let output = tg build $path
assert ($output == "true")
