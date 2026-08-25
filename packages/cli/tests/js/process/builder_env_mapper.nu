use ../../../test.nu *

# A command builder preserves its env mapper when creating a process builder.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.directory({ bin: {} });
			type Env = tg.Command.Arg.Env | tg.Directory;
			let process = await tg
				.command({ host: tg.host.current, executable: "echo" })
				.envMapper<Env>((env) =>
					env instanceof tg.Directory
						? { PATH: tg.template(env, "/bin") }
						: env
				)
				.spawn()
				.env(directory)
				.sandbox();
			let env = await process.env();
			let path = env.PATH;
			if (path === undefined || path.kind !== "string") {
				return false;
			}
			let value = path.value;
			return (
				value instanceof tg.Template &&
				value.components[0]?.id === directory.id &&
				value.components[1] === "/bin"
			);
		}
	'
}

let output = tg build $path
assert ($output == "true")
