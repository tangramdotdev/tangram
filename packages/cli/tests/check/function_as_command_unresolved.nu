# This test verifies that tg.Unresolved<tg.Command<A, R>> properly accepts functions of the correct type.
use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		type Arg = { host?: string };

		const build = async (...args: tg.Args<Arg>): Promise<tg.Directory> => {
			return tg.directory();
		};

		type EnvArg = tg.Command<Array<Arg>, tg.Directory> | tg.Directory;

		const env = async (...args: tg.Args<EnvArg>) => tg.directory();

		export default async () => {
			return env(build);
		};
	'
}

tg check $path
