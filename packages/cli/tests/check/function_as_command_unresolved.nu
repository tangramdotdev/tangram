# This test verifies that tg.Unresolved<tg.Command<..., tg.Directory>> properly
# accepts functions that return tg.Directory. Functions passed where Commands
# are expected will be wrapped into Commands at runtime.

use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		type Arg = { host?: string };

		const build = async (...args: tg.Args<Arg>): Promise<tg.Directory> => {
			return tg.directory();
		};

		type EnvArg = tg.Command<Array<tg.Value>, tg.Directory> | tg.Directory;

		const env = async (...args: tg.Args<EnvArg>) => {};

		export default async () => {
			return env(build);
		};
	'
}

tg check $path
