# This test verifies that when a function is resolved to a Command,
# the Command preserves the function's parameter and return types.

use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		type BuildArg = { host?: string; debug?: boolean };

		const build = async (...args: tg.Args<BuildArg>): Promise<tg.Directory> => {
			return tg.directory();
		};

		// Resolve the function and verify the Command has correct types
		export default async () => {
			const resolved = await tg.resolve(build);

			// Verify the resolved type matches what tg.Resolved computes
			const _checkType: tg.Resolved<typeof build> = resolved;

			return resolved;
		};
	'
}

tg check $path
