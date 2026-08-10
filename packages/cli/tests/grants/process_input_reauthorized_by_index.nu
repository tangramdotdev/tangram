use ../../test.nu *

# A process is entitled to its own input by construction, but a handle built from an id alone carries no token, so the server authorizes that input through the index on every load rather than once.

let server = spawn --config {
	tracing: {
		filter: 'tangram=info,tangram_server=info,tangram_server::authorization=debug',
		stderr_format: 'json',
	},
}

let source = '
	export const measure = async (dir: tg.Directory) => {
		const id = dir.id;
		for (let i = 0; i < 10; i++) {
			await tg.Directory.withId(id).load();
		}
		return tg.file(id);
	};

	export default () => tg.build(measure, tg.directory({ "libfoo.so": tg.file("foo") }));
'

let id = tg build (artifact { tangram.ts: $source }) | str trim | tg cat $in | str trim

let walks = open --raw $server.log
	| lines
	| each { from json }
	| where { |event| $event.fields.message? == 'authorizing through the index' and $event.fields.resource? == $id }
	| length

assert ($walks <= 1) $"a process must not re-authorize its own input, but the index authorized ($id) ($walks) times"
