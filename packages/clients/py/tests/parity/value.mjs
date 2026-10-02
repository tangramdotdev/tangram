import * as tg from "../../../js/src/index.ts";

tg.setEncoding({
	base64: {
		encode: (value) => Buffer.from(value).toString("base64"),
		decode: (value) => new Uint8Array(Buffer.from(value, "base64")),
	},
});

const output = {};
const shared = Promise.resolve({ nested: [Promise.resolve(42)] });
output.resolve = tg.Value.toData(await tg.resolve([shared, shared]));
output.value = tg.Value.toData({
	bytes: new Uint8Array([0, 128, 255]),
	null: null,
	list: [true, 3, "text"],
});
output.template = tg.Template.toData(
	await tg.Template.join(
		":",
		null,
		Promise.resolve("before"),
		await tg.template("after"),
	),
);
output.tagged = tg.Template.toData(
	await tg.template`
  before ${Promise.resolve("middle")}
  after
`,
);
output.raw = tg.Template.toData(
	await tg.Template.raw`
  before ${"middle"}
  after
`,
);
const mutations = [
	tg.Mutation.unset(),
	await tg.Mutation.set(shared),
	await tg.Mutation.setIfUnset(null),
	await tg.Mutation.append([Promise.resolve(2)]),
	await tg.Mutation.prepend([1]),
	await tg.Mutation.prefix("before", ":"),
	await tg.Mutation.suffix("after"),
	await tg.Mutation.merge({ nested: Promise.resolve([3]) }),
];
output.mutations = mutations.map(tg.Mutation.toData);
output.append = tg.Value.toData(await mutations[3].apply([1]));
output.prefix = tg.Value.toData(await mutations[5].apply("body"));
output.null = await mutations[2].apply(null);
const target = { keep: null, remove: 2, list: [1] };
await (
	await tg.Mutation.merge({
		remove: tg.Mutation.unset(),
		list: await tg.Mutation.append([2]),
	})
).apply(target);
output.merge = tg.Value.toData(target);
const file = tg.File.withId(
	"fil_010000000000000000000000000000000000000000000000000000",
);
file.state.location = {};
file.state.tokens = { local: ["proof"] };
output.artifact = tg.Value.toData(file);
output.artifactTemplate = tg.Template.toData(
	await tg.template("prefix/", file, "/", tg.output),
);
const referent = {
	node: "node with spaces",
	options: {
		name: "same name",
		location: { name: "remote name", region: "eu" },
		tokens: { local: ["proof"] },
	},
};
output.referent = tg.Referent.toDataString(referent, (node) => node);
output.location = tg.Location.Arg.toDataString({
	components: [{}, { name: "remote name", regions: ["us", "eu"] }],
});
console.log(JSON.stringify(output));
