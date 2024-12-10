export default tg.target(async () => {

	console.log("here!");
	let target = await tg.target("set -x && echo 'hi' > $OUTPUT");

	console.log("target id", await target.id());
	console.log("target args", await target.args());
	console.log("target env", await target.env());
	console.log("target executable", await target.executable());
	let output = await target.output();
	return output;
});
