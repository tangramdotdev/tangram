export let js = tg.target(() => {
	console.log("hello, from js!");
	return true;
});

export let shell = tg.target(async () => {
	let script = "echo 'hello, from the shell!' ; echo '' > $OUTPUT";
	return (await tg.target(script).output()).then(tg.File.expect);
});
