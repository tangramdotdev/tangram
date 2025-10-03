for (let i = 0; i < 1000; i++) {
	await Bun.$`/usr/bin/printf 'hello\n'`;
}
