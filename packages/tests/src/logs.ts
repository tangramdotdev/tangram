export let js = tg.target(() => {
  console.log("hello, from js!");
  return true;
});

export let shell = tg.target(async () => {
  let script = "echo 'hello, from the shell!' ; echo '' > $OUTPUT"
  return tg.target(script);
});
