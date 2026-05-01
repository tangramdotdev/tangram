use ../../test.nu *

let server = spawn

# Create a directory with nested structure.
let path = artifact {
	tangram.ts: r#'
        export default () => {
            return tg.graph({
                nodes: [
                    {
                        kind: "file",
                        contents: tg.blob("foo"),
                        dependencies: { "./bar": 1 },
                    },
                    {
                        kind: "file",
                        contents: tg.blob("bar"),
                        dependencies: { "./foo": 0 },
                    },
                ],
            });
        }
    '#
}

let output = tg build $path | complete
success $output

let graph = $output.stdout | str trim

let output = tg get --pretty --depth=inf $graph | complete
success $output
# Verify the output is a file ID.
snapshot $output.stdout '
	tg.graph({
	  "nodes": [
	    {
	      "kind": "file",
	      "contents": blb_01mvpyxe78tzxqkeymgte23s41m6vb93pey2v0jr8pes81h34j8bm0,
	      "dependencies": {
	        "./bar": {
	          "item": {
	            "index": 1,
	            "kind": "file",
	          },
	        },
	      },
	    },
	    {
	      "kind": "file",
	      "contents": blb_01p5qf596t7vpc0nnx8q9c5gpm3271t2cqj16yb0e5zyd880ncc3tg,
	      "dependencies": {
	        "./foo": {
	          "item": {
	            "index": 0,
	            "kind": "file",
	          },
	        },
	      },
	    },
	  ],
	})

'