(array
  "[" @open
  "]" @close)

(inline_table
  "{" @open
  "}" @close)

(table
  "[" @open
  "]" @close
  (#set! rainbow.exclude))

(table_array_element
  "[[" @open
  "]]" @close
  (#set! rainbow.exclude))

(("\"" @open
  "\"" @close)
  (#set! rainbow.exclude))

(("'" @open
  "'" @close)
  (#set! rainbow.exclude))
