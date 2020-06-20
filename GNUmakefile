SOURCE_DIR = src
SOURCES = $(wildcard ${SOURCE_DIR}/*.[he]rl)

TAGS: $(SOURCES)
	erl -src_dir "${SOURCE_DIR}" -tag_file "$@" -noinput \
            -eval "{ok, SrcDir} = init:get_argument(src_dir), \
                   {ok, TagFile} = init:get_argument(tag_file), \
                   tags:subdir(SrcDir, [{outfile, TagFile}]), \
                   init:stop(0)."

.PHONY: TAGS.root
TAGS.root:
	erl -tag_file "$@" -noinput \
            -eval "{ok, TagFile} = init:get_argument(tag_file), \
                   tags:root([{outfile, TagFile}]), \
                   init:stop(0)."

.PHONY: watch
watch:
	ls $(SOURCE_DIR)/*.[he]rl | entr -n $(MAKE) TAGS
