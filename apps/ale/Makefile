APP_SRC    := src/ale.app.src
APP_SRC_IN := $(APP_SRC).in

define get_version
`([ -f VERSION ] && cat VERSION) || git describe --always`
endef

.PHONY: all clean dist shell

all: $(APP_SRC)
	@./rebar compile

$(APP_SRC): $(APP_SRC_IN)
	@TMPFILE=$(APP_SRC).$$$$ && \
         VERSION=$(call get_version) && \
           sed "s/@ALE_VERSION@/$${VERSION}/g" $< > "$${TMPFILE}" && \
           mv "$${TMPFILE}" "$@"

clean:
	@rm -f "$(APP_SRC)" "$(VERSION)"
	@./rebar clean
	@rm -rf dist/ ebin/

dist:
	@rm -rf $@
	@mkdir -p $@
	@git archive --format=tar --prefix=ale/ HEAD | (cd $@ && tar xf -)
	@VERSION=$(call get_version) && echo $${VERSION} > $@/ale/VERSION

shell:
	@erl -pa ebin
