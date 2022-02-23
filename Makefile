SHELL = /bin/bash
.DEFAULT_GOAL = all

GO := go
MD5 := md5
BUILD_DIR := build
MD5_DIR := $(BUILD_DIR)/md5
OS_ARCHS := linux-amd64 darwin-amd64 darwin-arm64
PROGRAMS := wrgl wrgld
BINARIES := $(foreach prog,$(PROGRAMS),$(foreach osarch,$(OS_ARCHS),$(BUILD_DIR)/$(prog)-$(osarch)/bin/$(prog)))
TAR_FILES := $(foreach osarch,$(OS_ARCHS),$(BUILD_DIR)/wrgl-$(osarch).tar.gz)

.PHONY: all clean
all: $(TAR_FILES)
clean:
	rm -rf $(BUILD_DIR)

define binary_rule =
echo "\$$(BUILD_DIR)/wrgl-$(3)-$(2)/bin/$(1): \$$(MD5_DIR)/go.sum.md5 \$$(MD5_DIR)/cmd/$(1)/VERSION.md5 \$$($(1)_SOURCES)" >> $(4) && \
echo -e "\t@-mkdir -p \$$(dir \$$@) 2>/dev/null" >> $(4) && \
echo -e "\tCGO_ENABLED=0 GOARCH=$(2) GOOS=$(3) go build -a -o \$$@ github.com/wrgl/wrgl/$(1)" >> $(4) && \
echo "" >> $(4)

endef

define program_mk_file_rule =
$(BUILD_DIR)/$(1).d: $(MD5_DIR)/$(1)/main.go.md5 | $(BUILD_DIR)
	echo "$(1)_SOURCES =" > $$@
	echo "$$$$($(GO) list -deps github.com/wrgl/wrgl/wrgl | \
		grep github.com/wrgl/wrgl/ | \
		sed -r -e 's/github.com\/wrgl\/wrgl\/(.+)/\1/g' | \
		xargs -n 1 -I {} find {} -maxdepth 1 -name '*.go' \! -name '*_test.go' -print | \
		sed -r -e 's/(.+)/$$(subst /,\/,$(1)_SOURCES += $(MD5_DIR))\/\1.md5/g')" >> $$@
	echo "" >> $$@
	$$(foreach osarch,$$(OS_ARCHS),$$(call binary_rule,$(1),$$(word 2,$$(subst -, ,$$(osarch))),$$(word 1,$$(subst -, ,$$(osarch))),$$@))
endef

define tar_rule =
$(BUILD_DIR)/wrgl-$(1).tar.gz: $$(foreach prog,$(PROGRAMS),$(BUILD_DIR)/wrgl-$(1)/bin/$$(prog)) LICENSE
	cp LICENSE $(BUILD_DIR)/wrgl-$(1)/LICENSE
	cd $(BUILD_DIR) && \
	tar -czvf $$(notdir $$@) wrgl-$(1)
endef

# calculate md5
$(MD5_DIR)/%.md5: % | $(MD5_DIR)
	@-mkdir -p $(dir $@) 2>/dev/null
	$(if $(filter-out $(shell cat $@ 2>/dev/null),$(shell $(MD5) $<)),$(MD5) $< > $@)

cmd/%/VERSION: VERSION
	cp $< $@

$(foreach prog,$(PROGRAMS),$(eval $(call program_mk_file_rule,$(prog))))

$(foreach osarch,$(OS_ARCHS),$(eval $(call tar_rule,$(osarch))))

$(BUILD_DIR): ; @-mkdir $@ 2>/dev/null
$(MD5_DIR): | $(BUILD_DIR) ; @-mkdir $@ 2>/dev/null

include $(patsubst %,$(BUILD_DIR)/%.d,$(PROGRAMS))