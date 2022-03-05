SHELL = /bin/bash
.DEFAULT_GOAL = all

GO := go
MD5 := md5
DOCKER := docker
BUILD_DIR := build
MD5_DIR := $(BUILD_DIR)/md5
OS_ARCHS := linux-amd64 darwin-amd64 darwin-arm64
PROGRAMS := wrgl wrgld
BINARIES := $(foreach prog,$(PROGRAMS),$(foreach osarch,$(OS_ARCHS),$(BUILD_DIR)/$(prog)-$(osarch)/bin/$(prog)))
TAR_FILES := $(foreach osarch,$(OS_ARCHS),$(BUILD_DIR)/wrgl-$(osarch).tar.gz)
IMAGES := $(foreach prog,$(PROGRAMS),$(patsubst %,$(BUILD_DIR)/%.image,$(prog)))
WRGLD_STATIC_ASSETS := $(wildcard cmd/wrgld/auth/oauth2/static/*) $(wildcard cmd/wrgld/auth/oauth2/templates/*)

.PHONY: all clean images
all: $(TAR_FILES)
images: $(IMAGES)
clean:
	rm -rf $(BUILD_DIR)

define binary_rule =
echo "\$$(BUILD_DIR)/wrgl-$(3)-$(2)/bin/$(1): \$$(MD5_DIR)/go.sum.md5 \$$($(1)_SOURCES)" >> $(4) && \
echo -e "\t@-mkdir -p \$$(dir \$$@) 2>/dev/null" >> $(4) && \
echo -e "\tcp VERSION cmd/$(1)/VERSION" >> $(4) && \
echo -e "\tCGO_ENABLED=0 GOARCH=$(2) GOOS=$(3) go build -a -o \$$@ github.com/wrgl/wrgl/$(1)" >> $(4) && \
echo "" >> $(4)

endef

define program_mk_file_rule =
$(BUILD_DIR)/$(1).d: | $(BUILD_DIR)
	echo "$(1)_SOURCES =" > $$@
	echo "$$$$($(GO) list -deps github.com/wrgl/wrgl/$(1) | \
		grep github.com/wrgl/wrgl/ | \
		sed -r -e 's/github.com\/wrgl\/wrgl\/(.+)/\1/g' | \
		xargs -n 1 -I {} find {} -maxdepth 1 -name '*.go' \! -name '*_test.go' -print | \
		sed -r -e 's/(.+)/$$(subst /,\/,$(1)_SOURCES += $(MD5_DIR))\/\1.md5/g')" >> $$@
	echo "" >> $$@
	$$(foreach osarch,$$(OS_ARCHS),$$(call binary_rule,$(1),$$(word 2,$$(subst -, ,$$(osarch))),$$(word 1,$$(subst -, ,$$(osarch))),$$@))
endef

define wrgld_bin_rule =
$(BUILD_DIR)/wrgl-$(1)/bin/wrgld: $$(patsubst %,$$(MD5_DIR)/%.md5,$$(WRGLD_STATIC_ASSETS))
endef

define license_rule =
$(BUILD_DIR)/wrgl-$(1)/LICENSE: LICENSE
	cp $$< $$@
endef

define tar_rule =
$(BUILD_DIR)/wrgl-$(1).tar.gz: $$(foreach prog,$(PROGRAMS),$(BUILD_DIR)/wrgl-$(1)/bin/$$(prog)) $(BUILD_DIR)/wrgl-$(1)/LICENSE
	cd $(BUILD_DIR) && \
	tar -czvf $$(notdir $$@) wrgl-$(1)
endef

# calculate md5
$(MD5_DIR)/%.md5: % | $(MD5_DIR)
	@-mkdir -p $(dir $@) 2>/dev/null
	$(if $(filter-out $(shell cat $@ 2>/dev/null),$(shell $(MD5) $<)),$(MD5) $< > $@)

$(foreach prog,$(PROGRAMS),$(eval $(call program_mk_file_rule,$(prog))))

$(foreach osarch,$(OS_ARCHS),$(eval $(call wrgld_bin_rule,$(osarch))))

$(foreach osarch,$(OS_ARCHS),$(eval $(call license_rule,$(osarch))))

$(foreach osarch,$(OS_ARCHS),$(eval $(call tar_rule,$(osarch))))

$(BUILD_DIR)/%.image: %.Dockerfile $(BUILD_DIR)/wrgl-linux-amd64/bin/% $(BUILD_DIR)/wrgl-linux-amd64/LICENSE VERSION
	$(DOCKER) build -t $*:latest -f $*.Dockerfile $(BUILD_DIR)/wrgl-linux-amd64
	$(DOCKER) tag $*:latest $*:$(file < VERSION)
	$(DOCKER) images --format '{{.ID}}' $*:latest > $@

$(BUILD_DIR): ; @-mkdir $@ 2>/dev/null
$(MD5_DIR): | $(BUILD_DIR) ; @-mkdir $@ 2>/dev/null

include $(patsubst %,$(BUILD_DIR)/%.d,$(PROGRAMS))