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
WRGL_TAR_FILES := $(foreach osarch,$(OS_ARCHS),$(BUILD_DIR)/wrgl-$(osarch).tar.gz)
WRGLD_TAR_FILES := $(foreach osarch,$(OS_ARCHS),$(BUILD_DIR)/wrgld-$(osarch).tar.gz)
IMAGES := $(BUILD_DIR)/wrgld.image
WRGLD_STATIC_ASSETS := $(wildcard wrgld/pkg/auth/oauth2/static/*) $(wildcard wrgld/pkg/auth/oauth2/templates/*)

.PHONY: all clean images
all: $(WRGL_TAR_FILES) $(WRGLD_TAR_FILES)
images: $(IMAGES)
clean:
	rm -rf $(BUILD_DIR)

define binary_rule =
echo "\$$(BUILD_DIR)/$(1)-$(3)-$(2)/bin/$(1): \$$(MD5_DIR)/go.sum.md5 VERSION \$$($(1)_SOURCES)" >> $(4) && \
echo -e "\t@-mkdir -p \$$(dir \$$@) 2>/dev/null" >> $(4) && \
echo -e '\tcp VERSION $(if $(findstring wrgld,$(1)),$(1)/cmd,cmd/$(1))/VERSION' >> $(4) && \
(if [ "$(3)" == "linux" ]; then \
  echo -e "\tenv CC=x86_64-linux-musl-gcc CXX=x86_64-linux-musl-g++ GOARCH=amd64 GOOS=linux CGO_ENABLED=1 go build -ldflags \"-linkmode external -extldflags -static\" -a -o \$$@ github.com/wrgl/wrgl/$(1)" >> $(4); \
else \
  echo -e "\tCGO_ENABLED=1 GOARCH=$(2) GOOS=$(3) go build -a -o \$$@ github.com/wrgl/wrgl/$(1)" >> $(4); \
fi) && \
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
$(BUILD_DIR)/wrgld-$(1)/bin/wrgld: $$(patsubst %,$$(MD5_DIR)/%.md5,$$(WRGLD_STATIC_ASSETS))
endef

define license_rule =
$(BUILD_DIR)/wrgl-$(1)/LICENSE: LICENSE
	cp $$< $$@

$(BUILD_DIR)/wrgld-$(1)/LICENSE: wrgld/LICENSE
	cp $$< $$@
endef

define tar_rule =
$(BUILD_DIR)/$(1)-$(2).tar.gz: $(BUILD_DIR)/$(1)-$(2)/bin/$(1) $(BUILD_DIR)/$(1)-$(2)/LICENSE
	cd $(BUILD_DIR) && \
	tar -czvf $$(notdir $$@) $(1)-$(2)
endef

# calculate md5
$(MD5_DIR)/%.md5: % | $(MD5_DIR)
	@-mkdir -p $(dir $@) 2>/dev/null
	$(if $(filter-out $(shell cat $@ 2>/dev/null),$(shell $(MD5) $<)),$(MD5) $< > $@)

$(foreach prog,$(PROGRAMS),$(eval $(call program_mk_file_rule,$(prog))))

$(foreach osarch,$(OS_ARCHS),$(eval $(call wrgld_bin_rule,$(osarch))))

$(foreach osarch,$(OS_ARCHS),$(eval $(call license_rule,$(osarch))))

$(foreach prog,$(PROGRAMS),$(foreach osarch,$(OS_ARCHS),$(eval $(call tar_rule,$(prog),$(osarch)))))

$(BUILD_DIR)/wrgld.image: wrgld.Dockerfile $(BUILD_DIR)/wrgld-linux-amd64/bin/wrgld $(BUILD_DIR)/wrgld-linux-amd64/LICENSE VERSION
	$(DOCKER) build -t wrgld:latest -f wrgld.Dockerfile $(BUILD_DIR)/wrgld-linux-amd64
	$(DOCKER) tag wrgld:latest wrgld:$(file < VERSION)
	$(DOCKER) images --format '{{.ID}}' wrgld:latest > $@

$(BUILD_DIR): ; @-mkdir $@ 2>/dev/null
$(MD5_DIR): | $(BUILD_DIR) ; @-mkdir $@ 2>/dev/null

include $(patsubst %,$(BUILD_DIR)/%.d,$(PROGRAMS))