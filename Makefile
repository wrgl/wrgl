SHELL = /bin/bash
.DEFAULT_GOAL = all

GO := go
MD5 := md5
DOCKER := docker
BUILD_DIR := build
MD5_DIR := $(BUILD_DIR)/md5
OS_ARCHS := linux-amd64 darwin-amd64 darwin-arm64
BINARIES := $(foreach osarch,$(OS_ARCHS),$(BUILD_DIR)/wrgl-$(osarch)/bin/wrgl)
WRGL_TAR_FILES := $(foreach osarch,$(OS_ARCHS),$(BUILD_DIR)/wrgl-$(osarch).tar.gz)

.PHONY: all clean
all: $(WRGL_TAR_FILES)
clean:
	rm -rf $(BUILD_DIR)

define binary_rule =
echo "\$$(BUILD_DIR)/wrgl-$(2)-$(1)/bin/wrgl: \$$(MD5_DIR)/go.sum.md5 \$$(wrgl_SOURCES)" >> $(3) && \
echo -e "\t@-mkdir -p \$$(dir \$$@) 2>/dev/null" >> $(3) && \
(if [ "$(2)" == "linux" ]; then \
  echo -e "\tenv CC=x86_64-linux-musl-gcc CXX=x86_64-linux-musl-g++ GOARCH=amd64 GOOS=linux CGO_ENABLED=1 go build -ldflags \"-linkmode external -extldflags -static\" -a -o \$$@ github.com/wrgl/wrgl" >> $(3); \
else \
  echo -e "\tCGO_ENABLED=1 GOARCH=$(1) GOOS=$(2) go build -a -o \$$@ github.com/wrgl/wrgl" >> $(3); \
fi) && \
echo "" >> $(3)

endef

$(BUILD_DIR)/wrgl.d: | $(BUILD_DIR)
	echo "wrgl_SOURCES =" > $@
	echo "$$($(GO) list -deps github.com/wrgl/wrgl | \
		grep github.com/wrgl/ | \
		sed -r -e 's/github.com\/wrgl\/(.+)/\1/g' | \
		xargs -n 1 -I {} find {} -maxdepth 1 -name '*.go' \! -name '*_test.go' -print | \
		sed -r -e 's/(.+)/$(subst /,\/,wrgl_SOURCES += $(MD5_DIR))\/\1.md5/g')" >> $@
	echo "" >> $@
	$(foreach osarch,$(OS_ARCHS),$(call binary_rule,$(word 2,$(subst -, ,$(osarch))),$(word 1,$(subst -, ,$(osarch))),$@))

define license_rule =
$(BUILD_DIR)/wrgl-$(1)/LICENSE: LICENSE
	cp $$< $$@
endef

define tar_rule =
$(BUILD_DIR)/wrgl-$(1).tar.gz: $(BUILD_DIR)/wrgl-$(1)/bin/wrgl $(BUILD_DIR)/wrgl-$(1)/LICENSE
	cd $(BUILD_DIR) && \
	tar -czvf $$(notdir $$@) wrgl-$(1)
endef

# calculate md5
$(MD5_DIR)/%.md5: % | $(MD5_DIR)
	@-mkdir -p $(dir $@) 2>/dev/null
	$(if $(filter-out $(shell cat $@ 2>/dev/null),$(shell $(MD5) $<)),$(MD5) $< > $@)

$(foreach osarch,$(OS_ARCHS),$(eval $(call wrgld_bin_rule,$(osarch))))

$(foreach osarch,$(OS_ARCHS),$(eval $(call license_rule,$(osarch))))

$(foreach osarch,$(OS_ARCHS),$(eval $(call tar_rule,$(osarch))))

$(BUILD_DIR): ; @-mkdir $@ 2>/dev/null
$(MD5_DIR): | $(BUILD_DIR) ; @-mkdir $@ 2>/dev/null

include $(BUILD_DIR)/wrgl.d