GO          ?= go
GOARCH      := amd64
NAME        := elastic-vandelay
BUILD_FLAGS := -tags netgo
BIN_DIR     := ./bin

# By default, build darwin, windows, and linux binaries.
.PHONY: default
default: all

# `make all` will build darwin windows linux binaries.
.PHONY: all
all: darwin windows linux

# `make <os>` will build binary for that OS (amd64 only darwin windows linux).
.PHONY: darwin windows linux
darwin: $(BIN_DIR)/$(NAME)_darwin_amd64
windows: $(BIN_DIR)/$(NAME)_windows_amd64.exe
linux: $(BIN_DIR)/$(NAME)_linux_amd64

# Windows AMD64 build rule
$(BIN_DIR)/$(NAME)_%_$(GOARCH).exe: *.go
	$(info Building binary for $*...)
	@env CGO_ENABLED=0 GOOS=$* GOARCH=$(GOARCH) $(GO) build -o $(BIN_DIR)/$(NAME)_$*_$(GOARCH).exe $(BUILD_FLAGS) $(PACKAGE)

# Linux/Darwin AMD64 build rule
$(BIN_DIR)/$(NAME)_%_$(GOARCH): *.go
	$(info Building binary for $*...)
	@env CGO_ENABLED=0 GOOS=$* GOARCH=$(GOARCH) $(GO) build -o $(BIN_DIR)/$(NAME)_$*_$(GOARCH) $(BUILD_FLAGS) $(PACKAGE)

.PHONY: clean
clean:
	$(info Cleaning binaries...)
	@rm -rf bin
