release:
	@echo "[+] releasing $(VERSION)"
	@echo "[+] re-generating"
	@echo "[+] building"
	@$(MAKE) build
	@echo "[+] comitting"
	@git tag $(VERSION)
	@git release $(VERSION)
	@echo "[+] complete"
.PHONY: release

test:
	@go test
.PHONY: test

build:
	@gox -os="linux darwin" ./...
	@mv cmd_darwin_386 s3f_darwin_386
	@mv cmd_darwin_amd64 s3f_darwin_amd64
	@mv cmd_linux_386 s3f_linux_386
	@mv cmd_linux_amd64 s3f_linux_amd64
	@mv cmd_linux_arm s3f_linux_arm
.PHONY: build

clean:
	@git clean -f
.PHONY: clean
