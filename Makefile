release:
	@echo "[+] releasing $(VERSION)"
	@echo "[+] re-generating"
	@echo "[+] building"
	@$(MAKE) build
	@echo "[+] comitting"
	@git tag $(VERSION)
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

builddocker:
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main cmd/gofakes3/main.go
	docker build -t gofakes3 .

clean:
	@git clean -f
.PHONY: clean
