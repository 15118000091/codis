.DEFAULT_GOAL := build-all

build-all: codis-server codis-dashboard codis-proxy codis-admin codis-ha codis-fe

codis-deps:
	@mkdir -p bin && bash version

codis-dashboard: codis-deps
	go build -i -o bin/codis-dashboard ./cmd/dashboard

codis-proxy: codis-deps
	go build -i -o bin/codis-proxy ./cmd/proxy

codis-admin: codis-deps
	go build -i -o bin/codis-admin ./cmd/admin

codis-ha: codis-deps
	go build -i -o bin/codis-ha ./cmd/ha

codis-fe: codis-deps
	go build -i -o bin/codis-fe ./cmd/fe
	@rm -rf bin/assets; cp -rf cmd/fe/assets bin/

codis-server:
	@mkdir -p bin
	make -j4 -C extern/redis-2.8.21/ --no-print-directory
	@rm -f bin/codis-server
	@cp -f extern/redis-2.8.21/src/redis-server bin/codis-server
	@cp -f extern/redis-2.8.21/src/redis-benchmark bin/
	@cp -f extern/redis-2.8.21/src/redis-cli bin/

clean:
	@rm -rf bin

distclean: clean
	@make --no-print-directory --quiet -C extern/redis-2.8.21 clean

gotest: codis-deps
	go test ./cmd/... ./pkg/...

gobench:
	go test -v -bench=. ./pkg/proxy/...

docker:
	docker build --force-rm -t codis-image .
