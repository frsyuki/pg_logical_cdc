EXE := $(shell pwd)/src/pg_logical_cdc

all: test

build:
	cd src && make

test: build
	cd test && EXE=$(EXE) ./bundlerw exec rake

clean:
	cd src && make clean
	rm -rf test/vendor/bundle
	rm -rf test/vendor/gems
	rm -rf test/.bundle
	rm -rf test/.bundlerw

docker:
	docker build --rm -t pg_logical_cdc:latest .

.PHONY: test all clean docker
