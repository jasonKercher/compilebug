all: debug
SRC = $(wildcard ./*.odin)

clean:
	-@rm -v streamql

vet: $(SRC)
	odin build ./ -out:streamql -debug -opt:0 -vet

debug: $(SRC)
	odin build ./ -out:streamql -opt:0

release: $(SRC)
	odin build ./ -out:streamql -o:speed

