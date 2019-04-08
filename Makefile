.PHONY: all clean

DC ?= dmd

all: build

build:
	$(DC) -O -of=dupecheck dupecheck.d

clean:
	$(RM) *.o dupecheck
