.PHONY: default

DIST_BUILD=~/test/demo/tool/dist_build
ROOT=~/test/r_dist/make
ALL_TARGET=sdm tulip arch

default:clean_ws
	@$(DIST_BUILD)  -g -j2 $(ROOT)  target  rule.cfg  log
	@echo done
$(ALL_TARGET):
	@if [ ! -d obj ]; then mkdir obj; fi
	@touch obj/$@.target 
	@echo $@ >  obj/$@.target 

clean_ws:
	@git clean -df -q