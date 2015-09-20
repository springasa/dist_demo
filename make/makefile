.PHONY: default

DIST_BUILD=~/test/demo/tool/dist_build
ROOT=~/test/r_dist/make
ALL_TARGET=sdm tulip arch

default:clean_ws
	@$(DIST_BUILD)  -j2 $(ROOT)  target  rule.cfg  log
	@echo done
$(ALL_TARGET):
	@[ ! -d target ]  && mkdir -p target
	@touch target/$@.target 
	@echo $@ >  target/$@.target 

clean_ws:
	@git clean -df -q