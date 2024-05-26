test:
	@$(MAKE) -sk test-all

test-all:	test-miles

test-scripts:
	@curl -sLO https://www3.nd.edu/~pbui/teaching/cse.20289.sp24/static/txt/homework06/miles.test
	@chmod +x ./*.test

test-miles:	test-scripts miles.py
	@echo Testing miles ...
	@./miles.test -v
	@echo

clean:
	@rm -f *.test
