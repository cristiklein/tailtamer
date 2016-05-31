help:
	$(ECHO) $(MAKE) init => install Python requirements
	$(ECHO) $(MAKE) test => run test suite
	$(ECHO) $(MAKE) run  => run simulator and gather results
	$(ECHO) $(MAKE) plot => plot results

run:
	python3 -m tailtamer

plot:
	R --vanilla < plot-results.R
	xdg-open Rplots.pdf

init:
	pip3 install -r requirements.txt

test:
	python3 -m nose tests
