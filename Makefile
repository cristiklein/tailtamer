help:
	$(ECHO) $(MAKE) init => install Python requirements
	$(ECHO) $(MAKE) test => run test suite
	$(ECHO) $(MAKE) run  => run simulator and gather results
	$(ECHO) $(MAKE) plot => plot results

run:
	python3 -OO -m tailtamer

plot: plot-results.R *.csv
	R --vanilla < plot-results.R

view: plot
	xdg-open results-ar.csv.pdf
	xdg-open results-deg.csv.pdf
	xdg-open results-mul.csv.pdf
	xdg-open results-var.csv.pdf

init:
	pip3 install -r requirements.txt

test:
	python3 -m nose tests
