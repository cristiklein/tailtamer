help:
	@echo "make init => install Python requirements"
	@echo "make test => run test suite"
	@echo "make run  => run simulator and write results"
	@echo "make plot => plot results"
	@echo "make view => view results"

init: .init.timestamp
	
.init.timestamp: requirements.txt
	pip3 install -r $^
	touch $@

test:
	python3 -m nose tests

run: .run.timestamp

.run.timestamp: init tailtamer.py
	python3 -OO -m tailtamer

plot: .plot.timestamp
	
.plot.timestamp: plot-results.R *.csv
	R --vanilla < plot-results.R
	touch $@

view: plot
	pdftk *.csv*pdf output results.pdf
	evince results.pdf

.PHONY: help init test run plot view
