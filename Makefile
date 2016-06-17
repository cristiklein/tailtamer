help:
	$(ECHO) $(MAKE) init => install Python requirements
	$(ECHO) $(MAKE) test => run test suite
	$(ECHO) $(MAKE) run  => run simulator and gather results
	$(ECHO) $(MAKE) plot => plot results
	$(ECHO) $(MAKE) view => view results

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

.PHONY: init test run plot view
