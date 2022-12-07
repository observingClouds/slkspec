# makefile used for testing
#
#
all: install test

.PHONY: docs
install:
	python3 -m pip install -e .[tests]

test:
	python3 -m pytest -vv $(PWD)/slkspec/tests

test_coverage:
	python3 -m pytest -vv \
	    --cov=$(PWD)/slkspec --cov-report html:coverage_report \
		--cov-report=xml --junitxml report.xml
	rm -rf '='
	python3 -m coverage report


lint:
	mypy --install-types --non-interactive
	black --check -t py310 .
	flake8 slkspec --count --max-complexity=10 --max-line-length=88 --statistics --doctests
