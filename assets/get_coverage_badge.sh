#!/bin/bash
# First run the coverage test:
coverage run -m unittest discover
coverage_val=$(coverage report -m | tail -n 1 | awk '{print $4}')

#echo "[![cov](https://mylonasc.github.io/pygraphdb/badges/coverage.svg)](https://github.com/we-cli/jayin/actions)"

python3 -m pybadges --left-text="test coverage" --right-text="${coverage_val}" --right-color='#0b0' >> coverage_badge.svg
