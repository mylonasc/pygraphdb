#!/bin/bash
# First run the coverage test:
cd ../ && coverage run -m unittest discover 
coverage_val=$(coverage report -m | tail -n 1 | awk '{print $4}')
cd assets

badge_color="#97CA00"

#echo "[![cov](https://mylonasc.github.io/pygraphdb/badges/coverage.svg)](https://github.com/we-cli/jayin/actions)"

#python3 -m pybadges --left-text="test coverage" --right-text="${coverage_val}" --right-color="${badge_color}" --browser #> coverage_badge.svg
python3 -m pybadges --left-text="test coverage" --right-text="${coverage_val}" --right-color="${badge_color}"  > coverage_badge.svg
