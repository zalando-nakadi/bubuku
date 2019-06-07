#!/bin/bash

cd `dirname ${0}`
PYTHONPATH=.. python3 generate_cli_docs.py
cd - > /dev/null
