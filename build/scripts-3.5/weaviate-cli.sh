#!/bin/bash

PYTHON_VERSION=$(python -c 'import sys; print(".".join(map(str, sys.version_info[0:1])))')

if [ $PYTHON_VERSION -eq 3 ]
then
    python main.py "$@"
else
    python3 main.py "$@"
fi