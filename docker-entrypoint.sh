#!/bin/sh
service cron start
/opt/venv/bin/python -m $1
