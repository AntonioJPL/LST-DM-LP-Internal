#!/bin/bash

python3 -m venv .venv
source ./.venv/bin/activate
pip install -r requirements.txt
deactivate
python3 -m venv DriveMonitoringWeb
source ./DriveMonitoringWeb/bin/activate
pip install -r DriveMonitoringApp/requierements-django.txt
python3 DriveMonitoringApp/manage.py runserver