#! /bin/bash    

cd /home/iv_savin/
. env/bin/activate
cd goodnews/goodnews/
python3 clustering.py json_clustering.txt >> /var/log/goodnews/parser.log
python3 events_evolution.py json_clustering.txt >> /var/log/goodnews/parser.log
