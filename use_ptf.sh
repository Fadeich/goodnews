#! /bin/bash    

cd /home/iv_savin/
. env/bin/activate
cd goodnews/final/
python3 use_ptf.py json_parser.txt >> /var/log/goodnews/parser.log
