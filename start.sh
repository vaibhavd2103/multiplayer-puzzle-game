python server.py --role primary --port 6000
python server.py --role backup --id 1 --primary-host 127.0.0.1 --primary-port 6000 --all-backup-ids 1,2,3
python server.py --role backup --id 2 --primary-host 127.0.0.1 --primary-port 6000 --all-backup-ids 1,2,3
python server.py --role backup --id 3 --primary-host 127.0.0.1 --primary-port 6000 --all-backup-ids 1,2,3
