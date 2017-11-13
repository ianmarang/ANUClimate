#! /usr/bin/env bash
export SSH_CLIENT=10.39.227.115
cd /srv/ANUClimate_auto/processed/fenner
chmod -R 777 .
dt=$(date "+%Y-%m-%d")
rsync -ru --perms --chmod=a+rwx --itemize-changes --log-file /srv/ANUClimate_auto/log/rsync_$dt.log -e "ssh -i /home/imar0002/.ssh/ANUClimate_auto_raijin" ./* ijm576@raijin.nci.org.au:/g/data/rr9/fenner/prerelease/fenner/
echo "ANUClimate_auto log file attached" | mailx -s "ANUClimate_auto diagnostics" -a /srv/ANUClimate_auto/log/ANUClimate_log.csv ian.marang@sydney.edu.au
