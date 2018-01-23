#!/bin/bash
dt=$(date "+%Y-%m-%d")
tar -czvf /srv/ANUClimate_auto/log/old_logs/rsync_logs_$dt.tar.gz  /srv/ANUClimate_auto/log/rsync*.logrm -f /srv/ANUClimate_auto/log/rsync*.log
tar cf - /srv/ANUClimate_auto/backup/ANU* | 7za a -si /srv/ANUClimate_auto/backup/backup_ANC_$dt.tar.7z
