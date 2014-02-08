#!/bin/bash

rsync -av * transam@transam:~/server/

rsync -av etc/init.d/transam root@transam:/etc/init.d/

ssh root@transam "/etc/init.d/transam restart"

