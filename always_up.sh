#!/bin/bash
#
# Ensure TransAm service is always up
#

/etc/init.d/transam status > /dev/null 2>&1

if [ $? -ne 0 ] ; then
  /etc/init.d/transam restart
fi

