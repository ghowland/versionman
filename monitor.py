#!/usr/local/bin/python3

import sys
import xmlrpc.client

proxy = xmlrpc.client.ServerProxy('http://transam.your.domain.com:1967/', allow_none=True)
result = proxy.GetMany("", 'test_db', 'test_item', ['1001'])

if '1001' in result and 'name' in result['1001']:
  print('Success: test_db item 1001 found')
  sys.exit(0)
else:
  print('Failure: test_db item 1001 not found: %s' % result)
  sys.exit(1)

