"""
Query Library for TransAm API
"""


import MySQLdb
import MySQLdb.cursors
import threading
import os
import sys
import imp
import gc


# Default database connection: The OPs DB
#TODO(g):HARDCODED: Fix.  Move login information to sane location.
DEFAULT_DB_HOST = 'db.you.domain.com'
DEFAULT_DB_USER = 'transam'
DEFAULT_DB_PASSWORD = 'SECRETPASSWORDHERE'
DEFAULT_DB_DATABASE = 'transam'
DEFAULT_DB_PORT = 3306


# Global to store DB connections
#NOTE(g): We store connections to each database separately, even if
#   they go to the same DB host, so that we dont have to keep track
#   of which DB we are currently connected to.
DB_CONNECTION = {}
DB_CURSOR = {}


# Create a Global Write Lock
#NOTE(g): You have to grab this to do an UPDATE/INSERT, so we are
#   never doing these at exactly the same time.  Avoids all kinds
#   of problems immediately and can be refactored out later once
#   the project is up and running with all other things being stable.
GLOBAL_WRITE_LOCK = threading.Lock()


class QueryFailure(Exception):
  """Failure to query the DB properly"""


def CloseAll():
  """Forcibly close all the MYSQLdb connections"""
  global DB_CONNECTION
  global DB_CURSOR
  
  # Loop through all our connection cache keys, and close them all
  keys = list(DB_CONNECTION.keys())
  for cache_key in keys:
    DB_CONNECTION[cache_key].close()
    del DB_CURSOR[cache_key]
    del DB_CONNECTION[cache_key]
  
  # Reload the MySQLdb module to try to clear more cache
  imp.reload(MySQLdb)
  
  # Force GC to run now and do whatever terrible things MySQLdb is doing to 
  #   close our connections and not allow new ones to open
  gc.collect()


def Connect(host, user, password, database, port, reset=False):
  """Connect to the specified MySQL DB"""
  global DB_CONNECTION
  global DB_CURSOR

  # Convert to proper empty DB
  if database == None:
    database = ''

  # Create the cache key (tuple), for caching the DB connection and cursor
  cache_key = (host, user, password, database, port)

  # Close any connection we have persistent, if resetting
  if reset and cache_key in DB_CURSOR:
    Log('Resetting cache key: %s' % str(cache_key))
    DB_CONNECTION[cache_key].close()
    del DB_CURSOR[cache_key]
    del DB_CONNECTION[cache_key]
    # Force module reload to clear out anything that is being incorrectly cached
    imp.reload(MySQLdb)
    #Log('Connections: %s  Cursors: %s' % (DB_CONNECTION, DB_CURSOR))

  # If no cached cursor/connection exists, create them
  if reset or cache_key not in DB_CURSOR:
    Log('Creating MySQL connection: %s' % str(cache_key))
    conn = MySQLdb.Connect(host, user, password, database, port=port, cursorclass=MySQLdb.cursors.DictCursor)
    cursor = conn.cursor()

    DB_CURSOR[cache_key] = cursor
    DB_CONNECTION[cache_key] = conn
    #Log('Connections: %s  Cursors: %s' % (DB_CONNECTION, DB_CURSOR))

  # Else, use the cache
  else:
    conn = DB_CONNECTION[cache_key]
    cursor = DB_CURSOR[cache_key]

  return (conn, cursor)


def Query(sql, host=DEFAULT_DB_HOST, user=DEFAULT_DB_USER, 
		password=DEFAULT_DB_PASSWORD, database=DEFAULT_DB_DATABASE, 
		port=DEFAULT_DB_PORT):
  """Execute and Fetch All results, or reutns last row ID inserted if INSERT."""
  # Try to reconnect and stuff
  success = False
  tries = 0
  last_error = None
  while tries <= 3 and success == False:
    tries += 1

    try:
      # Connect (will save connections)
      (conn, cursor) = Connect(host, user, password, database, port)

      # Query
      Log('Query: %s' % sql)
      cursor.execute(sql)
      
      #Log('Query complete, committing')
      
      # Force commit
      conn.commit()
      
      # Command didnt throw an exception
      success = True
    
    except MySQLdb.DatabaseError as exc:
      (error_code, error_text) = (exc.code, exc.message)
      last_error = '%s: %s (Attempt: %s): %s: %s: %s' % (error_code, error_text, tries, host, database, sql)
      Log(last_error)
      
      # Connect lost, reconnect
      if error_code in (2006, '2006'):
        Log('Lost connection: %s' % last_error)

        # Enforce we always close the connections, because 
        CloseAll()

        # Reset connection
        Connect(host, user, password, database, port, reset=True)
      else:
        Log('Unhandled MySQL query error: %s' % last_error)

  # If we made the query, get the result
  if success:
    if sql.upper()[:6] not in ('INSERT', 'UPDATE', 'DELETE'):
      result = cursor.fetchall()
    elif sql.upper()[:6] == 'INSERT':
      # This is 0 unless we were auto_incrementing, and then it is accurate
      result = cursor.lastrowid

    else:
      result = None

  # We failed, no result for you
  else:
    raise QueryFailure(str(last_error))

  return result


def Log(text, reset=False, logfile=None):
  """Log things we are doing"""
  # Generate the log file from the file name, if it wasnt specified
  if logfile == None:
    logfile = os.path.basename(sys.argv[0]).replace('.py', '.log')
  
  # If we dont have a log file, we're using the REPL interactively, print
  if not logfile:
    print(text)
  
  # Else, log it
  else:
    if not reset:
      fp = open(logfile, 'a')
    else:
      fp = open(logfile, 'w')
  
    fp.write('%s\n' % str(text))
    fp.close()


def SanitizeSQL(sql):
  """Convert singled quotes to dual single quotes, so SQL doesnt terminate the string improperly"""
  sql = str(sql).replace("'", "''")
  
  return sql
