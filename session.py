"""
Session Management for TransAm
"""

import hashlib
import time

import query
from query import Log, Query, SanitizeSQL


# Keep a lookup dictionary of known sessions to reduce DB latency
SESSION_CACHE = {}

# Default timeout (8 hours)
TIMEOUT_DEFAULT = 60*60*8


def GetSessionInfo(session_id):
  """Returns dict with session information or None if this is not a valid session_id
  
  Relevant keys: 'user', 'application', 'expire', 'created'
  """
  global SESSION_CACHE
  
  # If we have this session cached, return it
  if session_id in SESSION_CACHE:
    return SESSION_CACHE[session_id]
  
  # Fetch the session by it's ID from the database
  sql = "SELECT * FROM `session` WHERE `key` = '%s'" % session_id
  result = Query(sql)
  
  if not result:
    info = None
  else:
    info = result[0]
  
  # Cache this session, whether it is valid or not
  SESSION_CACHE[session_id] = info
  
  return info


def GetUser(session_id):
  """Returns string (user name) or None if this is not a valid session_id"""
  session_info = GetSessionInfo(session_id)
  
  if session_info == None:
    user = None
  else:
    user = session_info['user']
  
  return user


def CreateSession(user, application, timeout=TIMEOUT_DEFAULT):
  """Returns a session_id (str) for this user, and stores it in the DB.
  
  TODO(g): Take timeout(int) and add to NOW() to get effective timeout for
      this session, when user will have to authenticate again.
  """
  # Create a session_id
  key = '%s_%s_%s' % (str(user), str(application), str(time.time()))
  session_id = hashlib.sha1(key.encode('utf-8')).hexdigest()
  
  # Cleanup any expired sessions in the database
  #NOTE(g): Has to be done sometime, might as well do it now so no cron-type 
  #   system has to be created
  #TODO(g): Would it better to do this more often?  I think so...
  _CleanupSessions()
  
  # Store in the database
  sql = "INSERT INTO `session` (`key`, `application`, `user`, `expire`) VALUES ('%s', '%s', '%s', NOW() + INTERVAL %s SECOND)" % \
        (SanitizeSQL(session_id), SanitizeSQL(application), SanitizeSQL(user), int(timeout))
  result = Query(sql)
  
  # If successful, return the session_id
  if result:
    return session_id
  
  # Else, return None (invalid session_id)
  else:
    return None


def _CleanupSessions():
  """Clean up any expired sessions.  Returns list of strings, removed session keys"""
  global SESSION_CACHE
  
  sql = "SELECT * FROM `session` WHERE NOW() > `expire`"
  result = Query(sql)
  
  removed_session_keys = []
  for item in result:
    # Get the session_id that has expired, and add to our list
    session_id = item['key']
    removed_session_keys.append(session_id)
    
    # Delete this key from the database
    #NOTE(g): Doing it 1 by 1 ensures we always keep the cache and DB in sync
    sql = "DELETE FROM `session` WHERE `id` = %s" % item['id']
    Query(sql)
    
    # Delete this key from cache, if it exists
    if session_id in SESSION_CACHE:
      del SESSION_CACHE[session_id]
  
  return removed_session_keys

