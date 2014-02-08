"""
Versioning Module for TransAm

Handles all versioning issues for process.py, to separate concerns.
"""

import json

import session
import query
from query import Log, Query, SanitizeSQL


def CreateCommitVersion(session_id, comment=None):
  """Create the commit_version entry to reference all records stored.
  
  Returns: int, commit_version.id (or None on failure)
  """
  #TODO(g): Finish Authorize() and fetch the user from session.key
  
  # Get the user for this session
  user_name = session.GetUser(session_id)
  
  # Create INSERT SQL without or with comment
  if not comment:
    sql = "INSERT INTO commit_version (`user`) VALUES ('%s')" % SanitizeSQL(user_name)
  else:
    sql = "INSERT INTO commit_version (`user`, `comment`) VALUES ('%s', '%s')" % (SanitizeSQL(user_name), SanitizeSQL(comment))
  
  # Insert the commit and get the version
  version = Query(sql)
  
  return version


def CommitRecordVersion(commit_version, schema, database, table, key, data=None, delete=False):
  """Store a version of a record.  Assume not deleting unless specified."""
  # Create the INSERT SQL for the data storage
  if not delete:
    sql = "INSERT INTO record_version (`version`, `database`, `table`, `record`, `data`) VALUES (%s, '%s', '%s', '%s', '%s')" % \
          (commit_version, SanitizeSQL(database), SanitizeSQL(table), SanitizeSQL(key), SanitizeSQL(json.dumps(data)))
  # Create the INSERT SQL for the delete entry
  else:
    sql = "INSERT INTO record_version (`version`, `database`, `table`, `record`, `is_deleted`) VALUES (%s, '%s', '%s', '%s', 1)" % \
          (commit_version, SanitizeSQL(database), SanitizeSQL(table), SanitizeSQL(key))
  
  # Execute the record version INSERT
  Query(sql)  


def ListCommits(session_id, before_version=None, after_version=None):
  """List all the commits, optionally before/after version to limit view.
  
  Returns: dict, keyed on version number, value is dict of commit info
  """
  # Create the SQL for the list of versions
  sql = 'SELECT * FROM `commit_version`'
  where = ''
  
  # If we want versions before a specified version
  if before_version:
    where += '`id` < %s' % int(before_version)
  
  # If we want versions after a specific version
  if after_version:
    if where:
      where += ' AND '
    
    where += '`id` > %s' % int(after_version)
  
  # If we had WHERE clauses, add them to the SQL statement
  if where:
    sql = '%s WHERE %s' % (sql, where)
  
  # Query our versions
  result = query.Query(sql)
  
  # Return as a dict, keyed on the id
  data = {}
  for item in result:
    data[str(item['id'])] = item
  
  return data


def GetRecordVersions(session_id, database, table, key):
  """Returns all the versions of the database/table/key.
  
  Returns: dict, key is the commit_version.id(int) and value is dict of the entry.  
      Relevant keys are 'data' and 'is_deleted'
  """
  data = {}
  
  sql = "SELECT * FROM `record_version` WHERE `database` = '%s' AND `table` = '%s' AND `record`='%s'" % \
        (SanitizeSQL(database), SanitizeSQL(table), SanitizeSQL(key))
  result = Query(sql)
  
  # Return the versions, key on version number for this record
  for item in result:
    data[str(item['version'])] = item
  
  return data

