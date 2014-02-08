"""
Process TransAm RPC commands

Keeping transam.py about the RPC handling, and query.py about database stuff,
the heavy lifting of processing, generating SQL and handling success and
failure will occur here.

Design Notes (ghowland):

 - I tried converting the PKEY into an md5 digest, but Python3 is very weird 
    about passing unicode strings into hashlib, I would have to base64 encode
    it and then base64 is weird about passing unicode strings in.  With all
    this conversion weirdness I cant guarantee that another language could do
    it the same way Python3 would do it, and while not critical I felt this
    was probably not worth it, as it also hid information.  Hopefully no one
    will try to parse this data instead of just using the GetSchemaInfo data.
"""


from traceback import format_tb
import json

import query
from query import Log, Query, SanitizeSQL

import versioning
import session


def Authenticate(user, password, application):
  """Authenticate this user, returns session ID (string)"""
  #TODO(g): Do LDAP password test, and return a session ID which we store and validate future API calls against
  session_id = session.CreateSession(user, application)
  
  return {'session':session_id}


def GetSchemaInfo(session_id, database, table):
  """Returns a dict of schema info to assist in processing."""
  data = {'schema':{}, 'key_fields':[]}
  
  # Get the table DESC
  sql = 'DESC `%s`' % table
  result = query.Query(sql, database=database)
  field_order = 0
  for item in result:
    data['schema'][item['Field']] = item
    data['schema'][item['Field']]['_Order'] = field_order
    field_order += 1
  
  # Get the table PRIMARY KEY INDEX
  sequence = {}
  sql = 'SHOW INDEXES IN `%s`' % table
  result = query.Query(sql, database=database)
  for item in result:
    if item['Key_name'] == 'PRIMARY':
      sequence[item['Seq_in_index']] = item
  
  # Add the PRIMARY KEY keys by their sequence order (ensure its correct)
  sequence_keys = list(sequence.keys())
  sequence_keys.sort()
  for sequence_key in sequence_keys:
    data['key_fields'].append(sequence[sequence_key]['Column_name'])
  
  return data


def GetDatabaseTables(session_id, database):
  """Returns a dict of schema info to assist in processing."""
  data = []
  
  # Get the table DESC
  sql = 'SHOW TABLES'
  result = query.Query(sql, database=database)
  for item in result:
    data.append(item[list(item.keys())[0]])
  
  data.sort()
  
  return data


def GetDatabases(session_id):
  """Returns a list of database tables."""
  data = []
  
  # Get the table DESC
  sql = 'SHOW DATABASES'
  result = query.Query(sql)
  for item in result:
    data.append(item[list(item.keys())[0]])
  
  data.sort()
  
  return data


def _CreateSchemaKey(schema, record):
  """Takes the schema result of GetSchemaInfo() and record data dict and returns the key as string."""
  key = ''
  
  # Keep appending keys in CSV format to determine the full PKEY
  for key_field in schema['key_fields']:
    if key:
      key += ','
    
    key += str(record[key_field])
  
  return key


def GetMany(session_id, database, table, keys=None, version=None):
  """Returns dict with PKEY digest as key, and dict of key/value for the fields of this Row/Record
  
  Args:
    session_id: string, session ID
    database: string, database name
    table: string, table name
    keys: sequence of strings or None, if a sequence of strings, only records who have a key
        that matches one in this sequence will be returned
    version: int or None, if an int, the data will be returned from the specified version number
  
  Returns: dict with PKEY digest as key, and dict of key/value for the fields of this Row/Record
  """
  schema = GetSchemaInfo(session_id, database, table)
  
  try:
    # If we dont want versioned data
    if version == None:
      # Get all the records in this table, no versioning
      #TODO(g): Optimize this by restricting them first.  Because PKEY can be 
      #   multiple fields this is harder than if it was always an "id" type 
      #   field, so this will take some work.
      sql = "SELECT * FROM `%s`" % table
    
      sql_result = query.Query(sql, database=database)
      result = {}
      for item in sql_result:
        key = _CreateSchemaKey(schema, item)
        if keys == None or key in keys:
          # Clean out object garbage put in by MySQLdb, so we have pure data
          #NOTE(g): I'd turn off the feature, but that doesnt seem to be an
          #   option.  Later I can possibly monkey-path the __repr__ function
          #   but that seems more dangerous than this.  Faster though...
          item = _CleanObjectGarbage(schema, item)
        
          result[key] = item
    
    # Else, we want a specific version of the data
    else:
      # Get all the record versions for this database/table, where the 
      #   version is less than or equal to the specified version
      #NOTE(g): This allows us to collect up deleted data at this version
      #   and to skip data that did not exist yet at this version.
      sql = "SELECT * FROM `record_version` WHERE `database` = '%s' AND `table` = '%s' AND `version` <= %s ORDER BY `version` DESC" % \
            (SanitizeSQL(database), SanitizeSQL(table), int(version))
      sql_result = query.Query(sql)
      
      # Keep track of record keys that have been deleted, so we dont add them.  
      #   This is due to not being able to use "LIMIT 1" on individual 
      #   records, so I track it here to provide the protection to avoid 
      #   adding in records that were marked as deleted and NOT added to the 
      #   result set.
      deleted_records = []
      
      # Create a result after pulling out the 'data' field, unless 
      #   'is_deleted'==1, and then do not include this record in the results
      result = {}
      for item in sql_result:
        # Skip deleted entries
        if item['is_deleted'] == 1:
          # Add this record key to deleted items, to skip any possible add 
          #   to result set on already-found deleted items
          deleted_records.append(item['record'])
          continue
        
        # Skip if we specified record keys, and this record isnt in them
        #TODO(g):OPTIMIZE: Gets all the data, throws away what isnt needed.  
        #   This has scaling issues, but is the fastest way to get it working
        #   and typically all our data sets are small enough in Corp that this
        #   method should be fine.  Fix when its a problem...
        if keys != None and item['record'] not in keys:
          continue
        
        # Get the data and unpack it
        #TODO(g):OPTIMIZE: Second optimization problem, I cant use "LIMIT 1"
        #   in the SQL because I want all the data for different records,
        #   but only the top version.  Im discarding all earlier versions
        #   so only the first one shows up, which should be the top
        if item['record'] not in result and item['record'] not in deleted_records:
          result[item['record']] = json.loads(item['data'])
      
  
  except Exception as exc:
    error = 'Error:\n%s\n%s\n' % ('\n'.join(format_tb(exc.__traceback__)), str(exc))
    Log(error)
    result = {'[error]':error}
  
  #Log('Result: %s' % str(result))
  
  return result


def SetMany(session_id, database, table, records, comment=None):
  """Sets many records for a given database and table
  
  Args:
    session_id: string, session ID
    database: string, database name
    table: string, table name
    keys: sequence of strings or None, if a sequence of strings, only records who have a key
        that matches one in this sequence will be returned
    version: int or None, if an int, the data will be returned from the specified version number
  
  Returns: dict with PKEY digest as key, and dict of key/value for the fields of this Row/Record
  """
  schema = GetSchemaInfo(session_id, database, table)
  current_data = GetMany(session_id, database, table)
  
  # Return data, this will have our updated/inserted keys and records
  data = {}
  
  # List of our table keys to fetch after we're done to get the real DB contents
  set_keys = []
  
  # Create Commit Version
  commit_version = versioning.CreateCommitVersion(session_id, comment=comment)
  
  # Go through the items we want to set, compare them to our current keys (update or add)
  for key in records:
    # Commit the record version
    versioning.CommitRecordVersion(commit_version, schema, database, table, key, data=records[key])
    
    # If this key exists, we are UPDATEing this record
    if key in current_data:
      Log('Updating key: %s: Currently: %s' % (key, current_data[key]))
      
      # Create the SQL for the UPDATE
      sql = _CreateRecordUpdateSql(schema, table, records[key])
      
      # Execute the SQL
      query.Query(sql, database=database)
      
      # Update the set_keys, so we can retrieve all the touched data
      set_keys.append(key)
    
    # Else, this is a new key so we are INSERTing this record
    else:
      Log('Inserting key: %s: New data: %s' % (key, records[key]))
      
      # Create the SQL for the UPDATE
      sql = _CreateRecordInsertSql(schema, table, records[key])
      
      # Execute the SQL
      last_inserted_key = query.Query(sql, database=database)
      
      # If we were auto-incrementing, then get the data
      if last_inserted_key != 0:
        insert_key = str(last_inserted_key)
      
      # Else, we passed in the primary key values, so extract them from the record
      else:
        insert_key = _CreateSchemaKey(schema, records[key])
      
      Log('Inserted Key: %s' % insert_key)
      
      # Update the set_keys, so we can retrieve all the touched data
      set_keys.append(insert_key)
      
  
  # Get all the data in the database currently
  #NOTE(g): Immediately tells us what the real data in the DB is
  data = GetMany(session_id, database, table, set_keys)
  
  return data


def DeleteMany(session_id, database, table, keys, comment=None):
  """Sets many records for a given database and table
  
  Args:
    session_id: string, session ID
    database: string, database name
    table: string, table name
    keys: sequence of strings or None, if a sequence of strings, only records who have a key
        that matches one in this sequence will be returned
  
  Returns: None
  """
  schema = GetSchemaInfo(session_id, database, table)
  records = GetMany(session_id, database, table)
  
  # Create Commit Version
  commit_version = versioning.CreateCommitVersion(session_id, comment=comment)
  
  # Go through the items we want to set, compare them to our current keys (update or add)
  for key in keys:
    # Commit the record version
    versioning.CommitRecordVersion(commit_version, schema, database, table, key, data=None, delete=True)
    
    # If this key exists, we are DELETEing this record
    if key in records:
      Log('Deleting key: %s: Currently: %s' % (key, records[key]))
      
      # Create the SQL for the UPDATE
      sql = _CreateRecordDeleteSql(schema, table, records[key])
      #Log('DELETE SQL: %s' % sql)
      
      # Execute the SQL
      query.Query(sql, database=database)
    
    # Else, this is a new key so we are INSERTing this record
    else:
      Log('Delete key is missing: %s: %s: %s' % (database, table, key))
      
      #TODO(g): Return errors on what we couldnt delete.  Should we delete anything if any data is invalid?
      pass
  
  return {}


def _CreateRecordUpdateSql(schema, table, record):
  """Returns SQL (string) for an UPDATE on this record, for this key"""
  sql = 'UPDATE `%s`' % table
  sql_where = ''
  sql_values = ''
  
  # Create the WHERE section, which specifies the PRIMARY KEY (multi-key possible)
  for field in schema['key_fields']:
    # Add seperators after the first field is added
    if sql_where:
      sql_where += ' AND '
    
    sql_where += '`%s` = %s' % (field, _SanitizeSQL(schema, field, record[field]))
  
  # Create the WHERE section, which specifies the PRIMARY KEY (multi-key possible)
  for field in schema['schema']:
    # Skip fields that are part of the PRIMARY KEY, these never need to be updated
    if field in schema['key_fields']:
      continue
    
    # Add seperators after the first field is added
    if sql_values:
      sql_values += ', '
    
    sql_values += '`%s` = %s' % (field, _SanitizeSQL(schema, field, record[field]))
  
  # Put it all together
  sql_final = '%s SET %s WHERE %s' % (sql, sql_values, sql_where)
  
  return sql_final


def _CreateRecordInsertSql(schema, table, record):
  """Returns SQL (string) for an INSERT on this record, for this key"""
  sql = 'INSERT INTO `%s`' % table
  sql_fields = ''
  sql_values = ''
  
  # Create the Field and Values section of the INSERT
  for field in schema['schema']:
    # Add seperators after the first field is added
    if sql_values:
      sql_fields += ', '
      sql_values += ', '
    
    sql_fields += '`%s`' % field
    sql_values += '%s' % _SanitizeSQL(schema, field, record[field])
  
  # Put it all together
  sql_final = '%s (%s) VALUES (%s)' % (sql, sql_fields, sql_values)
  
  return sql_final


def _CreateRecordDeleteSql(schema, table, record):
  """Returns SQL (string) for an INSERT on this record, for this key"""
  sql = 'DELETE FROM `%s`' % table
  sql_where = ''
  
  # Loop over the Primary Key fields to preoperly delete this data (and only this data)
  for field in schema['key_fields']:
    # Add seperators after the first field is added
    if sql_where:
      sql_where += ' AND '
    
    sql_where += '`%s` = %s' % (field, _SanitizeSQL(schema, field, record[field]))
  
  # Put it all together
  sql_final = '%s WHERE %s' % (sql, sql_where)
  
  return sql_final


def _SanitizeSQL(schema, field, value):
  """Returns a single-quoted or non-single quoted string, depending on whether 
  SQL requires it for this schema field
  
  Args:
    schema: dict, from GetSchemaInfo()
    field: string, name of the table field
    value: any, value to be single-quoted, or not
  
  Returns: string, valid data to be assigned to as a SQL value (UPDATE or INSERT)
  """
  # Get a simplified SQL field tyle
  sql_type = schema['schema'][field]['Type'].lower()
  if '(' in sql_type:
    sql_type = sql_type.split('(')[0]
  
  # -- Sanitize by Type and Value --
  
  # If value is None, then set it to NULL
  if value == None:
    result = 'NULL'
  
  # Do not quote these forms
  elif sql_type in ('integer', 'int', 'smallint', 'tinyint', 'mediumint', 'bigint', 'decimal', 'numeric', 'float', 'double', 'bit', 'year'):
    result = str(value)
    
  # Single quote everything else
  else:
    result = "'%s'" % SanitizeSQL(value)
  
  return result


def _CleanObjectGarbage(schema, record):
  """MySQLdb irresponsibly pollutes the data it returns with the useless 
  DateTime object, which does not convert cleanly back to a string that 
  can be inserted back into MySQL.
  
  This function un-does the work MySQLdb does to convert dates/times/etc 
  into DateTime objects, back to their string counterparts.
  """
  data = {}

  # Clean all the fields in the record
  for (field, value) in record.items():
    # Get a simplified SQL field tyle
    sql_type = schema['schema'][field]['Type'].lower()
    if '(' in sql_type:
      sql_type = sql_type.split('(')[0]
    
    # Date Time
    if sql_type in ('datetime', 'timestamp') and hasattr(value, 'timetuple'):
      timetuple = value.timetuple()
      # Convert to YYYY-MM-DD HH:MM:SS format
      data[field] = "%d-%02d-%02d %02d:%02d:%02d" % (timetuple.tm_year, timetuple.tm_mon, timetuple.tm_mday, timetuple.tm_hour, timetuple.tm_min, timetuple.tm_sec)
    
    # Date
    elif sql_type == 'date' and hasattr(value, 'timetuple'):
      timetuple = value.timetuple()
      # Convert to YYYY-MM-DD
      data[field] = "%d-%02d-%02d" % (timetuple.tm_year, timetuple.tm_mon, timetuple.tm_mday)
    
    # Time
    elif sql_type == 'time' and hasattr(value, 'timetuple'):
      timetuple = value.timetuple()
      # Convert to HH:MM:SS format
      data[field] = "%02d:%02d:%02d" % (timetuple.tm_hour, timetuple.tm_min, timetuple.tm_sec)
    
    # Else, no changes required, just store it (as it should be)
    else:
      data[field] = value

  return data

