#!/usr/local/bin/python3

"""
TransAm database versioning system

Python3.x was required for consist Unicode processing, stored in the DB
"""


__author__ = 'Geoff Howland <geoff@gmail.com>'


import sys
import os
import socketserver
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from traceback import format_tb

import process
import versioning
import session
from query import Log


# Bind on this port (first year of TransAm production)
LISTEN_PORT = 1967
# Use this when testing, to not conflict with the "production" service
TEST_LISTEN_PORT = 7691


# Threaded mix-in
class AsyncXMLRPCServer(socketserver.ThreadingMixIn,SimpleXMLRPCServer):
  """Handles simultaneous requests via threads.  No code needed."""


class TransAm:
  """Primary class"""
  
  def Authenticate(self, user, password, application):
    try:
      return process.Authenticate(user, password, str(application))
    except Exception as exc:
      error = 'Error:\n%s\n%s\n' % ('\n'.join(format_tb(exc.__traceback__)), str(exc))
      Log(error)
      return {'[error]':error}
    
  
  def GetSessionInfo(self, session_id):
    try:
      return session.GetSessionInfo(session_id)
    except Exception as exc:
      error = 'Error:\n%s\n%s\n' % ('\n'.join(format_tb(exc.__traceback__)), str(exc))
      Log(error)
      return {'[error]':error}
    
  
  def GetMany(self, session_id, database, table, keys=None, version=None):
    try:
      return process.GetMany(session_id, database, table, keys=keys, version=version)
    except Exception as exc:
      error = 'Error:\n%s\n%s\n' % ('\n'.join(format_tb(exc.__traceback__)), str(exc))
      Log(error)
      return {'[error]':error}
    
  
  def SetMany(self, session_id, database, table, records, comment=None):
    try:
      return process.SetMany(session_id, database, table, records, comment=comment)
    except Exception as exc:
      error = 'Error:\n%s\n%s\n' % ('\n'.join(format_tb(exc.__traceback__)), str(exc))
      Log(error)
      return {'[error]':error}
    
  
  def DeleteMany(self, session_id, database, table, keys, comment=None):
    try:
      return process.DeleteMany(session_id, database, table, keys, comment=comment)
    except Exception as exc:
      error = 'Error:\n%s\n%s\n' % ('\n'.join(format_tb(exc.__traceback__)), str(exc))
      Log(error)
      return {'[error]':error}
  
    
  def GetSchemaInfo(self, session_id, database, table):
    try:
      return process.GetSchemaInfo(session_id, database, table)
    except Exception as exc:
      error = 'Error:\n%s\n%s\n' % ('\n'.join(format_tb(exc.__traceback__)), str(exc))
      Log(error)
      return {'[error]':error}
  
      
  def GetDatabaseTables(self, session_id, database):
    try:
      return process.GetDatabaseTables(session_id, database)
    except Exception as exc:
      error = 'Error:\n%s\n%s\n' % ('\n'.join(format_tb(exc.__traceback__)), str(exc))
      Log(error)
      return {'[error]':error}
  
  
  def GetDatabases(self, session_id):
    try:
      return process.GetDatabases(session_id)
    except Exception as exc:
      error = 'Error:\n%s\n%s\n' % ('\n'.join(format_tb(exc.__traceback__)), str(exc))
      Log(error)
      return {'[error]':error}
  
  
  def ListCommits(self, session_id, before_version=None, after_version=None):
    try:
      return versioning.ListCommits(session_id, before_version=before_version, after_version=after_version)
    except Exception as exc:
      error = 'Error:\n%s\n%s\n' % ('\n'.join(format_tb(exc.__traceback__)), str(exc))
      Log(error)
      return {'[error]':error}
  
  
  def GetRecordVersions(self, session_id, database, table, key):
    try:
      return versioning.GetRecordVersions(session_id, database, table, key)
    except Exception as exc:
      error = 'Error:\n%s\n%s\n' % ('\n'.join(format_tb(exc.__traceback__)), str(exc))
      Log(error)
      return {'[error]':error}


def Main(args=None):
  if not args:
    args = []
  
  # Instantiate and bind our listening port
  server = AsyncXMLRPCServer(('', LISTEN_PORT), SimpleXMLRPCRequestHandler, allow_none=True)
 
  # Register example object instance
  server.register_instance(TransAm())
 
  # Run!  Forever!
  #TODO(g): Switch to polling, and look for SIGTERM to quit nicely, finishing 
  #   any transactions in progress
  server.serve_forever()


if __name__ == '__main__':
  Main(sys.argv[1:])

