import json
from sqlalchemy import UnicodeText
from sqlalchemy.dialects.mysql import JSON
from sqlalchemy.types import TypeDecorator

class JSONType(TypeDecorator):
  '''
  JSON datatype class for assigning dialect specific datatype
  It will assign JSON datatype for mysql tables and unicodetext for sqlite
  '''
  impl=UnicodeText

  def load_dialect_impl(self, dialect):
    if dialect.name == 'mysql':
      return dialect.type_descriptor(JSON())
    else:
      return dialect.type_descriptor(self.impl)

  def process_bind_param(self, value, dialect):
    if dialect.name == 'mysql':
      return value
    if value is not None:
      value = json.dumps(value)
      return value

  def process_result_value(self, value, dialect):
    if dialect.name == 'mysql':
      return value
    if value is not None:
      value = json.loads(value)
      return value

