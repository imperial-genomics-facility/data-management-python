import json
from sqlalchemy import UnicodeText
from sqlalchemy.types import TypeDecorator
from sqlalchemy.dialects import mysql #import DECIMAL, JSON
from sqlalchemy.dialects import postgresql #import NUMERIC, JSON

class JSONType(TypeDecorator):
  '''
  JSON datatype class for assigning dialect specific datatype
  It will assign JSON datatype for mysql tables and unicodetext for sqlite
  '''
  impl=UnicodeText

  def load_dialect_impl(self, dialect):
    if dialect.name == 'mysql':
      return dialect.type_descriptor(mysql.JSON())
    elif dialect.name == 'postgresql':
      return dialect.type_descriptor(postgresql.JSON())
    else:
      return dialect.type_descriptor(self.impl)

  def process_bind_param(self, value, dialect):
    if dialect.name == 'mysql' or \
       dialect.name == 'postgresql':
      return value
    if value is not None:
      value = json.dumps(value)
      return value

  def process_result_value(self, value, dialect):
    if dialect.name == 'mysql' or \
       dialect.name == 'postgresql':
      return value
    if value is not None:
      value = json.loads(value)
      return value


class DECIMALType(TypeDecorator):
  impl = UnicodeText
  cache_ok = True
  def __init__(self, precision=10, scale=2, **kwargs):
        self.precision = precision
        self.scale = scale
        super().__init__(**kwargs)

  def load_dialect_impl(self, dialect):
    if dialect.name == 'mysql':
      return dialect.type_descriptor(
        mysql.DECIMAL(
          precision=self.precision,
          scale=self.scale))
    elif dialect.name == 'postgresql':
      return dialect.type_descriptor(
        postgresql.NUMERIC(
          precision=self.precision,
          scale=self.scale
        ))
    else:
      return dialect.type_descriptor(self.impl)

  def process_bind_param(self, value, dialect):
    if dialect.name == 'mysql' or \
       dialect.name == 'postgresql':
      return value
    if value is not None:
      return str(value)

  def process_result_value(self, value, dialect):
    if dialect.name == 'mysql' or \
       dialect.name == 'postgresql':
      return value
    if value is not None:
      return str(value)