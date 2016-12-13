"""Copyright 2009 Chris Davis

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import os
import re
import whisper

from os.path import join, exists, sep, dirname
from carbon.conf import OrderedConfigParser, settings
from carbon.util import pickle
from carbon import log
from carbon.exceptions import CarbonConfigException

STORAGE_SCHEMAS_CONFIG = join(settings.CONF_DIR, 'storage-schemas.conf')
STORAGE_AGGREGATION_CONFIG = join(settings.CONF_DIR, 'storage-aggregation.conf')
STORAGE_LISTS_DIR = join(settings.CONF_DIR, 'lists')


def getFilesystemPath(metric):
  metric_path = metric.replace('.', sep).lstrip(sep) + '.wsp'
  return join(settings.LOCAL_DATA_DIR, metric_path)

def createWhisperFile(metric, dbFilePath):
    archiveConfig = None
    xFilesFactor, aggregationMethod = None, None

    for schema in SCHEMAS:
      if schema.matches(metric):
        log.creates('new metric %s matched schema %s' % (metric, schema.name))
        archiveConfig = [archive.getTuple() for archive in schema.archives]
        break

    for schema in AGGREGATION_SCHEMAS:
      if schema.matches(metric):
        log.creates('new metric %s matched aggregation schema %s' % (metric, schema.name))
        xFilesFactor, aggregationMethod = schema.archives
        break

    if not archiveConfig:
      raise Exception("No storage schema matched the metric '%s', check your storage-schemas.conf file." % metric)

    dbDir = dirname(dbFilePath)
    try:
        if not exists(dbDir):
            os.makedirs(dbDir)
    except OSError, e:
        log.err("%s" % e)
    log.creates("creating database file %s (archive=%s xff=%s agg=%s)" %
                (dbFilePath, archiveConfig, xFilesFactor, aggregationMethod))
    whisper.create(
        dbFilePath,
        archiveConfig,
        xFilesFactor,
        aggregationMethod,
        settings.WHISPER_SPARSE_CREATE,
        settings.WHISPER_FALLOCATE_CREATE)

def reloadStorageSchemas():
  global SCHEMAS
  try:
    SCHEMAS = loadStorageSchemas()
  except Exception:
    log.msg("Failed to reload storage SCHEMAS")
    log.err()

def reloadAggregationSchemas():
  global AGGREGATION_SCHEMAS
  try:
    AGGREGATION_SCHEMAS = loadAggregationSchemas()
  except Exception:
    log.msg("Failed to reload aggregation SCHEMAS")
    log.err()

class Schema:
  def test(self, metric):
    raise NotImplementedError()

  def matches(self, metric):
    return bool(self.test(metric))


class DefaultSchema(Schema):

  def __init__(self, name, archives):
    self.name = name
    self.archives = archives

  def test(self, metric):
    return True


class PatternSchema(Schema):

  def __init__(self, name, pattern, archives):
    self.name = name
    self.pattern = pattern
    self.regex = re.compile(pattern)
    self.archives = archives

  def test(self, metric):
    return self.regex.search(metric)


class ListSchema(Schema):

  def __init__(self, name, listName, archives):
    self.name = name
    self.listName = listName
    self.archives = archives
    self.path = join(settings.WHITELISTS_DIR, listName)

    if exists(self.path):
      self.mtime = os.stat(self.path).st_mtime
      fh = open(self.path, 'rb')
      self.members = pickle.load(fh)
      fh.close()

    else:
      self.mtime = 0
      self.members = frozenset()

  def test(self, metric):
    if exists(self.path):
      current_mtime = os.stat(self.path).st_mtime

      if current_mtime > self.mtime:
        self.mtime = current_mtime
        fh = open(self.path, 'rb')
        self.members = pickle.load(fh)
        fh.close()

    return metric in self.members


class Archive:

  def __init__(self, secondsPerPoint, points):
    self.secondsPerPoint = int(secondsPerPoint)
    self.points = int(points)

  def __str__(self):
    return "Archive = (Seconds per point: %d, Datapoints to save: %d)" % (self.secondsPerPoint, self.points)

  def getTuple(self):
    return (self.secondsPerPoint, self.points)

  @staticmethod
  def fromString(retentionDef):
    (secondsPerPoint, points) = whisper.parseRetentionDef(retentionDef)
    return Archive(secondsPerPoint, points)


def loadStorageSchemas():
  schemaList = []
  config = OrderedConfigParser()
  config.read(STORAGE_SCHEMAS_CONFIG)

  for section in config.sections():
    options = dict(config.items(section))
    matchAll = options.get('match-all')
    pattern = options.get('pattern')
    listName = options.get('list')

    retentions = options['retentions'].split(',')
    archives = [Archive.fromString(s) for s in retentions]

    if matchAll:
      mySchema = DefaultSchema(section, archives)

    elif pattern:
      mySchema = PatternSchema(section, pattern, archives)

    elif listName:
      mySchema = ListSchema(section, listName, archives)

    archiveList = [a.getTuple() for a in archives]

    try:
      whisper.validateArchiveList(archiveList)
      schemaList.append(mySchema)
    except whisper.InvalidConfiguration, e:
      log.msg("Invalid schemas found in %s: %s" % (section, e))

  schemaList.append(defaultSchema)
  return schemaList


def loadAggregationSchemas():
  # NOTE: This abuses the Schema classes above, and should probably be refactored.
  schemaList = []
  config = OrderedConfigParser()

  try:
    config.read(STORAGE_AGGREGATION_CONFIG)
  except (IOError, CarbonConfigException):
    log.msg("%s not found, ignoring." % STORAGE_AGGREGATION_CONFIG)

  for section in config.sections():
    options = dict(config.items(section))
    matchAll = options.get('match-all')
    pattern = options.get('pattern')
    listName = options.get('list')

    xFilesFactor = options.get('xfilesfactor')
    aggregationMethod = options.get('aggregationmethod')

    try:
      if xFilesFactor is not None:
        xFilesFactor = float(xFilesFactor)
        assert 0 <= xFilesFactor <= 1
      if aggregationMethod is not None:
        assert aggregationMethod in whisper.aggregationMethods
    except ValueError:
      log.msg("Invalid schemas found in %s." % section)
      continue

    archives = (xFilesFactor, aggregationMethod)

    if matchAll:
      mySchema = DefaultSchema(section, archives)

    elif pattern:
      mySchema = PatternSchema(section, pattern, archives)

    elif listName:
      mySchema = ListSchema(section, listName, archives)

    schemaList.append(mySchema)

  schemaList.append(defaultAggregation)
  return schemaList

defaultArchive = Archive(60, 60 * 24 * 7)  # default retention for unclassified data (7 days of minutely data)
defaultSchema = DefaultSchema('default', [defaultArchive])
defaultAggregation = DefaultSchema('default', (None, None))

SCHEMAS = loadStorageSchemas()
AGGREGATION_SCHEMAS = loadAggregationSchemas()
