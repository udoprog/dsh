from .dsh import DshProtocol

class DshServerProtocol(DshProtocol):
  """
  The default server protocol, with sane implementations for storing files.
  """
  enableGetFile = True
  enablePutFile = True

  def __init__(self):
    self._files = dict()

  def buildGenerator(self, path):
    return FileGenerator(path)

  def buildWriter(self, path, size):
    writer = FileWriter()
    writer.open(path, size)
    return writer

class DshClientPutProtocol(DshProtocol):
  """
  The default client protocol.
  """
  def __init__(self, path):
    self.path = path

  def connectionMade(self):
    self.putFile(self.path)

  def buildGenerator(self, path):
    return FileGenerator(path)

  def buildWriter(self, path, size):
    writer = FileWriter()
    writer.open(path, size)
    return writer

  def putComplete(self, uid):
    self.transport.loseConnection()

class DshClientGetProtocol(DshProtocol):
  """
  The default client protocol.
  """
  def __init__(self, path):
    self._files = dict()
    self.path = path

  def connectionMade(self):
    self.get(self.path)

  def buildGenerator(self, path):
    return FileGenerator(path)

  def buildWriter(self, path, size):
    writer = FileWriter()
    writer.open(path, size)
    return writer

  def getComplete(self, uid):
    self.transport.loseConnection()

from zope.interface import implements

from .interface import IWriter
from .dsh import DshProtocol

class FileWriter:
  implements(IWriter)

  POPULATE_BUFFER_SIZE = 2 ** 20

  def __init__(self):
    self._fp = None
    self._size = None
    self._realsize = None

  def open(self, path, size, populate=False):
    if self._fp:
      raise RuntimeError, "file already open"

    self._fp = open(path, "w")
    self._size = size
    self._realsize = 0

    if not populate:
      return

    nil = 4096 * "\x00"
    div, mod = divmod(size, self.POPULATE_BUFFER_SIZE)
    [self._fp.write(nil) for i in xrange(div)]
    self._fp.write("\x00" * mod)
    self._realsize = self._size

  def write(self, count, data):
    pos = count * DshProtocol.DATA_MAX
    if self._realsize < pos:
      raise ValueError, "cannot seek to position, realsize too small"
    self._fp.seek(pos)
    self._fp.write(data)

  def close(self):
    self._fp.close()
    self._fp = None
    self._size = None
    self._realsize = None

  def parts(self):
    parts, mod = divmod(self._size, DshProtocol.DATA_MAX)
    if mod != 0: parts += 1
    return parts

from zope.interface import implements

from .interface import IGenerator
from .dsh import DshProtocol

class FileGenerator:
  implements(IGenerator)

  def __init__(self, path):
    import os
    self._path = path
    self._size = os.path.getsize(path)

    self._fp = open(path, "r")

  def read(self, count):
    pos = count * DshProtocol.DATA_MAX
    if self._size < pos:
      raise ValueError, "cannot seek to position, size too small"
    self._fp.seek(pos)
    return self._fp.read(DshProtocol.DATA_MAX)

  def size(self):
    return self._size

  def parts(self):
    parts, mod = divmod(self._size, DshProtocol.DATA_MAX)
    if mod != 0: parts += 1
    return parts
