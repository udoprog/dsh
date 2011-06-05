from twisted.internet import protocol
from twisted.python import log

import struct
import cStringIO

from .interface import IWriter
from .interface import IGenerator

class DshProtocol(protocol.Protocol):
  enablePutFile = False
  enableGetFile = False
  checkDigest = True

  """
  The size of the digest from digestFunction..
  """
  digestSize = 20

  def digestFunction(self, data):
    """
    The digestfunction to use, defaults to 'sha1'.
    """
    import hashlib
    return hashlib.sha1(data).digest();

  def generateUuid(self):
    import uuid
    return uuid.uuid4().bytes

  HEADER           = "!I"

  PUT             = "\x10"
  PUT_st          = HEADER + "16sQ"
  PUT_ACK         = "\x15"
  PUT_ACK_st      = HEADER + "16s"
  REQUEST_PART    = "\x40"
  REQUEST_PART_st = HEADER + "16sI"
  SEND_PART       = "\x41"
  SEND_PART_st    = HEADER + "16sI%ss" % ( str(digestSize) )
  GET             = "\x20"
  GET_st          = HEADER + ""
  GET_RESPONSE    = "\x25"
  GET_RESPONSE_st = HEADER + "16sQ"
  ERROR           = "\x80"
  ERROR_st        = HEADER + ""
  DISABLED        = "\x90"
  DISABLED_st     = HEADER + ""

  DATA_MAX         = 2**20
  READ_MAX         = DATA_MAX + 32
  RESEND_MAX       = 16

  frames = {
    PUT           : struct.Struct(PUT_st),
    PUT_ACK       : struct.Struct(PUT_ACK_st),
    REQUEST_PART  : struct.Struct(REQUEST_PART_st),
    SEND_PART     : struct.Struct(SEND_PART_st),
    GET           : struct.Struct(GET_st),
    GET_RESPONSE  : struct.Struct(GET_RESPONSE_st),
    ERROR         : struct.Struct(ERROR_st),
    DISABLED      : struct.Struct(DISABLED_st),
  }

  commands = set(frames.keys())

  def dsh_GET(self, info, data):
    if not self.enableGetFile:
      self.disabled("GET")
      return

    self.dsh_get(data)

  def dsh_GET_RESPONSE(self, info, data):
    uid, size = info
    self.dsh_get_response(uid, size, data)

  def dsh_PUT(self, info, data):
    uid, size = info

    if not self.enablePutFile:
      self.disabled("PUT")
      return

    self.dsh_put(uid, size, data)

  def dsh_PUT_ACK(self, info, data):
    uid = info[0]
    self.dsh_put_ack(uid)

  def dsh_REQUEST_PART(self, info, data):
    uid, count = info
    self.dsh_request_part(uid, count)

  def dsh_SEND_PART(self, info, data):
    uid, count, digest = info
    self.dsh_send_part(uid, count, digest, data)

  def dsh_ERROR(self, info, data):
    self.dsh_error(data)

  def dsh_DISABLED(self, info, data):
    self.dsh_disabled(data)

  def dsh_get(self, path):
    """
    Override to implement GET.
    """
    uid = self.generateUuid()
    generator = self.buildGenerator(path) 

    if not IGenerator.providedBy(generator):
      log.msg("invalid generator: %s" % (repr(generator)))
      self.error("invalid generator")
      return
    
    self._send[uid] = (uid, path, generator)

    size = generator.size()

    self.get_response(uid, size, path)

  def dsh_get_response(self, uid, size, path):
    """
    Override to implement GET_RESPONSE
    """
    recv, writer = self.recvFile(uid, size, path)

    try:
      count = recv.index(False)
    except ValueError, e:
      self.put_ack(uid)
      return

    self.request_part(uid, count)

  def dsh_put(self, uid, size, path):
    recv, writer = self.recvFile(uid, size, path)

    try:
      count = recv.index(False)
    except ValueError, e:
      self.put_ack(uid)
      return

    self.request_part(uid, count)

  def dsh_put_ack(self, uid):
    """
    Override to implement PUT ACK.
    """
    if uid not in self._send:
      self.error("no transfer in progress")
      return

    self._send.pop(uid)
    self.putComplete(uid)

  def dsh_send_part(self, uid, count, digest, data):
    if uid not in self._recv:
      self.error("no matching transfer in progress")
      return

    recv, writer = self._recv[uid]

    if self.checkDigest:
      if self.digestFunction(data) != digest:
        self.request_part(uid, count)
        return

    recv[count] = True
    done = False

    try:
      missing = recv.index(False)
      self.request_part(uid, missing)
    except ValueError, e:
      self._recv.pop(uid)
      done = True
      self.put_ack(uid)
    
    self.receivedPart(writer, count, data, done)

    if done:
      self.getComplete(uid)

  def receivedPart(self, writer, count, data, done):
    """
    Override this to handle file part reception.
    """
    if done:
      writer.write(count, data)
      writer.close()
      return

    writer.write(count, data)

  def buildGenerator(self, path):
    """
    Implement to create a generator, a source of data.
    """

  def buildWriter(self, path, size):
    """
    Implemen to create a writer, a target of data.
    """

  def putComplete(self, uid):
    """
    Put is completed
    """

  def getComplete(self, uid):
    """
    Put is completed
    """

  functions = {
    PUT              : dsh_PUT,
    PUT_ACK          : dsh_PUT_ACK,
    GET              : dsh_GET,
    GET_RESPONSE     : dsh_GET_RESPONSE,
    REQUEST_PART     : dsh_REQUEST_PART,
    SEND_PART        : dsh_SEND_PART,
    ERROR            : dsh_ERROR,
    DISABLED         : dsh_DISABLED,
  }

  def dsh_error(self, message):
    """
    Override to implement error handling, default is to log and loose connection.
    """
    log.msg("dsh_error: %s" % (message))
    self.transport.loseConnection()

  def dsh_disabled(self, message):
    """
    Override to implement error handling, default is to log and loose connection.
    """
    log.msg("dsh_disabled : %s" % (message))

  def dsh_request_part(self, uid, count):
    """
    Override to handle part request.
    """
    if uid not in self._send:
      self.error("no transfer in progress")
      return

    uid, path, generator = self._send.get(uid)

    try:
      data = generator.read(count)
      self.send_part(uid, count, data)
    except Exception, e:
      import sys
      self.error("internal error: " + str(e))
      exc = sys.exc_info()
      raise exc[1], None, exc[2]

  def get(self, path):
    self.sendFrame(self.GET, data=path)

  def get_response(self, uid, size, path):
    self.sendFrame(self.GET_RESPONSE, info=(uid, size), data=path)

  def put(self, uid, size, path):
    self.sendFrame(self.PUT, info=(uid, size), data=path)
    return uid

  def request_part(self, uid, count):
    self.sendFrame(self.REQUEST_PART, info=(uid, count))

  def send_part(self, uid, count, data):
    if self.checkDigest:
      digest = self.digestFunction(data)
    else:
      digest = "\x00" * self.digestSize

    self.sendFrame(self.SEND_PART, info=(uid, count, digest), data=data)

  def put_ack(self, uid):
    self.sendFrame(self.PUT_ACK, info=(uid,))

  def _feeder(self):
    while True:
      c = yield 1

      if not str(c) in self.commands:
        self.error("no such command")
        return

      st = self.frames.get(c)
      
      res = yield st.size
      infodata = st.unpack(res)

      data_size, info = infodata[0], infodata[1:]

      if data_size > self.DATA_MAX:
        self.error("data_size greater than DATA_MAX")
        return

      if data_size > 0:
        data = yield data_size
      else:
        data = None

      fn = self.functions.get(c)
      log.msg("R:%s" % (fn.func_name))
      fn(self, info, data)

  def sendFrame(self, c, info=tuple(), data=""):
    if c not in self.commands:
      raise RuntimeError, "not a command %s" % (repr(c))
    fn = self.functions.get(c)

    log.msg("S:%s" % (fn.func_name))

    st = self.frames.get(c)
    self.transport.write(c)
    self.transport.write(st.pack(len(data), *info))
    if data != "":
      self.transport.write(data)

  def putFile(self, path):
    generator = self.buildGenerator(path)

    if not IGenerator.providedBy(generator):
      log.msg("invalid generator: %s" % (repr(generator)))
      self.error("invalid generator")
      return

    uid = self.generateUuid()
    self._send[uid] = (uid, path, generator)
    size  = generator.size()
    self.put(uid, size, path)

  def recvFile(self, uid, size, path):
    if uid in self._recv:
      raise RuntimeError, "transfer already active"

    writer = self.buildWriter(path, size)
    self._recv[uid] = ([False] * writer.parts(), writer)
    return self._recv[uid]

  def error(self, message):
    log.msg("ERROR: " + message)
    self.sendFrame(self.ERROR, data=message)

  def disabled(self, message):
    self.sendFrame(self.DISABLED, data=message)

  def makeConnection(self, transport):
    self._feeder = self._feeder()
    self._required = self._feeder.next()
    self._buffer = bytearray(self.READ_MAX)
    self._p = 0
    self._received = 0

    self._send = dict()
    self._recv = dict()
    protocol.Protocol.makeConnection(self, transport)

  def dataReceived(self, data):
    self._received += len(data)

    while self._received >= self._required:
      data_part = self._required - self._p

      if self._p == 0:
        feed = data[0:data_part]
      else:
        feed = self._buffer[0:self._p] + data[0:data_part]

      rest = data[data_part:]

      self._received -= len(feed)
      self._required = self._feeder.send(feed)
      data = rest
      self._p = 0

    self._buffer[self._p:self._p + len(data)] = data
    self._p += len(data)
