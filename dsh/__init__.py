#

import sys

ez={
    "M2Crypto" : "m2crypto",
    "twisted"  : "twisted",
    "yaml"     : "pyyaml",
}

for k,i in ez.items():
  try:
    __import__(k)
  except ImportError, e:
    print("required module '%s' could not be imported: %s" % (k, e))
    print("please run: easy_install %s" % (i))
    sys.exit(255)

from twisted.internet import reactor
from twisted.internet import protocol
from twisted.python import log

from .impl import DshServerProtocol
from .impl import DshClientPutProtocol
from .impl import DshClientGetProtocol

def server():
  import sys
  import yaml

  log.startLogging(sys.stdout)

  factory = protocol.ServerFactory()
  factory.protocol = DshServerProtocol
  factory.db = "server.db"
  reactor.listenTCP(1211, factory)
  reactor.run()

from twisted.internet import protocol

class DshClientFactory(protocol.ClientFactory):
  def __init__(self, reactor, protocolClass, *args, **kwargs):
    self.reactor = reactor
    self.protocolClass = protocolClass
    self.args = args
    self.kwargs = kwargs

  def buildProtocol(self, addr):
    p = self.protocolClass(*self.args, **self.kwargs)
    return p

  def clientConnectionFailed(self, transport, reason):
    print transport, reason
    self.reactor.stop()

  def clientConnectionLost(self, transport, reason):
    print transport, reason
    self.reactor.stop()

def client():
  import sys
  import yaml

  if len(sys.argv) != 4:
    print "usage: dsh <host> <oper> <file>"
    sys.exit(1)

  pp = sys.argv[1].split(":", 2)

  oper = sys.argv[2]
  path = sys.argv[3]

  if len(pp) == 2:
    host, port = pp
    port = int(port)
  else:
    host, port = pp[0], 1211

  factory = None

  if oper.upper() == "GET":
    factory = DshClientFactory(reactor, DshClientGetProtocol, path)
  elif oper.upper() == "PUT":
    factory = DshClientFactory(reactor, DshClientPutProtocol, path)
  else:
    print "invalid operation", oper
    sys.exit(2)

  reactor.connectTCP(host, port, factory)
  reactor.run()
