#

import sys

ez={
    "M2Crypto": "m2crypto",
    "yaml": "pyyaml",
    "twisted": "twisted"
}

for k,i in ez.items():
  try:
    __import__(k)
  except ImportError, e:
    print("required module '%s' could not be imported: %s" % (k, e))
    print("please run: easy_install %s" % (i))
    sys.exit(255)

from twisted.internet import protocol
from twisted.internet import reactor

class ServerProtocol(protocol.Protocol):
  def dataReceived(self, data):
    print data

def server():
  import sys
  import yaml

  factory = protocol.ServerFactory()
  factory.protocol = ServerProtocol()
  reactor.listenTCP(1211, factory)
