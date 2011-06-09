from zope.interface import Interface, Attribute, implements

class IGenerator(Interface):
  def read(self, count):
    """Read and return a string which is of maximum size DshProtocol.DATA_MAX"""

  def size(self):
    """Return the total amount of parts from this generator"""

  def parts(self):
    """Return the number of parts this generator will occupy"""

class IWriter(Interface):
  def open(self, path, size, populate=False):
    """
    Allocate and open a stream to the target.
    
    path     - path to write size
    size     - size of the file in bytes
    populate - indicates weither file should be pre-populated with zeroes.
    """

  def write(self, count, data):
    """
    Write some data to a specific position.
    @digest if digest is None, do not check.
    @raise RuntimeError if problem arises during writing.
    """
  
  def close(self):
    """
    Close the writer.
    @raise RuntimeError if problem arises.
    """

  def parts(self):
    """
    Return the number of parts this writer is expected to yield.
    """
