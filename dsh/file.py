import hashlib
import os

import bencode
import uuid

encode = bencode.bencode
decode = bencode.bdecode
hashDigest = lambda s: hashlib.sha1(s).digest()
generateUuid = lambda: uuid.uuid4().bytes

class FileVerif:
  """
  File verification class, which associates a File to a MetaFile and is able to
  perform detailed checking.
  """
  def __init__(self, f, m):
    """
    Verify file 'f' against meta 'm'
    """
    self.f = f
    self.fm = None
    self.m = m
    self.ok_size   = None
    self.ok_hashes = None

  def update(self):
    self.ok_size    = self.ok_size or m.size == self.f.get_size()
    z = zip(self.ok_hashes, xrange(len(self.ok_hashes)))
    self.ok_hashes  = [ok_hash or self.m.get_hash(i) == self.f.hashdigest_one(i) for ok_hash, i in z]

  def create(self):
    self.ok_size = self.m.size == self.f.get_size()
    self.fm = self.f.metadigest()
    a = self.fm.get_hash
    b = self.m.get_hash

    if self.fm.hashsize == self.m.hashsize:
      self.ok_hashes  = [a(i) == b(i) for i in xrange(self.m.hashsize)]

  def all_ok(self):
    return self.ok_size and all(self.ok_hashes)

class FileMeta:
  HASH_SIZE = 20

  def __init__(self, path, size, hashes):
    self.path     = path
    self.size     = size
    self.hashes   = hashes
    self.hashsize = len(hashes) / self.HASH_SIZE

  def get_hash(self, i):
    if len(self.hashes) < i * self.HASH_SIZE:
      raise ValueError, "hash out of range"
    return self.hashes[i*self.HASH_SIZE:i*self.HASH_SIZE+self.HASH_SIZE]

  def dump(self):
    return {
      "path": self.path,
      "hashes": self.hashes,
      "size": self.size
    }

  @classmethod
  def load(klass, h):
    path    = h.get("path")
    size    = h.get("size")
    hashes  = h.get("hashes")
    return klass(path, size, hashes)

  def tofile(self, base):
    return File(base, *self.path)

  def __repr__(self):
    return "<FileMeta path=%s, hashes=%s, size=%s>" % (repr(self.path), "None" if self.hashes is None else len(self.hashes), repr(self.size))

class File:
  CHUNK_SIZE = 2 ** 20

  def __init__(self, base, *path):
    self.base = base
    self.path = path

  def metadigest(self):
    size = self.get_size()
    hashes = "".join((h for h in self.hashdigest_all()))
    return FileMeta(self.path, size, hashes)

  def hashdigest_all(self):
    fp = open(self.get_path(), "r")

    try:
      while True:
        part = fp.read(self.CHUNK_SIZE)
        if not part: break
        yield hashDigest(part)
    finally:
      fp.close()

  def hashdigest_one(self, i):
    fp = open(self.get_path(), "r")

    try:
      fp.seek(self.CHUNK_SIZE * i)
      part = fp.read(self.CHUNK_SIZE)
      return hashDigest(part)
    finally:
      fp.close()

  def get_size(self):
    return os.path.getsize(self.get_path())
  
  def verify(self, meta):
    if not os.path.isfile(self.get_path()):
      raise RuntimeError, "no such file"

    verif = FileVerif(self, meta)
    verif.create()
    return verif

  def isfile(self):
    return os.path.isfile(self.get_path())

  def get_path(self):
    return os.path.join(self.base, *self.path)

  @classmethod
  def opendir(klass, base):
    pjoin = os.path.join
    psplit = os.path.split

    def generator():
      for dirpath, dirnames, filenames in os.walk(base):
        for f in filenames:
          path = [f]
          root = dirpath
          c    = f
          while root != base:
            root, c = psplit(root)
            path.append(c)
          path.reverse()
          yield tuple(path), pjoin(dirpath, f)

    for p, real in generator():
      yield klass(base, *p)

class MetaPackage:
  def __init__(self, files, uuid=None):
    self.files = files

    if not uuid:
      self.uuid = generateUuid()
    else:
      self.uuid = uuid

  def dump(self):
    return {
      "files": [f.dump() for f in self.files],
      "uuid": self.uuid
    }

  def dumps(self):
    return encode(self.dump())

  @classmethod
  def load(klass, h):
    files = [FileMeta.load(fh) for fh in h.get("files", [])]
    uuid  = h.get("uuid", [])
    return klass(files, uuid)

  @classmethod
  def loads(klass, s):
    return klass.load(decode(s))

if __name__ == "__main__":
  files = list(File.opendir("."))
  package = MetaPackage([f.metadigest() for f in files])
  s = package.dumps()

  package = MetaPackage.loads(s)

  metas = [f.metadigest() for f in files]

  #for meta, f in zip(metas, files):
    #verif = f.verify(meta)
    #verif.update()

  for meta in metas:
    f = meta.tofile("../testdir")

    if not f.isfile():
      print "not a file", f.get_path()
      continue

    verif = f.verify(meta)
    if not verif.all_ok():
      print f.get_path(), verif.ok_size, verif.ok_hashes
