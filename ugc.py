#!/usr/bin/python3

################################################################################
#
# ugc.py
#
# User Generated Content store.
#
# Manages a directory tree full of files that come from outside the app and
# therefore cannot be trusted. At the moment this just involves not using
# user-supplied filenames on disk. Instead we use the SHA-256 of the file
# contents. Once stored, files are immutable.
# This module also transparently manages compression and de-compression.
#
#
# Requires zstandard
# https://github.com/indygreg/python-zstandard
# https://python-zstandard.readthedocs.io/
#
# $ pip3 install zstandard
#
#
# Andy Bennett <andyjpb@register-dynamics.co.uk>, 2024/08/09 17:33.
#
################################################################################


import io
import os
import errno
import hashlib
import pathlib
import zstandard


################################################################################
### Support Objects for internal use only

# Writer Object States
READY = 1
FINAL = 2

# A file-like object that allows User Generated Content to be written to disk.
# The maximum size must be known up front.
# A name that can be used to retrive the file later is provided when the user
# calls finalise().
class UGCWriter(io.BufferedWriter):

    # Allocate a UGC object.
    # A maximum size must be specified.
    def __init__(self, max_size, *args):

        self.filename = "temp-%d" % os.getpid()
        self.fd       = io.open("tmp/%s" % self.filename, "xb")
        self.digest   = hashlib.sha256()
        self.max_size = max_size

        io.BufferedWriter.__init__(self, self.fd, *args)

        self.state    = READY


    # Write the supplied chunk provided the file can accept it.
    def write(self, chunk):

        if self.state != READY:
            raise ValueError("write to finalised storage")

        if (self.tell() + len(chunk)) > self.max_size:
            raise OSError(errno.ENOSPC, "write would exceed allocated storage")

        n = io.BufferedWriter.write(self, chunk)
        self.digest.update(chunk)

        return n


    # Mark the storage as finalised, flush it to disk and return its name.
    # It will not be permenantly durable until the user calls commit().
    def close(self):

        name = ""

        if self.state == READY:

            self.state = FINAL
            name       = self.digest.hexdigest()

            io.BufferedWriter.close(self)
            os.rename("tmp/%s" % self.filename, "tmp/%s" % name)

        else:

            name = self.digest.hexdigest()


        return name



################################################################################
### Module Public API

# Allocates a new file in ugc and returns an open, writeable, binary file-like
# object for it.
# The file will not be permenantly durable until it is successfully committed.
# The space used by files that do not get committed can be easily cleaned up.
def allocate(size):

    return UGCWriter(size)


# Move a named file to permenant storage so that it can be opened again later.
# Commit is a separate step after UGCWriter.close() so that the allocated space
# can be cleaned up if the caller is in the middle of a transaction that fails.
# For example, writing the name of the written file to a database.
def commit(name):

    os.rename("tmp/%s" % name, "ugc/%s" % name)


# Open a named file.
# Looks for an uncompressed version first followed by a compressed version.
# Returns an open, readable, binary file-like object that transparently handles
# the de-compression if necessary.
def open(name):

    # Try the uncompressed version first.
    try:

        fd = io.open("ugc/%s" % name, 'rb')

        return fd

    except FileNotFoundError:
        pass

    # Next try the compressed version.
    try:

        fd   = io.open("ugc/zstd/%s" % name, 'rb')
        dctx = zstandard.ZstdDecompressor()

        return dctx.stream_reader(fd)

    except FileNotFoundError:
        pass

    # We can't find it.
    raise FileNotFoundError("No such UGC object: '%s'" % name)


# Initialise the module and ensure the backing store has been created.
def init():
    # Initialise the filesystem.

    # "User Generated Content" - i.e. stuff we've downloaded.
    pathlib.Path("ugc/").mkdir(exist_ok=True)

    # UGC that's been compressed with zstandard.
    pathlib.Path("ugc/zstd/").mkdir(exist_ok=True)

    # Temporary files.
    pathlib.Path("tmp/").mkdir(exist_ok=True)

