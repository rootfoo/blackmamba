#!/usr/bin/env python

from distutils.core import setup

setup(name          = 'blackmamba',
      version       = '0.1',
      description   = 'Coroutine Based Concurrent Networking Library',
      author        = 'Marcus Hodges',
      author_email  = 'meta@rootfoo.org',
      url           = 'http://rootfoo.org/blackmamba',
      packages      = ['blackmamba'],
      install_requires  = ['adns-python'],
     )

