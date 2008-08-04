Program to backup MP3 from your blog
====================================

Dependency
----------

* Python 2.5,

* hachoir-metadata used by ``analyze.py``. You can install this
  package with easy_install.

* curl, used to make backup.

Use
---

Copy ``barbipes.cfg.in`` to ``barpipes.cfg``, and edit to your
settings.

Add an URL::

  $ ./barpipes.py add url-of-your-blog

Occasionally, harvest blog entries::

  $ ./barpipes.py

And if you have some file in the listing::

  $ ./barpipes.py list

You can download them::

  $ ./barpipes.py download

After, it nice to analyze them, and add them to your player::

  $ ./analyze.py

And remove crappy ones::

  $ ./barpipes.py clean


