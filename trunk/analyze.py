#! /usr/bin/env python2.5

import sqlite3
import os, sys
import md5
from hachoir_core.error import HachoirError
from hachoir_core.stream import InputStreamError
from hachoir_parser import createParser
from hachoir_metadata import extractMetadata

from torvalddj.config import configuration
from torvalddj.utils import url_to_filename, log_stdout

SQL_SET_TAGGED = "UPDATE file_urls SET tagged = 1 WHERE url = ?"
SQL_SET_INVALID = "UPDATE file_urls SET invalid = 1 WHERE url = ?"
SQL_SET_DUPLICATE = "UPDATE file_urls SET duplicate = 1 WHERE url = ?"

def compute_md5sum(filename):
    """Compute the MD5 checksum of a file.
    """
    hash = md5.new('')
    f = file(filename)
    chunk = f.read(2**16)
    while chunk:
        hash.update(chunk)
        chunk = f.read(2**16)
    f.close()
    return hash.hexdigest()

def hash_name(name):
    """Give a hash for the given name.
    """
    letter = name[0]
    if letter.isalpha():
	return letter.upper()
    return '0-9'

def get_metadata(metadata, key):
    """Return metadata entry for key.
    """
    if metadata.has(key):
	value = metadata.get(key).strip()
	value = value.replace(os.path.sep, '_')
	return value.title()
    return None

def order_files():
    connection = sqlite3.connect(configuration.database)
    cursor = connection.cursor()
    cursor.execute("SELECT url FROM file_urls WHERE downloaded = 1 AND invalid ISNULL AND tagged ISNULL")
    added_files = 0
    invalids = 0
    invalids_mime = 0
    invalids_copy = 0
    for entry in cursor.fetchall():
	filename = url_to_filename(entry[0])
	if not os.path.exists(filename):
	    continue
	try:
	    parser = createParser(filename)
	except InputStreamError, err:
	    log_stdout(unicode(err))
	    continue
	if not parser:
	    log_stdout("[error] With file: %s" % filename)
	    log_stdout("[error] Unable to parse file")
	    cursor.execute(SQL_SET_INVALID, (entry[0],))
	    connection.commit()
	    invalids += 1
	    continue
	if not (parser.mime_type in (u'audio/mpeg', u'audio/vorbis',)):
	    log_stdout("[error] With file: %s" % filename)
	    log_stdout("[error] Invalid mime type %s" % parser.mime_type)
	    cursor.execute(SQL_SET_INVALID, (entry[0],))
	    connection.commit()
	    invalids_mime += 1
	    continue
        try:
            metadata = extractMetadata(parser, 0.5)
        except HachoirError, err:
            log_stdout(unicode(err))
            continue
	title = get_metadata(metadata, 'title')
	artist = get_metadata(metadata,'author')
	album = get_metadata(metadata, 'album')
	if not title or not artist:
	    log_stdout("[error] With file: %s" % filename)
	    log_stdout('[error] No artist (%s) or title (%s), album (%s)' %
		       (artist, title, album))
	    continue

	new_destination = os.path.join(configuration.analyze['destination'],
				       hash_name(artist),
				       artist)
	if album:
	    new_destination = os.path.join(new_destination, album)
	try:
	    if not os.path.exists(new_destination):
		os.makedirs(new_destination)
	except:
	    log_stdout("[error] With file: %s" % filename)
	    log_stdout('[error] Invalid filename computed')
	    continue
	new_filename = os.path.join(new_destination, '%s.mp3' % title)
	try:
	    os.link(filename, new_filename)
	except OSError, err:
	    if err.errno == 17:
		log_stdout("[error] With file: %s" % filename)
		log_stdout("[error] File already here !")
		original_md5 = compute_md5sum(new_filename)
		new_md5 = compute_md5sum(new_filename)
		if original_md5 == new_md5:
		    cursor.execute(SQL_SET_INVALID, (entry[0],))
		    cursor.execute(SQL_SET_DUPLICATE, (entry[0],))
		    connection.commit()
		    invalids_copy += 1
	    else:
		log_stdout(str(err))
	    continue
	except TypeError:
	    continue

	# Add to iTunes
	configuration.player.add(new_filename)
	
	cursor.execute(SQL_SET_TAGGED, (entry[0],))
	connection.commit()
	added_files += 1
	
	
    log_stdout("%d invalids files." % invalids)
    log_stdout("%d invalids mime type." % invalids_mime)
    log_stdout("%d duplicates." % invalids_copy)
    log_stdout("%d new files." % added_files)
    connection.close()

def create_db():
    pass

if __name__ == '__main__':
    if len(sys.argv) < 2 or sys.argv[1] == 'analyze':
	order_files()
    elif sys.argv[1] == 'createdb':
	create_db()
    
