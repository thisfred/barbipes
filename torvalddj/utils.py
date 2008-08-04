
import urllib2
import os.path

from config import configuration

def log_stdout(msg):
    """Print msg to the screen."""
    msg_ascii = msg.encode('ascii', 'replace')
    print msg_ascii


def url_to_filename(url):
    """ Return the url associated with this filename.
    """
    return os.path.join(configuration.repository,
			urllib2.unquote(url.split('://')[1]))
