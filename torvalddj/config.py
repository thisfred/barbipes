
from ConfigParser import ConfigParser

DEFAULT_CONFIGURATION = 'barbipes.cfg'

class ConfigurationObject(object):
    """Generic access to configuration.
    """

    def __init__(self):
	self._config = ConfigParser()
	self._config.read(DEFAULT_CONFIGURATION)
	self._player = None

    @property
    def player(self):
	if self._player is None:
	    name = self._config.get('main', 'player')
	    assert not ('.' in name)
	    self._player = __import__(name,  globals(), locals(), ['Player',]).Player()
	return self._player

    @property
    def database(self):
	return self._config.get('main', 'database')

    @property
    def repository(self):
	return self._config.get('main', 'repository')

    @property
    def analyze(self):
	return dict(self._config.items('analyze'))
	    

configuration = ConfigurationObject()
