

from zope import interface		# Waiting for Python 3.1

class IPlayer(interface.Interface):
    """Define a player.
    """

    def add(filename):
	"""Add this song to the library.
	"""
