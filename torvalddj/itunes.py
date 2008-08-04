
# iTunes stuff
import appscript
import mactypes


class Player(object):

    def __init__(self):
	self.player = appscript.app('iTunes.app')

    def add(self, filename):
	itunes.add(mactypes.Alias(filename))

    
