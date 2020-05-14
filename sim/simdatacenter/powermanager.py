import clues.clueslib.platform

class PowerManager_dummy(clues.clueslib.platform.PowerManager):
    def __init__(self, nodepool):
        clues.clueslib.platform.PowerManager.__init__(self)
        self.nodepool = nodepool
    def power_on(self, nname):
        # print nname
        if nname in self.nodepool.nodenames():
            return self.nodepool[nname].power_on(), nname
        return False, nname
    def power_off(self, nname):
        if nname in self.nodepool.nodenames():
            return self.nodepool[nname].power_off(), nname
        return False, nname