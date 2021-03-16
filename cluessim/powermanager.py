from clueslib.platform import PowerManager

class PowerManager_dummy(PowerManager):
    def __init__(self, nodepool):
        PowerManager.__init__(self)
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
