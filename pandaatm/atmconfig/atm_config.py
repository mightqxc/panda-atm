import re
import sys

from pandacommon.liveconfigparser.LiveConfigParser import LiveConfigParser

# get ConfigParser
tmpConf = LiveConfigParser()

# read
tmpConf.read('panda_atm.cfg')

# dummy section class
class _SectionClass(object):
    pass

# loop over all sections
for tmpSection in tmpConf.sections():
    # read section
    tmpDict = getattr(tmpConf, tmpSection)
    # make section class
    tmpSelf = _SectionClass()
    # update module dict
    sys.modules[ __name__ ].__dict__[tmpSection] = tmpSelf
    # expand all values
    for tmpKey, tmpVal in tmpDict.items():
        # convert string to bool/int
        if tmpVal in ('True', 'true'):
            tmpVal = True
        elif tmpVal in ('False', 'false'):
            tmpVal = False
        elif re.match('^\d+$', tmpVal):
            tmpVal = int(tmpVal)
        # update dict
        setattr(tmpSelf, tmpKey, tmpVal)
