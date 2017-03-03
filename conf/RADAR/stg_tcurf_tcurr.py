# -*- coding: UTF-8 -*-
# Date: 2017-1-9 10:08:41
# Author: zhen-peng.yang@hpe.com (Arvin)


# imports
import datetime


# classes
class stg_tcurf_tcurr:
    from_curr = ""
    to_curr = ""
    curr_ratio = 0
    curr_from_dt = datetime.datetime
    curr_to_dt = datetime.datetime
    curr_exch_rate = 0
    curf_ratio = 0
    curf_from_dt = datetime.datetime
    curf_to_dt = datetime.datetime
    curr_drvd_exch_rate = 0

    def getFrom_curr(self):
        return self.from_curr

    def setFrom_curr(self, from_curr):
        self.from_curr = from_curr

    def getTo_curr(self):
        return self.to_curr

    def setTo_curr(self, to_curr):
        self.to_curr = to_curr

    def getCurr_ratio(self):
        return self.curr_ratio

    def setCurr_ratio(self, curr_ratio):
        self.curr_ratio = curr_ratio

    def getCurr_from_dt(self):
        return self.curr_from_dt

    def setCurr_from_dt(self, curr_from_dt):
        self.curr_from_dt = curr_from_dt

    def getCurr_to_dt(self):
        return self.curr_to_dt

    def setCurr_to_dt(self, curr_to_dt):
        self.curr_to_dt = curr_to_dt

    def getCurr_exch_rate(self):
        return self.curr_exch_rate

    def setCurr_exch_rate(self, curr_exch_rate):
        self.curr_exch_rate = curr_exch_rate

    def getCurf_ratio(self):
        return self.curf_ratio

    def setCurf_ratio(self, curf_ratio):
        self.curf_ratio = curf_ratio

    def getCurf_from_dt(self):
        return self.curf_from_dt

    def setCurf_from_dt(self, curf_from_dt):
        self.curf_from_dt = curf_from_dt

    def getCurf_to_dt(self):
        return self.curf_to_dt

    def setCurf_to_dt(self, curf_to_dt):
        self.curf_to_dt = curf_to_dt

    def getCurr_drvd_exch_rate(self):
        return self.curr_drvd_exch_rate

    def setCurr_drvd_exch_rate(self, curr_drvd_exch_rate):
        self.curr_drvd_exch_rate = curr_drvd_exch_rate