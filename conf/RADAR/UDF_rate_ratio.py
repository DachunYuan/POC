# -*- coding: UTF-8 -*-
# Date: 2017-1-9 10:08:41
# Author: zhen-peng.yang@hpe.com (Arvin)


# imports
import datetime
from stg_tcurf_tcurr import stg_tcurf_tcurr


class rate:

    # attributes
    list_tcurf_tcurr = []

    def ReturnList(self, file):
        with open(file, 'rt') as file:
            while True:
                stg = stg_tcurf_tcurr()
                line = file.readline()
                # print(line)
                if not line:
                    break
                # process each line
                from_curr, to_curr, curr_ratio, curr_from_dt, curr_to_dt, curr_exch_rate, curf_ratio, curf_from_dt, \
                curf_to_dt, curr_drvd_exch_rate = [item for item in line.split(",")]

                # set value
                stg.setFrom_curr(from_curr=from_curr)
                stg.setTo_curr(to_curr=to_curr)
                stg.setCurr_ratio(curr_ratio=curf_ratio)
                stg.setCurr_from_dt(curr_from_dt=datetime.datetime.strptime(curr_from_dt,'%Y-%m-%d'))
                stg.setCurr_to_dt(curr_to_dt=datetime.datetime.strptime(curr_to_dt,'%Y-%m-%d'))
                stg.setCurr_exch_rate(curr_exch_rate=curr_exch_rate)
                stg.setCurf_ratio(curf_ratio=curf_ratio)
                stg.setCurf_from_dt(curf_from_dt=datetime.datetime.strptime(curf_from_dt,'%Y-%m-%d'))
                stg.setCurf_to_dt(curf_to_dt=datetime.datetime.strptime(curf_to_dt,'%Y-%m-%d'))
                stg.setCurr_drvd_exch_rate(curr_drvd_exch_rate=curr_drvd_exch_rate)

                self.list_tcurf_tcurr.append(stg)

    def __init__(self, file):
        self.ReturnList(file)

    def evaluate(self, *args):
        FROM_CURR = args[0]
        TO_CURR = args[1]
        CURR_FROM_DT = datetime.datetime.strptime(args[2],'%Y-%m-%d')

        # to debug, remove later
        # print(FROM_CURR, TO_CURR, CURR_FROM_DT)
        # print("________________________________")

        # here, we declare a empty dictionary to put curr_from_dt:curr_drvd_exch_rate, get 1st record.
        dic_curr_from_dt = {}
        for i in range(len(self.list_tcurf_tcurr)):
            # print(self.list_tcurf_tcurr[i].getFrom_curr(), self.list_tcurf_tcurr[i].getTo_curr(),
            #        self.list_tcurf_tcurr[i].getCurr_from_dt())
            if FROM_CURR == self.list_tcurf_tcurr[i].getFrom_curr() and TO_CURR == self.list_tcurf_tcurr[i].getTo_curr()\
                    and CURR_FROM_DT >= self.list_tcurf_tcurr[i].getCurr_from_dt():
                # return str(self.list_tcurf_tcurr[i].getCurr_drvd_exch_rate())
                dic_curr_from_dt.setdefault(self.list_tcurf_tcurr[i].getCurr_from_dt(), self.list_tcurf_tcurr[i].getCurr_drvd_exch_rate())
        
        # here, we sort dictionary by key, and extract corresponding value
        ls_sorted = sorted(dic_curr_from_dt.items(),key=lambda item: item[0])
        
        if len(ls_sorted) != 0 and len(ls_sorted[0]) == 2:
            # t = ls_sorted.pop()
            # return t[1]
            return ls_sorted[0][1]
        else:
            return None


def main():
    rt = rate("C:\\Users\\yanzhenp\\Documents\\radar\\trunk\\03.ETL\\Code\\udf\\stg_tcurf_tcurr")
    print(rt.evaluate('EUR', 'USD', '2004-07-01'))


if __name__ == "__main__":
    main()
