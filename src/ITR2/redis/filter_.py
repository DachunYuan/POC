def dstct(res,inpstr,outstr):
    res.replace('[','').replace(']','')
    l_emp = list(eval(res))
    t_emp = []
    out_res = []
    for item in l_emp:
        if inpstr:
            for i in inpstr:
                if i in item:
                    t_emp.append(tuple(item))
    t_emp = list(tuple(set(t_emp)))
    for q in t_emp:
        if outstr:
            for out in outstr:
                out_res.append(tuple(q)[outstr[out-1]])
    if outstr:
        return out_res
    else:
        return t_emp
