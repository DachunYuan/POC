from redis_hash import RedisOp

rs = RedisOp(["get","CI_D"])
if rs[0]:
    rs = list(eval(rs[1]))
    for i in rs:
        key = "CI_D" + '.'+ i[1]
        r=RedisOp(["set",key,i])
        if r[0]:
            pass
        else:
            print("set faild")
