with open("./RLT_ESCLT_D.conf") as f:
    with open("./111.conf","w") as t:
        for i in f:
            i=i.strip().strip("\n")
            i = i+"\"" +"," +"\"" +"\"" +"]" + "\n"
            t.write(i)

