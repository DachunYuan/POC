# -*- coding:utf-8 -*- 
#argument example
#fileName ="./RLT_ESCLT_EVNT_D.conf"
#tableName = "ITR23.RLT_ESCLT_EVNT_D"
def generateInsertSQL(tableName,fileName):
    a = ["NVL(TO_NUMBER(%s),NULL)","NULLIF('%s','NULL')","NVL(TO_DATE(NULLIF('%s','NULL'),'YYYY-MM-DD HH24:MI:SS'),null)"]
    value_update = ''
    column_insert = ''
    with open(fileName) as f:
        content = f.readlines()
        lineNumber = len(content)
        count = 1
        for line in content:
            l = list(eval(line.strip('\n')))
            if count < lineNumber:
                column_insert = column_insert + l[0] + ','
                if l[1]=="number":
                    value_update = value_update+a[0] + ','
                elif l[1]=="char":
                    value_update = value_update+a[1] + ','
                else:
                    value_update = value_update+a[2] + ','
                count = count + 1
            else:
                column_insert = column_insert + l[0] 
                if l[1]=="number":
                    value_update = value_update+a[0] 
                elif l[1]=="char":
                    value_update = value_update+a[1] 
                else:
                    value_update = value_update+a[2] 

    result = 'INSERT INTO '+ tableName + '('+column_insert+')'+'VALUES' +'('+value_update+')'
    return result
#argument example
#fileName ="./RLT_ESCLT_EVNT_D.conf"
#tableName = "ITR23.RLT_ESCLT_EVNT_D"
#primaryKey = ["ESCLT_DVC_D_KY","1"]
def generateUpdateSql(tableName,fileName,primaryKey):
    a = ["NVL(TO_NUMBER(%s),NULL)","NULLIF('%s','NULL')","NVL(TO_DATE(NULLIF('%s','NULL'),'YYYY-MM-DD HH24:MI:SS'),null)"]
    value_update = ''
    key_update = ''
    for i in range(len(primaryKey)):
        num = len(primaryKey)
        if i%2 ==1:
            if i < (num-1):
                key_update = key_update + primaryKey[i-1] + '=' +primaryKey[i] + ','
            else:
                key_update = key_update + primaryKey[i-1] + '=' +primaryKey[i] 
                   
    with open(fileName) as f:
        content = f.readlines()
        lineNumber = len(content)
        count = 1
        for line in content:
            l = list(eval(line.strip('\n')))
            if count < lineNumber:
                if l[1]=="number":
                    value_update = value_update+l[0]+'='+ a[0] + ','
                elif l[1]=="char":
                    value_update = value_update+l[0]+'='+ a[1] + ','
                else:
                    value_update = value_update+l[0]+'='+ a[2] + ','
                count = count + 1
            else:
                if l[1]=="number":
                    value_update = value_update+l[0]+'='+ a[0] 
                elif l[1]=="char":
                    value_update = value_update+l[0]+'='+ a[1] 
                else:
                    value_update = value_update+l[0]+'='+ a[2] 
    result = "UPDATE " + tableName +" SET " +value_update + " WHERE " + key_update
    return result
