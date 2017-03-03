import os,datetime,cx_Oracle,shutil,json,logging,time
from kafka import KafkaClient,SimpleProducer,SimpleConsumer

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='tranfer_data.log',
                filemode='a')

# Define global variance
path = os.getcwd()
names = locals()
sql_json_file=open('extract_sql.json','r')
sql_json_decode=json.load(sql_json_file)
sql_json_file.close()
for entry in sql_json_decode:
	 names[entry['table']]=[]

# Keep modified/updated/new data to file
def writeFile(data_file,diff_data):
	# Write data to file	
	index=1
	ff=open(data_file,'w')
	for i in diff_data:
		if index < len(diff_data):
			ff.write(i)
			ff.write('\n')
			index+=1
		else:
			ff.write(i)
	ff.close()
	# Backup data file under archve directory
	src_dir=path+'\ArcFile'
	if os.path.exists(src_dir):
		shutil.move(data_file,src_dir)
	else:
		os.mkdir(src_dir)
		shutil.move(data_file,src_dir)

# Compare data between 2 files		
def compareFile(file_name,previous_file,current_file):
	data_file=file_name+datetime.datetime.strftime(datetime.datetime.now(),'%Y%m%d_%H%M%S')+'.DAT'
	os.chdir(path)
	f1=open(previous_file)
	s1=f1.read().split('\n') #Got a list
	f1.close()
	f2=open(current_file)
	s2=f2.read().split('\n')
	f2.close()
	diff_data=list(set(s2).difference(s1))	
	
	if len(diff_data)>=1: 
		#have data need to be transer
		#writeFile(path,data_file,diff_data)
		
		#Tranfer each line to kafka
		for msg in diff_data:
			sendMsgToKafka(file_name,msg)
	else:
		logging.info ('%s No change need to be tranferred'%(file_name))
		
	#keep ths latest file as basic to compare with the next
	par=open(file_name,'w')
	par.write(current_file)
	par.close()
	
	os.remove(previous_file)
	#remove previous file,just keep the latest one to compare with the new one 
	#arc_dir=path+'\ArcFiles'
	#if os.path.exists(arc_dir):
	#	shutil.move(previous_file,arc_dir)
	#else:
	#	os.mkdir(arc_dir)
	#	shutil.move(previous_file,arc_dir)
		
def sendMsgToKafka(obj,msg):
	#msg=msg
	#obj=obj
	client = KafkaClient("c9t26359.itcs.hpecorp.net:9092")# 
	producer = SimpleProducer(client)
	producer.send_messages(obj,msg)
	#response=producer.send_messages(obj,msg)
	#print response
	client.close()

def connectDB():
	# Read TNS_entry info from json
	db_json_file=open('db.json','r')
	db_json_decode=json.load(db_json_file)
	user=db_json_decode["HPSM_PRO"][0]["user"]
	pwd=db_json_decode["HPSM_PRO"][0]["password"]
	db=db_json_decode["HPSM_PRO"][0]["db"]
	db_json_file.close()	
	conn=cx_Oracle.connect(user,pwd,db)
	return conn

def extractData():
	conn=connectDB()
	cur=conn.cursor()		
	#Read extract SQL from json
	sql_json_file=open('extract_sql.json','r')
	sql_json_decode=json.load(sql_json_file)

	conn=connectDB()
	cur=conn.cursor()
	for entry in sql_json_decode:
		cur.execute(entry['sql'])
		# Returning a list within all rows
		rowlist=cur.fetchall()
		# list_row uses to store rows' data
		list_row = []
		for i in rowlist:
		    # list_filed: store each fields' value
			list_field = []
			for j in i:
				# cx_Oracle.LOB.read(): Read data details from LOB object
				if type(j) == cx_Oracle.LOB:
					list_field.append(j.read())
				else :
					list_field.append(j)
			list_row.append(list_field)
		#print len(list_row)

		if len(list_row) >=1:
			sendMsgToKafka(entry['table'],str(list_row))
			logging.info ('Send object:%s to kafka'%(entry['table']))
			#if len(globals()[entry['table']]) ==0: # The first time send data to kafka, no need to compare
			#	globals()[entry['table']]=list_row
			#	sendMsgToKafka(entry['table'],str(list_row))
			#	logging.info ('The first time to send object:%s to kafka'%(entry['table']))
			#else:
			#	diff_data=list(set(rowlist).difference(globals()[entry['table']])) # Compare current result with pre-result
			#	if len(diff_data)>0: # Existing difference
			#		globals()[entry['table']]=list_row
			#		sendMsgToKafka(entry['table'],str(diff_data))
			#		logging.info ('Compare then send %d rows data to kafka for %s'%(len(diff_data),entry['table']))
			#	else:
			#		#No difference
			#		logging.info ('Compared,current results is same as before for %s'%(entry['table']))
		else: #No data from DB at current period
			logging.info ('No %s need to be extracted from DB at this period'%(entry['table']))
	cur.close()
	conn.close()


def repeat():
	start_time = datetime.date.today()
	end_time=datetime.date(2017,2,24)
	interval =(end_time - start_time).days
	while interval >=0:
		extractData()
		time.sleep(60)
		interval = (end_time - start_time).days

if __name__ == '__main__':
	repeat()
	#extractData()
