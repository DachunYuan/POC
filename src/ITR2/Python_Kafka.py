import os,datetime,cx_Oracle,shutil

from kafka import KafkaClient,SimpleProducer,SimpleConsumer

class myFilter():
		#def __init__(self):
		#	self.previous_file=previous_file
		#	self.current_file=current_file
		#	self.path=path
		#	self.data_file=self.current_file[0:-19]+datetime.datetime.strftime(datetime.datetime.now(),'%Y%m%d_%H%M%S')+'.DAT'
		def compareFile(self,path,file_name,previous_file,current_file):
			data_file=file_name+datetime.datetime.strftime(datetime.datetime.now(),'%Y%m%d_%H%M%S')+'.DAT'
			os.chdir(path)
			f1=open(previous_file)
			s1=f1.read().split('\n') #Got a list
			f1.close()
			f2=open(current_file)
			s2=f2.read().split('\n')
			f2.close()
			s=list(set(s2).difference(s1))	
			index=1
			if len(s)>=1: #have data need to be transer
							# Write data to file
							#ff=open(data_file,'w')
							#for i in s:
							#	if index < len(s):
							#		ff.write(i)
							#		ff.write('\n')
							#		index+=1
							#	else:
							#		ff.write(i)
							#ff.close()
							#print 'Tranfer file',data_file
							#
							## send file ---?
							#src_dir=path+'\inbound'
							#if os.path.exists(src_dir):
							#	shutil.move(data_file,src_dir)
							#else:
							#	os.mkdir(src_dir)
							#	shutil.move(data_file,src_dir)
			
				for msg in s:
					self.sendMsgToKafka(file_name,msg)
			else:
				print '%s No change need to be tranferred at %s:'%(file_name,datetime.datetime.strftime(datetime.datetime.now(),'%Y/%m/%d %H:%M:%S'))
			
				#remove previous file,just keep the latest one to compare with the new one
				#os.remove(previous_file) 
		
			par=open(file_name,'w')
			par.write(current_file)
			#par.write(current_file+'\n')
			par.close()
		
			os.remove(previous_file)
					#arc_dir=path+'\ArcFiles'
					#if os.path.exists(arc_dir):
					#	shutil.move(previous_file,arc_dir)
					#else:
					#	os.mkdir(arc_dir)
					#	shutil.move(previous_file,arc_dir)
			
		def sendMsgToKafka(self,topic,msg):
		#msg=msg
		#obj=obj
			kafka = KafkaClient("c9t26359.itcs.hpecorp.net:9092")
			producer = SimpleProducer(kafka)
			producer.send_messages(topic,msg)
		
		#response=producer.send_messages(obj,msg)
		#print response
			kafka.close()
		
		def connectDB(self):
			path=os.getcwd()
			dict_file={
				'HPESCLTA1':'''SELECT A1.HP_ESCLT_ID,TO_CHAR(A1.RECORD_NUMBER),NVL(TO_CHAR(A1.HP_PRBLM_CI_NM),'NULL') FROM ITSMP.HPESCLTM1 M, ITSMP.HPESCLTA1 A1 where M.HP_ESCLT_ID=A1.HP_ESCLT_ID AND M.SYSMODTIME>=SYSDATE - 7''',
				'HPESCLTA2':'''SELECT A2.HP_ESCLT_ID,TO_CHAR(A2.RECORD_NUMBER),NVL(TO_CHAR(A2.ML_KM_UPD_TYPE_KY),'NULL'),NVL(A2.HP_KM_UPD_DTL_TX,'NULL') FROM ITSMP.HPESCLTM1 M, ITSMP.HPESCLTA2 A2 where M.HP_ESCLT_ID=A2.HP_ESCLT_ID AND M.SYSMODTIME>=SYSDATE - 7''',
				'HPESCLTA3':'''SELECT A3.HP_ESCLT_ID,TO_CHAR(A3.RECORD_NUMBER),NVL(TO_CHAR(A3.ML_CPCTY_ISS_TYPE_KY),'NULL'),NVL(A3.HP_CPCTY_ISS_TDL_TX,'NULL') FROM ITSMP.HPESCLTM1 M, ITSMP.HPESCLTA3 A3 where M.HP_ESCLT_ID=A3.HP_ESCLT_ID AND M.SYSMODTIME>= SYSDATE -7'''
			}
			#file_names=dict_file.viewkeys()
			conn=cx_Oracle.connect("HPSM_ODS_RO/oprpt!Usr894Feb@ITSMP")
			cur=conn.cursor()
			for(k,v) in dict_file.iteritems():
				cur.execute(v)
				rowlist=cur.fetchall()
				current_file=k+datetime.datetime.strftime(datetime.datetime.now(),'_%Y%m%d_%H%M%S')+'.DAT'
				rows=1
				if len(rowlist)>=1:
					ff=open(current_file,'w+')
					for row in rowlist:
						column=1
						for filed in row:	
							ff.write(filed)
							if column<len(row):ff.write('\x1f');column+=1 #'\037 in ETL'
						if rows<len(rowlist):ff.write('\n');rows+=1
						else:pass
					ff.close()
				
					if os.path.exists(k) and len(open(k,'rU').readlines())>=1: #compare file
						par=open(k)
						#previous_file=par.readline()[0:-1]
						previous_file=par.readline()
						par.close()
						#print previous_file
						#print ('compare: %s and %s'%(previous_file,current_file))
						self.compareFile(path,k,previous_file,current_file)
					else: # create par file and send data file
						par=open(k,'a+')
						par.write(current_file)
						#par.write(current_file+'\n')
						par.close
						# send file --?
						
						fdate=open(current_file)
						list_rows=fdate.read().split('\n')
						for row in list_rows:
							self.sendMsgToKafka(k,row)	
						fdate.close()
					
						#src_dir=path+'\inbound'
						#if os.path.exists(src_dir):
						#	shutil.copy(current_file,src_dir)
						#else:
						#	os.mkdir(src_dir)
						#	shutil.copy(current_file,src_dir)	
						#print ('Finished extracting file %s at %s,contains %d rows data'%(current_file,datetime.datetime.strftime(datetime.datetime.now(),'%Y/%m/%d %H:%M:%S'),len(rowlist)))
				else:print ('No new/modified data need to be extracted at:%s'%(datetime.datetime.strftime(datetime.datetime.now(),'%Y/%m/%d %H:%M:%S')))
			cur.close()
			conn.close()

m=myFilter()
m.connectDB()
