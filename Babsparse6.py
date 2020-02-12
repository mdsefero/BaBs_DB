#This scrpt deposits the contesnts of BaBs extraction log to a searchable SQL database and parses the contents
#for matching samples/visits of interest. Save the Excel log file as .CSV
#sqlite3 dependancy
#!/usr/bin/env python2

import os
from datetime import datetime
import sqlite3
import copy
import re

def tofile(data, type):
	DT = datetime.now()
	savename = "BabsDBout_" + type + "_%d-%d-%d_%d%d.csv" % (DT.month, DT.day, DT.year, DT.hour, DT.minute)
	f = open(savename,'w')
	f.write('Sample ID,Visit No.,sample,,Date Received,Date DNA Extracted,Date quantitated,Time quantitated,ng/ul,A260,A280,260/280,260/230,Constant,Cursor Pos.,Cursor abs.,340 raw,Sample ID,note,project site,sample box,,\n'.upper())
	for i, line in enumerate(data):
		f.write(line)
		if i < len(data):
			f.write("\n")
	f.close()
	print "\nSaved as: ", savename

def jswrite(data,fname):
	#DT = datetime.now()
	savename = "BabsGraphOut_" + fname + ".js" # + "_%d-%d-%d_%d%d.js" % (DT.month, DT.day, DT.year, DT.hour, DT.minute)
	fhand = open(savename,'w')
	fhand.write("gline = [")
	fhand.write("['Month','Total','Baylor','UTMB','Harvey']")
	for i in data:
	    fhand.write(",\n%s" % (i))
	fhand.write("\n];\n")
	fhand.close()
	print "\n\tGraphic output written to ", savename

def strp (var):
	var = re.sub('[^A-Za-z0-9_/,]+', '', var)
	var = re.sub('([a-z])', r'\1', var).upper()
	return var

def typecln(var):
	var = re.sub('[^A-Z_]+', '', var)
	if len(var) >= 1:
		var = re.sub('[_]{2}',"_", var)
		if var[-1] == "_": var = var[:-1]
		if var[0] == "_": var = var[1:]
	return var

def databasecreate():
	cur.executescript('''DROP TABLE IF EXISTS SubjectNum; DROP TABLE IF EXISTS Sampletype;
						DROP TABLE IF EXISTS Visitnumber; DROP TABLE IF EXISTS BabsDB_Samples;
						DROP TABLE IF EXISTS DNA_RNA;DROP TABLE IF EXISTS Date_E;
						DROP TABLE IF EXISTS Duplicate;''')
	cur.executescript('''
	CREATE TABLE SubjectNum (
	   	id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	   	name   TEXT UNIQUE
	);
	CREATE TABLE Sampletype (
	   	id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	   	type  TEXT UNIQUE
	);
	CREATE TABLE Visitnumber (
	   	id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	   	visit  TEXT UNIQUE
	);
	CREATE TABLE DNA_RNA (
	   	id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	   	dnarna  TEXT UNIQUE
	);
	CREATE TABLE Date_E (
	   	id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	   	date_extracted  TEXT UNIQUE
	);
	CREATE TABLE Duplicate (
	   	id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	   	duplicate  TEXT UNIQUE
	);
	CREATE TABLE BabsDB_Samples (
	   	subject_id INTEGER,
	   	sample_id INTEGER,
	   	visit_id INTEGER,
		dnarna_id TEXT,
		date_extracted_id TEXT,
		duplicate_id INTEGER,
	   	Conc REAL,
		Notes TEXT,
		Used INTEGER,
	   	Origional_line TEXT,
	   	PRIMARY KEY (subject_id, sample_id, visit_id, dnarna_id, date_extracted_id, duplicate_id)
	)
	''')
	print "\n*'BabsExtractedDB.sqlite' created/replaced*"

def databasecheck(var, sw1):
		if 'existing' not in globals():
			cur.execute('SELECT origional_line FROM BabsDB_Samples')
			global existing
			existing = []
			for row in cur:
				existing.append(row[0].encode('utf-8'))
		if sw1 == False:
			if var in existing:
				var = None
			return var

def databaseupdate(sw2):
	while True:
		name = raw_input("\nFilename to add/create to DB (Enter for 'BABS Microbiome study _Extraction Log_July 2014 -.csv'): ")
		if len(name) == 0 : name = "BABS Microbiome study _Extraction Log_July 2014 -.csv"
		elif name.upper() == "Q": quit()
		try:
			handle = open(name)
			handle.close()
			break
		except IOError :
			print "Could not find/open file"
			continue
	if sw2 == False:
		print "Wait while cross-referencing to existing DB entries."
	#parse file and add to DB
	count = 0
	errorlist = []
	duplicatelist = []
	handle = open(name)
	firstline = handle.readline()
	for line in handle:
		if line == firstline: continue
		else:
			line = strp(line)
			l = str(copy.copy(line)).decode()
			try:
				line = line.split(",")
				sID = line[0]
				dnarna = line[3]
				date_extracted = line[6]
				if len(date_extracted) >= 9:
					date_extracted = date_extracted.split('/')
					dt = datetime.strptime(date_extracted[0],'%m')
					date_extracted = "%s%s%s" % (date_extracted[1],dt.strftime('%b').upper(), date_extracted[2][2:4])
				if line[8] == "": conc = None
				else: conc = float(line[8])
				ID = sID.split("BAB_")
				IDnum = ID[1][:4]
				visit = ID[1][5:7]
				stype = ID[1][8:]
				try: int(visit)
				except:
					if ID[1][5] == "S":
						visit =  ID[1][6:9]
						stype = "S_" + stype
					else: raise
				try:
					sample = int(line[0][-1])
					stype = stype[:-1]
				except: sample = ""
				stype = typecln(stype)
				visit = visit.replace("0", "")
				visit = visit.replace("_", "")
				if "HARVEY" in l[-15:]:
					notes = "HARVEY"
				elif "UTMB" in l[-15:]: notes = "UTMB"
				else: notes = None
				if float(visit) >= 7: raise
				if date_extracted == "": continue
			except:
				errorlist.append(l)
				continue

			if sw2 == True: pass
			else:
				l = databasecheck (l, False)
				if l == None:
					continue

			cur.execute('''INSERT OR IGNORE INTO SubjectNum (name)
				VALUES ( ? )''', ( IDnum, ))
			cur.execute('SELECT id FROM SubjectNum WHERE name = ? ', (IDnum, ))
			subject_id = cur.fetchone()[0]

			cur.execute('''INSERT OR IGNORE INTO Sampletype (type)
				VALUES ( ? )''', ( stype, ))
			cur.execute('SELECT id FROM Sampletype WHERE type = ? ', (stype, ))
			sample_id = cur.fetchone()[0]

			cur.execute('''INSERT OR IGNORE INTO Visitnumber (visit)
				VALUES ( ? )''', ( visit, ))
			cur.execute('SELECT id FROM Visitnumber WHERE visit = ? ', (visit, ))
			visit_id = cur.fetchone()[0]

			cur.execute('''INSERT OR IGNORE INTO DNA_RNA (dnarna)
				VALUES ( ? )''', ( dnarna, ))
			cur.execute('SELECT id FROM DNA_RNA WHERE dnarna = ? ', (dnarna, ))
			dnarna_id = cur.fetchone()[0]

			cur.execute('''INSERT OR IGNORE INTO Date_E (date_extracted)
				VALUES ( ? )''', ( date_extracted, ))
			cur.execute('SELECT id FROM Date_E WHERE date_extracted = ? ', (date_extracted, ))
			date_extracted_id = cur.fetchone()[0]

			cur.execute('''INSERT OR IGNORE INTO Duplicate (duplicate)
				VALUES ( ? )''', ( sample, ))
			cur.execute('SELECT id FROM Duplicate WHERE duplicate = ? ', (sample, ))
			duplicate_id = cur.fetchone()[0]

			try:
				cur.execute('''INSERT INTO BabsDB_Samples
					(subject_id, sample_id, visit_id, dnarna_id, date_extracted_id, duplicate_id, Conc, Notes, origional_line) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ? );
					''', ( subject_id, sample_id, visit_id, dnarna_id, date_extracted_id, sample, conc, notes, l ));
			except:
				cur.execute('''INSERT OR REPLACE INTO BabsDB_Samples
					(subject_id, sample_id, visit_id, dnarna_id, date_extracted_id, duplicate_id, Conc, Notes, origional_line) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ? );
					''', ( subject_id, sample_id, visit_id, dnarna_id, date_extracted_id, sample, conc, notes, l ));
				duplicatelist.append(l)

			count += 1
		if count % 1000 == 0:
			print count, "...",
			conn.commit()
	conn.commit()
	handle.close()

	print "\n\n\n---------------\n%i SAMPLE ENTRIES SCANNED IN '%s' FOR DEPOSIT INTO 'BabsExtractedDB.sqlite'" % (count, name)
	if len(errorlist) != 0:
		print "COULD NOT PARSE THE FOLLOWING ENTRIES, CHECK '%s' FOR TYPOS':" % (name)
		for i in errorlist:
			print "-->", i
	else: "NO ERRORS FOUND"
	print "---------------"
	if len(duplicatelist) != 0:
		print "THE FOLLOWING ENTRIES ARE LIKELY IN DUPLICATES IN '%s': " % (name)
		for i in duplicatelist:
			print "-->", i
		print "---------------"
	del errorlist
	del duplicatelist

def DBsummary():
	Hcount = 0
	UTMBcount = 0
	cur.execute('SELECT Notes FROM BabsDB_Samples')
	rows = cur.fetchall()
	for i in rows:
		if i[0] == "HARVEY": Hcount+=1
		if i[0] == "UTMB": UTMBcount+=1
	count = 0
	cur.execute('SELECT Type FROM Sampletype')
	rows = cur.fetchall()
	for i in rows:
		if i[0][0] != "S": count+=1
	size = []
	tables = ('BabsDB_Samples','SubjectNum','Sampletype','Visitnumber')
	comment= ('Total samples in DB', 'Discrete subject numbers', 'Sample types', 'Visit timepoints')
	size.append("\n\n\n\n\tSummary of extracted BaBs samples: \n")
	for i, cont in enumerate(tables):
		cur.execute('SELECT COUNT(*) FROM %s' % (cont))
		t = cur.fetchone()
		report = "\t\t%s\t%s" % (t[0], comment[i])
		size.append(report)
	size.insert(4, "\t\t%s\tSample types excluding \'S\'" % (count))
	size.insert(3, "\t\t%s\tHarvey subject samples" % (Hcount))
	size.insert(3, "\t\t%s\tUTMB origin samples" % (UTMBcount))
	for i in size:
		print i
	lgraph()
	hgraph()

def lgraph():
	cur.execute('SELECT id, date_extracted FROM Date_E')
	rows = cur.fetchall()
	dateskey = {}
	for i in rows:
		dateskey[i[0]] = i[1].encode('utf-8')
	cur.execute('SELECT date_extracted_id, Notes FROM BabsDB_Samples')
	Tdata= {}
	rows = cur.fetchall()
	for i in rows:
		date = dateskey.get(int(i[0]))
		date = date[-5:]
		if len(date) <= 4: date = None
		if date == "32016": date = "MAR16"
		temp = Tdata.get(date)
		if temp == None: temp = [0,0,0,0]
		temp[0] += 1
		if i[1] == None: temp[1] += 1
		elif i[1] == "HARVEY": temp[3] += 1
		elif i[1] == "UTMB": temp[2] += 1
		else: print "Error: Unrecognized sample note"
		Tdata[date] = temp
	flist = []
	for k,v in Tdata.items():
		if k == None: continue
		month = datetime.strptime(k[:3], '%b')
		month = str(month.month)
		if len(month) == 1: month ='0' + month
		temp = "%s-%s" % (k[-2:], month)
		v.insert(0, temp)
		flist.append(v)
		flist.sort(key=lambda x: x[0])
	del Tdata
	jswrite(flist,'bymonth')
	temp = [0,0,0,0]
	for i, item in enumerate(flist):
		temp = [temp[0]+item[1], temp[1]+item[2],temp[2]+item[3],temp[3]+item[4]]
		flist[i] = [item[0],temp[0],temp[1],temp[2],temp[3]]
	jswrite(flist,'cumulative')
	#print "\tOpen using \'gline.htm\' to visualize"
	try:
		os.startfile('graphmonth.htm') #for windows
		os.startfile('graphcumulative.htm')
	except:
		os.system('open graphmonth.htm') #for mac/unix
		os.system('open graphcumulative.htm')

def hgraph ():
	types = []
	typesS = []
	gettypes(types,typesS)
	types += typesS
	for num, i in enumerate(types):
		cur.execute('SELECT id FROM Sampletype WHERE type = ?', (i,))
		line = cur.fetchall()
		types[num] = [line[0][0], i]
	for num, i in enumerate(types):
		cur.execute('SELECT Origional_line FROM BabsDB_Samples WHERE sample_id = ?', ( i[0], ))
		line = cur.fetchall()
		types[num].append(len(line))
	types.sort(key=lambda x: x[2], reverse=True)
	#DT = datetime.now()
	savename = "BabsGraphOut_hist.js" # + "_%d-%d-%d_%d%d.js" % (DT.month, DT.day, DT.year, DT.hour, DT.minute)
	fhand = open(savename,'w')
	fhand.write("hline = [")
	fhand.write("['Samples','N'],")
	for item in types:
		fhand.write("\n['%s',%i]," % (item[1],item[2]))
	fhand.write("\n];\n")
	fhand.close()
	print "\n\tGraphic output written to ", savename
	try: os.startfile('graphtype.htm') #for windows
	except:os.system('open graphtype.htm') #for mac/unix

def gettypes(a,b):
	cur.execute('SELECT Type FROM Sampletype')
	rows = cur.fetchall()
	for i in rows:
		if i[0][0] != "S": a.append(i[0].encode("utf-8"))
		else: b.append(i[0].encode("utf-8"))
	return a, b

def list():
	types = []
	typesS = []
	gettypes(types,typesS)
	while True:
		listq = raw_input("\n\n\nWould you like to include supplemental ('S') samples? (Y/N, Enter 'B' to go back): ")
		if listq.upper() == "Y":
			types += typesS
			break
		elif listq.upper() == "N": break
		elif listq.upper() == "B": return
		else: continue
	while True:
		print "\nSample types currently in DB: "
 		nl = 0
		col_width = max(len(item) for item in types) + 2
		for i in sorted(types):
			print i.ljust(col_width),
			nl+=1
			if nl % 8 == 0: print "\n",
		listq = raw_input("\n\nEnter all sample types you would you like? (Enter 'B' to go back): ")
		if listq.upper() == "B": return
		listsamples = re.split('[;:,.|\s\-]', listq)
		try:
			for i in xrange (len(listsamples)):
				if listsamples[i].upper() in types: pass
				else: raise
		except:
			print "Error, sample type not found in DB"
			continue
		break
	while True:
		listq = raw_input("\n\nEnter vist numbers to include, (Blank for all. Enter 'B' to go back): ")
		if listq.upper() == "B": return
		listvisits = re.split('[;:,.|\s\-]', listq)
		if listvisits == "": break
		try:
			for i in listvisits:
				if i in ['','1','2','3','4','5','6']: pass
				else: raise
		except:
			print "Error, invalid vist entry (1-6)"
			continue
		break
	getsamples(listsamples, listvisits, False)

def origin(o):
	if o == "H": o = "HARVEY"
	elif o == "U": o = "UTMB"
	output = []
	cur.execute('SELECT Origional_line FROM BabsDB_Samples WHERE Notes = ?', (o,))
	rows = cur.fetchall()
	for i in rows:
		output.append(i[0].encode("utf-8"))
		print i[0]
	print "\n\nThere are %s total %s samples in database\n" % (len(output), o)
	while True:
		ques =  raw_input("Do you want to output to file? (Y/N)")
		if ques.upper() == "Y":
			tofile(output, o)
			return
		elif ques.upper() == "N": return
		else: continue

def IDs():
	ids = []
	output = []
	while True:
		listq = raw_input("\n\nEnter all subject numbers you would you like? (Enter 'B' to go back): ")
		if listq.upper() == "B": return
		listsamples = re.split('[;:,.|\s\-]', listq)
		try:
			for i in xrange (len(listsamples)):
				cur.execute('SELECT id FROM SubjectNum WHERE name = ?', (listsamples[i],))
				line = cur.fetchall()
				ids.append(line[0][0])
		except:
			print "Error: One ore more subject numbers not found in DB"
			continue
		break

	for i in ids:
		cur.execute('SELECT Origional_line FROM BabsDB_Samples WHERE subject_id = ?', ( i, ))
		rows = cur.fetchall()
		for i in rows:
			output.append(i[0].encode("utf-8"))
			print i[0]
	print "\n\nThere are %s total samples in your list\n" % (len(output))
	while True:
		ques =  raw_input("Do you want to output to file? (Y/N)")
		if ques.upper() == "Y":
			tofile(output, "IDLst")
			del output
			return
		elif ques.upper() == "N":
			del output
			return
		else: continue

def getsamples(s,v,t):
	v = filter(None, v)
	s = filter(None, s)
	s = [element.upper() for element in s]
	if v == []: v = ['1','2','3','4','5','6']
	output = []
	vc = []
	sc = []
	for i in v:
		cur.execute('SELECT id FROM Visitnumber WHERE visit = ?', (i,))
		line = cur.fetchall()
		vc.append(line[0])
	for i in s:
		cur.execute('SELECT id FROM Sampletype WHERE type = ?', (i,))
		line = cur.fetchall()
		sc.append(line[0])
	for iter in xrange (len(vc)):
		for i in sc:
			cur.execute('SELECT Origional_line FROM BabsDB_Samples WHERE sample_id = ? AND visit_id = ?', ( i[0], vc[iter][0] ))
			rows = cur.fetchall()
			rows = [x[0].encode('utf-8') for x in rows]
			if t == True:
				cur.execute('SELECT subject_id FROM BabsDB_Samples WHERE sample_id = ? AND visit_id = ?', ( i[0], vc[iter][0] ))
				IDS = cur.fetchall()
				for x, line in enumerate(rows):
					rows[x] = [rows[x], IDS[x][0], i[0], vc[iter][0]]
			for i in rows:
				output.append(i)
				if t != True: print i
	if t == True:
		return(output)

	print "\n\nThere are %s total samples in your list\n" % (len(output))
	while True:
		ques =  raw_input("Do you want to output to file? (Y/N)")
		if ques.upper() == "Y":
			tofile(output, "TYPESLst")
			del output
			return
		elif ques.upper() == "N":
			del output
			return
		else: continue

def match():
	matchitems = []
	types = []
	typesS = []
	gettypes(types,typesS)
	types += typesS
	while True:
		while True:
			print "\nSample types currently in DB: "
			nl = 0
			col_width = max(len(item) for item in types) + 2
			for i in sorted(types):
				print i.ljust(col_width),
				nl+=1
				if nl % 8 == 0: print "\n",
			listq = raw_input("\n\nEnter the sample type you would you like to match? Only enter two types if you want to include its suplemental \"S_\" (Enter 'B' to go back): ")
			if listq.upper() == "B": return
			listsamples = re.split('[;:,.|\s\-]', listq)
			for i,item in enumerate(listsamples):
				listsamples[i] = item.upper()
			if len(listsamples) >= 3:
				print "Error: Too many arguments/sample types"
				continue
			if len(listsamples) == 2:
				listsamples = sorted(listsamples, key=len)
				if listsamples[1] == "S_" + listsamples[0]:
					pass
				else:
					print "Error: One sample type only, or two if including matching \"S_\" please"
					continue
			try:
				for i in xrange (len(listsamples)):
					if listsamples[i].upper() in types: pass
					else: raise
			except:
				print "Error: Sample type not found in DB"
				continue
			break
		while True:
			listq = raw_input("\n\nEnter vist numbers to include, (Blank for all. Enter 'B' to go back): ")
			if listq.upper() == "B": return
			listvisits = re.split('[;:,.|\s\-]', listq)
			if listvisits == "": break
			try:
				for i in listvisits:
					if i in ['','1','2','3','4','5','6']: pass
					else: raise
			except:
				print "Error: Invalid vist entry (1-6)"
				continue
			break
		matchitem = getsamples(listsamples, listvisits, True)
		print "\nNumber of samples found : ", len(matchitem)
		matchitems.append(matchitem)
		while True:
			question = raw_input("\n\nDo you want to match to another sample type/visit (Y/N)?")
			if question.upper() == "Y": break
			elif question.upper() == "N": break
			else: continue
		if question.upper() == "Y": continue
		if question.upper() == "N" :break

	try:
		matchdict = {}
		size = 0
		output = []
		for i in matchitems[0]:
			matchdict[i[-3]] = [[0], i[0]]
		for i in xrange(1,len(matchitems)):
			for item in matchitems[i]:
				if item[-3] in matchdict.keys():
					matchdict[item[-3]].append(item[0])
					matchdict[item[-3]][0].append(i)
		for k,v in matchdict.items():
			for i in xrange(len(matchitems)):
				if i not in v[0]:del matchdict[k]
		del matchitems
	except KeyError:
		print "\n\n*Could not find any matches for specified samples/visits*"
		return
	for k,v in matchdict.items():
		size += len(v) - 1
		for i in xrange(1,len(v)):
			print v[i]
			output.append(v[i])
	print "\n\n%i matching sample types, for %i total matching samples including vists" % (len(matchdict), size)
	del matchdict

	while True:
		ques =  raw_input("Do you want to output to file? (Y/N)")
		if ques.upper() == "Y":
			tofile(output, "MATCHLst")
			del output
			return
		elif ques.upper() == "N":
			del output
			return
		else: continue

# Main body of Code
conn = sqlite3.connect('BabsExtractedDB.sqlite')
cur = conn.cursor()

while True:
	dbtest = raw_input(	"\n\n\n\n This script generates a searchable database from the Babs Extraction log\n Save the Excel log file as .CSV\n\n"
						"\tOptions:\n\n\t\t-C\tCREATE\tMake/overwrite DB from file\n\n\t\t-A\tADD\tAdds new entries only to DB from file\n\n\t\t"
						"-S\tSUMMARY\tSample entry statistics from database\n\n\t\t-U\tUTMB\tList all UTMB samples\n\n\t\t-H\tHARVEY\tList all Harvey samples"
						"\n\n\t\t-I\tID\tList all samples for specified subjects\n\n\t\t-T\tTYPE\tList all for specified sampletypes\n\n\t\t-M\tMATCH"
						"\tList matching samples by sample type and/or specified visits only\n\n\t\t-N\tNOTES\tAppend usage notes to DB"
						" (not built)\n\n\t\t-Q\tQUIT\n\n\t\t-")
	if dbtest.upper() == "C":
		databasecreate()
		databaseupdate(True)
	elif dbtest.upper() == "A":
		databasecheck(None, True)
		databaseupdate(False)
	elif dbtest.upper() == "S": DBsummary()
	elif dbtest.upper() == "U": origin(dbtest.upper())
	elif dbtest.upper() == "H": origin(dbtest.upper())
	elif dbtest.upper() == "I": IDs()
	elif dbtest.upper() == "T": list()
	elif dbtest.upper() == "M": match()
	#elif dbtest.upper() == "U":
	elif dbtest.upper() == "Q": quit()
	else: continue
