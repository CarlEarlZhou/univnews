import json
import sys
import pymysql
import datetime
import urllib2
import urllib
from bosonnlp import BosonNLP
import traceback
import codecs


class sqlConnect(object):
	localflag = True
	localpath = "C:/Users/Carl/Documents/news/"
	serverpath = "/var/www/html/"


	def __init__(self, school, cntlimit):
		super(sqlConnect, self).__init__()
		self._conn = pymysql.connect(
			host = '123.206.13.98',
			port = 3306,
			user = 'root',
			passwd = '(buaasoftware)',
			db = 'news',
			charset = 'utf8'
		)
		self._school = school
		self._cntlimit = cntlimit
		self._avai = []
		self._date = []
		self._title = []
		self._contextUrl = []
		self._context = []
		self._imgUrl = []

	@property
	def date(self):
		return self._date
	@date.setter
	def date(self, value):
		self._date = value[0 : self._cntlimit]


	@property
	def title(self):
		return self
	@title.setter
	def title(self, value):
		self._title = value

		self._avai = []
		cur = self._conn.cursor()
		cur.execute("SELECT MAX(Time_stamp) FROM `data` WHERE School = %s", (self._school,))
		res = cur.fetchall()
		maxdate = datetime.date(2000, 1, 1) if (len(res) == 0 or type(res[0][0]).__name__ == 'NoneType') else res[0][0]
		#print maxdate
		for idx, dt in enumerate(self._date):
			tup = dt.split('-')
			curdate = datetime.date(int(tup[0]), int(tup[1]), int(tup[2]))
			if(curdate > maxdate) :
				self._avai.append(idx)
			elif(curdate == maxdate) :
				sql = "SELECT * FROM `data` WHERE School = %s AND Time_stamp = %s AND Title = %s"
				cur.execute(sql, (self._school, str(curdate), self._title[idx]))
				res = cur.fetchall()
				if(len(res) == 0 or type(res[0][0]).__name__ == 'NoneType') :
					self._avai.append(idx)
		cur.close()

		#print self._avai
		self._title = sqlConnect.selectAvaiElememt(self, self._title)
		#print self._title
		self._date = sqlConnect.selectAvaiElememt(self, self._date)


	@property
	def contextUrl(self):
		return self._contextUrl
	@contextUrl.setter
	def contextUrl(self, value):
		self._contextUrl = sqlConnect.selectAvaiElememt(self, value)


	@property
	def context(self):
		return self._context
	@context.setter
	def context(self, value):
		self._context = value
		self._abstract = sqlConnect.getAbstract(self, self._context)


	@property
	def imgUrl(self):
		return self._imgUrl
	@imgUrl.setter
	def imgUrl(self, value):
		self._imgUrl = value


	def updateSql(self):
		audioprefix = "http://tts.baidu.com/text2audio?lan=zh&ie=UTF-8&spd=2&text="

		cur = self._conn.cursor()
		cur.execute("SELECT MAX(ID) FROM `data`")
		res = cur.fetchall()
		maxid = 0 if (len(res) == 0 or type(res[0][0]).__name__ == 'NoneType') else res[0][0]
		sql = "INSERT INTO `data`(Title, Abstract, Context, Imagepath, Audiopath, School, Time_stamp) VALUES"
		for i in range(len(self._date)) :
			maxid = maxid + 1
			try :
				print("downloading %d.img from %s" %(maxid, self._school))
				path = (sqlConnect.localpath if sqlConnect.localflag == True else sqlConnect.serverpath) + ("img/%s/%s.jpg" %(self._school, maxid))
				urllib.urlretrieve(self._imgUrl[i], path)
			except :
				print("error when downloading %d.img from %s" %(maxid, self._school))
			try :
				print("downloading %d.mp3 from %s" %(maxid, self._school))
				encodetext = urllib2.quote(self._abstract[i].encode('utf8'))
				url = audioprefix + encodetext
				path = (sqlConnect.localpath if sqlConnect.localflag == True else sqlConnect.serverpath) + ("audio/%s/%s.mp3" %(self._school, maxid))
				urllib.urlretrieve(url, path)
			except :
				print("error when downloading %d.mp3 from %s" %(maxid, self._school))
			sql = sql + "('" + self._title[i] + "','" + self._abstract[i] + "','" + self._context[i] + "'," + "'http://www.vdebug.xyz/img/"+ self._school +"/" + str(maxid) + ".jpg'" + "," + "'http://www.vdebug.xyz/audio/"+ self._school +"/" + str(maxid) + ".mp3', '"+ self._school +"', DATE('" + str(self._date[i]) + "')),"
		sql = sql[0 : len(sql) - 1]
		if(len(self._date) > 0) :
			cur.execute(sql)
			self._conn.commit()
		cur.close()


	def mysqlToJson(self):
		cur = self._conn.cursor()
		cur.execute("SELECT * FROM `data` WHERE context <> 'error' AND abstract <> 'error' AND school = %s", (self._school,))
		data = cur.fetchall()
		cur.close()
		jsonsrc = []
		jsonsrc.append(0)
		for news, i in zip(data, range(self._cntlimit)) :
			dic = {}
			dic['id'] = news[0]
			dic['title'] = news[1]
			dic['abstract'] = news[2]
			consp = news[3].split('\n')
			dic['context'] = [len(consp), consp]
			dic['imagepath'] = news[4]
			dic['audiopath'] = news[5]
			jsonsrc.append(dic)
		jsonsrc[0] = len(jsonsrc) - 1
		jsfile = json.dumps(jsonsrc, ensure_ascii = False)
		path = (sqlConnect.localpath if sqlConnect.localflag == True else sqlConnect.serverpath) + "json/"+ self._school +".json"
		f = codecs.open(path, 'w+', 'utf-8')
		f.write(jsfile)
		self._conn.close()
		f.close()


	def selectAvaiElememt(self, src) :
		ret = []
		for idx in self._avai :
			ret.append(src[idx].encode('utf8'))
		return ret


	def getAbstract(self, allContext) :
		apitoken = "XB2l3mQj.14588.GJCICyNoqghJ"
		nlp = BosonNLP(apitoken)
		ret = []
		for i, text in enumerate(allContext) :
			try :
				print("handling %dth abstract from %s" %(i + 1, self._school))
				result = nlp.summary('', text, 50)
				ret.append(result.replace('\n', ''))
			except :
				print("error when handling %dth abstract from %s" %(i + 1, self._school))
				ret.append('error')
				print(traceback.print_exc())
		return ret