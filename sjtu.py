#coding: utf-8

import urllib2
import urllib
import re
import pymysql
import json
import sys
from bosonnlp import BosonNLP
import traceback
import codecs
import datetime
import HTMLParser
from HTMLParser import HTMLParser
import pku
from myHTMLParser import myHTMLParser

localflag = True
localpath = "C:/Users/Carl/Documents/news/"
serverpath = "/var/www/html/"
srcprefix = "http://news.sjtu.edu.cn/"
audioprefix = "http://tts.baidu.com/text2audio?lan=zh&ie=UTF-8&spd=2&text="
apitoken = "XB2l3mQj.14588.GJCICyNoqghJ"
cntlimit = 5
abstractlimit = 50
thisYear = '2017-'

allImgUrl = []
avai = []

class imgParser(myHTMLParser):

	def __init__(self):
		super(imgParser, self).__init__()

	def handle_starttag(self, tag, attrs):
		if(tag == 'img'):
			for i, s in enumerate(attrs):
				if(attrs[i][0] == "src"):
					self._imgUrl = attrs[i][1][6:]
					
	def handle_data(self, data):
		self._context += data
		if(self.lasttag == 'p'):
			self._context += '\n'



class mHTMLParser(myHTMLParser):

	titles = []
	contexts = []

	def handle_starttag(self, tag, attrs):
		if(tag == 'a'):
			count = 0
			for i, s in enumerate(attrs):
				if(attrs[i][0] == "class" and attrs[i][1] == "f1335"):
					count += 1
				elif(attrs[i][0] == "href"):
					count += 1
				elif(attrs[i][0] == "target"):
					count += 1
				elif(attrs[i][0] == "title"):
					count += 1
			if(count == 5):
				for i, s in enumerate(attrs):
					if(attrs[i][0] == "title"):
						self.titles.append(attrs[i][1])
					elif(attrs[i][0] == "href"):
						self.contexts.append(attrs[i][1])

	def getTitles(self):
		return self.titles

	def getContexts(self):
		return self.contexts

def completeUrl(src) :
	for i in range(len(src)) :
		src[i] = srcprefix + src[i]
	return src

def mysqlToJson() :
	conn = pymysql.connect(
		host = '123.206.13.98',
		port = 3306,
		user = 'root',
		passwd = '(buaasoftware)',
		db = 'news',
		charset = 'utf8'
	)

	cur = conn.cursor()
	cur.execute("SELECT * FROM `data` WHERE context <> 'error' AND abstract <> 'error' AND school = 'sjtu' ORDER BY Time_stamp DESC")
	data = cur.fetchall()
	cur.close()
	jsonsrc = []
	jsonsrc.append(0)
	for news, i in zip(data, range(cntlimit)) :
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
	conn.close()

	jsfile = json.dumps(jsonsrc, ensure_ascii = False)
	path = (localpath if localflag == True else serverpath) + "json/sjtu.json"
	f = codecs.open(path, 'w+', 'utf-8')
	f.write(jsfile)
	f.close()

def getContext(c_url) :
	ret = []
	for i, url in enumerate(c_url) :
		try :
			print "handling %dth context from sjtu" %(i + 1)
			link = urllib2.urlopen(url)
			srcCode = link.read()
			srcCode = srcCode[srcCode.find(r'<div id="vsb_content_2" style="word-break:break-all;overflow:auto;width:100%"') : srcCode.find(r'td class="pagestyle1366"')]

			iParser = imgParser()
			text = iParser.unescape(srcCode)
			iParser.feed(text)
			allImgUrl.append(iParser.imgUrl)
			text = iParser.context
			iParser.close()
			ret.append(text)
		except :
			ret.append('error')
			print "error when handling %dth context from sjtu" %(i + 1)
			print traceback.print_exc()

	return ret

def getAbstract(allContext) :
	nlp = BosonNLP(apitoken)
	ret = []
	for i, text in enumerate(allContext) :
		try :
			print "handling %dth abstract from sjtu" %(i + 1)
			result = nlp.summary('', text, 50)
			ret.append(result.replace('\n', ''))
		except :
			print "error when handling %dth abstract from sjtu" %(i + 1)
			ret.append('error')
			print traceback.print_exc()
	return ret


def updateMySQL(tits, abss, txts, imgs, dts) :
	conn = pymysql.connect(
		host = '123.206.13.98',
		port = 3306,
		user = 'root',
		passwd = '(buaasoftware)',
		db = 'news',
		charset = 'utf8'
	)

	cur = conn.cursor()
	cur.execute("SELECT MAX(ID) FROM `data`")
	res = cur.fetchall()
	maxid = 0 if(len(res) == 0 or type(res[0][0]).__name__ == 'NoneType') else res[0][0]
	sql = "INSERT INTO `data`(Title, Abstract, Context, Imagepath, Audiopath, School, Time_stamp) VALUES"
	for i in range(len(dts)) :
		maxid = maxid + 1
		sql = sql + "('" + pymysql.escape_string(tits[i]) + "','" + abss[i] + "','" + txts[i] + "',"
		try :
			print "downloading %d.img from sjtu" %maxid
			path = (localpath if localflag == True else serverpath) + ("img/sjtu/%s.jpg" %maxid)
			urllib.urlretrieve(imgs[i], path)
			sql = sql + "'http://www.vdebug.xyz/img/sjtu/" + str(maxid) + ".jpg'" + ","
		except :
			print "error when downloading %d.img from sjtu" %maxid
			sql = sql + "'error'" + ","
		try :
			print "downloading %d.mp3 from sjtu" %maxid
			encodetext = urllib2.quote(abss[i].encode('utf8'))
			url = audioprefix + encodetext
			path = (localpath if localflag == True else serverpath) + ("audio/sjtu/%s.mp3" %maxid)
			urllib.urlretrieve(url, path)
		except :
			print "error when downloading %d.mp3 from sjtu" %maxid
		sql = sql + "'http://www.vdebug.xyz/audio/sjtu/" + str(maxid) + ".mp3', 'sjtu', DATE('" + str(dts[i]) + "')),"
	sql = sql[0 : len(sql) - 1]
	if(len(dts) > 0) :
		cur.execute(sql)
		conn.commit()

	cur.close()
	conn.close()

def getAvailableIndex(dts, tits) :
	conn = pymysql.connect(
		host = '123.206.13.98',
		port = 3306,
		user = 'root',
		passwd = '(buaasoftware)',
		db = 'news',
		charset = 'utf8'
	)
	cur = conn.cursor()
	cur.execute("SELECT MAX(Time_stamp) FROM `data` WHERE School = 'sjtu'")
	res = cur.fetchall()
	maxdate = datetime.date(2000, 1, 1) if(len(res) == 0 or type(res[0][0]).__name__ == 'NoneType') else res[0][0]
	for idx, dt in enumerate(dts) :
		tup = dt.split('-')
		curdate = datetime.date(int(tup[0]), int(tup[1]), int(tup[2]))
		if(curdate > maxdate) :
			avai.append(idx)
		elif(curdate == maxdate) :
			sql = "SELECT * FROM `data` WHERE School = 'sjtu' AND Time_stamp = " + "'" + str(curdate) + "' AND Title = '" + tits[idx] + "'"
			cur.execute(sql)
			res = cur.fetchall()
			if (len(res) == 0 or type(res[0][0]).__name__ == 'NoneType') :
				avai.append(idx)

def selectAvaiElememt(src) :
	ret = []
	for idx in avai :
		ret.append(src[idx])
	return ret


if __name__ == '__main__':

	reload(sys)
	sys.setdefaultencoding('utf8')

	link = urllib2.urlopen('http://news.sjtu.edu.cn/jdyw.htm')
	srcCode = link.read()

	datePattern = r'([0-9]{4,}-[0-9]{2,}-[0-9]{2,})'
	allDate = re.compile(datePattern).findall(srcCode)[0 : cntlimit]

	parser = mHTMLParser()
	parser.feed(srcCode)
	allTitle = parser.getTitles()

	getAvailableIndex(allDate, allTitle)
	allDate = selectAvaiElememt(allDate)
	allTitle = selectAvaiElememt(allTitle)

	allContextUrl = parser.getContexts()
	allContextUrl = selectAvaiElememt(allContextUrl)
	allContextUrl = completeUrl(allContextUrl)

	allContext = getContext(allContextUrl)
	parser.close()
	allAbstract = getAbstract(allContext)

	updateMySQL(allTitle, allAbstract, allContext, allImgUrl, allDate)
	mysqlToJson()





















