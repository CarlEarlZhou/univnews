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

localflag = False
localpath = "D:/SEBASE/news/spider/"
serverpath = "/var/www/html/"
srcprefix = "http://news.buaa.edu.cn/zhxw/"
imgSrcPreFix = "http://news.buaa.edu.cn/"
audioprefix = "http://tts.baidu.com/text2audio?lan=zh&ie=UTF-8&spd=2&text="
apitoken = "XB2l3mQj.14588.GJCICyNoqghJ"
cntlimit = 10
abstractlimit = 50
thisYear = '2017-'

allImgUrl = []

avai = []

def normalize(src) :
	ret = []
	for s, i in zip(src, range(cntlimit)) :
		split = s.index(':') + 2
		ret.append(s[split : len(s) - 2])
	return ret

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
	cur.execute("SELECT * FROM `data` WHERE context <> 'error' AND abstract <> 'error' AND school = 'buaa'")
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
	path = (localpath if localflag == True else serverpath) + "json/buaa.json"
	f = codecs.open(path, 'w+', 'utf-8')
	f.write(jsfile)
	f.close()

def getContext(c_url) :
	ret = []
	for i, url in enumerate(c_url) :
		try :
			print("handling %dth context from buaa" %(i + 1))
			link = urllib2.urlopen(url) 
			srcCode = link.read()

			contextPattern = r'<p>.[^a][\s\S]+?</p>'
			context = re.compile(contextPattern).findall(srcCode[srcCode.find("newsleftconbox auto") : srcCode.find("<!--rightOver-->")]);

			imgUrlPattern = r'style="text-align: center;*"><img[\s\S]+?src=".+?"'
			imgUrl = re.compile(imgUrlPattern).findall(srcCode)
			l = imgUrl[0].find('src="')
			imgUrl[0] = imgUrl[0][l + 8 : len(imgUrl[0])-1]
			imgUrl[0] = imgSrcPreFix + imgUrl[0]
			allImgUrl.append(imgUrl[0])

			text = context

			stk = []
			_str = ""
			parser = HTMLParser.HTMLParser()
			for i, s in enumerate(text) :
				text[i] = eraseAngleBrackets(text[ i ])
				text[i] = parser.unescape(text[i])
				_str += text[i]+'\n'

			ret.append(_str)
		except : 
			ret.append('error')
			print("error when handling %dth context from buaa" %(i + 1))
			print(traceback.print_exc())

	return ret

def getAbstract(allContext) :
	nlp = BosonNLP(apitoken)
	ret = []
	for i, text in enumerate(allContext) :
		try :
			print("handling %dth abstract from buaa" %(i + 1))
			result = nlp.summary('', text, 50)
			ret.append(result.replace('\n', ''))
		except :
			print("error when handling %dth abstract from buaa" %(i + 1))
			ret.append('error')
			print(traceback.print_exc())
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
	maxid = 0 if (len(res) == 0 or type(res[0][0]).__name__ == 'NoneType') else res[0][0]
	sql = "INSERT INTO `data`(Title, Abstract, Context, Imagepath, Audiopath, School, Time_stamp) VALUES"
	for i in range(len(dts)) :
		maxid = maxid + 1
		try :
			print("downloading %d.img from buaa" %maxid)
			path = (localpath if localflag == True else serverpath) + ("img/buaa/%s.jpg" %maxid)
			urllib.urlretrieve(imgs[i], path)
		except :
			print("error when downloading %d.img from buaa" %maxid)
		try :
			print("downloading %d.mp3 from buaa" %maxid)
			encodetext = urllib2.quote(abss[i].encode('utf8'))

			url = audioprefix + encodetext
			path = (localpath if localflag == True else serverpath) + ("audio/buaa/%s.mp3" %maxid)
			urllib.urlretrieve(url, path)
		except :
			print("error when downloading %d.mp3 from buaa" %maxid)
		sql = sql + "('" + tits[i] + "','" + abss[i] + "','" + txts[i] + "'," + "'http://www.vdebug.xyz/img/buaa/" + str(maxid) + ".jpg'" + "," + "'http://www.vdebug.xyz/audio/buaa/" + str(maxid) + ".mp3', 'buaa', DATE('" + str(dts[i]) + "')),"
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
	cur.execute("SELECT MAX(Time_stamp) FROM `data` WHERE School = 'buaa'")
	res = cur.fetchall()
	'''WTF???'''
	maxdate = datetime.date(2000, 1, 1) if (len(res) == 0 or type(res[0][0]).__name__ == 'NoneType') else res[0][0]
	for idx, dt in enumerate(dts) :
		tup = dt.split('-')
		curdate = datetime.date(int(tup[0]), int(tup[1]), int(tup[2]))
		if(curdate > maxdate) :
			avai.append(idx)
		elif(curdate == maxdate) :
			sql = "SELECT * FROM `data` WHERE School = 'buaa' AND Time_stamp = " + "'" + str(curdate) + "' AND Title = '" + tits[idx] + "'"
			cur.execute(sql)
			res = cur.fetchall()
			if(len(res) == 0 or type(res[0][0]).__name__ == 'NoneType') :
				avai.append(idx)

def selectAvaiElememt(src) :
	ret = []
	for idx in avai :
		ret.append(src[idx])
	return ret 

def eraseAngleBrackets(s):
	pattern = r'<[\s\S]+?>'
	allAngleBrackets = re.compile(pattern).findall(s)
	for i in range(len(allAngleBrackets)):
		s = s.replace(allAngleBrackets[i],'')
	return s


if __name__ == '__main__':

	reload(sys)
	sys.setdefaultencoding('utf8')

	link = urllib2.urlopen('http://news.buaa.edu.cn/zhxw/index.htm')
	srcCode = link.read()

	datePattern = r'[[0-9]{4,}-[0-9]{2,}-[0-9]{2,}]'
	allDate = re.compile(datePattern).findall(srcCode)[0 : cntlimit]

	for i in range(len(allDate)) :
		allDate[i] = allDate[i][1 : len(allDate[i]) - 1]

	titlePattern = r'<h2><a href="[\s\S]+?" target="_blank">[\s\S]+?</a>'
	allTitle = re.compile(titlePattern).findall(srcCode)

	for i in range(len(allTitle)):
		l = len(allTitle[i])
		allTitle[i] = eraseAngleBrackets(allTitle[i])

	getAvailableIndex(allDate, allTitle)
	allDate = selectAvaiElememt(allDate)
	allTitle = selectAvaiElememt(allTitle)

	contextPattern = r'<p><a href="[\s\S]+?" target="_blank">'
	allContextUrl = re.compile(contextPattern).findall(srcCode)

	for i in range(len(allContextUrl)):
		l = len(allContextUrl[ i ])
		allContextUrl[i] = allContextUrl[ i ][ len('<p><a href="') : l - len('" target="_blank">') ]
	allContextUrl = selectAvaiElememt(allContextUrl)
	allContextUrl = completeUrl(allContextUrl)

	allContext = getContext(allContextUrl)
	allAbstract = getAbstract(allContext)

	updateMySQL(allTitle, allAbstract, allContext, allImgUrl, allDate)

	mysqlToJson()





















