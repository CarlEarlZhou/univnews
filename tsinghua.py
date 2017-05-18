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

localflag = False
localpath = "D:/SEBASE/news/spider/"
serverpath = "/var/www/html/"
srcprefix = "http://www.tsinghua.edu.cn"
audioprefix = "http://tts.baidu.com/text2audio?lan=zh&ie=UTF-8&spd=2&text="
apitoken = "XB2l3mQj.14588.GJCICyNoqghJ"
cntlimit = 30
abstractlimit = 50

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
	cur.execute("SELECT * FROM `data` WHERE context <> 'error' AND abstract <> 'error' AND school = 'tsinghua' ORDER BY Time_stamp DESC")
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
	path = (localpath if localflag == True else serverpath) + "json/tsinghua.json"
	f = codecs.open(path, 'w+', 'utf-8')
	f.write(jsfile)
	f.close()

def getContext(c_url) :
	ret = []
	for i, url in enumerate(c_url) :
		try :
			print "handling %dth context from tsinghua" %(i + 1)
			link = urllib2.urlopen(url)
			srcCode = link.read()
			contextPattern = r'<section class="article">[\s\S]+?</section>'
			context = re.compile(contextPattern).findall(srcCode)
			text = pymysql.escape_string(context[0])

			stk = []
			_str = ""
			for i, s in enumerate(text) :
				if(s == '<' or s == '&') :
					stk.append(s)
					if(s == '<' and text[i + 1] == 'p') :
						_str += '\n'
					elif(s == '&' and text[i : i + 6] == "&ldquo") :
						_str += '“'
					elif(s == '&' and text[i : i + 6] == "&rdquo") :
						_str += '”'
				if(len(stk) == 0) :
					_str += s
				if(s == '>' and len(stk) > 0 and stk[len(stk) - 1] == '<') :
					stk.pop()
				elif(s == ';' and len(stk) > 0 and stk[len(stk) - 1] == '&') :
					stk.pop()
			ret.append(_str.replace(' ', ''))
		except :
			ret.append('error')
			print "error when handling %dth context from tsinghua" %(i + 1)
			print traceback.print_exc()

	return ret

def getAbstract(allContext) :
	nlp = BosonNLP(apitoken)
	ret = []
	for i, text in enumerate(allContext) :
		try :
			print "handling %dth abstract from tsinghua" %(i + 1)
			result = nlp.summary('', text, 50)
			ret.append(result.replace('\n', ''))
		except :
			print "error when handling %dth abstract from tsinghua" %(i + 1)
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
			print "downloading %d.img from tsinghua" %maxid
			path = (localpath if localflag == True else serverpath) + ("img/tsinghua/%s.jpg" %maxid)
			urllib.urlretrieve(imgs[i], path)
			sql = sql + "'http://www.vdebug.xyz/img/tsinghua/" + str(maxid) + ".jpg'" + ","
		except :
			print "error when downloading %d.img from tsinghua" %maxid
			sql = sql + "'error'" + ","
		try :
			print "downloading %d.mp3 from tsinghua" %maxid
			encodetext = urllib2.quote(abss[i].encode('utf8'))
			url = audioprefix + encodetext
			path = (localpath if localflag == True else serverpath) + ("audio/tsinghua/%s.mp3" %maxid)
			urllib.urlretrieve(url, path)
		except :
			print "error when downloading %d.mp3 from tsinghua" %maxid
		sql = sql + "'http://www.vdebug.xyz/audio/tsinghua/" + str(maxid) + ".mp3', 'tsinghua', DATE('" + str(dts[i]) + "')),"
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
	cur.execute("SELECT MAX(Time_stamp) FROM `data` WHERE School = 'tsinghua'")
	res = cur.fetchall()
	maxdate = datetime.date(2000, 1, 1) if (len(res) == 0 or type(res[0][0]).__name__ == 'NoneType') else res[0][0]
	for idx, dt in enumerate(dts) :
		tup = dt.split('-')
		curdate = datetime.date(int(tup[0]), int(tup[1]), int(tup[2]))
		if(curdate > maxdate) :
			avai.append(idx)
		elif(curdate == maxdate) :
			sql = "SELECT * FROM `data` WHERE School = 'tsinghua' AND Time_stamp = " + "'" + str(curdate) + "' AND Title = '" + tits[idx] + "'"
			cur.execute(sql)
			res = cur.fetchall()
			if(len(res) == 0 or type(res[0][0]).__name__ == 'NoneType') :
				avai.append(idx)

def selectAvaiElememt(src) :
	ret = []
	for idx in avai :
		ret.append(src[idx])
	return ret


if __name__ == '__main__':

	reload(sys)
	sys.setdefaultencoding('utf8')

	link = urllib2.urlopen('http://www.tsinghua.edu.cn/publish/newthu/index.html')
	srcCode = link.read()

	datePattern = r'"[0-9]{4,}-[0-9]{2,}-[0-9]{2,}"'
	allDate = re.compile(datePattern).findall(srcCode)[0 : cntlimit]
	for i in range(len(allDate)) :
		allDate[i] = allDate[i].strip('"')

	titlePattern = r'"title".+?,'
	allTitle = re.compile(titlePattern).findall(srcCode)
	allTitle = normalize(allTitle)

	getAvailableIndex(allDate, allTitle)
	allDate = selectAvaiElememt(allDate)
	allTitle = selectAvaiElememt(allTitle)

	contextPattern = r'"url".+?,'
	allContextUrl = re.compile(contextPattern).findall(srcCode)
	allContextUrl = completeUrl(normalize(allContextUrl))
	allContextUrl = selectAvaiElememt(allContextUrl)

	allContext = getContext(allContextUrl)
	allAbstract = getAbstract(allContext)

	imgPattern = r'"img".+?,'
	allImgUrl = re.compile(imgPattern).findall(srcCode)
	allImgUrl = completeUrl(normalize(allImgUrl))
	allImgUrl = selectAvaiElememt(allImgUrl)

	updateMySQL(allTitle, allAbstract, allContext, allImgUrl, allDate)

	mysqlToJson()





















