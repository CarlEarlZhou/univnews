#coding: utf-8

import urllib2
import urllib
from urllib import quote
import re
import pymysql
import json
import sys
import bosonnlp
import requests
from bosonnlp import BosonNLP
import traceback
import codecs
import datetime
import time

localflag = True
localpath = "C:/Users/Carl/Documents/news/"
serverpath = "/var/www/html/"
srcprefix = "http://pkunews.pku.edu.cn/xwzh/"
audioprefix = "http://tts.baidu.com/text2audio?lan=zh&ie=UTF-8&spd=2&text="
apitoken = "XB2l3mQj.14588.GJCICyNoqghJ"
cntlimit = 4
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
	cur.execute("SELECT * FROM `data` WHERE context <> 'error' AND abstract <> 'error'");
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
	conn.close();

	jsfile = json.dumps(jsonsrc, ensure_ascii = False)
	path = (localpath if localflag == True else serverpath) + "json/pku.json"
	f = codecs.open(path, 'w+', 'utf-8')
	f.write(jsfile)
	f.close()

def getContext(c_url) :
	ret = []
	for i, url in enumerate(c_url) :
		try :
			print("handling %dth context from pku" %(i + 1))
			link = urllib2.urlopen(url) 
			srcCode = link.read()

			contextPattern = r'<p>[^<][\s\S]+?[^>]</p>'
			context = re.compile(contextPattern).findall(srcCode)


			imgUrlPattern = r'<center><img[\s\S]+?src="[\s\S]+?"'
			imgUrl = re.compile(imgUrlPattern).findall(srcCode)
			if(len(imgUrl) == 0):
				imgUrlPattern = r'<center><strong><img[\s\S]+?src="[\s\S]+?"'
				imgUrl = re.compile(imgUrlPattern).findall(srcCode)
				if(len(imgUrl) == 0):
					allImgUrl.append("null")
				else:
					l = imgUrl[0].find('src="')
					imgUrl[0] = imgUrl[0][l + 11 : len(imgUrl[0])-1]
					imgUrl[0] = srcprefix + imgUrl[0]
					print imgUrl[0]
					allImgUrl.append(imgUrl[0])
			else:
				l = imgUrl[0].find('src="')
				imgUrl[0] = imgUrl[0][l + 11 : len(imgUrl[0])-1]
				imgUrl[0] = srcprefix + imgUrl[0]
				print imgUrl[0]
				allImgUrl.append(imgUrl[0])

			text = context

			stk = []
			_str = ""
			for i, s in enumerate(text) :
				text[i] = text[i][3:len(text[i]) - 4]
				pattern = r'<[\s\S]+?>'
				temText = re.compile(pattern).findall(text[i])
				for i in range(len(temText)):
					text[i].replace(temText[i], '')
				_str += text[i]+'\n';
			ret.append(_str)
		except : 
			ret.append('error')
			print("error when handling %dth context from pku" %(i + 1))
			print(traceback.print_exc())

	return ret

def getAbstract(allContext) :
	nlp = BosonNLP(apitoken)
	ret = []
	for i, text in enumerate(allContext) :
		try :
			print("handling %dth abstract from pku" %(i + 1))
			result = nlp.summary('', text, 50)
			ret.append(result.replace('\n', ''))
		except :
			print("error when handling %dth abstract from pku" %(i + 1))
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
	maxid = 0 if len(res) == 0 else res[0][0] 
	sql = "INSERT INTO `data`(Title, Abstract, Context, Imagepath, Audiopath, School, Time_stamp) VALUES"
	for i in range(len(dts)) :
		maxid = maxid + 1
		try :
			print("downloading %d.img from pku" %maxid)
			path = (localpath if localflag == True else serverpath) + ("img/pku/%s.jpg" %maxid)
			urllib.urlretrieve(imgs[i], path)
		except :
			print("error when downloading %d.img from pku" %maxid)
		try :
			print("downloading %d.mp3 from pku" %maxid)
			encodetext = urllib2.quote(abss[i].encode('utf8'))

			url = audioprefix + encodetext
			path = (localpath if localflag == True else serverpath) + ("audio/pku/%s.mp3" %maxid)
			urllib.urlretrieve(url, path)
		except :
			print("error when downloading %d.mp3 from pku" %maxid)
		sql = sql + "('" + tits[i] + "','" + abss[i] + "','" + txts[i] + "'," + "'http://www.vdebug.xyz/img/pku/" + str(maxid) + ".jpg'" + "," + "'http://www.vdebug.xyz/audio/pku/" + str(maxid) + ".mp3', 'pku', DATE('" + str(dts[i]) + "')),"
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
	cur.execute("SELECT MAX(Time_stamp) FROM `data` WHERE School = 'pku'")
	res = cur.fetchall()
	maxdate = datetime.date(2000, 1, 1) if len(res) == 0 else res[0][0]
	for idx, dt in enumerate(dts) :
		tup = dt.split('-')
		curdate = datetime.date(int(tup[0]), int(tup[1]), int(tup[2]))
		if(curdate > maxdate) :
			avai.append(idx)
		elif(curdate == maxdate) :
			sql = "SELECT * FROM `data` WHERE School = 'pku' AND Time_stamp = " + "'" + str(curdate) + "' AND Title = '" + tits[idx] + "'"
			cur.execute(sql)
			res = cur.fetchall()
			if(len(res) == 0) :
				avai.append(idx)

def selectAvaiElememt(src) :
	ret = []
	for idx in avai :
		ret.append(src[idx])
	return ret 


if __name__ == '__main__':

	reload(sys)
	sys.setdefaultencoding('utf8')

	link = urllib2.urlopen('http://pkunews.pku.edu.cn/xwzh/xwzh.htm')
	srcCode = link.read()

	datePattern = r'<td width="36" align="right" class="be12">[0-9]{2,}-[0-9]{2,}</td>'
	allDate = re.compile(datePattern).findall(srcCode)[0 : cntlimit]

	for i in range(len(allDate)) :
		allDate[i] = thisYear + allDate[i][42:47]

	titlePattern = r'target="_blank">[\s\S]+?</a></td><td width="36'
	allTitle = re.compile(titlePattern).findall(srcCode)

	for i in range(len(allTitle)):
		l = len(allTitle[i])
		allTitle[i]=allTitle[i][16:l-22]

	getAvailableIndex(allDate, allTitle)
	allDate = selectAvaiElememt(allDate)
	allTitle = selectAvaiElememt(allTitle)

	contextPattern = r'<td align="left" class="be12"><a href="[\s\S]+?" target="_blank">'
	allContextUrl = re.compile(contextPattern).findall(srcCode)

	for i in range(len(allContextUrl)):
		l = len(allContextUrl[i])
		allContextUrl[i] = allContextUrl[i][len('<td align="left" class="be12"><a href="'):l-len('" target="_blank">')]
	allContextUrl = selectAvaiElememt(allContextUrl)
	allContextUrl=completeUrl(allContextUrl)

	allContext = getContext(allContextUrl)
	allAbstract = getAbstract(allContext)

	updateMySQL(allTitle, allAbstract, allContext, allImgUrl, allDate)

	mysqlToJson()





















