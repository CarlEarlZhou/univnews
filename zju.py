#coding: utf-8

import urllib2
import urllib
import re
import pymysql
import json
import sys
import datetime
from HTMLParser import  HTMLParser
import sqlConnect
from sqlConnect import sqlConnect

localflag = False
localpath = "D:/SEBASE/news/spider/"
serverpath = "/var/www/html/"
srcprefix = "http://www.news.zju.edu.cn"
imgSrcPreFix = "http://www.news.zju.edu.cn"

allImgUrl = []
avai = []
tagMap = {'h1' : True, 'h2' : True, 'h3' : True, 'h4' : True, 'h5' : True, 'h6' : True, 'p' : True}

class myHTMLParser(HTMLParser) :
	myStr = ""
	myTitle = ""
	myUrl = ""
	myImgUrl = ""
	def handle_data(self, data) :
		self.myStr += pymysql.escape_string(data)
		if(tagMap.has_key(self.lasttag)) :
			self.myStr += "\n"
	def handle_starttag(self, tag, attrs) :
		if(tag == 'a') :
			for x in attrs :
				if(x[0] == 'title') :
					self.myTitle = pymysql.escape_string(x[1])
				elif(x[0] == 'href') :
					self.myUrl = srcprefix + x[1]
		elif(tag == 'img') :
			for x in attrs :
				if(x[0] == 'src') :
					self.myImgUrl = imgSrcPreFix + x[1]


def getContext(c_url) :
	ret = []
	for i, url in enumerate(c_url) :
		try :
			print("handling %dth context from zju" %(i + 1))
			link = urllib2.urlopen(url)
			srcCode = link.read()
			newsPattern = '<div class=\'wp_articlecontent\'>[\s\S]+?</div>'
			newsblock = re.compile(newsPattern).findall(srcCode)
			parser = myHTMLParser()
			stri = parser.unescape(newsblock[0])
			stri = re.sub(r'<span[\s\S]+?>', '', stri)
			stri = stri.replace("</span>", '').replace("<strong>", '').replace("</strong>", '').replace("<b>", '').replace("</b>", '')
			parser.feed(stri)
			allImgUrl.append(parser.myImgUrl)
			stri = parser.myStr
			parser.close()

			ret.append(stri)
		except :
			ret.append('error')
			print("error when handling %dth context from zju" %(i + 1))
			print(traceback.print_exc())

	return ret


if __name__ == '__main__':

	reload(sys)
	sys.setdefaultencoding('utf8')

	test = sqlConnect("zju", 10)

	parser = myHTMLParser()

	link = urllib2.urlopen('http://www.news.zju.edu.cn/773/list.htm')
	srcCode = link.read()

	datePattern = r'<div class="news_time fr">.+?</div>'
	allDate = re.compile(datePattern).findall(srcCode)

	for i in range(len(allDate)) :
		allDate[i] = allDate[i].lstrip('<div class="news_time fr">').rstrip('</div>')

	test.date = allDate

	allTitle = []
	allContextUrl = []

	newsPattern = r'<div class="new_tt fl">.+?</div>'
	newsblock = re.compile(newsPattern).findall(srcCode)
	for x in newsblock :
		parser.feed(x)
		allTitle.append(parser.unescape(parser.myTitle))
		allContextUrl.append(parser.myUrl)

	test.title = allTitle
	test.contextUrl = allContextUrl

	allContext = getContext(test.contextUrl)
	test.context = allContext

	test.updateSql()
	test.mysqlToJson()




















