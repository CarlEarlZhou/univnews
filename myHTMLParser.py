import HTMLParser
from HTMLParser import HTMLParser
import pymysql

class testmeta(type):
	def __new__(cls, name, bases, attrs):
		if(name != "myHTMLParser"):
			return type.__new__(cls, name, bases, attrs)
		proptys = attrs["ppts"]
		for k, w in proptys.iteritems():
			def temf(kk, ww):
				kk = '_' + kk
				@property
				def prop(self):
					try:
						return pymysql.escape_string(self.__dict__[kk])
					except:
						tem = []
						for i in self.__dict__[kk]:
							tem.append(pymysql.escape_string(i))
						return tem
				attrs[ww] = prop
			temf(k, k)
		return type.__new__(cls, name, bases, attrs)


class myHTMLParser(HTMLParser, object):

	__metaclass__ = testmeta

	def __init__(self):
		super(myHTMLParser, self).__init__()
		for i, v in myHTMLParser.ppts.iteritems():
			self.__dict__['_' + i] = v
		self._flag = True
		

	def __setattr__(self, key, value):
		if(key == "_imgUrl"):
			if(self.__dict__["_flag"]):
				self.__dict__["_flag"] = False
				self.__dict__[key] = value
		else:
			super(myHTMLParser, self).__setattr__(key, value)

	ppts = {"context":"", "title":[], "url":[], "imgUrl":"null"}


if __name__ == '__main__':
	t = myHTMLParser()
	t._title = "svvano'asnaoien\nasnoaew'anoevwa"