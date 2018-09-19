# -*- coding: utf-8 -*-
import pymysql
#数据库操作类，将数据库的常用操作封装到同一个类中，有关数据库的基本操作可以调用此类
class MYSQL(object):
	def __init__(self, HOST='127.0.0.1', DATABASE='stk_cn', USER='root', PASSWORD='ct_1234', PORT=3306, CHARSET="utf8"):
		self.host = HOST
		self.db = DATABASE
		self.user = USER
		self.pwd = PASSWORD
		self.port = PORT
		self.charset = CHARSET
		try:
			self.conn = pymysql.connect(host=self.host,port=self.port,user=self.user,passwd=self.pwd,db=self.db,charset=self.charset)
			self.cur = self.conn.cursor()
		except:
			raise(NameError,"连接数据库失败")
# 获取当前数据库中的所有表
	def tables(self):
		sql = 'show tables'
		self.cur.execute(sql)
		result = self.cur.fetchall()
		self.conn.commit()
		tablelist = []
		for name in result:
			tablelist.append(name[0])
		return tablelist
# 获取某个表名的所有字段名
	def tb_fields(self, tbname):
		sql = '''select COLUMN_NAME from information_schema.COLUMNS where table_name = '%s' and table_schema = '%s' ''' % (tbname, self.db)
		self.cur.execute(sql)
		result = self.cur.fetchall()
		self.conn.commit()
		tb_field = []
		for name in result:
			tb_field.append(name[0])
		return tb_field
# 执行一个sql语句，不需要返回语句执行后的结果
	def exesqlno(self, sql, param=None):
		if param == None:
			self.cur.execute(sql)
		else:
			self.cur.execute(sql, param)
		self.conn.commit()
# 执行一个sql语句，需要返回语句执行后的结果
	def exesql(self, sql, param=None):
		if param == None:
			self.cur.execute(sql)
		else:
			self.cur.execute(sql, param)
		result = self.cur.fetchall()
		self.conn.commit()
		return result
ms = MYSQL()
#获取两个日期之间的所有交易日
def tdays(start,end):
	result = []
	sql = '''
SELECT date FROM wsd_tdays WHERE date BETWEEN '%s' AND '%s' order by date'''%(start,end)
	try:
		result += ms.exesql(sql)
	except:
		raise(NameError,'exesql error: The input parameter of tdays may not be correct.')
	result = [temp[0] for temp in result]
	return result
#向前或者向后移动n个交易日
def tdaysoffset(ndays,stockdate):
	tbname = 'wsd_tdays'
	result = []
	if ndays <= 0:
		sql = '''
SELECT date FROM %s WHERE date <= '%s' order by date'''%(tbname,stockdate)
	else:
		sql = '''
SELECT date FROM %s WHERE date > '%s' order by date'''%(tbname,stockdate)
	try:
		result += ms.exesql(sql)
	except:
		raise(NameError,'exesql error: The input parameter of tdaysoffset may not be correct.')
	result = [temp[0] for temp in result]
	return result[ndays-1]
#对输入的股票代码统一形式供SQL调用
def code_tuple(code):
#	if type(code) == unicode: python3 没有unicode类型
#		code = str(code)
	if type(code) == str:
		code_num = len(code.split(','))
		codein = []
		for i in range(code_num):
			codein.append(code.split(',')[i])
		codein = tuple(codein)
	elif type(code) == list:
		if type(code[0]) != str:
			for i in range(len(code)):
				code[i] = str(code[i])
		code_num = len(code)
		codein = tuple(code)
	else:
		code_num = len(code)
		codein = code
	if code_num == 1:
		codein = '''('%s')'''%codein[0]
	return codein,code_num
#获取股票数据库中日数据的所有表
def get_wsdtablelist(start,end):
	ystart = int(start.split('-')[0])
	yend = int(end.split('-')[0])
	if yend < ystart:
		raise(NameError,'Error: the end_date is smaller than start_date!')
	tablelist = []
	for i in range(ystart,yend+1):
		tablelist.append('wsd_' + str(i))
	return tablelist
#股票分钟数据格式
class WSIDATA(object):
	def __init__(self,result,code_num):
		self.Times = []
		self.Data = []
		self.code_num = code_num
		if len(result) > 0:
			if code_num <= 1:
				for i in range(len(result[0])-2):
					self.Data.append([])
				for i in range(len(result)):
					self.Times.append(result[i][0])
					for j in range(len(result[i])-2):
						self.Data[j].append(result[i][j+2])
			else:
				for i in range(len(result[0])):
					self.Data.append([])
				for i in range(len(result)):
					self.Times.append(result[i][0])
					for j in range(len(result[i])):
						self.Data[j].append(result[i][j])
	def __str__(self):
		temp_times = []
		temp_data = []
		for i in range(len(self.Data)):
			temp_data.append([])
		if len(self.Times) > 10:
			for i in range(11):
				if i < 10:
					temp_times.append(self.Times[i].strftime('%Y%m%d %H:%M:%S'))
					for j in range(len(temp_data)):
						if j == 0 and self.code_num > 1:
							temp_data[j].append(self.Data[j][i].strftime("%Y-%m-%d %H:%M:%S"))
						else:
							temp_data[j].append(self.Data[j][i])
				else:
					temp_times.append('...')
					for j in range(len(temp_data)):
						temp_data[j].append('...')
			return '.Times=%s\n.Data=%s'%(temp_times,temp_data)
		else:
			return '.Times=%s\n.Data=%s'%(self.Times,self.Data)
#股票日数据格式
class WSDDATA(object):
	def __init__(self,result,code_num,codein):
		self.Codes = []
		self.Times = []
		self.Data = []
		self.code_num = code_num
		if len(result) > 0:
			if code_num > 1 and len(result[0]) > 3:
				raise(NameError,'The code_num > 1 and field_num > 1,just one of them could be larger than 1 ')
		if type(codein) == tuple:
			for i in range(len(codein)):
				self.Codes.append(codein[i])
		else:
			if code_num == 1:
				self.Codes.append(codein[2:-2])
			else:
				self.Codes.append(codein)
		if len(result) > 0:
			if code_num > 1:
				for i in range(code_num):
					self.Data.append([])
				for i in range(len(result)):
					temp_time = result[i][0]
					if temp_time not in self.Times:
						self.Times.append(temp_time)
					for j in range(code_num):
						if result[i][1] == codein[j]:
							self.Data[j].append(result[i][2])
							break
			elif len(result[0]) > 3:
				for i in range(len(result[0])-2):
					self.Data.append([])
				for i in range(len(result)):
					self.Times.append(result[i][0])
					for j in range(len(result[0])-2):
						self.Data[j].append(result[i][j+2])
			else:
				self.Data.append([])
				for i in range(len(result)):
					self.Times.append(result[i][0])
					self.Data[0].append(result[i][2])
		else:
			if code_num > 1:
				for i in range(code_num):
					self.Data.append([])
			else:
				self.Data.append([])
	def __str__(self):
		temp_times = []
		temp_data = []
		for i in range(len(self.Data)):
			temp_data.append([])
		if len(self.Times) > 10:
			for i in range(11):
				if i < 10:
					temp_times.append(self.Times[i].strftime("%Y-%m-%d"))
					for j in range(len(temp_data)):
						if len(self.Data[j]) > 10:
							temp_data[j].append(self.Data[j][i])
				else:
					temp_times.append('...')
					for j in range(len(temp_data)):
						if len(self.Data[j]) > 10:
							temp_data[j].append('...')
			return '.Codes=%s\n.Times=%s\n.Data=%s'%(self.Codes,temp_times,temp_data)
		else:
			return '.Codes=%s\n.Times=%s\n.Data=%s'%(self.Codes,self.Times,self.Data)
# 执行日数据wsd的查询
def select_wsd(tbname,codein,code_num,index,start,end):
	result = []
	for i in range(len(tbname)):
		sql = '''
SELECT date,stockcode,%s FROM %s WHERE stockcode in %s AND date BETWEEN '%s' AND '%s' order by date'''%(index,tbname[i],codein,start,end) 
		try:
			result += ms.exesql(sql)
		except:
			raise(NameError,'exesql error: The input parameter of wsd may not be correct.')
	return result
def select_wset(tbname,attribute,stockdate):
	if attribute == "":
		sql = '''
SELECT DISTINCT(stockcode) FROM %s WHERE date = '%s' order by stockcode'''%(tbname[0],stockdate)
	else:
		sql = '''
SELECT DISTINCT(stockcode) FROM %s WHERE %s = 1 AND date = '%s' order by stockcode'''%(tbname[0],attribute,stockdate)
	try:
		result = ms.exesql(sql)
	except:
		raise(NameError,'exesql error: The input parameter of wset may not be correct.')
	result = [temp[0] for temp in result]
	return result
def select_wsd_date(tbname,index,start,end):
	result = []
	sql = '''
SELECT date,%s FROM %s WHERE date BETWEEN '%s' AND '%s' order by date'''%(index,tbname,start,end)
	try:
		result += ms.exesql(sql)
	except:
		raise(NameError,'exesql error: The input parameter of wsd_date may not be correct.')
	return result
#获取股票日线数据
def wsd(code,index,start,end):
	tblist = get_wsdtablelist(start,end)
	tables = ms.tables()
	tbname = []
	for i in range(len(tblist)):
		if tblist[i] in tables:
			tbname.append(tblist[i])
		else:
			raise(NameError,'Error: The table %s is not in SQL')
	codein,code_num = code_tuple(code)
	result = select_wsd(tbname,codein,code_num,index,start,end)
	wsd_data = WSDDATA(result,code_num,codein)
	return wsd_data
#选取某天满足某一类条件的股票
def wset(attribute,stockdate):
	tblist = ['wsd_' + stockdate.split('-')[0]]
	tables = ms.tables()
	tbname = []
	for i in range(len(tblist)):
		if tblist[i] in tables:
			tbname.append(tblist[i])
		else:
			raise(NameError,'Error: The table %s is not in SQL')
	result = select_wset(tbname,attribute,stockdate)
	return result
#获取期货的手续费、保证金等
def fut_fee(cftype):
	tbname = 'wsdcf_info'
	sql = '''
SELECT fee_rate,multiplier_num,fee_base,min_margin FROM %s WHERE cf_type = '%s' '''%(tbname,cftype)
	result = ms.exesql(sql)[0]
	fee = {}
	if result[2] == u'1手':
		fee['fee_rate'] = result[0]/result[1]
	else:
		fee['fee_rate'] = result[0]
	fee['stamp'] = 0
	fee['margin'] = result[3]
	fee['fee_base'] = result[2]
	fee['mul_num'] = result[1]
	return fee
def fselect_wsd(codein,code_num,index,start,end):
	tbname = 'wsdcf'
	result = []
	sql = '''
SELECT date,cfcode,%s FROM %s WHERE cfcode in %s AND date BETWEEN '%s' AND '%s' order by date'''%(index,tbname,codein,start,end)
	try:
		result += ms.exesql(sql)
	except:
		raise(NameError,'exesql error: The input parameter of fwsd may not be correct.')
	return result
#获取期货日线数据
def fwsd(code,index,start,end):
	codein,code_num = code_tuple(code)
	result = fselect_wsd(codein,code_num,index,start,end)
	wsd_data = WSDDATA(result,code_num,codein)
	return wsd_data
#获取某种期货的某天的主力合约
def fcode(cftype,tdate):
	tbname = 'wsdcf'
	sql = '''
SELECT cfcode FROM %s WHERE cftype = '%s' AND date = '%s' AND main_contract = 1 '''%(tbname,cftype,tdate)
	result = ms.exesql(sql)[0][0]
	return result
#获取某种期货的某天的所有合约，从近到远
def fcodes(cftype,tdate):
	tbname = 'wsdcf'
	sql = '''
SELECT cfcode FROM %s WHERE cftype = '%s' AND date = '%s' ORDER by lastdelivery_date '''%(tbname,cftype,tdate)
	result = ms.exesql(sql)
	codes = []
	for i in range(len(result)):
		codes.append(result[i][0])
	return codes
#获取某日50ETF所有期权
def opt50codes(tdate):
	tbname = 'etf50base'
	sql = '''
SELECT code,expire_date,exercise_price,call_or_put FROM %s WHERE listed_date <= '%s' AND expire_date >= '%s' ORDER BY expire_date ''' % (tbname,tdate,tdate)
	result = ms.exesql(sql)
	pd = {}
	for i in range(len(result)):
		if result[i][1] not in pd.keys():
			pd[result[i][1]] = {}
			pd[result[i][1]][result[i][3]] = {}
			pd[result[i][1]][result[i][3]][result[i][0]] = result[i][2]
		else:
			if result[i][3] not in pd[result[i][1]].keys():
				pd[result[i][1]][result[i][3]] = {}
			pd[result[i][1]][result[i][3]][result[i][0]] = result[i][2]
	return pd
#50ETF期权0.05的整数倍才有意义,此函数将所有可能行权价列出来
def ex_prices():
	pvalue = range(160,400,5)
	prices = []
	for i in range(len(pvalue)):
		prices.append(pvalue[i]/100.0)
	return prices
#50ETF期权各个月份的当月、次月、当季、次季的月份索引
def opt50index():
	pmonth = {'01':{'nextm':'02','nowq':'03','nextq':'06'},
			'02': {'nextm': '03', 'nowq': '06', 'nextq': '09'},
			'03': {'nextm': '04', 'nowq': '06', 'nextq': '09'},
			'04': {'nextm': '05', 'nowq': '06', 'nextq': '09'},
			'05': {'nextm': '06', 'nowq': '09', 'nextq': '12'},
			'06': {'nextm': '07', 'nowq': '09', 'nextq': '12'},
			'07': {'nextm': '08', 'nowq': '09', 'nextq': '12'},
			'08': {'nextm': '09', 'nowq': '12', 'nextq': '03'},
			'09': {'nextm': '10', 'nowq': '12', 'nextq': '03'},
			'10': {'nextm': '11', 'nowq': '12', 'nextq': '03'},
			'11': {'nextm': '12', 'nowq': '03', 'nextq': '06'},
			'12': {'nextm': '01', 'nowq': '03', 'nextq': '06'},}
	return pmonth
#到期日在某一区间的所有符合条件的50ETF期权合约
def opt50expcodes(tdate,start,end):
	prices = ex_prices()
	tbname = 'etf50base'
	sql = '''
SELECT code,expire_date,exercise_price,call_or_put FROM %s WHERE listed_date <= '%s' AND expire_date between '%s' and '%s' \
ORDER BY expire_date ''' % (tbname,tdate,start,end)
	result = ms.exesql(sql)
	pd = {}
	for i in range(len(result)):
		if result[i][2] in prices:
			if result[i][1] not in pd.keys():
				pd[result[i][1]] = {}
				pd[result[i][1]][result[i][3]] = {}
				pd[result[i][1]][result[i][3]][result[i][0]] = result[i][2]
			else:
				if result[i][3] not in pd[result[i][1]].keys():
					pd[result[i][1]][result[i][3]] = {}
				pd[result[i][1]][result[i][3]][result[i][0]] = result[i][2]
	return pd
#获取50ETF正股的数据
def un50wsd(code,index,start,end):
	sql = '''
SELECT date,%s FROM etf50_unday WHERE code = '%s' AND date between '%s' and '%s' order by date '''%(index,code,start,end)
	result = ms.exesql(sql)
	pd = {'Times':[],'Data':[]}
	for i in range(len(result)):
		pd['Times'].append(result[i][0])
		pd['Data'].append(result[i][1])
	return pd
#获取某50ETF期权合约的三要素
def opt50att(code):
	pd = {}
	sql = '''
SELECT call_or_put,exercise_price,expire_date FROM etf50base WHERE code = '%s' '''%code
	result = ms.exesql(sql)
	pd['code'] = code
	pd['call_put'] = result[0][0]
	pd['exercise_price'] = result[0][1]
	pd['expire_date'] = result[0][2]
	return pd
#获取50ETF期权的日线数据
def opt50wsd(code,index,start,end):
	sql = '''
SELECT date,%s FROM dayetf50 WHERE code = '%s' AND date between '%s' and '%s' order by date '''%(index,code,start,end)
	result = ms.exesql(sql)
	pd = {'Times': [], 'Data': []}
	for i in range(len(result)):
		pd['Times'].append(result[i][0])
		pd['Data'].append(result[i][1])
	return pd
#计算某50ETF期权的交易所保证金
def opt50margin(code,tdate,num,flag):
	if flag == 'open':
		fdate = tdaysoffset(-1,tdate).strftime("%Y-%m-%d")
		try:
			pclose = opt50wsd(code,'close',fdate,fdate)['Data'][0]
		except:
			pclose = opt50wsd(code,'open',tdate,tdate)['Data'][0]
		if pclose == None:
			ffdate = tdaysoffset(-1,fdate).strftime("%Y-%m-%d")
			pclose = opt50wsd(code, 'close', ffdate, ffdate)['Data'][0]
		punclose = un50wsd('510050.SH','close_b',fdate,fdate)['Data'][0]
		att = opt50att(code)
		if att['call_put'] == '认购':
			vir_value = max(0.0,att['exercise_price']-punclose)
			marg = (pclose + max(0.12*punclose - vir_value,0.07*punclose))*num
		elif att['call_put'] == '认沽':
			vir_value = max(0.0, punclose - att['exercise_price'])
			marg = min(pclose + max(0.12*punclose - vir_value,0.07*att['exercise_price']),att['exercise_price'])*num
		else:
			raise(NameError,'call_put must be 认购 or 认沽.')
	elif flag == 'hold':
		pclose = opt50wsd(code, 'close', tdate, tdate)['Data'][0]
		punclose = un50wsd('510050.SH', 'close_b', tdate, tdate)['Data'][0]
		att = opt50att(code)
		if att['call_put'] == '认购':
			vir_value = max(0.0, att['exercise_price'] - punclose)
			marg = (pclose + max(0.12 * punclose - vir_value, 0.07 * att['exercise_price'])) * num
		elif att['call_put'] == '认沽':
			vir_value = max(0.0, punclose - att['exercise_price'])
			marg = min(pclose + max(0.12 * punclose - vir_value, 0.07 * att['exercise_price']), att['exercise_price']) * num
		else:
			raise(NameError,'call_put must be 认购 or 认沽.')
	else:
		raise(NameError,'call_put must be 认购 or 认沽.')
	return marg


