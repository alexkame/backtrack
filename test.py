# -*- coding: utf-8 -*-
import q

def main():
	ms = q.MYSQL(HOST='127.0.0.1', DATABASE='backtrack', USER='root', PASSWORD='ct_1234')
	print (ms.tables())
	print ('tdays: ')
	start = '2015-03-05'
	end = '2015-05-10'
	trdays = q.tdays(start,end)
	print (trdays)
	print ('tdaysoffset: n=-3,tdate = 2015-03-10')
	fdate = q.tdaysoffset(-3,end)
	print (fdate)
#	print ('wset: ')
#	codes = q.wset('not_st_suspend_new',start)
#	print (codes)
#	print ('wsd: ')
#	pda = q.wsd(codes,'close_b',start,end)
#	print (pda)
	cftype = 'IF'
	fee = q.fut_fee(cftype)
	print ('fee: ')
	print (fee)
	code = q.fcode(cftype,start)
	print ('fcode: ')
	print (code)
	codes = q.fcodes(cftype,start)
	print ('fcodes: ')
	print (codes)
	optcodes = q.opt50codes(start)
	print ('opt50codes: ')
	print (optcodes)
	expcodes = q.opt50expcodes(start,start,end)
	print ('opt50expcodes: ')
	print (expcodes)
	uncode = '510050.SH'
	pclose = q.un50wsd(uncode,'close_u',start,end)
	print ('un50wsd: ')
	print (pclose)
	optcode = '10000025.SH'
	att = q.opt50att(optcode)
	print ('opt50att: ')
	print (att)
	price = q.opt50wsd(optcode,'close',start,end)
	print ('opt50wsd: ')
	print (price)
	margin = q.opt50margin(optcode,start,10000,'hold')
	print ('opt50margin: ')
	print (margin)
if __name__ == '__main__':
	main()
