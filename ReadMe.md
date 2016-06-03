所有A股指数成分股的抓取
=======================

各指数成分股部分组成：
====================

1：上交所：
----------
数据->上证系列指数列表：

http://www.sse.com.cn/market/sseindex/indexlist/
    
2： 深交所
---------
(1): 市场数据->行情与指数->指数与样本股

  http://www.szse.cn/main/marketdata/hqcx/zsybg/
  
（2）：市场数据->行情与指数->中证指数系列

  http://www.csindex.com.cn/sseportal/csiportal/xzzx/queryindexdownloadlist.do?type=1
  
  http://www.csindex.com.cn/sseportal/csiportal/xzzx/queryindexdownloadlist.do?type=2
  
  
3：CNINDEX:
----------
(1)：该链接所有指数

http://www.cnindex.com.cn/zstx/szxl/

4: 关于字段的说明：
---------------
字段s: 指数的简称 该字段值必须有

字段 p_code: 指数的代码 该字段值必须有

字段s_code: 相应的指数下面对应的公司代码 该字段值必须有

字段in_dt: 公司被纳入该指数的日期	该字段值必须有	‘20160603’

字段out_dt:公司被剔除该指数的日期	该字段值必须可选择有的话如‘20160603’没有为None

字段sign: 公司是否被剔除该指数的标志	该字段值必须有默认为0	‘0’ or ‘1’

字段 cat: 从哪里抓取的数据分类  该字段值必须有

	上交所： ‘sse’
	深交所：‘szse‘
	CNINDEX: ’cni‘
	
字段ct: 该记录创建的时间 ‘20160602094201’


