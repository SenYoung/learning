#encoding:UTF-8
import urllib.request
from lxml import etree
from urllib.request import quote
import pandas as pd
import time


#query_list = ['桂庙 转租', '深大 转租','常兴新村 转租','科苑 转租', '桃园 转租', '龙屋新村 转租', '后海 转租', '大冲新村 转租', '后海村 转租',
#              '马家龙 转租', '南苑新村 转租','大新村 转租','华侨新村 转租','荔芳村 转租 ', '大陆庄园 转租','亿利达村 转租']
query_list = ['坪洲 转租', '西乡 转租', '宝体 转租','宝安中心 转租', '新安 转租', '前海湾 转租','鲤鱼门 转租', '白石洲 转租']
groupID_list = [551176, 17261, 106955,537027,498004,134156,512841,116930,601992]
main_url = 'http://www.douban.com/group/search?start={start_num}&cat=1013&group={group_id}&q={query}&sort=time'
file = r'C:\Users\Administrator\Desktop\douban_3.csv' 
error_file = r'C:\Users\Administrator\Desktop\douban_error_3.csv'

header_selfdefine={
     'Host':'https://m.douban.com/',
     'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:59.0) Gecko/20100101 Firefox/59.0',
     'Accept': '*/*',
     'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
     'Accept-Encoding': 'gzip, deflate',
     'Referer': 'https://m.douban.com/pwa/cache_worker'
}


def get_page_data(page_source, search_word):
	title_list = []
	href_list = []
	time_list = []
	reply_list = []
	search_list = []
	for i in range(1,51):
		try:
			title = page_source.xpath('//*[@id="content"]/div/div[1]/div[2]/table/tbody/tr[%d]/td[1]/a/@title'%i)[0]
			href = page_source.xpath('//*[@id="content"]/div/div[1]/div[2]/table/tbody/tr[%d]/td[1]/a/@href'%i)[0]
			time = page_source.xpath('//*[@id="content"]/div/div[1]/div[2]/table/tbody/tr[%d]/td[2]/span/text()'%i)[0]
			reply = page_source.xpath('//*[@id="content"]/div/div[1]/div[2]/table/tbody/tr[%d]/td[3]/span/text()'%i)[0]
			print(title,time)
			title_list.append(title)
			href_list.append(href)
			time_list.append(time)
			reply_list.append(reply)
			search_list.append(search_word)
		except:
			pass 

	out = 1
	try:
		t_month = time.split("-")
		t_month = int(t_month[0])
		out = 0 if 12 > t_month >= 10 else 1
	except:
		pass
	return title_list, href_list, time_list, reply_list, search_list, out


title_list = []
href_list = []
time_list = []
reply_list = []
error_list = []
search_list = []

for group_id in groupID_list:
	for query in query_list:
		start_num = 0
		out = 0
		while out != 1:	
			encode_q = quote(query)
			url = main_url.format(start_num=start_num, group_id=group_id, query=encode_q)
			print('Open and download data at %s, start_num %d' % (query, start_num))
			try:
				open_obj = urllib.request.urlopen(url)
				html = open_obj.read()
			except:
				error_list.append(url)
				out = 1
				continue
			page_source = etree.HTML(html)
			t_list, h_list, ti_list, r_list, s_list, out = get_page_data(page_source, query)
			title_list += t_list
			href_list += h_list
			time_list += ti_list
			reply_list += r_list
			search_list += s_list
			start_num += 50
		time.sleep(3)


out = pd.DataFrame({'title_list':title_list, 'href_list':href_list, 'time_list':time_list, 'reply_list': reply_list,'search_list':search_list})
out.to_csv(file, encoding="gbk")
error = pd.DataFrame({'error':error_list})
error.to_csv(error_file)