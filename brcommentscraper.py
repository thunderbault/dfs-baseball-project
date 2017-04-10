
#scrape baseball-reference comments for data tables
#by Adam Thibault
from bs4 import BeautifulSoup
from bs4 import Comment
import re
import urllib


def brscraper_in_comments(resource, table_ids=None, server_url="http://www.baseball-reference.com/", table_found = False):
    data = {}
    datas = []
    print server_url + resource
    soup = BeautifulSoup(urllib.urlopen(server_url + resource))
    urllib.urlopen(server_url + resource).close()
    tables = soup.find_all(text=lambda text:isinstance(text, Comment))
    for table in tables:
        for table_id in table_ids:
            if 'table class="row_summable sortable stats_table" id="%s"'%table_id in table:
                data[table_id] = []
                rows = table.string.split("</tr>")
                for row in rows:
                    stats = []
                    if 'table class="row_summable sortable stats_table" id="%s"'%table_id in row:
                        headers = re.findall(r'<th.*>([^<]*)</th>', row)
                        table_found = True
                        continue
                    if table_found:
                        entries = row.split(">")
                        for entry in entries:
                            if "</th" in entry:
                                if "</thead" in entry:
                                    continue
                                stat = re.findall(r'(.*)<', unicode(entry))
                                for each in stat:
                                    stats.append(str(each))
                            if "</td" in entry:
                                stat = re.findall(r'(.*)<', str(entry))
                                for each in stat:
                                    stats.append(unicode(each))
                        if len(stats) > 0:
                            data[table_id].append(dict(zip(headers, stats)))
                table_and_data = [data,table_id]
                datas.append(table_and_data)
    return datas

#print brscraper_in_comments("players/split.fcgi?t=b&id=troutmi01&year=2016", table_ids = "total")