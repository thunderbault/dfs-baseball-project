import urllib
from BeautifulSoup import *
import brscraper
import sqlite3
import time 
start_time = time.time()

def SQLExtract():

    with sqlite3.connect('players.sqlite') as conn:
        cur = conn.cursor()
        cur.execute('''SELECT * FROM 'Players';''')
        existing_data = cur.fetchall()
        for data in existing_data:
            BRlookup[data[0]] = data[1]
            
def SQLNameExtract(position, year):

    with sqlite3.connect('%s'%year + '%s'%position + '.sqlite') as conn:
        cur = conn.cursor()
        cur.execute('''SELECT player_name FROM '%s';'''%(year+'totals'))
        existing_data = cur.fetchall()
        return existing_data
        
def createURL(year, BRid):
    getVars = {'id' : '%s' % BRid, 'year' : '%s' % year, 't' : 'b'}
    split_url = 'players/split.fcgi?'
    return split_url + urllib.urlencode(getVars)
        
def updateSplits(player, BRID, position):

    data_tables = []
    data_tables = brscraper.BRScraper().brscraper_in_comments(createURL(year, BRID), table_ids = ["total", "plato", "hmvis"])
    for data_table in data_tables:
        for table_row in data_table[0][data_table[1]]:
            sql_table_name = None
            if data_table[1] == "total":
                if table_row["Split"] == "Last 7 days":
                    sql_table_name = "{}last7days".format(year)
                elif table_row["Split"] == "Last 14 days":
                    sql_table_name = "{}last14days".format(year)
                elif table_row["Split"] == "Last 28 days":
                    sql_table_name = "{}last28days".format(year)
                elif table_row["Split"] == "{} Totals".format(year):
                    sql_table_name = "{}totals".format(year)
                else: 
                    continue
            elif data_table[1] == "plato":
                if table_row["Split"] == "vs RH Starter":
                    sql_table_name = "{}vsRHStarter".format(year)
                elif table_row["Split"] == "vs LH Starter":
                    sql_table_name = "{}vsLHStarter".format(year)
                else: 
                    continue
                
            elif data_table[1] == "hmvis":
                if year is not max(years):
                    continue
                if table_row["Split"] == "Home":
                    sql_table_name = "{}home".format(year)
                elif table_row["Split"] == "Away":
                    sql_table_name = "{}vis".format(year)
                else:
                    continue
            else:
                print "%s was skipped at data_table[1]" %self.name
                skipped_players.append(self.name)
            print sql_table_name
            if sql_table_name and data_table:
                with sqlite3.connect('%s'%year + position + '.sqlite') as conn:
                    cur = conn.cursor()
                    update= '''UPDATE '%s' ''' %sql_table_name
                    set =  '''SET PA = ?, H = ?, DOUBLE = ?, TRIPLE = ?, HR = ?, RBI = ?, R = ?, BB = ?, HBP = ?, Steals = ?
                                        WHERE player_name = ?'''
                    cur.execute(update+set, (table_row["PA"], table_row["H"], table_row["2B"], table_row["3B"], 
                                table_row["HR"], table_row["RBI"], table_row["R"], 
                                table_row["BB"], table_row["HBP"], table_row["SB"], player))
                    conn.commit()


if __name__ == '__main__':                        
    years = ['2015','2016','2017', '2018']
    positions = ['catcher', 'firstbaseman', 'outfielder', 'secondbaseman', 'shortstop', 'thirdbaseman']
    existing_data = []
    BRlookup = {}      
    SQLExtract()
    for year in years:
        for position in positions:
            existing_data = SQLNameExtract(position, year)
            print existing_data
            for player in existing_data:
                updateSplits(player[0],BRlookup[player[0]], position)
end_time = time.time()
print('Took %s seconds to calculate.' % (end_time - start_time))