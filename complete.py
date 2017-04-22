#Complete baseball-reference scraper and SQL writer for data for daily optimizer
#By Adam Thibault
from BeautifulSoup import *
import sqlite3
import urllib
import brscraper
import brcommentscraper
import time 
start_time = time.time()


def daily_starters(object):
    '''
    scrape starters_website for baseball-reference playerid, handedness, and starting players
    '''
    call_starters_url = urllib.urlopen(object)
    starters_website = call_starters_url.read()	
    soup = BeautifulSoup(starters_website)
    call_starters_url.close()
    for a in soup.findAll('a'):

        check_player_name = a.get("data-razz")
        if check_player_name is None:
            continue 
        player = a.text.lower()
        player_id = a.get("data-bref")
        if player_id:
            player_id_lookup[player] = player_id
        handedness[player] = a.parent.text.split("(")[1][0]
        if "(L) " in a.parent.text or "(R) " in a.parent.text or "(S) " in a.parent.text:
            starting_players.append(player)
            player_lineup_spot[player] = a.parent.text.split(".")[0]
        else:
            starting_pitcher.append(player)
        
def create_url(player_full_name, website, year, server_url="http://www.baseball-reference.com/"):
    '''
    create url from player name and playerid for scraping baseball-reference based on current url called
    '''
    player_id = player_id_lookup[player_full_name]
    if website == "splits_url":
        if player_and_position[player_full_name][0] == 1: 
            getVars = {'id' : '%s' % player_id, 'year' : '%s' % year, 't' : 'p'}
        else:
            getVars = {'id' : '%s' % player_id, 'year' : '%s' % year, 't' : 'b'}
        split_url = 'players/split.fcgi?'
        player_url = split_url + urllib.urlencode(getVars)
    
    if website == "bvp_url":
        bvp_url = "play-index/batter_vs_pitcher.cgi?batter="
        player_url = bvp_url + player_id
    return player_url
    
def write_to_database(line, player_full_name, sql_table_name, year):
    '''
    write to sql database, separate databases based on the website called and the stats desired
    '''
    if not line:
        print "%s was skipped at if not data_table" %player_full_name
        skipped_players.append(player_full_name)
    else:
        if player_and_position[player][0] == 1:
            conn = sqlite3.connect('%s'%year + 'pitcher.sqlite')
            cur = conn.cursor()
            create_table = """CREATE TABLE IF NOT EXISTS '%s'
            (player_name TEXT UNIQUE, position_id_1 INTEGER, position_id_2 INTEGER, innings_pitched SINGLE, games_started INTEGER,
            K SINGLE, W INTEGER, ER INTEGER, WHIP SINGLE);""" %sql_table_name

        else:
            conn = sqlite3.connect('%s'%year + 'batter.sqlite')
            cur = conn.cursor()
            create_table = """CREATE TABLE IF NOT EXISTS '%s'
            (player_name TEXT UNIQUE, position_id_1 INTEGER, position_id_2 INTEGER, position_id_3 INTEGER, position_id_4 INTEGER, 
            PA INTEGER, H INTEGER, DOUBLE INTEGER, TRIPLE INTEGER, HR INTEGER, 
            RBI INTEGER, R INTEGER, BB INTEGER, HBP INTEGER, Steals INTEGER);""" %sql_table_name
        
        cur.execute(create_table)

        if player_and_position[player_full_name][0] == 1:
            try:
                cur.execute('''INSERT INTO '%s' (player_name, position_id_1,
                        innings_pitched, games_started, K, W, ER, WHIP)
                        VALUES ( ?, ?, ?, ?, ?, ?, ?, ?)'''%sql_table_name, (player_full_name, player_and_position[player_full_name][0], 
                        line["IP"] ,line["GS"] ,line["SO"] ,line["W"] ,
                        line["ER"] ,line["WHIP"]))
            except:
            
                skipped_players.append(player_full_name)
                pass
        else:
            if sql_table_name == "{}bvp".format(year):
                try:
                    cur.execute('''INSERT INTO '%s' (player_name, position_id_1,
                        PA, H, DOUBLE, TRIPLE, HR, RBI, BB, HBP)
                        VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''%sql_table_name, (player_full_name, player_and_position[player_full_name][0], 
                        line["PA"], line["H"], line["2B"], line["3B"], 
                        line["HR"], line["RBI"], line["BB"], line["HBP"]) )
                except: 
                    print "%s was skipped at try to append data to sql for batter" %player_full_name
                    skipped_players.append(player_full_name)
                    pass          
            else:
                try:
                    cur.execute('''INSERT INTO '%s' (player_name, position_id_1,
                        PA, H, DOUBLE, TRIPLE, HR, RBI, R, BB, HBP, Steals)
                        VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''%sql_table_name, (player_full_name, player_and_position[player_full_name][0], 
                        line["PA"], line["H"], line["2B"], line["3B"], 
                        line["HR"], line["RBI"], line["R"], 
                        line["BB"], line["HBP"], line["SB"]) )
                except: 
                    print "%s was skipped at try to append data to sql for batter" %player_full_name
                    skipped_players.append(player_full_name)
                    pass                     
            try:
                cur.execute('''INSERT INTO '%s' (position_id_2
                    VALUES ( ?, )'''%sql_table_name, (player_and_position[player_full_name][1]) )
            except: pass
            try:
                cur.execute('''INSERT INTO '%s' (position_id_3
                    VALUES ( ?, )'''%sql_table_name, (player_and_position[player_full_name][2]) )
            except: pass
            try:
                cur.execute('''INSERT INTO '%s' (position_id_4
                    VALUES ( ?, )'''%sql_table_name, (player_and_position[player_full_name][3]) )
            except: pass

    conn.commit()
    conn.close()               
                    
def extract_dk_info(csv_from_draftkings):
    '''
    extract salary and position information from draftkings .csv downloaded locally daily 
    '''
    import csv
    global player_and_position
    global player_and_salaries
    global player_team
    global opposing_team_pitcher
    global opposing_team  
    with open(csv_from_draftkings, 'rb') as csvfile:
        file = csv.reader(csvfile)
        for row in file:
            player_full_name = str(row[1]).lower()
            #placeholder for dealing with tough characters (accents, etc)
            if '\xed' in player_full_name:
                player_full_name = player_full_name.replace('\xed', 'i')
            if '\xc1' in player_full_name:
                player_full_name = player_full_name.replace('\xc1', 'a')
            if '\xe9' in player_full_name:
                player_full_name = player_full_name.replace('\xe9', 'e')
            if '\xf3' in player_full_name:
                player_full_name = player_full_name.replace('\xf3', 'o')
            if '\xfa' in player_full_name:
                player_full_name = player_full_name.replace('\xfa', 'u')
            if '\xf1' in player_full_name:
                player_full_name = player_full_name.replace('\xf1', 'n')
            if '\xe1' in player_full_name:
                player_full_name = player_full_name.replace('\xe1', 'a')

            #attempt to deal with tough characters, will come back to it later
            
            # player_full_name = column[1].decode('utf-8', "replace")
            # remove_accents(player_full_name)
            # player_full_name = unicodedata.normalize('NFD', unicode(column[1], "utf8")).encode('ascii', 'ignore')
            # player_full_name = PyUnicode_DecodeUTF32(column[1])
            # player_full_name = ''.join((c for c in unicodedata.normalize('NFD', player_full_name) if unicodedata.category(c) != 'Mn'))
            # player_full_name = unidecode(player_full_name)
            
            if player_full_name in starting_players or player_full_name in starting_pitcher:

                if player_full_name in player_full_names_list:
                    continue
                else:
                    player_team[player_full_name] = row[5]
                    if player_team[player_full_name] in manually_entered_teams_to_skip: continue
                    position = str(row[0])
                    if position == "Position": continue
                    dk_salary = str(row[2])
                    if row[3] == "Postponed":
                        continue
                    else:
                        teams = row[3].split("@")
                    player_full_names_list.append(player_full_name)
                    try:
                        dk_salary = int(dk_salary)
                        player_and_salaries[player_full_name] = dk_salary
                    except: 
                        dk_salary = row[3]
                        player_and_salaries[player_full_name] = dk_salary
                    player_and_position[player_full_name] = position.split('/')
                    player_team[player_full_name] = row[5]
                    players_team= row[5]
                    the_home_team = teams[1]
                    the_home_team_clean = the_home_team[:3].strip()
                    if the_home_team_clean not in home_team:
                        home_team.append(the_home_team_clean)
                    for team in teams:
                        team = team[:3].strip()
                        if team == players_team:
                            pass
                        else:
                            opposing_team[player_full_name] = team
                    if player_full_name in starting_pitcher:
                        pitcher_name = opposing_team_pitcher.get(players_team, None)
                        try:
                            pitcher_name = opposing_team_pitcher[players_team]
                        except:
                            opposing_team_pitcher[players_team] = player_full_name                        
                    count = 0
                    for position in player_and_position[player_full_name]:
                        if position == 'SP' or position == 'RP':
                            player_and_position[player_full_name][count] = 1                        
                        elif position == 'C':
                            player_and_position[player_full_name][count] = 2
                        elif position == '1B':
                            player_and_position[player_full_name][count] = 3
                        elif position == '2B':
                            player_and_position[player_full_name][count] = 4
                        elif position == '3B':
                            player_and_position[player_full_name][count] = 5
                        elif position == 'SS':
                            player_and_position[player_full_name][count] = 6
                        elif position == 'OF':
                            player_and_position[player_full_name][count] = 7
                        else:
                            player_and_position[player_full_name][count] = 0
                        count = count + 1
            else: continue
def check_data(player_full_name, position, sql_table, year, line):
    '''
    check if data already exists, and pass over existing data
    '''
        if position == 1:
            innings_pitched = None
            conn = sqlite3.connect('%s'%year + 'pitcher.sqlite')
            cur2= conn.cursor()
            try:
                cur2.execute('''SELECT innings_pitched, games_started, K, W, ER, WHIP FROM '%s' WHERE player_name=?'''%sql_table, (player_full_name,))
                stats_from_database = cur2.fetchall()
                innings_pitched = stats_from_database[0][0]
                conn.close()
            except:
                conn.close()
            if innings_pitched:
                if innings_pitched== line["IP"]:
                    return True
                else:
                    return False
        else:
            plate_appearances = None
            conn = sqlite3.connect('%s'%year + 'batter.sqlite')
            cur2 = conn.cursor()
            try:
                cur2.execute('''SELECT PA, H, DOUBLE, TRIPLE, HR, RBI, R, BB, HBP, Steals FROM '%s' WHERE player_name=?'''%sql_table, (player_full_name,))
                stats_from_database = cur2.fetchall()
                plate_appearances = stats_from_database[0][0]
                conn.close()
            except:
                conn.close()
            if plate_appearances:
                if plate_appearances == line["PA"]:
                    return True
                else:
                    return False
scraper = brscraper.BRScraper()      
#websites correspond to: 7day/14/28/365, platoon LHB/RHB, home/away, batter vs pitcher           
manually_entered_teams_to_skip = []
websites = ["splits_url", "bvp_url"]
hmvis_check = {}
player_team = {}
player_and_position = {} 
handedness = {}
home_team = []
opposing_team_pitcher = {}
opposing_team = {}
player_full_names_list = []
player_and_salaries = {}
starting_players = []
starting_pitcher = []
player_id_lookup = {}
player_lineup_spot = {}  
starters_url = 'http://www.baseballpress.com/lineups/2017-04-14'
daily_starters(starters_url)
csv_from_draftkings = 'DKSalaries.csv' 

skipped_players = []
years = ['2015', '2016', '2017']

extract_dk_info(csv_from_draftkings)
for year in years:
    sql_tables = ["{}last365days".format(year), "{}last7days".format(year), "{}last14days".format(year), "{}last28days".format(year), "{}vsRHStarter".format(year), "{}vsLHStarter".format(year), "{}hmvis".format(year), "{}bvp".format(year)]
    for sql_table in sql_tables:
        #loop through and erase each SQL table
        conn = sqlite3.connect('%s'%year + 'pitcher.sqlite')
        cur = conn.cursor()
        cur.execute('''DROP TABLE IF EXISTS '%s' '''%sql_table)
        conn.commit()
        conn.close()
        conn = sqlite3.connect('%s'%year + 'batter.sqlite')
        cur = conn.cursor()
        cur.execute('''DROP TABLE IF EXISTS '%s' '''%sql_table)
        conn.commit()
        conn.close()
    for player in player_full_names_list:
        for website in websites:
            #construct correct URL and scrape all tables of interest cooresponding to the chosen website
            # total = total stats, plato = platoon stats, hmvis = home/away stats
            splits_tables = ["total", "plato", "hmvis"]
            data_tables = []
            if website == "splits_url":    
                if player_and_position[player][0] == 1:
                    splits_tables.remove("plato")
                    splits_tables = [table + "_extra" for table in splits_tables]
                try:
                    player_id = player_id_lookup[player]
                except:
                    continue
                data_tables = brcommentscraper.brscraper_in_comments(create_url(player, website, year), table_ids = splits_tables)
            if website == "bvp_url":
                if player_and_position[player][0] == 1:
                    continue
                try:
                    player_id = player_id_lookup[player]
                except:
                    continue
                data_tables.append([scraper.parse_tables(create_url(player, website, year), table_ids = "ajax_result_table"), "ajax_result_table"])
                data_tables[0][0]["ajax_result_table"] = [row for row in data_tables[0][0]["ajax_result_table"] if row["Name"].lower() == "%s"%opposing_team_pitcher[opposing_team[player]]]
                data_tables[0][0]["ajax_result_table"].append("ajax_result_table")
            for data_table in data_tables:
                #some websites have more than one data structure embedded in the data_tables data structure if more than one table called
                for line in data_table[0][data_table[1]]:
                    sql_table_name = None
                    if data_table[1] is not "ajax_result_table":
                        try: print line["Split"]
                        except: 
                            print "%s was skipped at try print line[Split]" %player
                            skipped_players.append(player)
                            continue
                    else:
                        try: print line["Name"]
                        except: 
                            print "%s was skipped at print line[Name]" %player
                            skipped_players.append(player)
                            continue
                    if data_table[1] == "total" or data_table[1] == "total_extra":
                        if line["Split"] == "Last 7 days" and year is max(years):
                            sql_table_name = "{}last7days".format(year)
                        elif line["Split"] == "Last 14 days" and year is max(years):
                            sql_table_name = "{}last14days".format(year)
                        elif line["Split"] == "Last 28 days" and year is max(years):
                            sql_table_name = "{}last28days".format(year)
                        elif line["Split"] == "{} Totals".format(year):
                            sql_table_name = "{}last365days".format(year)
                        else: continue
                    elif data_table[1] == "plato":
                        if handedness[opposing_team_pitcher[opposing_team[player]]] == "R":
                            if line["Split"] == "vs RH Starter":
                                sql_table_name = "{}vsRHStarter".format(year)
                            else: continue
                        
                        elif handedness[opposing_team_pitcher[opposing_team[player]]] == "L":
                            if line["Split"] == "vs LH Starter":
                                sql_table_name = "{}vsLHStarter".format(year)
                            else: continue
                        
                    elif data_table[1] == "hmvis" or data_table[1] == "hmvis_extra":
                        
                        if player_team[player] in home_team:
                            if line["Split"] == "Home":
                                if player_and_position[player][0] == 1:
                                    hmvis_check[player] = hmvis_check.get(player, 0)
                                    if hmvis_check[player] is not 0:
                                        sql_table_name = "{}hmvis".format(year)
                                    else:   
                                        hmvis_check[player] = year
                                        continue
                                else:
                                    sql_table_name = "{}hmvis".format(year)
                            else:
                                continue
                        else:
                            if line["Split"] == "Away":
                                if player_and_position[player][0] == 1:
                                    hmvis_check[player] = hmvis_check.get(player, 0)
                                    if hmvis_check[player] is not 0:
                                        sql_table_name = "{}hmvis".format(year)
                                    else:   
                                        hmvis_check[player] = year
                                        continue
                                else:
                                    sql_table_name = "{}hmvis".format(year)
                            else:
                                continue
                    elif data_table[1] == "ajax_result_table":
                        if year is not min(years):
                            continue
                        if line["Name"].lower() == opposing_team_pitcher[opposing_team[player]]:
                            sql_table_name = "{}bvp".format(year)
                        else: continue
                    else:
                        print "%s was skipped at data_table[1]" %player
                        skipped_players.append(player)

                    if sql_table_name is not None:
                            # if year is not max(years):
                            # if sql_table_name in ["{}last7days".format(year), "{}last14days".format(year), "{}last28days".format(year), "{}last365days".format(year)]:
                                # empty_check = check_data(player, player_and_position[player][0], sql_table_name, year)
                                # if empty_check:
                                    # write_to_database(line, player, sql_table_name, year)
                                    # if "last7days" in sql_table_name or "last14days" in sql_table_name or "last28days" in sql_table_name:
                                        # continue
                                    # else:
                                        # break
                                # else:
                                    # print "%s data already exists"%player
                                    # break
                            # else:
                        try:
                        #0.1 innings pitched means one of three outs was acheived, or 1/3 inning pitched
                            if ".1" in line["IP"]:
                                line["IP"] = line["IP"].replace(".1", ".33")
                            if ".2" in line["IP"]:
                                line["IP"] = line["IP"].replace(".2", ".66")
                        except:
                            pass

                        if "last7days" in sql_table_name or "last14days" in sql_table_name or "last28days" in sql_table_name:
                            if check_data(player, player_and_position[player][0], sql_table_name, year, line):
                                continue
                        write_to_database(line, player, sql_table_name, year)
                        if "{}last365days".format(year) in sql_table_name or "last7days" in sql_table_name or "last14days" in sql_table_name:
                            continue
                        else:
                            break

end_time = time.time()
print('Took %s seconds to calculate.' % (end_time - start_time))


