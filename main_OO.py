import urllib
from BeautifulSoup import *
from abc import abstractmethod
import brscraper
import sqlite3


class Player:

    def __init__(self, name, player_id, handedness, lineup_spot, team, dksalary, home, opposing_team, years = ['2015', '2016', '2017']):
        self.name = name
        self.player_id = player_id
        self.handedness = handedness
        selflineup_spot = lineup_spot
        self.team = team
        self.dksalary = dksalary
        self.home = home
        self.opposing_team = opposing_team
        self.opposing_pitcher = None
        self.years = years
        self.dkscore = 0
        self.fdscore = 0
        self.dkdivisor = 0
        self.fddivisor = 0
        self.stats_from_database = None
        self.table_factor = {"{}last7days".format(year): 0.10, "{}last14days".format(year): 0.15, "{}last28days".format(year): 0.20, "{}totals".format(year): 0.25, "{}vsRHStarter".format(year): 0.10, "{}vsLHStarter".format(year): 0.10, "{}home".format(year): 0.10, "{}vis".format(year): 0.10} 

    @abstractmethod
    def updateSplits(self):
        pass

    @abstractmethod
    def updateBVP(self):
        pass        
                
    @abstractmethod
    def getStats(self):
        pass
    
    @abstractmethod
    def calcDKScore(self):
        pass
        
    @abstractmethod
    def calcFDScore(self):
        pass
     
    @abstractmethod
    def checkStats(self):
        pass
    
    @abstractmethod
    def createURL(self, year, website):
        pass

class Batter(Player):

    splits_tables = ["total", "plato", "hmvis"]

    def updateSplits(self):
        for year in self.years:
            data_tables = []
            data_tables = scraper.brscraper_in_comments(self.createURL(year, splits_url), table_ids = self.splits_tables)
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
                        if self.opposing_pitcher.handedness == "R":
                            if table_row["Split"] == "vs RH Starter":
                                sql_table_name = "{}vsRHStarter".format(year)
                            else: 
                                continue
                        
                        elif self.opposing_pitcher.handedness == "L":
                            if table_row["Split"] == "vs LH Starter":
                                sql_table_name = "{}vsLHStarter".format(year)
                            else: 
                                continue
                        
                    elif data_table[1] == "hmvis":
                        if year is not max(self.years):
                            continue
                        if self.home:
                            if table_row["Split"] == "Home":
                                sql_table_name = "{}home".format(year)
                            else:
                                continue
                        else:
                            if table_row["Split"] == "Away":
                                sql_table_name = "{}vis".format(year)
                            else:
                                continue
                    else:
                        print "%s was skipped at data_table[1]" %self.name
                        skipped_players.append(self.name)
                    print sql_table_name
                    if sql_table_name and data_table:
                        with sqlite3.connect('%s'%year + str(self) + '.sqlite') as conn:
                            cur = conn.cursor()
                            cur.execute("""CREATE TABLE IF NOT EXISTS '%s'
                                (player_name TEXT UNIQUE, 
                                PA INTEGER, H INTEGER, DOUBLE INTEGER, TRIPLE INTEGER, HR INTEGER, 
                                RBI INTEGER, R INTEGER, BB INTEGER, HBP INTEGER, Steals INTEGER);""" %sql_table_name)
                            cur.execute('''SELECT * FROM '%s' WHERE player_name=?'''%sql_table_name, (self.name,))
                            existing_data = cur.fetchall()
                            if existing_data:
                                update= '''UPDATE '%s' ''' %sql_table_name
                                set =  '''SET PA = ?, H = ?, DOUBLE = ?, TRIPLE = ?, HR = ?, RBI = ?, R = ?, BB = ?, HBP = ?, Steals = ?
                                                    WHERE player_name = ?'''
                                cur.execute(update+set, (table_row["PA"], table_row["H"], table_row["2B"], table_row["3B"], 
                                            table_row["HR"], table_row["RBI"], table_row["R"], 
                                            table_row["BB"], table_row["HBP"], table_row["SB"], self.name))
                            else:
                                cur.execute('''INSERT INTO '%s' (player_name,
                                        PA, H, DOUBLE, TRIPLE, HR, RBI, R, BB, HBP, Steals)
                                        VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''%sql_table_name, (self.name, 
                                        table_row["PA"], table_row["H"], table_row["2B"], table_row["3B"], 
                                        table_row["HR"], table_row["RBI"], table_row["R"], 
                                        table_row["BB"], table_row["HBP"], table_row["SB"]))
                            conn.commit()

    def updateBVP(self):
        data_tables = []
        data_tables.append([scraper.parse_tables(self.createURL(year, "bvp_ur;"), table_ids = "batter_vs_pitcher_1"), "batter_vs_pitcher_1"])
        for data_table in data_tables[0][0]["batter_vs_pitcher_1"]:
            if data_table[''] == "RegSeason":
                data_tables[0][0]["batter_vs_pitcher_1"] = [1]
                data_tables[0][0]["batter_vs_pitcher_1"][0] = data_table
        for table_row in data_tables[0][0][data_tables[0][1]]:
            sql_table_name = "{}bvp".format(year)
            if sql_table_name and table_row:
                with sqlite3.connect('%s'%year + str(self) + '.sqlite') as conn:
                    cur = conn.cursor()
                    cur.execute("""CREATE TABLE IF NOT EXISTS '%s'
                        (player_name TEXT UNIQUE, 
                        PA INTEGER, H INTEGER, DOUBLE INTEGER, TRIPLE INTEGER, HR INTEGER, 
                        RBI INTEGER, R INTEGER, BB INTEGER, HBP INTEGER, Steals INTEGER);""" %sql_table_name)
                    cur.execute('''SELECT * FROM '%s' WHERE player_name=?'''%sql_table_name, (self.name,))
                    existing_data = cur.fetchall()
                    if existing_data:
                        update= '''UPDATE '%s' ''' %sql_table_name
                        set =  '''SET PA = ?, H = ?, DOUBLE = ?, TRIPLE = ?, HR = ?, RBI = ?, BB = ?, HBP = ?
                                            WHERE player_name = ?'''
                        cur.execute(update+set, (table_row["PA"], table_row["H"], table_row["2B"], table_row["3B"], 
                                    table_row["HR"], table_row["RBI"], 
                                    table_row["BB"], table_row["HBP"], self.name))                
                    else:
                        cur.execute('''INSERT INTO '%s' (player_name,
                                PA, H, DOUBLE, TRIPLE, HR, RBI, BB, HBP)
                                VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?)'''%sql_table_name, (self.name, 
                                table_row["PA"], table_row["H"], table_row["2B"], table_row["3B"], 
                                table_row["HR"], table_row["RBI"], table_row["BB"], table_row["HBP"]) )
                    conn.commit()

    
    def getStats(self, year, sql_table):
        with sqlite3.connect('%s'%year + str(self) + '.sqlite') as conn:
            cur = conn.cursor()
            cur.execute('''SELECT PA, H, DOUBLE, TRIPLE, HR, RBI, R, BB, HBP, Steals FROM '%s' WHERE player_name=?'''%sql_table, (self.name,))
            self.stats_from_database = cur2.fetchall()


    def calcDKScore(self, year, sql_table):
        if stats_from_database[0][0] < 10:
            continue
        if sql_table == "{}bvp".format(year):
            self.dkscore += float(3.0*ballpark_data[self.name]["Singles"]*float(stats_from_database[0][1]-stats_from_database[0][2]-stats_from_database[0][3]-stats_from_database[0][4])
                    +5.0*ballpark_data[self.name]["Doubles"]*stats_from_database[0][2]
                    +8.0*ballpark_data[self.name]["Triples"]*stats_from_database[0][3]
                    +10.0*ballpark_data[self.name]["HR"]*stats_from_database[0][4]
                    +2.0*stats_from_database[0][5]
                    +2.0*float(stats_from_database[0][7]+stats_from_database[0][8]))/stats_from_database[0][0]*0.10*lineup_factor[player_lineup_spot[self.name]]
            self.dkdivisor += 0.10

        else:
            self.dkscore += float(3.0*ballpark_data[self.name]["Singles"]*float(stats_from_database[0][1]-stats_from_database[0][2]-stats_from_database[0][3]-stats_from_database[0][4])
                    +5.0*ballpark_data[self.name]["Doubles"]*stats_from_database[0][2]
                    +8.0*ballpark_data[self.name]["Triples"]*stats_from_database[0][3]
                    +10.0*ballpark_data[self.name]["HR"]*stats_from_database[0][4]
                    +2.0*stats_from_database[0][5]
                    +2.0*ballpark_data[self.name]["R"]*stats_from_database[0][6]
                    +2.0*float(stats_from_database[0][7]+stats_from_database[0][8])
                    +5.0*stats_from_database[0][9])/stats_from_database[0][0]*self.table_factor[sql_table]*lineup_factor[player_lineup_spot[self.name]]
            self.dkdivisor += self.table_factor[sql_table] 
    
    def calcFDScore(self):
        if stats_from_database[0][0] < 10:
            continue
        if sql_table == "{}bvp".format(year):
            self.fdscore += float(3.0*ballpark_data[player_full_name]["Singles"]*float(stats_from_database[0][1]-stats_from_database[0][2]-stats_from_database[0][3]-stats_from_database[0][4])
                                        +6.0*ballpark_data[player_full_name]["Doubles"]*stats_from_database[0][2]
                                        +9.0*ballpark_data[player_full_name]["Triples"]*stats_from_database[0][3]
                                        +12.0*ballpark_data[player_full_name]["HR"]*stats_from_database[0][4]
                                        +3.5*stats_from_database[0][5]
                                        +3.0*float(stats_from_database[0][7]+stats_from_database[0][8]))/stats_from_database[0][0]*0.10*lineup_factor[player_lineup_spot[player_full_name]]
            self.fddivisor += 0.10
        
        
        else:
            self.fdscore += float(3.0*ballpark_data[player_full_name]["Singles"]*float(stats_from_database[0][1]-stats_from_database[0][2]-stats_from_database[0][3]-stats_from_database[0][4])
                                        +6.0*ballpark_data[player_full_name]["Doubles"]*stats_from_database[0][2]
                                        +9.0*ballpark_data[player_full_name]["Triples"]*stats_from_database[0][3]
                                        +12.0*ballpark_data[player_full_name]["HR"]*stats_from_database[0][4]
                                        +3.5*stats_from_database[0][5]
                                        +3.2*ballpark_data[player_full_name]["R"]*stats_from_database[0][6]
                                        +3.0*float(stats_from_database[0][7]+stats_from_database[0][8])
                                        +6.0*stats_from_database[0][9])/stats_from_database[0][0]*self.table_factor[sql_table]*lineup_factor[player_lineup_spot[player_full_name]]
            self.fddivisor += self.table_factor[sql_table] 
            
    def createURL(self, year, website):
        if website == "splits_url":
            getVars = {'id' : '%s' % self.player_id, 'year' : '%s' % year, 't' : 'b'}
            split_url = 'players/split.fcgi?'
            return split_url + urllib.urlencode(getVars)
    
        if website == "bvp_url":
            bvp_url = "play-index/batter_vs_pitcher.cgi?batter="
            return bvp_url + self.player_id + '&pitcher=' + self.opposing_pitcher.player_id
    

class Catcher(Batter):
    
    position_id = 2
    
    def calcScore(self):
        
        print "write to SQL for batter " + self.name
    
    def __str__(self):
        return "catcher"
        
class FirstBaseman(Batter):

    position_id = 3
    
    def calcScore(self):
        pass
        print "write to SQL for batter " + self.name

        
    def __str__(self):
        return "firstbaseman"
        
class SecondBaseman(Batter):
    
    position_id = 4
    
    def calcScore(self):
        pass
        print "write to SQL for batter " + self.name
    def __str__(self):
        return "secondbaseman"
class ThirdBaseman(Batter):
    
    position_id = 5
    
    def calcScore(self):
        pass
        print "write to SQL for batter " + self.name

    def __str__(self):
        return "thirdbaseman"
class Shortstop(Batter):
    
    position_id = 6
    
    def calcScore(self):
        pass
        print "write to SQL for batter " + self.name
        
    def __str__(self):
        return "shortstop"
        
class Outfielder(Batter):
    
    position_id = 7
    
    def calcScore(self):
        pass
        print "write to SQL for batter " + self.name
      
    def __str__(self):
        return "outfielder"
        
class Pitcher(Player):
    
    splits_tables = ["total_extra", "hmvis_extra"]
    
    def updateSplits(self):
        for year in self.years:
            data_tables = []
            data_tables = scraper.brscraper_in_comments(self.createURL(year), table_ids = self.splits_tables)
            for data_table in data_tables:
                for table_row in data_table[0][data_table[1]]:
                    sql_table_name = None
                    if data_table[1] == "total_extra":
                        if table_row["Split"] == "Last 7 days":
                            sql_table_name = "{}last7days".format(year)
                        elif table_row["Split"] == "Last 14 days":
                            sql_table_name = "{}last14days".format(year)
                        elif table_row["Split"] == "Last 28 days":
                            sql_table_name = "{}last28days".format(year)
                        elif table_row["Split"] == "{} Totals".format(year):
                            sql_table_name = "{}totals".format(year)
                        else: continue
                    elif data_table[1] == "hmvis_extra":
                        if year is not max(self.years):
                            continue
                        if self.home:
                            if table_row["Split"] == "Home":
                                sql_table_name = "{}hmvis".format(year)
                            else:
                                continue
                        else:
                            if table_row["Split"] == "Away":
                                sql_table_name = "{}hmvis".format(year)
                            else:
                                continue
                    else:
                        print "%s was skipped at data_table[1]" %self.name
                        skipped_players.append(self.name)
                    print sql_table_name
                    if sql_table_name and data_table:
                        try:
                            if ".1" in table_row["IP"]:
                                table_row["IP"] = table_row["IP"].replace(".1", ".33")
                            if ".2" in table_row["IP"]:
                                table_row["IP"] = table_row["IP"].replace(".2", ".66")
                        except:
                            pass
                        with sqlite3.connect('%s'%year + 'pitcher.sqlite') as conn:
                            cur = conn.cursor()
                            cur.execute("""CREATE TABLE IF NOT EXISTS '%s'
                                (player_name TEXT UNIQUE, innings_pitched SINGLE, games_started INTEGER,
                                K SINGLE, W INTEGER, ER INTEGER, WHIP SINGLE);""" %sql_table_name)
                            cur.execute('''SELECT * FROM '%s' WHERE player_name=?'''%sql_table_name, (self.name,))
                            existing_data = cur.fetchall()
                            if existing_data:
                                cur.execute('''UPDATE '%s' 
                                            SET innings_pitched = ?, games_started = ?, K = ?, W = ?, ER = ?, WHIP = ?
                                            WHERE player_name = ?'''%sql_table_name, 
                                            (table_row["IP"], table_row["GS"], table_row["SO"], table_row["W"], table_row["ER"], table_row["WHIP"], self.name))
                            else:
                                cur.execute('''INSERT INTO '%s' (player_name,
                                            innings_pitched, games_started, K, W, ER, WHIP)
                                            VALUES ( ?, ?, ?, ?, ?, ?, ?)'''%sql_table_name, (self.name,
                                            table_row["IP"] ,table_row["GS"] ,table_row["SO"] ,table_row["W"] ,
                                            table_row["ER"] ,table_row["WHIP"]))
                            conn.commit()

    
    def updateBVP(self):
        pass
    
    def getStats(self, year, sql_table):
        while sqlite3.connect('%s'%year + 'pitcher.sqlite') as conn:
            cur= conn.cursor()
            cur.execute('''SELECT innings_pitched, games_started, K, W, ER, WHIP FROM '%s' WHERE player_name=?'''%sql_table, (player_full_name,))
            stats_from_database = cur2.fetchall()

    def calcDKScore(self, year, sql_table):
        if innings_pitched < 4:
            continue
        dkscore += float(2.25*stats_from_database[0][0]+2.0*stats_from_database[0][2]+4.0*stats_from_database[0][3]
            -2.0*stats_from_database[0][4]-0.6*stats_from_database[0][0]*stats_from_database[0][5])/stats_from_database[0][1]*self.table_factor[sql_table]
        dkdivisor += self.table_factor[sql_table]
        
    def calcFDScore(self, year, sql_table):
        if innings_pitched < 4:
            continue
        self.fdscore += float(3.0*stats_from_database[0][0]
                            +3.0*stats_from_database[0][2]
                            +6.0*stats_from_database[0][3]
                            -3.0*stats_from_database[0][4])/stats_from_database[0][1] * self.table_factor[sql_table]
        self.fddivisor += self.table_factor[sql_table]
        
    def createURL(self, year):
        getVars = {'id' : '%s' % self.player_id, 'year' : '%s' % year, 't' : 'p'}
        split_url = 'players/split.fcgi?'
        return split_url + urllib.urlencode(getVars)

    def __str__(self):
        return "pitcher"

def daily_starters(object):

    call_starters_url = urllib.urlopen(object)
    starters_website = call_starters_url.read()    
    soup = BeautifulSoup(starters_website)
    call_starters_url.close()
    for a in soup.findAll('a'):

        check_player_name = a.get("data-bref")
        if check_player_name:
            player = a.text.lower()
            if "." in player:
                player = player.replace(".", "")
            player_info[player] = []
            player_id = a.get("data-bref")
            if player_id:
                player_info[player].append(player_id)
                player_info[player].append(a.parent.text.split("(")[1][0])
            if "(L) " in a.parent.text or "(R) " in a.parent.text or "(S) " in a.parent.text:
                player_info[player].append(a.parent.text.split(".")[0])
            else:
                player_info[player].append(0) 

def extract_dk_info(object):
    import csv

    with open(object, 'rb') as csvfile:
        file = csv.reader(csvfile)
        for row in file:
            player_full_name = str(row[1]).lower()
            if "." in player_full_name:
                player_full_name = player_full_name.replace(".", "")
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
            
            if player_info.has_key(player_full_name):
                if row[3] == "Postponed":
                    continue
                player_info[player_full_name].append(row[5])
                player_info[player_full_name].append(row[0].split('/'))
                player_info[player_full_name].append(row[2])
                teams = row[3].split("@")
                home_team = teams[1]
                home_team_clean = home_team[:3].strip()
                if home_team_clean == row[5]:
                    player_info[player_full_name].append(True)
                    player_info[player_full_name].append(teams[0])
                else:
                    player_info[player_full_name].append(False)
                    player_info[player_full_name].append(home_team_clean)
            else: continue

# def extract_fd_info(object):
        # import csv

        # global fd_player_and_salaries

        # with open(object, 'rb') as csvfile:
            # file = csv.reader(csvfile)
            # for row in file:
                # player_full_name = str(row[3]).lower()
                # if player_full_name == 'nickname':
                    # continue
                # #placeholder for dealing with tough characters (accents, etc)
                # if '\xed' in player_full_name:
                    # player_full_name = player_full_name.replace('\xed', 'i')
                # if '\xc1' in player_full_name:
                    # player_full_name = player_full_name.replace('\xc1', 'a')
                # if '\xe9' in player_full_name:
                    # player_full_name = player_full_name.replace('\xe9', 'e')
                # if '\xf3' in player_full_name:
                    # player_full_name = player_full_name.replace('\xf3', 'o')
                # if '\xfa' in player_full_name:
                    # player_full_name = player_full_name.replace('\xfa', 'u')
                # if '\xf1' in player_full_name:
                    # player_full_name = player_full_name.replace('\xf1', 'n')
                # if '\xe1' in player_full_name:
                    # player_full_name = player_full_name.replace('\xe1', 'a')

                # #attempt to deal with tough characters, will come back to it later
                
                # # player_full_name = column[1].decode('utf-8', "replace")
                # # remove_accents(player_full_name)
                # # player_full_name = unicodedata.normalize('NFD', unicode(column[1], "utf8")).encode('ascii', 'ignore')
                # # player_full_name = PyUnicode_DecodeUTF32(column[1])
                # # player_full_name = ''.join((c for c in unicodedata.normalize('NFD', player_full_name) if unicodedata.category(c) != 'Mn'))
                # # player_full_name = unidecode(player_full_name)
                
                # if player_full_name in starting_players or player_full_name in starting_pitcher:
                    # fd_salary = str(row[7])
                    # fd_salary = int(fd_salary)
                    # fd_player_and_salaries[player_full_name] = fd_salary

            
def constructPlayerList:
    starters_url = 'http://www.baseballpress.com/lineups/2017-06-30'
    daily_starters(starters_url)
    csv_from_draftkings = 'DKSalaries.csv'
    extract_dk_info(csv_from_draftkings)
    for player, info in player_info.iteritems():
    # build a position to list of players map
    # A position is a key to a list of players that play that position
        if len(info) > 3:
            if "SP" in info[4] or "RP" in info[4]:
                new_player = Pitcher(player, info[0],info[1],info[2],info[3], info[5], info[6], info[7])
                if pitchers_hitters.get("Pitcher") is None:
                    pitchers_hitters["Pitcher"] = []
                pitchers_hitters["Pitcher"].append(new_player)
            else:
                for position in info[4]:
                    if position == "C":
                        new_player = Catcher(player, info[0],info[1],info[2],info[3], info[5], info[6], info[7])
                    if position == "1B":
                        new_player = FirstBaseman(player, info[0],info[1],info[2],info[3], info[5], info[6], info[7])
                    if position =="2B":
                        new_player = SecondBaseman(player, info[0],info[1],info[2],info[3], info[5], info[6], info[7])
                    if position == "SS":
                        new_player = Shortstop(player, info[0],info[1],info[2],info[3], info[5], info[6], info[7])
                    if position == "3B":
                        new_player = ThirdBaseman(player, info[0],info[1],info[2],info[3], info[5], info[6], info[7])
                    if position == "OF":
                        new_player = Outfielder(player, info[0],info[1],info[2],info[3], info[5], info[6], info[7])
                    if pitchers_hitters.get(position) is None:
                        pitchers_hitters[position] = []
                    pitchers_hitters[position].append(new_player)
            player_list.append(new_player)
        
if __name__ == '__main__':
    #key = player name, value = [player_id, handedness, lineup_spot, player_team, position, dksalary, home/away boolean, opposing team]
    player_info = {}
    pitchers_hitters = {}
    player_list = []
    scraper = brscraper.BRScraper() 
    constructPlayerList()
    for player in player_list:
        if str(player) is not "pitcher":
            for pitcher in pitchers_hitters["Pitcher"]:
                if pitcher.team == player.opposing_team:
                    player.opposing_pitcher = pitcher
        player.updateSplits()
