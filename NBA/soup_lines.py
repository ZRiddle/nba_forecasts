# -*- coding: utf-8 -*-
"""
Beautiful Soup for pulling betting lines
This does not hit the nba endpoint
"""

from bs4 import BeautifulSoup
import urllib2
import pandas as pd
from nba_py.constants import TEAMS
import time
import random


base_url = 'http://www.covers.com/pageLoader/pageLoader.aspx?page=/data/nba/teams/pastresults/{}/team{}.html'

season_all = ['2010-2011','2011-2012','2012-2013','2013-2014','2014-2015','2015-2016']

headers = ['TeamID','SeasonID','Date','Vs','WinLoss','Score','VsScore','GameType',
           'BettingLine','BettingLineResult','OverUnder','OverUnderResult']

data = pd.DataFrame()

start_time = time.time()

for season in season_all[::-1]:
    season_time = time.time()
    seasonid = int(season[:4]+season[-2:])
    print ('\nScraping Season '+season+'...')
    for teamname in TEAMS.keys():
        print ('  Scraping Lines for '+teamname+'...')
        teamid = TEAMS[teamname]['lineid']
    
        # Read Page
        page = urllib2.urlopen(base_url.format(season,teamid))
        soup = BeautifulSoup(page.read())
        lst = soup.find_all('td')
        
        # Every group of 6 is a row
        line_data = []
        
        for i in range(len(lst)/6):
            i = i*6
            if lst[i].string == 'Date':
                team = lst[i+4].string.replace('Line','').replace(' ','')
            
            else:
                row = []
                # Add TeamID
                row.append(TEAMS[teamname]['id'])
                row.append(seasonid)
                
                # Last 8 chars is the date
                row.append(lst[i].string[-8:])
                
                # Add opponent
                row.append(lst[i+1].a.string)
                
                # Add Result and Score
                res = lst[i+2].get_text().replace('(OT)','').replace(' ','').split('\r\n')
                row.append(res[1])
                scores = res[2].split('-')
                row.append(int(scores[0]))
                row.append(int(scores[1]))
                
                # Type of Game
                row.append(lst[i+3].string.replace('\r\n','').replace(' ',''))
                
                # Add Line On Team and W/L for Line
                res = lst[i+4].string.replace('\r\n','').replace(' ','').replace('PK','0')
                row.append(float(res[1:]))
                row.append(res[0])
                
                
                # Add O/U Line and which side won
                res = lst[i+5].string.replace('\r\n','').replace(' ','')
                row.append(float(res[1:]))
                row.append(res[0])
                
                line_data.append(row)
                
                
        # Wait Randomly here to not get flagged by their server
        time.sleep(random.random())      
        temp = pd.DataFrame(line_data,columns=headers)
        data = data.append(temp)
    
    print ('  Writing data for season...')
    data.to_csv('data/line_data_'+str(seasonid)+'.csv',index=False)
    print ('  Season Runtime = %.1f secs'%((time.time()-season_time)))
   

print ('Total Runtime = %.1f mins'%((time.time()-start_time)/60))


