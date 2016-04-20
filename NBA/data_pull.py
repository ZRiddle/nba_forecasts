# -*- coding: utf-8 -*-
"""
API + ETL Script for stats.nba.com

Goal:
    Build model to predict individual player points per game
    Be 55%+ accurate (Over Betting Lines)

Starting datasets
    Players
    Games
    Player stats by game
    


"""

import pandas as pd
import numpy as np
import time

from nba_py import team, player
from nba_py.constants import *

seasons = ['2012-13','2011-12']#,'2015-16']

total_start_time = time.time()

'''
Player Table

Get All Recent Players
Columns:
    PERSON_ID - PK
    DISPLAY_LAST_COMMA_FIRST
    ROSTERSTATUS
    FROM_YEAR
    TO_YEAR
    PLAYERCODE
    TEAM_ID - FK
    GAMES_PLAYED_FLAG
'''


ap = player.PlayerList(season='2015-16',only_current=0)
player_list = ap.info()
#time.sleep(5)

# Filter for only recent players
mask = player_list.TO_YEAR.astype(int) > 2006
player_list = player_list[mask]
player_list.set_index('PERSON_ID',inplace=True)

#print player_list.head()
print ('Total Players Queried =',player_list.shape[0])

'''
Team Table
Create Team Table from Player Table
Columns:
    TEAM_ID - PK
    TEAM_CITY
    TEAM_NAME
    TEAM_ABBREVIATION
    TEAM_CODE
'''

cols = ['TEAM_CITY','TEAM_NAME','TEAM_ABBREVIATION','TEAM_CODE']
team_list = player_list.groupby('TEAM_ID')[cols].max()
#time.sleep(5)

# Fix The Zero Column
team_list.ix[0] = ['-1','-1','-1','-1']

print ('\nUnique Teams Queried =',team_list.shape[0]-1)


# Drop Team Columns from player
player_list.drop(cols,axis=1,inplace=True)


player_list.to_csv('data/PlayerTable.csv')
team_list.to_csv('data/TeamTable.csv')

'''
Game Table

Columns:
    Game_ID - PK, with Home Flag
    Team_ID - FK
    MIN
    FGM
    FGA
    FG_PCT
    FG3M
    FG3A
    FG3_PCT
    FTM
    FTA
    FT_PCT
    OREB
    DREB
    REB
    AST
    STL
    BLK
    TOV
    PF
    PTS
    Home_Flag
    Vs_Team_ID - FK
    Win_Flag
'''


for season in seasons:
    seasonid = int(season[:4]+season[-2:])
    
    #if 1:
    print ('\nQuerying Team Data...')
    
    start_time = time.time()
    
    game_list = pd.DataFrame()
    # Loop through teams here
    for teamid in team_list.index:
        if teamid:
            #error_ct = 0
            #while error_ct < 3:
                #try:
            print ('  Querying '+team_list.ix[teamid].TEAM_NAME+' data...')
            game_temp = team.TeamGameLogs(teamid,season=season).info()
            '''
                    error_ct = 3
                except:
                    print '  Failed!'
                    error_ct += 1
                    time.sleep(1)
            '''
            
            game_temp.set_index('Game_ID',inplace=True)
            
            # Add Home Game flag for unique games
            mask = game_temp.MATCHUP.str.contains('vs.')
            
            game_temp['Home_Flag'] = mask.astype(int)
            
            # Create an 'Vs_Team_ID' column, drop MATCHUP
            game_temp['Vs_Team_ID'] = ''
            for i in game_temp.index:
                team_abrev = game_temp.ix[i]['MATCHUP'][-3:]
                # Pelicans
                if team_abrev == 'NOH':
                    team_abrev = 'NOP'
                # Nets
                if team_abrev == 'NJN':
                    team_abrev = 'BKN'
                    
                game_temp.ix[i,'Vs_Team_ID'] = TEAMS[team_abrev]['id']
            game_temp.drop('MATCHUP',axis=1,inplace=True)
            
            # Convert Game Date
            game_temp.GAME_DATE = pd.to_datetime(game_temp.GAME_DATE)
            
            # Create Win Flag, Drop WL
            game_temp['Win_Flag'] = (game_temp.WL == 'W').astype(int)
            game_temp.drop('WL', axis=1, inplace=True)
            
            game_temp['Season_ID'] = seasonid
            
            game_list = game_list.append(game_temp)
            
            time.sleep(.2)
            
    
    print ('\n  Number of Games Queried =',np.unique(game_list.index).shape[0])
    print ('  Runtime (seconds) = %.2f'%(time.time()-start_time))
    
    
    
    game_list.to_csv('data/GameTable_'+str(seasonid)+'.csv')
    
    
    '''
    Player Game Log Table
    
    This will be useful for building metrics over time
    
    Columns:
        SEASON_ID - PK
        Player_ID - PK
        Game_ID - PK
        WL
        MIN
        FGM
        FGA
        FG_PCT
        FG3M
        FG3A
        FG3_PCT
        FTM
        FTA
        FT_PCT
        OREB
        DREB
        REB
        AST
        STL
        BLK
        TOV
        PF
        PTS
        PLUS_MINUS
    '''
    
    bad_cols = ['MATCHUP','GAME_DATE','VIDEO_AVAILABLE']
    
    
    print ('\nQuerying Player Log Data...')
    start_time = time.time()
    
    playergame = pd.DataFrame()
    
    plyrs = player_list.index
    # Loop through players
    for i in range(plyrs.shape[0]):
        plyr = plyrs[i]
        data = player.PlayerGameLogs(plyr,season=season).info()
        data.drop(bad_cols,axis=1,inplace=True)
        
        playergame = playergame.append(data)
    
       
        if not i%50 and i:
            print ('  %.0f of %.0f Players complete'%(i,plyrs.shape[0]))
    
    print ('  Number of rows Queried =',playergame.shape[0])
    print ('  Runtime (minutes) = %.2f'%((time.time()-start_time)/60))
    
    playergame['SEASON_ID'] = seasonid
    
    playergame.to_csv('data/PlayerGameStats_'+str(seasonid)+'.csv',index=False)




