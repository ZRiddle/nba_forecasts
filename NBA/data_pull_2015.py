# -*- coding: utf-8 -*-
"""
API + ETL Script for stats.nba.com
"""

import pandas as pd
import numpy as np
import time

from nba_py import game, team, player
from nba_py.constants import *

seasons = ['2015-16']#,'2015-16']

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


ap = player.PlayerList(season='2014-15',only_current=0)
player_list = ap.info()
#time.sleep(5)

# Filter for only recent players
mask = player_list.TO_YEAR.astype(int) > 2010
player_list = player_list[mask]
player_list.set_index('PERSON_ID',inplace=True)

#print player_list.head()
print 'Total Players Queried =',player_list.shape[0]

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

print '\nUnique Teams Queried =',team_list.shape[0]-1


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
    print '\nQuerying Team Data...'
    
    start_time = time.time()
    
    game_list = pd.DataFrame()
    # Loop through teams here
    for teamid in team_list.index:
        if teamid:
            #error_ct = 0
            #while error_ct < 3:
                #try:
            print '  Querying '+team_list.ix[teamid].TEAM_NAME+' data...'
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
                game_temp.ix[i,'Vs_Team_ID'] = TEAMS[game_temp.ix[i]['MATCHUP'][-3:]]['id']
            game_temp.drop('MATCHUP',axis=1,inplace=True)
            
            # Convert Game Date
            game_temp.GAME_DATE = pd.to_datetime(game_temp.GAME_DATE)
            
            # Create Win Flag, Drop WL
            game_temp['Win_Flag'] = (game_temp.WL == 'W').astype(int)
            game_temp.drop('WL', axis=1, inplace=True)
            
            game_temp['Season_ID'] = seasonid
            
            game_list = game_list.append(game_temp)
            
            time.sleep(.2)
            
    
    print '  Number of Games Queried =',np.unique(game_list.index).shape[0]
    print '  Runtime (seconds) = %.2f'%(time.time()-start_time)
    
    
    
    game_list.to_csv('data/GameTable_'+str(seasonid)+'.csv')
    
    
    '''
    game_list = pd.read_csv('data/GameTable_201415.csv',
                            dtype = {'Game_ID' : np.str})
    game_list.set_index('Game_ID',inplace=True)
    '''
    
    
    '''
    Play by Play Table
    
    EVENTMSGTYPE
        1 - Make
        2 - Miss
        3 - Free Throw
        4 - Rebound
        5 - Turnover/Steal
        6 - Foul
        7 - Violation - delay of game / kick ball
        8 - Sub
        9 - Timeout
        10 - Jump Ball
        11 - Ejected
        12 - Start of Quarter
        13 - End of Quarter
    
    Columns:
        GAME_ID
        EVENTNUM
        EVENTMSGTYPE
        EVENTMSFACTIONTYPE
        PERIOD
        WCTIMESTRING
        PCTIMESTRING
        HOMEDESCRIPTION
        NEUTRALDESCRIPTION
        VISITORDESCRIPTION
        SCORE
        SCOREMARGIN
        PERSON1TYPE
        PLAYER1_ID
        PLAYER1_NAME
        PLAYER1_TEAM_ID
        PLAYER1_TEAM_CITY
        PLAYER1_TEAM_NICKNAME
        PLAYER1_TEAM_ABBREVIATION
        PERSON2TYPE
        PLAYER2_ID
        PLAYER2_NAME
        PLAYER2_TEAM_ID
        PLAYER2_TEAM_CITY
        PLAYER2_TEAM_NICKNAME
        PLAYER2_TEAM_ABBREVIATION
        PERSON3TYPE
        PLAYER3_ID
        PLAYER3_NAME
        PLAYER3_TEAM_ID
        PLAYER3_TEAM_CITY
        PLAYER3_TEAM_NICKNAME
        PLAYER3_TEAM_ABBREVIATION
    '''
    
    print '\nQuerying Play-by-Play Data...'
    
    good_cols = ['Season_ID','GAME_ID','EVENTNUM','EVENTMSGTYPE',
                 'PLAYER1_ID','PLAYER2_ID','PLAYER3_ID','PLAYER1_TEAM_ID']
    
    shot_cols = ['GAME_ID','GAME_EVENT_ID','TEAM_ID','SHOT_DISTANCE','LOC_X','LOC_Y',
                 'SHOT_ATTEMPTED_FLAG','SHOT_MADE_FLAG']
    
    start_time = time.time()
    
    u_games = np.unique(game_list.index)
    
    play_by_play = pd.DataFrame()
    shot_chart_list = pd.DataFrame()
    lineup_list = pd.DataFrame(columns=('P1','P2','P3','P4','P5'))
    groupid_index = 0
    
    n_games_save = 123
    
    for i in range(u_games.shape[0]):
        gameid = u_games[i]
        pbp = game.PlayByPlay(gameid).info()
        
        pbp['Season_ID'] = seasonid
        
        play_by_play = play_by_play.append(pbp[good_cols])
    
        teams = game_list.ix[gameid].Team_ID    
        
        for teamid in teams:
            # Get the lineups for the game
            groupids = team.TeamLineups(teamid,gameid,season=season).lineups().GROUP_ID
            
            # Loop through Group IDs
            for groupid in groupids:
                group_str = groupid.replace(' ','').split('-')
                lineup_list.loc[groupid_index] = group_str
                
                # Shot Chart Detail
                shotchart = player.PlayerShotChartLineupDetail(gameid,groupid,season=season).overall()
                shotchart = shotchart[shot_cols]
                shotchart['GROUP_ID'] = groupid_index
                
                shot_chart_list = shot_chart_list.append(shotchart)
                
                groupid_index += 1
        
        if not i%5 and i:
            print '  %.0f of %.0f games complete'%(i,u_games.shape[0])
        
        # Every 100 games, save data
        if not (i+1)%n_games_save and i and shot_chart_list.shape[0] > 200:
            print '  Saving data up to '+str(i)+'...'
            play_by_play.to_csv('data/PlayByPlay_'+str(seasonid)+'_'+str(i/n_games_save)+'.csv',index=False)
            lineup_list.to_csv('data/Lineups_'+str(seasonid)+'_'+str(i/n_games_save)+'.csv')
            shot_chart_list.to_csv('data/ShotChart_'+str(seasonid)+'_'+str(i/n_games_save)+'.csv',index=False)
            
            play_by_play = pd.DataFrame()
            shot_chart_list = pd.DataFrame()
            lineup_list = pd.DataFrame(columns=('P1','P2','P3','P4','P5'))
            
            
            
    
    print '  Number of Plays =',play_by_play.shape[0]
    print '  Number of GroupIDs =',lineup_list.shape[0]
    print '  Number of Shots =',shot_chart_list.shape[0]
    print '  Runtime (minutes) = %.2f'%((time.time()-start_time)/60)
    
    play_by_play.to_csv('data/PlayByPlay_'+str(seasonid)+'.csv',index=False)
    lineup_list.to_csv('data/Lineups_'+str(seasonid)+'.csv')
    shot_chart_list.to_csv('data/ShotChart_'+str(seasonid)+'.csv',index=False)
    
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
    
    
    print '\nQuerying Player Log Data...'
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
            print '  %.0f of %.0f Players complete'%(i,plyrs.shape[0])
    
    print '  Number of Rebounds Queried =',playergame.shape[0]
    print '  Runtime (minutes) = %.2f'%((time.time()-start_time)/60)
    
    playergame['SEASON_ID'] = seasonid
    
    playergame.to_csv('data/PlayerGameStats_'+str(seasonid)+'.csv',index=False)
    
    
    
    
    '''
    Passes Table
    
    Create Team Table from Player Table
    Columns:
        PLAYER_ID - PK
        TEAM_CITY
        TEAM_NAME
        TEAM_ABBREVIATION
        TEAM_CODE
    '''
    
    
    bad_cols = ['PLAYER_NAME_LAST_FIRST','TEAM_NAME','FGM','FGA','FG_PCT',
                'FG2M','FG2A','FG2_PCT','FG3M','FG3A','FG3_PCT','G','AST']
    
    
    print '\nQuerying Player Pass Data...'
    start_time = time.time()
    
    passes_made = pd.DataFrame()
    plyrs = player_list.index
    
    for i in range(plyrs.shape[0]):
        plyr = plyrs[i]
        passes = player.PlayerPassTracking(plyr,season=season).passes_made()
        
        passes_made = passes_made.append(passes.drop(bad_cols,axis=1))
    
        if not i%50 and i:
            print '  %.0f of %.0f Players complete'%(i,plyrs.shape[0])
            
    print '  Number of Passes Rows =',playergame.shape[0]
    print '  Runtime (minutes) = %.2f'%((time.time()-start_time)/60)
    
    passes_made['Season_ID'] = seasonid
    
    passes_made.to_csv('data/PassesMade_'+str(seasonid)+'.csv',index=False)
    
    
    print '\nRun Complete!\nTotal Runtime (minutes) = %.2f'%((time.time()-total_start_time)/60)
    
    









'''
# TODO - SHOTS ~~~ THIS IS VERY USEFUL
data = player.PlayerShotChartDetail(203900,'0021401206',season='2014-15')


groupids = ['202399 - 202688 - 203114 - 2585 - 201196',
            '202399 - 203507 - 203089 - 202688 - 202874',
            '202399 - 203089 - 202688 - 203114 - 201196',
            '203507 - 203089 - 202688 - 203114 - 202874',
            '202399 - 203507 - 203089 - 202688 - 203114',
            '203507 - 203089 - 203114 - 2585 - 201196',
            '203089 - 202688 - 203114 - 2585 - 201196',
            '203507 - 203089 - 203114 - 201196 - 202874']

player.PlayerShotChartLineupDetail(gameid,'203507 - 203089 - 2585 - 201196 - 202874',season=season).overall().GAME_EVENT_ID

plyr = '202399'
season = '2013-14'
teamid = '1610612749'
gameid = '0021301217'
groupid = '203544 - 201960 - 2594 - 203118 - 201952'
eventid = 2

# Loop Through Seasons
# Loop Through Teams
# Loop Through Games


# Loop through lineups and get shot detail
groupid = lineups.GROUP_ID.iloc[0]
shotchart = player.PlayerShotChartLineupDetail(gameid,groupids[0],season=season).overall()
pxp = game.PlayByPlay(gameid).info()
pxp.set_index('EVENTNUM',inplace=True)


rbnd = player.PlayerReboundTracking(plyr,season=season)

# Broken
#event_data = player.LocationGetMoments(gameid,eventid)
#rbnd2 = player.PlayerReboundLogTracking(plyr)


event_ids = shotchart['GAME_EVENT_ID']




# From Shot data... I know eventid, lineup, location, make/miss
# From Play by Play, I know the eventid, closest defender, rebound

# The fck is 'Pace'??? ... Possessions = (PACE*MIN*2) / (48*5) ???


from matplotlib import pyplot as plt

cols = ['WinFlag','BeatSpread','OverUnder']

# Add columns
betting_data['WinFlag'] = 0
betting_data['BeatSpread'] = 0
betting_data['OverUnder'] = 0

# Set wins to 1
betting_data.loc[betting_data.WinLoss == 'W','WinFlag'] = 1
betting_data.loc[betting_data.BettingLineResult == 'W','BeatSpread'] = 1
betting_data.loc[betting_data.OverUnderResult == 'O','OverUnder'] = 1

# Group by and means
grp = betting_data[mask].groupby('TeamID')[cols].mean()

# Combine for team name, and set index to name
grp = pd.DataFrame(grp).join(team_list,how='inner')
grp.set_index('TEAM_NAME',inplace=True)

grp.sort_values('BeatSpread',inplace=True)

plt.figure('Best Bet',figsize=(17,11))
grp[cols].plot(kind='bar')

plt.axhline(.5,color='black')

#plt.legend()
plt.tight_layout()

'''








