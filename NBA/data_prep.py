# -*- coding: utf-8 -*-
"""
Author: Zach Riddle
"""

import pandas as pd
import numpy as np

seasonid = 201415

playxplay = pd.read_csv('data/PlayByPlay_'+str(seasonid)+'.csv')
passes = pd.read_csv('data/PassesMade_'+str(seasonid)+'.csv')
playergame = pd.read_csv('data/PlayerGameStats_'+str(seasonid)+'.csv')


'''
What do I need?
Ideally sequence of events

Change of Possesion
    Steal
    Inbound
    Def Rebound

Player Control
    P1 - P5

Player action
    Pass
    Shoot
    Turnover
    Off Rebound

Shot event
    Foul -> Free Throw ->
        Last Make
        Last Miss
    Not Fouled ->
        Make
        Miss
    
    Make -> Inbound
    Miss ->
        Def Rebound
        Off Rebound
        
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
    7 - Violation delay of game / kick ball
    8 - Sub
    9 - Timeout
    10 - Jump Ball
    11 - Ejected
    12 - Start of Quarter
    13 - End of Quarter
'''

cols = ['GAME_ID','EVENTNUM','EVENTMSGTYPE','PLAYER1_ID','PLAYER1_NAME',
        'PLAYER1_TEAM_ID','HOMEDESCRIPTION','VISITORDESCRIPTION']

playxplay[playxplay.EVENTMSGTYPE==5][cols].head(20)

playxplay.iloc[75:80][cols]



























