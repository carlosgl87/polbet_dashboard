import pandas as pd
import streamlit as st
import numpy as np
from pymongo import MongoClient
import ssl
from bson.objectid import ObjectId
from datetime import date, timedelta

######################
######## Data ########
######################

CONNECTION_STRING = "mongodb+srv://carlos:carlos123@cluster0.cdx6k.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
client = MongoClient(CONNECTION_STRING, ssl_cert_reqs=ssl.CERT_NONE)
db = client['betpo-bel']

## Bets
collectionBets = db['bets']
cursorBets = collectionBets.find({})
resultBets = list(cursorBets)
dfBets = pd.DataFrame(resultBets)

## Contests
collectionContests = db['contests']
cursorContests = collectionContests.find({})
resultContests = list(cursorContests)
dfContests = pd.DataFrame(resultContests)

## Users
collectionUsers = db['users']
cursorUsers = collectionUsers.find({})
resultUsers = list(cursorUsers)
dfUsers = pd.DataFrame(resultUsers)

dfBets['contests'] = 1
dfBets['amount'] = pd.to_numeric(dfBets['amount'])
dfBets_users = dfBets.groupby('userId').agg({'createdAt':'last','amount':'sum','contests':'sum'}).reset_index()
dfBets_users.rename(columns={'amount': 'amount_bets', 'contests': 'contests_bets','createdAt':'createdAt_last_bet'}, inplace=True)
df_temp = pd.merge(dfUsers,dfBets_users,how='left',left_on='_id',right_on='userId')

## Main KPIS
num_users = len(dfUsers[dfUsers['role']=='user'])
num_users_active = len(df_temp[df_temp['amount_bets']>0])
total_amount_bets = df_temp['amount_bets'].sum()

## Secondary KPIS
total_events = len(dfContests)
total_events_active = len(dfContests[dfContests['isContestOpenStatus']==True])
users_events_active = len(dfBets[dfBets['contestId'].isin(list(dfContests[dfContests['isContestOpenStatus']==True]['_id']))]['userId'].unique())
number_bets_events_active = len(dfBets[dfBets['contestId'].isin(list(dfContests[dfContests['isContestOpenStatus']==True]['_id']))])
amount_bets_events_active = dfBets[dfBets['contestId'].isin(list(dfContests[dfContests['isContestOpenStatus']==True]['_id']))]['amount'].sum()

df_events_active = pd.DataFrame(columns=['Evento','Numero Apuestas','Monto Apuestas'])
cont = 0
for index, row in dfContests[dfContests['isContestOpenStatus']==True].iterrows():
    name_event = row['name']
    number_bets = len(dfBets[dfBets['contestId']==ObjectId(row['_id'])])
    amount_bets = dfBets[dfBets['contestId']==ObjectId(row['_id'])]['amount'].sum()
    df_events_active.loc[cont] = [name_event,number_bets,amount_bets]
    cont = cont + 1


df_temp_1 = df_temp[df_temp['amount_bets']>0][['email','amount_bets','contests_bets']]
df_temp_1.rename(columns={'email': 'Usuario', 'amount_bets': 'Monto Apostado','contests_bets':'Apuestas'}, inplace=True)
df_temp_1 = df_temp_1.sort_values('Monto Apostado', ascending=False).reset_index(drop=True)


######################
######## Page ########
######################

st.set_page_config(
    page_title = 'PolBet Dashboard',
    page_icon = 'polbet.png',
    layout = 'wide'
)


## First Rows KPIs
st.markdown("## Principales KPIs")
col1, col2, col3 = st.columns(3)
#col1.metric("Numero Usuarios", num_users, "+1%")
col1.metric("Numero Usuarios", num_users)
col2.metric("Numero Usuarios Activos", num_users_active)
col3.metric("Total Monto Apostado", total_amount_bets)

## Second Rows KPIs

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Eventos Totales", total_events)
col2.metric("Eventos Activos", total_events_active)
col3.metric("Usuarios Eventos Activos", users_events_active)
col4.metric("Numero Apuestas Eventos Activos", number_bets_events_active)
col5.metric("Monto Apuestas Eventos Activos", int(amount_bets_events_active))

