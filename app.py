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
col5.metric("Monto Apuestas Eventos Activos", int(amount_bets_events_active)
)

## Tabla Eventos Usuarios y Montos
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Eventos Activos")
df_events_active = df_events_active.sort_values('Monto Apuestas', ascending=False).reset_index(drop=True)
st.dataframe(df_events_active)

## Tabla Usuarios Principales
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Usuarios Principales")
st.dataframe(df_temp_1.style.format({"Monto Apostado": "{:.1f}", "Apuestas": "{:.0f}"}))

## Grafico evolucion apuestas por evento activo
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Evolucion apuestas eventos activos ultimos 10 dias")
option_event = st.selectbox(
    'Evento Activo',
    list(df_events_active['Evento']))
st.write('Evolucion montos apuestas: ', option_event)

id_event = dfContests[dfContests['name']==option_event]['_id'].reset_index(drop=True)[0]
dfBets['day'] = dfBets['createdAt'].dt.round("D")
today = date.today()
Dateslist = [today - timedelta(days = day) for day in range(10)]
df_days_contests = pd.DataFrame(Dateslist,columns=['day'])
df_days_contests['day'] = pd.to_datetime(df_days_contests['day'])
df_days_contests = pd.merge(df_days_contests,dfBets[dfBets['contestId']==id_event].groupby('day').agg({'amount':'sum'}).reset_index(),how='left',on='day')
df_days_contests['amount'] = df_days_contests['amount'].fillna(0)
df_days_contests = df_days_contests.set_index('day')

st.line_chart(df_days_contests)

## Tabla probabilidades del evento
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Probabilidades del evento")
df_odds = pd.DataFrame(columns=['Opcion','Probabilidad Pagina','Numero Apuestas','Monto Apuestas','Probabilidad Usuarios'])
options_dict = dfContests[dfContests['_id']==id_event]['options'].reset_index(drop=True).loc[0]
total_amount = float(dfBets[dfBets['contestId']==id_event]['amount'].sum())
for i in list(range(len(options_dict))):
    opti = options_dict[i]['option_explanation']
    prob = options_dict[i]['probability']
    option_amount = float(dfBets[(dfBets['contestId']==id_event)&(dfBets['option']==opti)]['amount'].sum())
    option_num_bets = len(dfBets[(dfBets['contestId']==id_event)&(dfBets['option']==opti)])
    df_odds.loc[i] = [opti,prob,option_num_bets,option_amount,option_amount/total_amount]

st.dataframe(df_odds.style.format({"Probabilidad Pagina": "{:.2f}", "Numero Apuestas": "{:.0f}",
                                   "Monto Apuestas": "{:.1f}", "Probabilidad Usuarios": "{:.2f}"}))


## Grafico evolucion montos apostados usuarios
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Evolucion apuestas usuarios ultimos 10 dias")
option_event_user = st.selectbox(
    'Usuario Activo',
    list(dfUsers[dfUsers['role']!='admin']['email']))
st.write('Evolucion montos apuestas: ', option_event_user)

id_user = dfUsers[dfUsers['email']==option_event_user]['_id'].reset_index(drop=True)[0]
df_days_users = pd.DataFrame(Dateslist,columns=['day'])
df_days_users['day'] = pd.to_datetime(df_days_users['day'])
df_days_users = pd.merge(df_days_users,dfBets[dfBets['userId']==id_user].groupby('day').agg({'amount':'sum'}).reset_index(),how='left',on='day')
df_days_users['amount'] = df_days_users['amount'].fillna(0)
df_days_users = df_days_users.set_index('day')

st.line_chart(df_days_users)