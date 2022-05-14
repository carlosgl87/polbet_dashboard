import pandas as pd
import streamlit as st
import numpy as np
from pymongo import MongoClient
import ssl
from bson.objectid import ObjectId
from datetime import date, timedelta

pd.set_option('display.float_format', lambda x: '%.2f' % x)

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

## depositos y retiros
num_depositos = 0
mon_depositos = 0
num_retiros = 0
mon_retiros = 0
df_retiros = pd.DataFrame(columns=['Usuario','Monto','Fecha'])
df_depositos = pd.DataFrame(columns=['Usuario','Monto','Fecha'])

for index, row in dfUsers.iterrows():
    name_user = row['email']
    lista_cuenta = dfUsers[dfUsers['email']==name_user]['balance_history'].values[0]
    if type(lista_cuenta) is list:
        for i in range(len(lista_cuenta)):
            if lista_cuenta[i]['state'] == 'approved':
                if lista_cuenta[i]['balanceType'] == 'deposit':
                    fecha = pd.to_datetime(lista_cuenta[i]['createdAt'])- pd.Timedelta(hours=5)
                    df_depositos.loc[num_depositos] = [name_user,lista_cuenta[i]['amount'],fecha]
                    num_depositos = num_depositos + 1
                    mon_depositos = mon_depositos + lista_cuenta[i]['amount']
                if lista_cuenta[i]['balanceType'] == 'withdrawal':
                    fecha = pd.to_datetime(lista_cuenta[i]['createdAt'])- pd.Timedelta(hours=5)
                    df_retiros.loc[num_retiros] = [name_user,lista_cuenta[i]['amount'],fecha]
                    num_retiros = num_retiros + 1
                    mon_retiros = mon_retiros + lista_cuenta[i]['amount']


dfBets['createdAt'] = dfBets['createdAt'] - pd.Timedelta(hours=5)

dfBets['contests'] = 1
dfBets['amount'] = pd.to_numeric(dfBets['amount'])
dfBets['potentialGain'] = pd.to_numeric(dfBets['potentialGain'])
dfBets = pd.merge(dfBets,dfContests[['_id','isContestOpenStatus']],how='left',left_on='contestId',right_on='_id')
dfBets['contest_open'] = dfBets['isContestOpenStatus'].map({True: 1, False: 0})
dfBets['contest_close'] = dfBets['isContestOpenStatus'].map({True: 0, False: 1})
dfBets['contest_winner'] = dfBets['winner'].map({True: 1, False: 0})
dfBets['contest_loser'] = dfBets['winner'].map({True: 0, False: 1})
dfBets['marginal_gain'] = dfBets['potentialGain'] - dfBets['amount']
dfBets['monto_curso'] = dfBets['contest_open'] * dfBets['amount']
dfBets['monto_perdido'] = dfBets['contest_close'] * dfBets['contest_loser'] * dfBets['amount']
dfBets['monto_ganado'] = dfBets['contest_close'] * dfBets['contest_winner'] * dfBets['marginal_gain']

dfBets_users = dfBets.groupby('userId').agg({'createdAt':'last','amount':'sum','monto_curso':'sum','monto_perdido':'sum','monto_ganado':'sum','contests':'sum'}).reset_index()
dfBets_users.rename(columns={'amount':'amount_bets','contests': 'contests_bets','createdAt':'createdAt_last_bet'}, inplace=True)
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

def dif_prob(id_event):
    df_odds = pd.DataFrame(columns=['Opcion','Probabilidad Pagina','Numero Apuestas','Monto Apuestas','Probabilidad Usuarios'])
    options_dict = dfContests[dfContests['_id']==id_event]['options'].reset_index(drop=True).loc[0]
    total_amount = float(dfBets[dfBets['contestId']==id_event]['amount'].sum())
    for i in list(range(len(options_dict))):
        opti = options_dict[i]['option_explanation']
        prob = options_dict[i]['probability']
        option_amount = float(dfBets[(dfBets['contestId']==id_event)&(dfBets['option']==opti)]['amount'].sum())
        option_num_bets = len(dfBets[(dfBets['contestId']==id_event)&(dfBets['option']==opti)])
        df_odds.loc[i] = [opti,prob,option_num_bets,option_amount,option_amount/total_amount]
    df_odds['error'] = df_odds['Probabilidad Pagina'] - df_odds['Probabilidad Usuarios']
    df_odds['error'] = df_odds['error'].abs()
    return df_odds['error'].sum()

df_events_active = pd.DataFrame(columns=['EVENTO','OPEN','NUM_PERSONAS','NUM_APUESTA','MONTO_APUESTA','TICKET_PROMEDIO','DIF_PROB'])
cont = 0
for index, row in dfContests.iterrows():
    name_event = row['name']
    open_status = row['isContestOpenStatus']
    number_pers = len(dfBets[dfBets['contestId']==ObjectId(row['_id'])]['userId'].unique())
    number_bets = len(dfBets[dfBets['contestId']==ObjectId(row['_id'])])
    amount_bets = dfBets[dfBets['contestId']==ObjectId(row['_id'])]['amount'].sum()
    mean_amount = amount_bets / number_bets
    if amount_bets > 0:
        diferencia_probabilidades = dif_prob(row['_id'])
    else:
        diferencia_probabilidades = np.nan
    df_events_active.loc[cont] = [name_event, open_status, number_pers, number_bets,amount_bets,mean_amount,diferencia_probabilidades]
    cont = cont + 1

## numero y monto de apuestas por evento (tanto cerrados como abiertos)
df2 = pd.DataFrame(columns=['EVENTO','OPEN','TOTAL_MONTO','TOTAL_APUESTAS','TICKET_PROMEDIO','FECHA_ULTIMA'])
cont = 0
for index, row in dfContests.iterrows():
    event_name = row['name']
    open_status = row['isContestOpenStatus']
    id_event = dfContests[dfContests['name']==event_name]['_id'].reset_index(drop=True)[0]
    options_dict = dfContests[dfContests['_id']==id_event]['options'].reset_index(drop=True).loc[0]
    total_amount = float(dfBets[dfBets['contestId']==id_event]['amount'].sum())
    mean_amount = float(dfBets[dfBets['contestId']==id_event]['amount'].mean())
    total_number = len(dfBets[dfBets['contestId']==id_event])
    fecha_ultima = dfBets[dfBets['contestId']==id_event]['createdAt'].max()
    df2.loc[cont] = [event_name,open_status,total_amount,total_number,mean_amount,fecha_ultima]
    cont = cont + 1

df_temp_1 = df_temp[['email','amount','monto_curso','monto_perdido','monto_ganado','contests_bets','createdAt_last_bet','createdAt']]
df_temp_1['createdAt_last_bet'] = df_temp_1['createdAt_last_bet'].dt.strftime('%y-%m-%d')
df_temp_1['createdAt'] = df_temp_1['createdAt'].dt.strftime('%y-%m-%d')
df_temp_1.rename(columns={'email': 'Usuario', 'amount':'Saldo','monto_curso': 'Apuestas en Curso','monto_perdido':'Monto Perdido','monto_ganado':'Monto Ganado','contests_bets':'Apuestas','createdAt_last_bet':'Fecha Ultima Apuesta','createdAt':'Fecha Registro'}, inplace=True)
df_temp_1 = df_temp_1.sort_values('Apuestas en Curso', ascending=False).reset_index(drop=True)




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
col1.metric("Numero Usuarios", num_users)
col2.metric("Numero Usuarios Activos", num_users_active)
col3.metric("Total Monto Apostado", int(total_amount_bets))

## Second Rows KPIs

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Eventos Totales", total_events)
col2.metric("Eventos Activos", total_events_active)
col3.metric("Usuarios Eventos Activos", users_events_active)
col4.metric("Numero Apuestas Eventos Activos", number_bets_events_active)
col5.metric("Monto Apuestas Eventos Activos", int(amount_bets_events_active))

## Third Rows KPIs

col1, col2, col3, col4 = st.columns(4)
col1.metric("Monto Depositos", mon_depositos)
col2.metric("Numero Depositos", num_depositos)
col3.metric("Monto Retiros", mon_retiros)
col4.metric("Numero Retiros", num_retiros)

## Tabla Eventos Usuarios y Montos
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Eventos Activos")
df_events_active = df_events_active.sort_values('MONTO_APUESTA', ascending=False).reset_index(drop=True)
st.dataframe(df_events_active)

## Evolucion montos de apuestas ultimos 20 dias
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Evolucion montos de apuestas ultimos 20 dias")
dfBets['day'] = dfBets['createdAt'].dt.floor("D")
today = date.today()
Dateslist = [today - timedelta(days = day) for day in range(20)]
df_days_bets = pd.DataFrame(Dateslist,columns=['day'])
df_days_bets['day'] = pd.to_datetime(df_days_bets['day'])
df_days_bets = pd.merge(df_days_bets,dfBets.groupby('day').agg({'amount':'sum'}).reset_index(),how='left',on='day')
df_days_bets['amount'] = df_days_bets['amount'].fillna(0)
df_days_bets['day'] = df_days_bets['day'].dt.strftime('%y-%m-%d')
df_days_bets = df_days_bets.set_index('day')

st.bar_chart(df_days_bets)

## Tabla Usuarios Principales
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Usuarios Principales")
#st.dataframe(df_temp_1)
st.dataframe(df_temp_1.style.format({"Saldo": "{:.1f}","Apuestas en Curso": "{:.1f}","Monto Perdido":"{:.1f}","Monto Ganado":"{:.1f}","Apuestas": "{:.0f}"}))
#st.dataframe(df_temp_1.style.format({"Saldo": "{:.1f}", "Apuestas en Curso": "{:.1f}", "Monto Perdido":"{:.1f}","Monto Ganado":"{:.1f}","Apuestas": "{:.0f}"}))

## Grafico evolucion apuestas por evento activo
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Evolucion apuestas eventos activos ultimos 10 dias")
option_event = st.selectbox(
    'Evento Activo',
    list(df_events_active['Evento']))
st.write('Evolucion montos apuestas: ', option_event)

id_event = dfContests[dfContests['name']==option_event]['_id'].reset_index(drop=True)[0]
dfBets['day'] = dfBets['createdAt'].dt.floor("D")
today = date.today()
Dateslist = [today - timedelta(days = day) for day in range(10)]
df_days_contests = pd.DataFrame(Dateslist,columns=['day'])
df_days_contests['day'] = pd.to_datetime(df_days_contests['day'])
df_days_contests = pd.merge(df_days_contests,dfBets[dfBets['contestId']==id_event].groupby('day').agg({'amount':'sum'}).reset_index(),how='left',on='day')
df_days_contests['amount'] = df_days_contests['amount'].fillna(0)
df_days_contests['day'] = df_days_contests['day'].dt.strftime('%y-%m-%d')
df_days_contests = df_days_contests.set_index('day')

st.bar_chart(df_days_contests)
#st.line_chart(df_days_contests)

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
df_days_users['day'] = df_days_users['day'].dt.strftime('%y-%m-%d')
df_days_users = df_days_users.set_index('day')

st.bar_chart(df_days_users)
#st.line_chart(df_days_users)


df_temp_2 = df_temp[df_temp['role']!='admin'][['email','amount_bets','contests_bets']]
df_temp_2.rename(columns={'email': 'Usuario', 'amount_bets': 'Monto Apostado','contests_bets':'Apuestas'}, inplace=True)
df_temp_2 = df_temp_2.sort_values('Monto Apostado', ascending=False).reset_index(drop=True)

## Tabla todos los usuarios
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Usuarios Registrados")
st.dataframe(df_temp_2.style.format({"Monto Apostado": "{:.1f}", "Apuestas": "{:.0f}"}))

## Tabla Depositos
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Depositos")
st.dataframe(df_depositos.style.format({"Monto": "{:.1f}"}))

## Tabla Retiro
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Retiros")
st.dataframe(df_retiros.style.format({"Monto": "{:.1f}"}))

## Simulacion Ganancia Cierre Apuesta
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Simulador cierre evento")

dfBets['amount'] = pd.to_numeric(dfBets['amount'])
dfBets['potentialGain'] = pd.to_numeric(dfBets['potentialGain'])

simulator_option_event = st.selectbox(
    'Evento',
    list(dfContests[dfContests['isContestOpenStatus']==True]['name']))

simulator_id_event = dfContests[dfContests['name']==simulator_option_event]['_id'].reset_index(drop=True)[0]
options_dict = dfContests[dfContests['_id']==simulator_id_event]['options'].reset_index(drop=True).loc[0]
options_list = []
for i in range(len(options_dict)):
    options_list.append(options_dict[i]['option_explanation'])

option_winner = st.selectbox(
    'Opcion Ganadora',
    options_list)

st.write('Evolucion montos apuestas: ', simulator_option_event)
st.write('Opcion Ganadora: ', option_winner)

amount_total = dfBets[dfBets['contestId']==simulator_id_event]['amount'].sum()
amount_winnet_total = dfBets[(dfBets['contestId']==simulator_id_event)&(dfBets['option']==option_winner)]['amount'].sum()
amount_winner_gain = dfBets[(dfBets['contestId']==simulator_id_event)&(dfBets['option']==option_winner)]['potentialGain'].sum() - amount_winnet_total
amount_loser = dfBets[(dfBets['contestId']==simulator_id_event)&(dfBets['option']!=option_winner)]['amount'].sum()

df_resultado_evento = pd.DataFrame(columns=['Concepto','Monto'])
df_resultado_evento.loc[0] = ['Monto Apostado',amount_total]
df_resultado_evento.loc[1] = ['Ganancia',amount_loser-amount_winner_gain]
df_resultado_evento.loc[2] = ['% Ganancia',(amount_loser-amount_winner_gain)/amount_total]

st.dataframe(df_resultado_evento.style.format({"Monto": "{:.2f}"}))

## Analisis Apuestas Cerradas
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Analisis Apuestas Cerradas")

# KPIS Eventos Cerrados
num_eventos_cerrados = len(dfContests[dfContests['isContestOpenStatus']==False])
num_apuestas_cerradas = len(dfBets[dfBets['isContestOpenStatus']==False])
monto_apuestas_cerradas = dfBets[(dfBets['isContestOpenStatus']==False)]['amount'].sum()

num_apuestas_cerradas_winner = len(dfBets[(dfBets['isContestOpenStatus']==False)&(dfBets['winner']==True)])
num_apuestas_cerradas_losser = len(dfBets[(dfBets['isContestOpenStatus']==False)&(dfBets['winner']==False)])
monto_apuestas_cerradas_losser = dfBets[(dfBets['isContestOpenStatus']==False)&(dfBets['winner']==False)]['amount'].sum()
monto_apuestas_cerradas_winner = dfBets[(dfBets['isContestOpenStatus']==False)&(dfBets['winner']==True)]['marginal_gain'].sum()

col1, col2, col3 = st.columns(3)
col1.metric("Numero Eventos Cerrados", int(num_eventos_cerrados))
col2.metric("Numero Apuestas", int(num_apuestas_cerradas))
col3.metric("Monto Apuestas", int(monto_apuestas_cerradas))

col1, col2, col3, col4 = st.columns(4)
col1.metric("Monto Ganado", int(monto_apuestas_cerradas_winner))
col2.metric("Monto Perdido", int(monto_apuestas_cerradas_losser))
col3.metric("Apuestas Ganadas", int(num_apuestas_cerradas_winner))
col4.metric("Apuestas Perdidas", int(num_apuestas_cerradas_losser))
