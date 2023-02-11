# -*- coding: utf-8 -*-
"""
Created on Tue Aug 16 00:42:24 2022
@author: Jordi Garcia Ruspira
"""


import streamlit as st
import pandas as pd
import requests
import json
import time
import plotly.graph_objects as go
import random
import plotly.io as pio
import streamlit_autorefresh 

st.set_page_config(
		page_title="Cosmos Validators - Sankey chart",
		page_icon=":atom_symbol:",
		layout="wide",
		menu_items=dict(About="It's a work of Jordi"),
	)


st.title(":atom_symbol: Cosmos Sankey :atom_symbol:")


st.success("This app only contains a chart! Please select a validator ")
st.text("")
st.subheader('Streamlit App by [Jordi R.](https://twitter.com/RuspiTorpi/). Powered by Flipsidecrypto')
st.text("")
st.markdown('Hi there. This streamlit app displays a Sankey chart showing redelegations from a selected validator to the rest of validators, starting on 2023-02-05.' )   


st.markdown(
			f"""
	<style>
		.reportview-container .main .block-container{{
			max-width: 90%;
			padding-top: 5rem;
			padding-right: 5rem;
			padding-left: 5rem;
			padding-bottom: 5rem;
		}}
		img{{
			max-width:40%;
			margin-bottom:40px;
		}}
	</style>
	""",
			unsafe_allow_html=True,
		) 
	pio.renderers.default = 'browser'




API_KEY = st.secrets["API_KEY"]



	  
SQL_QUERY = """   select distinct address, label, RANK() OVER (ORDER BY delegator_shares DESC) AS RANK from cosmos.core.fact_validators 
	 	order by rank
"""  

SQL_QUERY_2 = """ 
with table_8 as (
select distinct tx_id from cosmos.core.fact_msg_attributes
where tx_succeeded = 'TRUE'
and msg_type = 'message'
and attribute_key = 'action'
and attribute_value = '/cosmos.staking.v1beta1.MsgBeginRedelegate'
and to_date(block_timestamp) between '2023-02-05' and current_date 
),
  
table_9 as (select distinct tx_id, attribute_value as address from cosmos.core.fact_msg_attributes
where tx_succeeded = 'TRUE'
and msg_type = 'transfer'
and attribute_key = 'sender'
and msg_index = '2'
and tx_id in (select * from table_8)),
   
table_10 as (select distinct tx_id, attribute_value as validator_from from cosmos.core.fact_msg_attributes
where tx_succeeded = 'TRUE'
and msg_type = 'redelegate'
and attribute_key = 'source_validator'
and tx_id in (select * from table_8)),
  
table_11 as (select distinct tx_id, attribute_value as validator_to from cosmos.core.fact_msg_attributes
where tx_succeeded = 'TRUE'
and msg_type = 'redelegate'
and attribute_key = 'destination_validator'
and tx_id in (select * from table_8)),
  table_12 as (select distinct tx_id, to_number(SUBSTRING(attribute_value,0,CHARINDEX('uatom',attribute_value)-1))/pow(10,6) as amount from cosmos.core.fact_msg_attributes
where tx_succeeded = 'TRUE'
and msg_type = 'redelegate'
and attribute_key = 'amount'
and tx_id in (select * from table_8)),

table_13 as (
 select distinct address, label, RANK() OVER (ORDER BY delegator_shares DESC) AS RANK from cosmos.core.fact_validators 
order by rank)
  
select 
  val1.label as from_validator, val2.label as to_validator, val1.rank as from_validator_rank,
 val2.rank as  to_validator_rank,
sum(d.amount) as amount_redelegated  from table_9 a 
left join table_10 b 
on a.tx_id = b.tx_id 
left join table_13 val1
on b.validator_from = val1.address 
left join table_11 c 
on a.tx_id = c.tx_id
left join table_13 val2
on c.validator_to = val2.address 
left join table_12 d
on a.tx_id = d.tx_id 
group by from_validator, to_validator, from_validator_rank, to_validator_rank
	  
"""  
	 

TTL_MINUTES = 15
# return up to 100,000 results per GET request on the query id
PAGE_SIZE = 100000
# return results of page 1
PAGE_NUMBER = 1

def create_query():
	r = requests.post(
			'https://node-api.flipsidecrypto.com/queries', 
			data=json.dumps({
				"sql": SQL_QUERY,
				"ttlMinutes": TTL_MINUTES
			}),
			headers={"Accept": "application/json", "Content-Type": "application/json", "x-api-key": API_KEY},
	)
	if r.status_code != 200:
		raise Exception("Error creating query, got response: " + r.text + "with status code: " + str(r.status_code))
		
	return json.loads(r.text)    
	 
def create_query_2():
	r = requests.post(
			'https://node-api.flipsidecrypto.com/queries', 
			data=json.dumps({
				"sql": SQL_QUERY_2,
				"ttlMinutes": TTL_MINUTES
			}),
			headers={"Accept": "application/json", "Content-Type": "application/json", "x-api-key": API_KEY},
	)
	if r.status_code != 200:
		raise Exception("Error creating query, got response: " + r.text + "with status code: " + str(r.status_code))
		
	return json.loads(r.text)    
	 

def get_query_results(token):
	r = requests.get(
			'https://node-api.flipsidecrypto.com/queries/{token}?pageNumber={page_number}&pageSize={page_size}'.format(
			  token=token,
			  page_number=PAGE_NUMBER,
			  page_size=PAGE_SIZE
			),
			headers={"Accept": "application/json", "Content-Type": "application/json", "x-api-key": API_KEY}
	)
	if r.status_code != 200:
		raise Exception("Error getting query results, got response: " + r.text + "with status code: " + str(r.status_code))
		
	data = json.loads(r.text)
	if data['status'] == 'running':
		time.sleep(10)
		return get_query_results(token)

	return data



def get_query_results_2(token):
	r = requests.get(
			'https://node-api.flipsidecrypto.com/queries/{token}?pageNumber={page_number}&pageSize={page_size}'.format(
			  token=token,
			  page_number=PAGE_NUMBER,
			  page_size=PAGE_SIZE
		),
			headers={"Accept": "application/json", "Content-Type": "application/json", "x-api-key": API_KEY}
	)
	if r.status_code != 200:
		raise Exception("Error getting query results, got response: " + r.text + "with status code: " + str(r.status_code))
		
	data = json.loads(r.text)
	if data['status'] == 'running':
		time.sleep(10)
		return get_query_results_2(token)

	return data


query_2 = create_query_2()
token_2 = query_2.get('token')
data2 = get_query_results_2(token_2) 
df2 = pd.DataFrame(data2['results'], columns = ['FROM_VALIDATOR', 'TO_VALIDATOR','FROM_VALIDATOR_RANK','TO_VALIDATOR_RANK','AMOUNT_REDELEGATED'])


	  



query = create_query()
token = query.get('token')
data1 = get_query_results(token)
df1 = pd.DataFrame(data1['results'], columns = ['ADDRESS', 'LABEL','RANK']) 

randcolor = []
for i in range(1,len(df1['LABEL']) + 1):
	 
	randcolor.append("#{:06x}".format(random.randint(0, 0xFFFFFF))) 
		
df1['COLOR'] = randcolor


keys_list =  df1['RANK']
values_list = df1['LABEL']
zip_iterator = zip(keys_list, values_list) 
a_dictionary = dict(zip_iterator)

df3 = pd.DataFrame(a_dictionary.items(), columns = ['RANK','LABEL'], index = keys_list)
df3.index = df3.index
df3 = df3.sort_index()


with st.container():

		
	validator_choice = st.selectbox("Choose a validator", options = df2['FROM_VALIDATOR'].unique() )

		
	df_filtered = df2[df2['FROM_VALIDATOR'] == validator_choice]
	df_filtered['Link color'] = 'rgba(127, 194, 65, 0.2)'
	df_filtered['FROM_VALIDATOR_RANK'] = df_filtered['FROM_VALIDATOR_RANK']-1
	df_filtered['TO_VALIDATOR_RANK'] = df_filtered['TO_VALIDATOR_RANK'] - 1

	link = dict(source = df_filtered['FROM_VALIDATOR_RANK'].values , target = df_filtered['TO_VALIDATOR_RANK'].values, value = df_filtered['AMOUNT_REDELEGATED'], color = df1['COLOR'])
	node = dict(label = df3['LABEL'].values, pad = 35, thickness = 10)

	 
		
		 
	data = go.Sankey(link = link, node = node)
	fig = go.Figure(data)
	fig.update_layout(
			hovermode = 'x', 
			font = dict(size = 20, color = 'white'), 
			paper_bgcolor= 'rgba(0,0,0,0)',
			width=1000, height=1300
	) 
		
	st.plotly_chart(fig, use_container_width=True) 
