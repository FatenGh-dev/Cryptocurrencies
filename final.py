#!/usr/bin/env python
# coding: utf-8

# In[13]:


from datetime import timedelta, date, datetime
import requests
import pandas as pd


# In[14]:


def daterange(date1, date2, step):
    for n in range(0, int((date2 - date1).days)+1, step):
        yield date1 + timedelta(n)


# In[18]:


today = datetime.today()

start_dt = date(2013, 4, 28)
end_dt = date(2021, 11, 13)

end_dt = date(today.year, today.month, today.day)


# In[19]:


for dt in daterange(start_dt, end_dt, 7):

    headers = {
        'X-CMC_PRO_API_KEY': '0a27796b-108b-421e-8406-b1a3c01c10c7',
        'Accept': 'application/json',
    }

    params = (
        ('start', '1'),
        ('limit', '5000'), 
        ('convert', 'USD'),
        ("date", dt.strftime("%Y-%m-%d"))
    )

    response = requests.get('https://web-api.coinmarketcap.com/v1/cryptocurrency/listings/historical', headers=headers, params=params)

    response.raise_for_status()
    data = response.json()["data"]
    ### json_normalize Normalize semi-structured JSON data into a flat table.

    df = pd.json_normalize(data)

    df.set_index("cmc_rank", inplace=True)

    try:
        df.drop(["tags","date_added", "platform", "platform.id", "platform.name", "platform.symbol", "platform.slug", "platform.token_address"], axis=1, inplace=True)
    except KeyError:
        df.drop(["tags", "platform", "date_added"], axis=1, inplace=True)
        
    
    df["price_usd"] = df["quote.USD.price"]
    df["volume_24h"] = df["quote.USD.volume_24h"]
    df["percent_change_1h"] = df["quote.USD.percent_change_1h"]
    df["percent_change_24h"] = df["quote.USD.percent_change_24h"]
    df["percent_change_7d"] = df["quote.USD.percent_change_7d"]
    df["market_cap_usd"] = df["quote.USD.market_cap"]

    df["date"] = df["last_updated"].apply(lambda x: str(x)[:10])

    df.drop(["last_updated","id","slug", "quote.USD.price", "quote.USD.last_updated", "quote.USD.volume_24h", "quote.USD.percent_change_1h", "quote.USD.percent_change_24h", "quote.USD.percent_change_7d", "quote.USD.market_cap"], axis=1, inplace=True)
    
    df.to_csv(f"datasets/{df['date'][1]}.csv")
    
    print(f"datasets/{df['date'][1]}.csv created")
    


# In[20]:


top10 = {}
start_dt = date(2020, 11, 1)

for dt in daterange(start_dt, end_dt, 7):
    dt = str(dt)
##read the top 10 of each file to finally get top 10 + convert values to list
    df1 = pd.read_csv(f"datasets/{dt}.csv")
    top10[dt] = df1.head(10)["symbol"].values.tolist()    


# In[21]:


all_list = []
##after converting the top10 to list extend(add them) them into all_list 
for lst in top10.values():
    all_list.extend(lst)

set(all_list)


# In[22]:


ranking = {}
percentage = {}

for rank in range(10):
    ranking[rank] = []
    percentage[rank] = {}     #used in the next task  to not repeat the loop declaration 

for dt in daterange(start_dt, end_dt, 7):
    
    dt = str(dt)
    df1 = pd.read_csv(f"datasets/{dt}.csv")
    
    for rank in range(10):
        crypto = df1.iloc[rank,:]["symbol"]
        ###apply the  ranking on the rank then add crypto value which crypto is composed by rank + symbol
        ranking[rank].append(crypto)


# In[23]:


top_10_df = pd.DataFrame(ranking, index=daterange(start_dt, end_dt, 7)) 
top_10_df


# In[31]:


for rank in range(10):
    ##all_list contains all values of assets 
    for cryptocurrency in set(all_list):
        percentage[rank][cryptocurrency] = len(top_10_df[top_10_df[rank] == cryptocurrency]) / len(top_10_df)


# In[32]:


percentage


# In[35]:


df = pd.DataFrame.from_dict(percentage) 
df.to_csv(r'test8.csv', index = True, header=True)
df


# In[9]:


def get_key(val, rank):
    for key, value in percentage[rank].items():
        if val == value:
            return key

final_top10 = {}

classified_cryptos = []

for i in range(10) :
    MAX_Perc = max(percentage[i].values())
    crypto_key = get_key(MAX_Perc, i)
    
    if crypto_key not in classified_cryptos:
        final_top10[i] = crypto_key
        classified_cryptos.append(crypto_key)
    else:
        new_crypto_key = get_key(sorted(list(percentage[i].values()))[-2], i)
        final_top10[i] = new_crypto_key
        classified_cryptos.append(new_crypto_key)
classified_cryptos


# In[10]:


final_top10


# In[66]:


start_dt = date(2013, 4, 28)

for final_crypto in final_top10.values():
    first_time = True

    for dt in daterange(start_dt, end_dt, 7):

        final_df = pd.read_csv(f"datasets/{dt}.csv")
        final_df.set_index("date", inplace = True)

        if final_crypto in final_df["symbol"].values:
            if first_time == False:
                final_df[final_df["symbol"] == final_crypto].to_csv(f"top10_datasets/{final_crypto}.csv", mode='a', header=False)
            else:
                final_df[final_df["symbol"] == final_crypto].to_csv(f"top10_datasets/{final_crypto}.csv", mode='a', header=False)
            first_time = False


# In[ ]:




