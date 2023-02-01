# Connect to the database with pymysql
    #connection
# Get 2 tables (audible_transaction and audible_data) from database 
import pandas as pd

sql = " SELECT * FROM audible_transaction"
audible_transaction = pd.read_sql(sql, connection)

sql_au = " SELECT * FROM audible_data"
audible_data = pd.read_sql(sql_au, connection)
audible_data = audible_data.set_index("Book_ID") 

transaction = audible_transaction.merge(audible_data, how="left", left_on="book_id", right_on="Book_ID")

# Convert price to THB 
# Request API conversion rate 
import requests

url = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate"
response = requests.get(url)
data = response.json()
conversion_rate = pd.DataFrame(data)
conversion_rate = conversion_rate.reset_index().rename(columns = {"index":"date"})

# To be same date format
transaction["date"] = transaction["timestamp"]
transaction["date"] = pd.to_datetime(transaction["date"]).dt.date
conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

# Join 2 dataframes as final_df 
final_df = transaction.merge(conversion_rate, how="left", left_on= "date", right_on = "date")

# Calculate USD price to THB as new column
final_df["Price"] = final_df["Price"].apply(lambda x: x.replace("$",""))
final_df["Price"] = final_df["Price"].astype(float)
final_df["THB"] = final_df["Price"] * final_df["conversion_rate"]

final_df = final_df.drop("date", axis= 1)

# save to csv 
final_df.to_csv("book_transaction.csv", index = False)
