import pandas as pd

# read main file merged_transactions.csv，rename "date" to "order_datetime"
merged_transactions = pd.read_csv("merged_transactions.csv")
merged_transactions.rename(columns={"date": "order_datetime"}, inplace=True)

# read and merge consumer_fraud_probability.csv based on user_id and order_datetime
consumer_fraud_probability = pd.read_csv("consumer_fraud_probability.csv")
consumer_fraud_probability.rename(columns={"fraud_probability": "consumer_fraud_probability"}, inplace=True)
merged_transactions = pd.merge(
    merged_transactions,
    consumer_fraud_probability,
    how="left",
    left_on=["user_id", "order_datetime"],
    right_on=["user_id", "order_datetime"]
)
print("done1")
# read and merge merchant_fraud_probability.csv，based on  merchant_abn and order_datetime
merchant_fraud_probability = pd.read_csv("merchant_fraud_probability.csv")
merchant_fraud_probability.rename(columns={"fraud_probability": "merchant_fraud_probability"}, inplace=True)
merged_transactions = pd.merge(
    merged_transactions,
    merchant_fraud_probability,
    how="left",
    left_on=["merchant_abn", "order_datetime"],
    right_on=["merchant_abn", "order_datetime"]
)
print("done2")
# read consumer_user_details.csv to get the correlative relationship between user_id and consumer_id
consumer_user_details = pd.read_csv("consumer_user_details.csv")

# read tbl_consumer.csv ans add related field
tbl_consumer = pd.read_csv("tbl_consumer.csv", sep="|")

# merge consumer_user_details and tbl_consumer  to consumer information
consumer_details = pd.merge(
    consumer_user_details,
    tbl_consumer,
    how="left",
    on="consumer_id"
)
print("done3")
# merge consumer_details to main file，based on user_id
merged_transactions = pd.merge(
    merged_transactions,
    consumer_details,
    how="left",
    on="user_id"
)
print("done4")
# save the final merged file
merged_transactions.to_csv("final_merged_transactions.csv", index=False)

print(" merge completed, saved as final_merged_transactions.csv")
