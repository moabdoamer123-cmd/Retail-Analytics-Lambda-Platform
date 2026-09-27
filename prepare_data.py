import pandas as pd

print("⏳ Reading Excel File locally...")

df = pd.read_excel("online_retail_II.xlsx", sheet_name="Year 2009-2010")

print("⏳ Converting and saving as CSV...")
df.to_csv("online_retail_II.csv", index=False)

print("✅ Saved successfully as 'online_retail_II.csv'!")
print(df.head())
