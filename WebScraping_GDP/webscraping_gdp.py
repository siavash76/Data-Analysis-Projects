import pandas as pd
import numpy as np

url = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
tables = pd.read_html(url)
df = tables[3]
df.columns = range(df.shape[1])
df = df[[0, 2]]
df = df.iloc[1:11]
df.columns = ["Country", "GDP (Million USD)"]
df["GDP (Million USD)"] = (
    df["GDP (Million USD)"]
    .astype(str)
    .str.replace(",", "", regex=True)   # remove commas
    .astype(int)
)
df["GDP (Billion USD)"] = np.round(df["GDP (Million USD)"] / 1000, 2)
df = df[["Country", "GDP (Billion USD)"]]
print(df)
df.to_csv("Largest_economies.csv", index=False)