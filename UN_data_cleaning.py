import pandas as pd
import requests
from io import StringIO

urls = ["https://data.un.org/_Docs/SYB/CSV/SYB67_263_202411_Production,%20Trade%20and%20Supply%20of%20Energy.csv", "https://data.un.org/_Docs/SYB/CSV/SYB67_246_202411_Population%20Growth,%20Fertility%20and%20Mortality%20Indicators.csv"]

df_list = []
for url in urls:
    response = requests.get(url)
    data = response.text
    csv_data = StringIO(data)
    df = pd.read_csv(csv_data, skiprows=2, usecols=range(1,5), names=["Region/Country/Area", "Year", "Metric", "Value"])
    df['Value'] = df['Value'].replace({',':''}, regex=True)
    df['Value'] = df['Value'].astype(float)
    df_list.append(df)
#print(df[df['Metric'] == 'Primary energy production (petajoules)' or df['Metric'] == 'Net imports [Imports - Exports - Bunkers] (petajoules)'])


for df in df_list:
    for index, row in df.iterrows():
        if "joules" in row['Metric']:
            df_filtered = df[df["Metric"].str.contains("Primary|Net imports", case=False, na=False)]
            df_filtered.to_csv("./Energy_Consumption.csv")
            break
        elif "Population" in row['Metric']:
            df_filtered = df[df["Metric"].str.contains("increase", case=False, na=False)]
            df_filtered.to_csv("./Population_Increase.csv")
            break



    
