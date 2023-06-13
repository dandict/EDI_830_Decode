from pypdf import PdfReader
import re
import pandas as pd
from functools import reduce
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

reader = PdfReader('./release_raw/230502.pdf')

# turn the PDF to text
text = ""
for i in range(len(reader.pages)):

    page = reader.pages[i]

    text += page.extract_text()

    # Split the release by part number. first instance contains suppl and cust data.
pn_split = text.split("LIN**BP*")

rel_date = re.findall("(?<=DTM\*168\*)(.*?)(?=\*\*\*)", pn_split[0])[0]
#print(text)
#removing first instance leaves only the releases
pn_release = pn_split[1:]

pn_list = []
cum_list = []
rel_list = []
date_list = []
shp_list = []
for item in pn_release:

    # Grab list of part numbers in the release
    pn_list.append(re.findall("(.*?)(?=\*PO\*)", item)[0])

    # Grab the cumulative requirement up to the date before first release date
    cum_req = re.findall("(?<=ATH\*PQ\*)(.*)(?=\n)", item)
    cum_req = int(re.findall("(?<=[0-9]\*)(.*)(?=\*\*)", cum_req[0])[0])
    cum_list.append(cum_req)


    # Find the release quantities
    rel = re.findall("(?<=FST\*)(.*?)(?=\*)", item)
    rel = [int(qyt) for qyt in rel]
    rel_list.append(rel)

    # Get date of releases
    dates = re.findall("(?<=\*C\*W\*|\*D\*W\*)(\d+)(?=FST)", item.replace("\n", ""))
    date_list.append(dates)

    # Get shipped quantity
    shp = re.findall("(?<=SHP\*02\*)(\d+)(?=\*)", item.replace("\n", ""))

    shp_list.append(int(shp[0]))
    part_cum_list = []


# merges dates and quantities as zipped tuples
i=0
release_list = []
for part in pn_list:

    release_list.append(tuple(zip(date_list[i], rel_list[i])))

    i+=1


# convert individual releases as DF and add into a list of dfs
release_df_list = []
i=0
for part in pn_list:
    rel_df = pd.DataFrame(release_list[i], columns=['date', part + " qyt"]) #part + " CUM req." + part +  " CUM shipped"

    cum = cum_list[i]+rel_df[part+" qyt"].cumsum() # series of cumulative requirements
    cum_shp = shp_list[i]+rel_df[part+" qyt"].cumsum()
    pastdue = cum-cum_shp

    rel_df[part+" pastdue"] = pastdue
    release_df_list.append(rel_df)
    i+=1

data_merge = reduce(lambda left, right:  # Merge DataFrames in list
                        pd.merge(left, right,
                                 on=["date"],
                                 how="outer"),
                        release_df_list)
print(str(data_merge))

data_merge.to_excel("release_view.xlsx", index=False)