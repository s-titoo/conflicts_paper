import os
import pandas as pd

#///////////////////////////////////////
#//////////////PACKAGES/////////////////
#///////////////////////////////////////

# python==3.8.5
# pandas==1.2.1
# openpyxl==3.0.6

#///////////////////////////////////////
#////////ARMED CONFLICT DATASET/////////
#///////////////////////////////////////

# read data
acd = pd.read_excel("inputs/ucdp-prio-acd-201.xlsx")

# filter columns
mask = ["conflict_id", "location", "side_a", "side_b", "territory_name",
             "year", "intensity_level", "type_of_conflict", "start_date",
             "start_prec", "start_date2", "start_prec2", "region", "version"]
acd = acd[mask]

# convert data types
acd = acd.convert_dtypes()
acd[["start_date", "start_date2"]] = acd[["start_date", "start_date2"]]. \
    astype("datetime64[ns]")

# filter conflicts with precise start date
mask = acd['start_prec'].isin([1,2]) | acd['start_prec2'].isin([1,2])
acd = acd[mask]

# remove duplicates
acd = acd.drop_duplicates(["conflict_id", "start_date2"])

# define type of conflict and its intensity
acd.loc[acd['type_of_conflict'].isin([1,2,4]), "type_start_international"] = 1
acd.loc[acd['type_of_conflict'] == 3, "type_start_international"] = 0                                  
acd.loc[acd['intensity_level'] == 2, "high_intensity_start"] = 1
acd.loc[acd['intensity_level'] == 1, "high_intensity_start"] = 0

# find first conflict episode's date
acd = acd.join(acd.groupby('conflict_id')['year'].agg(['min']),
               on='conflict_id') 
acd.rename(columns={'min':'first_episode'}, inplace=True)

# find difference between start_date and start_date2
acd["date_diff"] = acd.start_date2 - acd.start_date

# create new variable start_date3, which is equal to:
#    * start_date - if difference between start_date and start_date2
#      for first conflict episode is <= 10 days
#    * start_date2 - if the same difference is > 10 days
#    * start_date2 - for conflict episodes other than the first one

mask_1 = (acd["year"] == acd["first_episode"]) & (acd["date_diff"].dt.days <= 10)
mask_2 = (acd["year"] == acd["first_episode"]) & (acd["date_diff"].dt.days > 10)
mask_3 = (acd["year"] != acd["first_episode"])
    
acd.loc[mask_1, "start_date3"] = acd.loc[mask_1, "start_date"]
acd.loc[mask_2, "start_date3"] = acd.loc[mask_2, "start_date2"]
acd.loc[mask_3, "start_date3"] = acd.loc[mask_3, "start_date2"]

# the same operation but for start_prec3
acd.loc[mask_1, "start_prec3"] = acd.loc[mask_1, "start_prec"]
acd.loc[mask_2, "start_prec3"] = acd.loc[mask_2, "start_prec2"]
acd.loc[mask_3, "start_prec3"] = acd.loc[mask_3, "start_prec2"]

# remove unneeded objects
del [mask_1, mask_2, mask_3]

# reset dataframe index
acd = acd.reset_index(drop = True)

# /////////////////////////////////////////////
# extract start_date column into separate lines
# /////////////////////////////////////////////

# subset data where difference between start_date & start_date2
# for first conflict episode is more than 10 days
# for these episodes we will create a separate line for each
# start_date

# subset data
mask = (acd["year"] == acd["first_episode"]) & (acd["date_diff"].dt.days > 10)
acd_sub = acd[mask].reset_index(drop = True)

# for episodes where start_date and start_date2 are in different years
# assign conflict intensity to low

mask = acd_sub.start_date.dt.year != acd_sub.start_date2.dt.year
acd_sub.loc[mask, "high_intensity_start"] = 0
acd_sub.loc[mask, "intensity_level"] = 1

# assign proper values to some of the columns
zeroes = pd.Series([0]*len(acd_sub.index))
data_replace = [acd_sub.start_date.dt.year, acd_sub.start_date,
                        acd_sub.start_prec, acd_sub.start_date,
                        acd_sub.start_prec, acd_sub.start_date.dt.year,
                        zeroes]
data_replace = [x.reset_index(drop = True) for x in data_replace]
data_replace = pd.concat(data_replace, axis = 1, ignore_index=True)

mask = ["year", "start_date2", "start_prec2", "start_date3", "start_prec3",
            "first_episode", "date_diff"]
acd_sub[mask] = data_replace
del data_replace, zeroes

# mark newly added lines
acd["zero_episode"] = 0
acd_sub["zero_episode"] = 1

# merge both datasets
acd = pd.concat([acd, acd_sub], ignore_index=True)
del acd_sub

# /////////////////////////////////////////////

# once again check that start_prec3 = 1,2 & drop lines if not
acd = acd[acd["start_prec3"].isin([1,2])]

# drop columns
mask = ["start_date", "start_prec", "start_date2", "start_prec2",
             "year", "intensity_level", "type_of_conflict", "first_episode"]
acd = acd.drop(mask, axis=1)

# create unique id for each conflict episode
# we need to replace year with start_date3 as after our manipulations
# conflict_id and year are not necessarily unique
acd["conflict_episode_id"] = acd["conflict_id"].astype(str) + "_" + \
    acd["start_date3"].dt.date.astype(str).str.replace("-", "")

# add official_start column
acd["official_start"] = 1

# rearrange columns
mask = ["conflict_id", "conflict_episode_id", "zero_episode", 
             "official_start", "start_date3", "start_prec3" ,
             "high_intensity_start", "type_start_international",
             "location", "side_a", "side_b", "territory_name",
             "region","version"]
acd = acd[mask]

# reset dataframe index
acd = acd.reset_index(drop = True)

#///////////////////////////////////////
#//////////////SIPRI////////////////////
#///////////////////////////////////////

# read data
sipri = pd.read_excel("inputs/SIPRI-Top-100-2002-2018_0.xlsx",
                      sheet_name = "2018", skiprows = 3, na_values = ". .")
sipri = sipri.convert_dtypes()

# companies with arms sales >=50% of total sales
companies_50 = list(sipri.loc[sipri["Arms sales as a % of total sales (2018)"] \
                              >= 50, "Company (c) "])

del sipri

#///////////////////////////////////////
#//////////////BLOOMBERG////////////////
#///////////////////////////////////////

# read data
prices_us = pd.read_csv("inputs/Data_Bloomberg_US.csv")
prices_other = pd.read_csv("inputs/Data_Bloomberg_Other.csv")
prices_indices = pd.read_csv("inputs/Data_Bloomberg_Indices.csv")

# convert data types (#1)
prices_us = prices_us.convert_dtypes()
prices_other = prices_other.convert_dtypes()
prices_indices = prices_indices.convert_dtypes()

# combine tables
prices = pd.concat([prices_us, prices_other], ignore_index=True)
del prices_us, prices_other

# remove empty Dates & full NA
prices = prices[prices["Dates"] != "#NAME?"]
mask = ~prices.loc[:, ["PX_LAST", "BOOK_VAL_PER_SH", "PX_TO_BOOK_RATIO", 
                        "CUR_MKT_CAP"]].isnull().all(axis=1)
prices = prices[mask]

# drop BOOK_VAL_PER_SH
prices = prices.drop("BOOK_VAL_PER_SH", axis=1)

# convert data types (#2)
prices["Dates"] =  pd.to_datetime(prices["Dates"], format="%d.%m.%Y")
prices["PX_LAST"] =  prices["PX_LAST"].astype("Float64")
prices[["company_ticker", "company_name", "country"]] = \
    prices[["company_ticker", "company_name", "country"]].astype("category")

prices_indices["Dates"] =  pd.to_datetime(prices_indices["Dates"],
                                          format="%d.%m.%Y")
prices_indices[["index_name", "country"]] = \
    prices_indices[["index_name", "country"]].astype("category")
    
# rename columns
mask = {'Dates':'trading_date', 'PX_LAST':'company_price',
        'PX_TO_BOOK_RATIO': 'company_price_to_book',
        'CUR_MKT_CAP': 'company_market_cap'}
prices.rename(columns=mask, inplace=True)
mask = {'Dates':'trading_date', 'PX_LAST':'index_price'}
prices_indices.rename(columns=mask, inplace=True)

# join prices vs indices
prices_all = pd.merge(prices, prices_indices, how="left",
                      on=["trading_date", "country"], sort=False)
prices_all.sort_values(['country', 'company_name', 'trading_date'],
                       inplace=True)
del prices

# filter companies with >=50% of arms sales
prices_all = prices_all[prices_all["company_name"].isin(companies_50)]
del companies_50

# extract list of trading dates for each stock exchange
trading_dates_indices = prices_indices[["trading_date", "country"]]
trading_dates_companies = prices_all[["trading_date", "country"]]
trading_dates = pd.merge(trading_dates_indices, trading_dates_companies,
                       how="outer", sort=False)
trading_dates["country"] = trading_dates["country"].astype("category")
trading_dates = trading_dates.reset_index(drop = True)
del prices_indices, trading_dates_companies, trading_dates_indices

# rearrange columns
mask = ["trading_date", "company_ticker", "company_name", "otc", "company_price",
             "company_price_to_book", "company_market_cap",
             "index_name", "index_price", "country"]
prices_all = prices_all[mask]

# convert data types (#3)
prices_all["country"] = prices_all["country"].astype("category")

# reset dataframe index
prices_all = prices_all.reset_index(drop = True)

#///////////////////////////////////////
#//////////////LEXIS NEXIS//////////////
#///////////////////////////////////////

# read data
lexis_nexis = pd.read_excel("inputs/Content Analysis Final.xlsx",
                          sheet_name = "CONTENT ANALYSIS")

# convert data types
lexis_nexis = lexis_nexis.convert_dtypes()
lexis_nexis["date"] = lexis_nexis["date"].astype("datetime64[ns]")
lexis_nexis["start_date3"] = lexis_nexis["start_date3"]. \
    astype("datetime64[ns]")

# rename columns
a = "conflict_episode_id"
b = "conflict_id"
c = "start_date3"
d = "location"
mask = {a:a+"_news", b:b+"_news", c:c+"_news", d:d+"_news"}
lexis_nexis.rename(columns=mask, inplace=True)
del a,b,c,d

#///////////////////////////////////////
#///////////PREPROCESS ACD//////////////
#///////////////////////////////////////

# extract list of countries
countries_unique = list(prices_all.country.unique())
countries = pd.DataFrame(countries_unique*len(acd.index), columns = ["country"])

# save original acd
acd_original = acd

# replicate rows
acd = acd.append([acd]*(len(countries_unique)-1),ignore_index=True)

# add country name
acd = pd.concat([acd, countries], axis = 1)
del countries_unique, countries

# add column for the closest trading date
acd = acd.reindex(columns=[*list(acd.columns), 'start_trading_date'])
# acd["start_trading_date"] = np.datetime64("NaT")

# find closest trading date for each start_date3

for i in range(len(acd.index)):
    mask = trading_dates["country"] == acd["country"][i]
    sub = trading_dates.loc[mask,"trading_date"]
    if len(sub) != 0:
        mask = sub >= acd["start_date3"][i]
        acd["start_trading_date"][i] = sub[mask].min()

del sub, i, trading_dates

# # find a number of days between start_date2 and closest trading date
# # if it's more than 3 - change it to NaN
acd["start_trading_date"] = acd["start_trading_date"].astype("datetime64")
acd["difference_dates"] = acd["start_trading_date"] - acd["start_date3"]
acd.loc[acd["difference_dates"].dt.days > 3, "start_trading_date"] = pd.NaT

# rearrange columns
mask = ["conflict_id", "conflict_episode_id", "zero_episode",
 "official_start", "start_date3", "start_prec3",
 "start_trading_date", "difference_dates", "high_intensity_start",
 "type_start_international", "location", "side_a", "side_b",
 "territory_name", "region", "version", "country"]
acd = acd[mask]

# change data types
acd = acd.convert_dtypes()
acd["country"] = acd["country"].astype("category")

#///////////////////////////////////////
#////////////ACD VS BLOOMBERG///////////
#///////////////////////////////////////

# join ACD and Bloomberg
master_acd_bl = pd.merge(prices_all, acd, how="left",
                         left_on = ["trading_date", "country"],
                         right_on = ["start_trading_date", "country"],
                         sort=False)
master_acd_bl.drop("start_trading_date", axis=1, inplace=True)

#///////////////////////////////////////
#////////LEXIS NEXIS VS BLOOMBERG///////
#///////////////////////////////////////

# join LexisNexis vs Bloomberg
master_lx_bl = pd.merge(prices_all, lexis_nexis, how="left",
                        left_on = "trading_date", right_on = "date",
                        sort=False)
master_lx_bl.drop("date", axis=1, inplace=True)

#///////////////////////////////////////
#/////////////SAVE RESULTS//////////////
#///////////////////////////////////////

# create folder to save results
os.mkdir("outputs")

# write results
master_acd_bl.to_csv("outputs/Bloomberg vs ACD.csv")
master_lx_bl.to_csv("outputs/Bloomberg vs LexisNexis.csv")
acd_original.to_csv("outputs/Armed Conflict Dataset.csv")
prices_all.to_csv("outputs/Bloomberg.csv")

# clear env
del acd, acd_original, prices_all, lexis_nexis, master_acd_bl, master_lx_bl, \
    mask
