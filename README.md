---
title: 'Conflict Returns Paper: Data Preprocessing'
author: "Oleksandr Titorchuk"
date: "01.02.2020"
output: html
urlcolor: blue
---

## Software

Packages installed:

 * python==3.8.5
 * pandas==1.2.1
 * openpyxl==3.0.6

## Data Sources 

### Armed Conflict Dataset

Data on conflicts was taken from [UCDP/PRIO Armed Conflict Dataset](https://ucdp.uu.se/downloads/index.html#armedconflict). This dataset was created at part of [Uppsala Conflict Data Program (UCDP)](https://www.prio.org/Data/Armed-Conflict/UCDP-PRIO/) at the department of Peace and Conflict Research, Uppsala University and the Centre for the Study of Civil War at the Peace Research Institute Oslo (PRIO).

**Citation:**

* Pettersson, Therese & Magnus Öberg (2020) Organized violence, 1989-2019. Journal of Peace Research 57(4)
* Gleditsch, Nils Petter, Peter Wallensteen, Mikael Eriksson, Margareta Sollenberg, and Håvard Strand (2002) Armed Conflict 1946-2001: A New Dataset. Journal of Peace Research 39(5)

### SIPRI

Data on world's top arms producing companies was taken from [SIPRI Arms Industry Database](https://www.sipri.org/databases/armsindustry). 

### Bloomberg

Data on historical share prices of arms producing companies was taken from Bloomberg. 

### LexisNexis

Data about news coverage of conflict was exported from [LexisNexis Academic](https://www.lexisnexis.com/communities/academic/w/wiki/30.lexisnexis-academic-general-information.aspx]).

## Armed Conflict Dataset

There are couple of things to consider before **Armed Conflict Dataset** can be used in the analysis.

1. Each conflict in the dataset is coded with unique `conflict_id` and can have multiple episodes separated from each other by years. The episode is defined as continuous conflict activity and a new episode starts after one or more year(s) of no military action.
1. `start_date` points to the start a conflict as a whole (date of first battle-related death) while `start_date2` the start of a particular episode in it (date when number of casualties for a particular episode reached 25). We will use both in our analysis: for first conflict episode - if difference between `start_date`and `start_date2` is <= 10 days we will use `start_date` in analysis, if no - both `start_date` and `start_date2`; for all other conflict episodes - `start_date2`.
1. We will move all `start_date` used in analysis from column into row dimension (i.e. will create separate lines for them). For these special dates, `intensity_level` will be assigned to `intensity_level` of the first conflict episode in case `start_date` and `start_date2` are in the same year and low intensity otherwise (because it means that in the year conflict started number of casualties didn't reach 25).
1. Each line in the dataset can be uniquely defined by the combination of `conflict_id` and `year` and represents the year in which a particular conflict was active. If conflict episode lasted uninterrupted for several years, dataset will contain several lines for the same `conflict_id` and `start_date2`. We will remove these duplicates from the dataset.
1. We are filtering only conflicts those start date can be traced back to a specific day (`start_prec` and `start_prec2` = 1,2).
1. We divide conflicts on international (`type_of_conflict` = 1,2,4) and internal (`type_of_conflict` = 3).

```python

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

```

## SIPRI

We used SIPRI data to get a list of companies selling arms and filtering the ones trading on stock exchange.

```python

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

```

## Bloomberg

In our analysis we are using information on:

* `PX_LAST` - share price / index;
* `PX_TO_BOOK_RATIO` - price to book ratio;
* `CUR_MKT_CAP` - current market capitalization.

```python

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

```

## LexisNexis

Details about how file "Content Analysis Final" was created can be found on file "Conflict Returns Paper - Content Analysis Data Preprocessing".

```python

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

```

## Armed Conflict Dataset vs Bloomberg

Let's join ACD and Bloomberg datasets together.

Few things to keep in mind:

1. Datasets will be joined based on `start_date3` (ACD) and `trading_date` (Bloomberg).
2. However, taking into consideration that `start_date3` often falls on non-trading day, we need to create an additional variable `start_trading_date`, which will correspond to the closest to `start_date3` trading date. List of trading dates needs to be taken per country as it might differ from state to state.

```python

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

```

## Bloomberg vs LexisNexis

Now let's join LexisNexis and Bloomberg datasets.

```python

#///////////////////////////////////////
#////////LEXIS NEXIS VS BLOOMBERG///////
#///////////////////////////////////////

# join LexisNexis vs Bloomberg
master_lx_bl = pd.merge(prices_all, lexis_nexis, how="left",
                        left_on = "trading_date", right_on = "date",
                        sort=False)
master_lx_bl.drop("date", axis=1, inplace=True)

```

## Save Results

We will generate several files as output:

1. **Bloomberg vs ACD.csv** - master file that joins data from LexisNexis & Bloomberg
1. **Bloomberg vs LexisNexis.csv** - master file that joins data from Armed Conflict Dataset & Bloomberg
1. **Armed Conflict Dataset.csv**: - dataset produced in section *Data Preprocessing - Armed Conflict Dataset*
1. **Bloomberg.csv** - dataset produced in section *Data Preprocessing - Bloomberg*.

```python,lko

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

```

**Bloomberg vs ACD** and **Bloomberg vs LexisNexis** files contain such columns:

1. **Bloomberg:**
   * Contains data for each defense company and corresponding stock exchange
   * `trading_date`
   * `company_ticker`
   * `company_name`
   * `otc` - 1: company trades OTC, 0: company trades on stock exchange 
   * `company_price`
   * `company_price_to_book` - company's price-to-book ratio
   * `company_market_cap` - company's market capitalization
   * `index_name` - name of the index for stock exchange where company trades
   * `index_price`
   * `country` - country where company trades
1. **Armed Conflict Dataset:**
   * Contains data about amred conflicts. Is filled only for dates, when some conflict occured, NA otherwise
   * `conflict_id` - unique ID for each conflict
   * `conflict_episode_id` - unique ID for each conflict episode
   * `zero_episode` - indicates if date corresponds to the beginning of a conflict in case it is different from the beginning of the first conflict episode (i.e. 25 battle-related deaths were reached later than conflict started)
   * `official_start` - 1: on this date (or closest to it trading date) some conflict / conflict episode started, 0: no conflict / conflict episode started on this date
   * `start_date3` - beginning of a conflict episode or a conflict itself in case it doesn't coincide with the start of the first conflict episode
   * `start_prec3` - precision of `start_date3` (1 - exact day, 2 - assigned day, i.e. in case there were several events & each could be treated as a start of conflict)
   * `difference_dates` - difference between `start_date3` and `trading_date` (i.e. in case that event fell under non-trading day)
   * `high_intensity_start` - 1: >1,000 deaths in a given year, 0: <=1,000 death, NA: if start of a conflict & start of the first conflict episode for `zero_episode` dates are from different years we cannot really assign any value to `high_intensity_start`
   * `type_start_international` - 1: international conflict, 0 - internal conflict
   * `location` - The name of the country/countries whose government(s) has a primary claim to the incompatibility (not necessarily the geographical location of the conflict)
   * `side_a` - the name of the country/countries of Side A in a conflict
   * `side_b` - identifying the opposition actor or country/countries of side B in the conflict
   * `territory_name` - the name of the territory over which the conflict is fought, provided that the incompatibility is over territory.
   * `region` - the region of the incompatibility (1 = Europe, 2= Middle East, 3= Asia, 4= Africa, 5= Americas)
   * `version` - the version of the Armed Conflict Dataset
1. **LexisNexis**:
   * Contains data about news coverage of particular conflicts
   * `media_coverage` - number of articles about particular conflict_episode issued that day
   * `conflict_episode_id_news`
   * `conflict_id_news`
   * `start_date3_news`
   * `location_news`
   * `mean_before` - mean for `media_coverage` calculated based on data for 30 days before a start of a conflict's episode
   * `sd_before` - standard deviation of `mean_before`
   * `rule_before1_3` - `mean_before` + 1.3 * `sd_before`
   * `rule_before1_5` - `mean_before` + 1.5 * `sd_before`
   * `rule_before1_8` - `mean_before` + 1.8 * `sd_before`
   * `rule_before2_0` - `mean_before` + 2.0 * `sd_before`
   * `unofficial_before1_3` - 1 if `media_coverage` > `rule_before1_3`, 0 otherwise
   * `unofficial_before1_5` - 1 if `media_coverage` > `rule_before1_5`, 0 otherwise
   * `unofficial_before1_8` - 1 if `media_coverage` > `rule_before1_8`, 0 otherwise
   * `unofficial_before2_0` - 1 if `media_coverage` > `rule_before2_0`, 0 otherwise
   * `mean_after` - same logic but for 30 days after a conflict episode start
   * `sd_after`
   * `rule_after1_3`
   * `rule_after1_5`
   * `rule_after1_8`
   * `rule_after2_0`
   * `news_dummy1_3`
   * `news_dummy1_5`
   * `news_dummy1_8`
   * `news_dummy2_0`