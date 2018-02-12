from pandas import Series, DataFrame
import pandas as pd
import numpy as np
from IPython.display import display
import re
import pickle
import re
import urllib.request
import bs4
import json
from pandas.io.json import json_normalize
import collections

WB_DATA_DF = pd.read_excel("world_bank_country_data.xlsx")

WIKIDATA =  pd.read_pickle( "wiki_country_data.pkl")
# "wiki_country_data.pkl", "rb+" ) )

G20_NAMES = []
COUNTRIES_NAMES = []
G20CONNECTIONS = {}
COUNTRYLEVELS = collections.defaultdict(dict)

def cleanDataGlobal():
    global WB_DATA_DF, COUNTRIES_NAMES
    countries = WIKIDATA.keys()
    mapper = {
      "Gambia, The"                    : "Gambia",
      "Venezuela, RB"                  : "Venezuela",
      "Cote d'Ivoire"                  : "Ivory Coast",
      "Macedonia, FYR"                 : "Macedonia",
      "Egypt, Arab Rep."               : "Egypt",
      "Korea, Dem. People’s Rep."      : "North Korea",
      "Sao Tome and Principe"          : "São Tomé and Príncipe",
      "Iran, Islamic Rep"              : "Iran",
      "Korea, Rep."                    : "South Korea",
      "St. Kitts and Nevis"            : "Saint Kitts and Nevis",
      "Congo, Dem. Rep."               : "Congo, Democratic Republic of the",
      "Russian Federation"             : "Russia",
      "Congo, Rep."                    : "Congo, Republic of the",
      "Kyrgyz Republic"                : "Kyrgyzstan",
      "Bahamas, The"                   : "Bahamas",
      "Slovak Republic"                : "Slovakia",
      "St. Vincent and the Grenadines" : "Saint Vincent and the Grenadines",
      "Brunei Darussalam"              : "Brunei",
      "Timor-Leste"                    : "East Timor",
      "Syrian Arab Republic"           : "Syria",
      "St. Lucia"                      : "Saint Lucia",
      "Cabo Verde"                     : "Cape Verde",
      "Micronesia, Fed. Sts."          : "Micronesia",
      "Yemen, Rep."                    : "Yemen",
      "Lao PDR"                        : "Laos"
    }

    WIKIDATA["South Korea"] = WIKIDATA["Korea South"]
    del WIKIDATA["Korea South"]

    WIKIDATA["North Korea"] = WIKIDATA["Korea North"]
    del WIKIDATA["Korea North"]

    WIKIPAGE = "South Korea"

    URL = "http://en.wikipedia.org/wiki/Special:Export/%s" % urllib.parse.quote(WIKIPAGE)
    req = urllib.request.Request( URL, headers={'User-Agent': 'OII class 2018.1/1025795'})

    infile = urllib.request.urlopen(req)

    wikitext = infile.read()

    soup = bs4.BeautifulSoup(wikitext.decode('utf8'), "lxml")

    koreaFix = soup.mediawiki.page.text

    WIKIDATA["South Korea"] = koreaFix

    WB_DATA_DF = WB_DATA_DF.applymap(lambda s: mapper.get(s) if s in mapper else s)
    check = set(WB_DATA_DF["Country Name"])


    included = []
    excluded = []

    for x in check:
        if x in countries:
            included.append(x)
        else:
            excluded.append(x)

    dropData = []

    g20Countries = set(G20_NAMES)
    allCountries = set(countries)
    COUNTRIES_NAMES = allCountries.difference(G20_NAMES)

    for index, row in WB_DATA_DF.iterrows():
        if row["Country Name"] in COUNTRIES_NAMES:
            continue
        else:
            dropData.append(index)

    WB_DATA_DF = WB_DATA_DF.drop(WB_DATA_DF.index[dropData])


def getGTwenty():
    WIKIPAGE = "G20"

    URL = "http://en.wikipedia.org/wiki/Special:Export/%s" % urllib.parse.quote(WIKIPAGE)
    req = urllib.request.Request( URL, headers={'User-Agent': 'OII class 2018.1/'})

    infile = urllib.request.urlopen(req)

    wikitext = infile.read()

    soup = bs4.BeautifulSoup(wikitext.decode('utf8'), "lxml")

    g20Data = soup.mediawiki.page.text

    step1Cut = g20Data.split("===Leaders===")
    text_to_parse = step1Cut[1].split("=== Member country data ===")[0]

    regCite = re.compile("\{flag(.*?)\}")
    resultCite = regCite.findall(text_to_parse)
    finalList = []

    for x in resultCite:
        c = x.split("|")
        if len(c) == 3:
            finalList.append(c[2])
        else:
            finalList.append(c[1])

    return(finalList)


def interOtherCountries():
    global G20CONNECTIONS
    countryLevelInclu = collections.defaultdict(dict)
    for c in G20_NAMES:
        if c == "European Union":
            continue

        else:
            countryRegex = "\[\[(.*?)\]\]"
            interlink = re.compile(countryRegex)

            preCountryData = interlink.findall(WIKIDATA[c])

            exclusiveCountry = []
            inclusiveCountry = []

            for country in preCountryData:
                fixCountry = country.split("|")
                for fix in fixCountry:
                    if fix in G20_NAMES:
                        if fix not in exclusiveCountry:
                            if fix != c:
                                exclusiveCountry.append(fix)
                    elif fix in COUNTRIES_NAMES:
                        if fix != c:
                            if fix not in inclusiveCountry:
                                inclusiveCountry.append(fix)
            countryLevelInclu[c][len(exclusiveCountry)] = exclusiveCountry
            countryLevelInclu[c][len(inclusiveCountry)] = inclusiveCountry
    G20CONNECTIONS = countryLevelInclu

def countryValues():

    global countryLevels
    connectedCountries = set()

    for country, connections in G20CONNECTIONS.items():
        for keys, connect in connections.items():
              connectedCountries.update(set(connect))

    for country in connectedCountries:
        print(country)
        preValueGDP   = WB_DATA_DF[(WB_DATA_DF["Country Name"] == country) & (WB_DATA_DF["Series Name"] == "Current account balance (% of GDP)")]
        preValueComms = WB_DATA_DF[(WB_DATA_DF["Country Name"] == country) & (WB_DATA_DF["Series Name"] == "Communications, computer, etc. (% of service imports, BoP)")]

        if country == "United States":
            display(WB_DATA_DF[(WB_DATA_DF["Country Name"] == country)])
        gdpArray = []
        comms2016 = []
        checkDates = ["2013 [YR2013]", "2014 [YR2014]", "2015 [YR2015]", "2016 [YR2016]", "2017 [YR2017]"]

        for dates in checkDates:
            try:
                gdpArray.append(preValueGDP[dates].values[0])

            except IndexError:
                gdpArray.append("Missing")

            try:
                comms2016.append(preValueComms[dates].values[0])

            except IndexError:
#                 print(preValueComms[dates])
                comms2016.append("Missing")

        COUNTRYLEVELS[country]["gdp"] = gdpArray
        COUNTRYLEVELS[country]["commns"] = gdpArray

#         COUNTRYLEVELS[country]["comms"]["2013"] = comms2013
#         COUNTRYLEVELS[country]["comms"]["2014"] = comms2014
#         COUNTRYLEVELS[country]["comms"]["2015"] = comms2015
#         COUNTRYLEVELS[country]["comms"]["2016"] = comms2016
#         COUNTRYLEVELS[country]["comms"]["2017"] = comms2017




### Interact with Pickle File to decode countries
    ## i.e. how many countries are connected?
    ## another value I can incorparate?


### Graphic Model?

    ## connected graphs
###

def main():
    global G20_NAMES
    G20_NAMES = getGTwenty()
    cleanDataGlobal()
    interOtherCountries()
    countryValues()
#     print(G20CONNECTIONS)
#     print(len(COUNTRIES_NAMES))
#     pprint.pprint(COUNTRYLEVELS)
#     pprint.pprint(G20CONNECTIONS)
main()
