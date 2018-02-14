from pandas import Series, DataFrame
import pandas as pd
import numpy as np
from IPython.display import display
import re
import pickle
import re
import urllib.request
import bs4
import collections
import pprint

import igraph
from igraph import *

warnings.filterwarnings('ignore')
%pylab inline

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
      "Lao PDR"                        : "Laos",
      ".."                             : 0,
    }

    WIKIDATA["South Korea"] = WIKIDATA["Korea South"]
    del WIKIDATA["Korea South"]

    WIKIDATA["North Korea"] = WIKIDATA["Korea North"]
    del WIKIDATA["Korea North"]


    addFixWiki("South Korea")

    WB_DATA_DF = WB_DATA_DF.applymap(lambda s: mapper.get(s) if s in mapper else s)

    dropData = []



    g20Countries = set(G20_NAMES)
    allCountries = set(countries)
    COUNTRIES_NAMES = allCountries.union(G20_NAMES)
    check = set(WB_DATA_DF["Country Name"])

    removeList = []

    for country in COUNTRIES_NAMES:
        if country not in check:
            removeList.append(country)

    COUNTRIES_NAMES = COUNTRIES_NAMES.difference(set(removeList))

    for index, row in WB_DATA_DF.iterrows():
        if row["Country Name"] in COUNTRIES_NAMES:
            continue
        elif row["Country Name"] in G20_NAMES:
            continue
        else:
            dropData.append(index)

    WB_DATA_DF = WB_DATA_DF.drop(WB_DATA_DF.index[dropData])


def addFixWiki(wikiEntry):
    global WIKIDATA
    URL = "http://en.wikipedia.org/wiki/Special:Export/%s" % urllib.parse.quote(wikiEntry)
    req = urllib.request.Request( URL, headers={'User-Agent': 'OII class 2018.1/1025795'})


    infile = urllib.request.urlopen(req)

    wikitext = infile.read()

    soup = bs4.BeautifulSoup(wikitext.decode('utf8'), "lxml")

    fix = soup.mediawiki.page.text

    WIKIDATA[wikiEntry] = fix

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

    regCite = re.compile("\{flagcountry(.*?)\}")
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
        countryLevelInclu[c]["Inside G20"] = exclusiveCountry
        countryLevelInclu[c]["Outside G20"] = inclusiveCountry

    G20CONNECTIONS = countryLevelInclu

def countryValues():

    global countryLevels
    connectedCountries = set()

    for country, connections in G20CONNECTIONS.items():
        for keys, connect in connections.items():
              connectedCountries.update(set(connect))

    for country in connectedCountries:

        #
        preValueGDP   = WB_DATA_DF[(WB_DATA_DF["Country Name"] == country) & (WB_DATA_DF["Series Name"] == "Current account balance (% of GDP)")]
        preValueExports = WB_DATA_DF[(WB_DATA_DF["Country Name"] == country) & (WB_DATA_DF["Series Name"] == "Exports of goods and services (BoP, current US$)")]


        gdpNewest= 0
        exportsNewest = 0
        checkDates = ["2013 [YR2013]", "2014 [YR2014]", "2015 [YR2015]", "2016 [YR2016]", "2017 [YR2017]"]

        for dates in checkDates:
            try:
                gdp = preValueGDP[dates].values[0]

                if gdpNewest != 0 and gdp != 0:
                    gdpNewest = gdp

                elif gdpNewest == 0 and gdp != 0:
                    gdpNewest = gdp

                elif gdpNewest != 0 and gdp == 0:
                    continue


            except IndexError:
                continue

            try:
                exports = preValueExports[dates].values[0]

                if exportsNewest != 0 and exports != 0:
                    exportsNewest = gdp

                elif exportsNewest == 0 and exports != 0:
                    exportsNewest = gdp

                elif exportsNewest != 0 and exports == 0:
                    continue


            except IndexError:
                continue

        COUNTRYLEVELS[country]["account-balance"] = gdpNewest
        COUNTRYLEVELS[country]["exports"] = exportsNewest



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

main()



### side plot work
import collections

warnings.filterwarnings('ignore')

%pylab inline


g = Graph()

CONNECTINDEX = collections.defaultdict(dict)

counter = 0
for key, items in COUNTRYLEVELS.items():
    g.add_vertices(key)
    g.vs[counter]["account-balance"] = items["account-balance"]
    g.vs[counter]["exports"] = items["exports"]
    CONNECTINDEX[key] = counter
    counter+=1

edgeListG20 = []
edgeListOutside = []
for keys, items in G20CONNECTIONS.items():

    cn = 0
    for key, item in items.items():
        if cn == 0:
            for country in item:
                edgeListG20.append(tuple([CONNECTINDEX[keys], CONNECTINDEX[country]]))
                cn += 1
            for country in item:
                edgeListOutside.append(tuple([CONNECTINDEX[keys], CONNECTINDEX[country]]))
                cn += 1

g.add_edges(edgeListG20)
g.add_edges(edgeListOutside)
g.vs["label"] = g.vs["name"]
