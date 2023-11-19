# Project Plan

## Title
<!-- Give your project a short title. -->
Effects of content present in water on vegetables yield

## Main Question

<!-- Think about one main question you want to answer based on the data. -->
1. How does the vegetable yield is effected by water quality? Does good water quality means more vegetable yield?
2. How does the heavy metal and nutritions in the water cause the vegetable yield.?

## Description

<!-- Describe your data science project in max. 200 words. Consider writing about why and how you attempt it. -->
The purpose this project is to analyse if having good water quality also ensures the more vegetable yield
or not. It has been known that some nutrituions are good for vegetable life and obviously they affect
the production. This is what I will analyse with project.

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource1: Vegetables: Yield and cultivated area per kind (type) of vegetable
* Metadata URL: https://opendata.cbs.nl/ODataApi/OData/37738ENG/$metadata#Cbs.OData.WebAPI.UntypedDataSet&$select=Vegetables,%20Periodes,%20GrossYield_1 
* Data URL:  https://opendata.cbs.nl/statline/#/CBS/en/dataset/37738ENG/table
* CSV Downloadable URL: https://opendata.cbs.nl/CsvDownload/csv/37738ENG/TypedDataSet?dl=90C91
* Data Type: CSV


This dataset contains information about the harvest of vegetables in the european countryNetherlands.
It concerns the harvest of vegetables (total yield in million-kg) and the corresponding cropping area (in hectares).

Gross yield = The yield of vegetables, in million kg..

Cropping area = The total cropping area is basically equal to the sown area per year.

The vegetables are broken down as follows:
- strawberries
- leaf and stem vegetables: endive; asparagus; fennel; leeks; celery; lettuce (iceberg, leaf and other); spinach.
- mushrooms
- tuberous and root vegetables: bunched and washed carrots; celeriac; beetroot; radish; scorzonera; onions; winter carrots
- kinds of cabbage: cauliflower; kale; broccoli; Chinese cabbage; green cabbage; red cabbage; conical cabbage; sprouts; white cabbage
- legumes: peas; French beans; broad beans
- fruit eaten as vegetables: eggplant; courgette; cucumber; pepper; tomato
- other vegetables

### Datasource2: Environmental accounts; emissions to water 1995 to 2014
* Metadata URL: https://opendata.cbs.nl/ODataApi/OData/83605ENG/$metadata#Cbs.OData.WebAPI.UntypedDataSet&$select=OriginDestination,%20Periods,%20ChromicCompoundsLikeCr_4,%20CopperCompoundsLikeCu_5,%20MercuryCompoundsLikeHg_6,%20LeadCompoundsLikePb_7,%20NickelCompoundsLikeNi_8,%20ZincCompoundsLikeZn_9,%20TotalNutrientsInEquivalents_10,%20PhosphorusCompoundsLikeP_11 
* Data URL: https://opendata.cbs.nl/statline/#/CBS/en/dataset/83605ENG/table?ts=1698675109480
* CSV Downloadable URL: https://opendata.cbs.nl/CsvDownload/csv/83605ENG/TypedDataSet?dl=13C85 
* Data Type: CSV

This datset provided information about the origin of emissions to water of nutrients and heavy metals into water. These data are part of the environmental accounts. Direct emissions are emitted directly into the environment. Indirect emissions reach the environment in an indirect way. For example, discharges to the sewer system partly reach the surface water after treatment in wastewater treatment plants.

Factors like:
1- Total origin emissions on water
2- Agriculture, forestry and fishing
3- Industry (no construction), energy
4- Chemistry and pharmaceutical

A group of metals with a high atomic weight. This concerns, in particular, the metals with a high toxicity purpose, such as arsenic, cadmium, chromium, copper, mercury, nickel, lead and zinc.

## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Example Issue [#1]: https://github.com/imbilalbutt/made-template-ws2324/issues/1
