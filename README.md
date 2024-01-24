# `Trends of crop yield in the Netherlands with respect to emission in water`

<img  src="https://images.unsplash.com/photo-1488459716781-31db52582fe9?q=80&w=2070&auto=format&fit=crop&ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D" style="float: left; margin-right: 10px;"  />

This code repository contains exercises and a project developed during the course **Methods of Advanced Data Engineering** in winter semester 2023/24 of the MSc. Data Science at FAU (Friedrich-Alexander-Universität Erlangen-Nürnberg). Herewith, published the automated data pipeline which download the data from official open source data repository in Netherlands. Then, pipeline transforms the data, and establishes a connection with database in order to load transformed data in it.

**[Exercises](https://github.com/imbilalbutt/made-template-ws2324/tree/main/exercises):** Each exercise contains a different URL and implementation of different tasks involved in data pipeline. Data pipelines are construected using different programming tools such python and jayvee (FAU home-made tool).


**[Project](https://github.com/imbilalbutt/made-template-ws2324/tree/main/project):**  A data engineering based project to understand the relationship of crops and water in a european country The Netherlands from year 2000-2015.

## Description of project

This projects tries to realize the trend and relationship of different vegetables yield with respect to the different compounds present in the water, which may be emitted from different factors such as Industrial, Chemical manufacturing and Pharmaceuticals manufacturing waste which are included in [report.ipynb](https://github.com/imbilalbutt/made-template-ws2324/blob/main/project/report.ipynb) using practices of advance data engineering (ie: automated data pipeline, test cases etc.).  It is important to note that above mentioned factors not only emit hard-elemenal compunds which are strictly damaging for the crop yield but also some nutrients like phospours and nitrogen based compounds are also emitted. Though, nutrients are good for crops but their excess can also lead to minimized yield also. Thus, the hypothesis that is analysed and confirmed in the report is: **With the decrease in heavy elements and nutrients; the yield of different vegetables will increase.** 

> **Important:** [PDF slides](https://github.com/imbilalbutt/made-template-ws2324/blob/main/project/slides.pdf) has been recorded by me in the [video](https://drive.google.com/file/d/1dcu1qkknQE8QwfH1KnhBmP63n8AAjfFO/view?usp=sharing).



## Datasets

For this project two datasets from CBS Open data StatLine have been used. This data repository is Netherlands statistics database. This database offers a wealth of data on the Dutch economy and society.

Following datasets have been used.

[1]: [Dataset 1: Vegetables: Yield and cultivated area per kind (type) of vegetable](https://opendata.cbs.nl/statline/#/CBS/en/dataset/37738ENG/table)

[2]:  [Dataset  2: Environmental accounts; emissions to water](https://opendata.cbs.nl/statline/#/CBS/en/dataset/83605ENG/table?ts=1698675109480)

The project is analysed on four years to understand the trend and relationship of crops yield and quantity of different elemental compunds in water:
 
 1. 2000
 2. 2005
 3. 2010
 4. 2015

## Context

This repository is the result of my participation in the course  [Advanced Methods of Software Engineering](https://oss.cs.fau.de/teaching/specific/amse/)  provided by the  [Professorship of Open-Source Software](https://oss.cs.fau.de/)  from FAU.  The task was to build a Data Engineering Project, which takes at least two public available datasources and processes them with an automated datapipeline, in order to report some findings from the result.

## Tools and requirements

 - attrs==22.2.0 
 - greenlet==2.0.2 
 - iniconfig==2.0.0 
 - numpy==1.24.2
 - packaging==23.0
 - pandas==1.5.3
 - pluggy==1.0.0
 - python-dateutil==2.8.2
 - pytz==2022.7.1
 - six==1.16.0
 - SQLAlchemy==1.4.46
 - typing_extensions==4.5.0