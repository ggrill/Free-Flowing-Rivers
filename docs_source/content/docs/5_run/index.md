---
title: '5 Results'
date: 2019-02-11T19:30:08+10:00
draft: false
weight: 60
---

### 5.1 Launching the assessment

The main script is the starting point for every model run. The main script reads the parameter and scenario settings from the excel file, prepares output folders and files; executes the defined processing steps. Run the starting script ```fra_start.py``` from the commandline or through the PyCharm environment.

### 5.2 Results

The model generates two types of results:

1. Tabular data in the form of an Excel workbook with several worksheets. These results are stored in a folder called ``STAT`` , which holds a workbook called ``results.xls``. The workbook is generated and opened automatically by the scripts.

2. Geodata as feature classes in a geodatabase. The scripts generate a Geodatabase called ``CSI.gdb`` which holds a streams feature class with the original attributes and with the results of the model run. The same results are also provided as a table without the geometry.
