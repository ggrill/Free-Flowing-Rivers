---
title: '2 Requirements'
date: 2019-02-11T19:27:37+10:00
weight: 20
---

The python scripts in this repository require third-party modules to run such as:

1. the source code requires that the software ``ArcGIS 10.5.1`` or newer is installed. ArcGIS provides essential python interfaces, such as ```arcpy```, as well as other frameworks such as ```numpy``` and ```pandas```, upon which the code relies on heavily. The python interpreter must be set to the one provided by the ArcGIS installation.
2. The scripts use an EXCEL workbook as input, and as such, require either ```Microsoft Excel```, or ```LibreOffice```
3. the ArcGIS 64-bit geoprocessing module may be beneficial depending on the data volume.
4. Other requirements are indicated by imports in the source code, and may need to be installed by the user.
