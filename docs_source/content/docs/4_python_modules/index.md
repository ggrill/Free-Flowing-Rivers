---
title: '4 Python modules'
date: 2019-02-11T19:27:37+10:00
weight: 50
---

The structure of this repository is composed of a starting script, and a set of 5 modules, each of which contain a set of python files. The following elements are described here:

1. **the python file ```fra_start.py```:** A starting script that triggers the assessment
* **the module ```CONFIG```:** An Excel file that is used to provide paths and parameters for the assessment
* **the module ```SCRIPTS```:** A module with scripts for automating the processing of scenarios
* **the module ```INDICES```:** A module with algorithms to calculate the indices (DOF, DOR, SED, DOM, STA)
* **the module ```STATS```:** A module to calculate statistics, and to conduct the benchmarking
* **the module ```TOOLS```:** A helper module with geoprocessing tools

#### 4.1 Python file ```fra_start.py```
The start of the assessment is triggered by running the python file ```fra_start.py``` from a command line editor or through a programming environment, such as Pycharm. The script reads the configuration file (```config.xlsx```), establishes which components of the assessment are to be executed, and creates an output directory structure for the results.

#### 4.2 Module ‘config’

The configuration file ``config.xlsx``, located in the folder ``config``, provides the main parameters for the analysis, and serves two main purposes:

1. It provides parameter settings for the model run, for example settings to control the Degree of Fragmentation (``DOF``) analysis. It furthermore provides input data and output folder paths (see sheet ``SET_GLO`` in ``config.xlsx``).
2. It provides the spreadsheet where existing scenarios are defined and weights to individual pressure indicators are allocated (see sheet ``SCE_100`` in ``config.xlsx``).

##### 4.2.1 Settings
The sheet ``SET_GLO`` in ``config.xlsx`` provides an example settings file, with a number of relevant parameter settings. These include path settings, general settings, and settings related to DOF, DOR, and SED calculations. The table contains the four columns ``Category``, ``Key``, ``Value``, and ``Description``. See the spreadsheet for more information on the keys and their function.

##### 4.2.2 Scenarios
The individual scenarios are defined in the worksheet ``SCE_100`` in ``config.xlsx``. Under this header row, each additional row corresponds to a distinct scenario. One hundred predefined scenarios already exist in the template spreadsheet. Another 22 scenarios exist for conducting sensitivity analysis. The following columns are present and explained below:

Field name | Descriptions
--- | ---
Scenario_name |	Provides the name of the Scenario. Please keep this name very simple (preferably something like ‘CSI01’; CSI02 etc.), and under 10 characters, do not use blanks or special characters, and do not start the name with a number.
Indicator_1 to Indicator_6 | 	These refer to pressure indicators to be used in the scenario. The name given here must reflect the field names in the input ``streams`` feature class in the geodatabase.
Weight_1 to Weight_6 | These are the weights assigned to each pressure indicator. The sum of the weights should equal to 100%, otherwise there may be a distortion.
CSI_threshold | The field defines the CSI threshold to use for determining the FFR status (0-100%).
Fld_damp | This parameter determines the strength of the floodplain weighting (0-100%).
filter_thresh |	Provides a parameter for filtering extreme outliers (% of total flow of river affected)
to_process | Determines if the scenario is to be included in the model run (0 or 1)
to_export | Determines if a result table and feature class will be produced (0 or 1)
---

#### 4.3 Module “scripts”
The python files in this module are triggered by the start script ```fra_start.py``` and each execute a series of functions and algorithms to calculate the DOF, DOR, and SED, respectively. The python scripts rely on functions defined in the modules ''indices'' and ''stats''.

#### 4.4 Module “indices”
The python files here provide source code related to calculating the main indices, i.e. DOF, DOR, and SED

#### 4.5 Module “stats”
This module contains functions to perform benchmarking, sensitivity analysis, and to create statistics.
