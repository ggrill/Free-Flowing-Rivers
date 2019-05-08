---
title: '3 Input data'
date: 2019-02-11T19:27:37+10:00
draft: false
weight: 30
---

A typical Free-flowing Rivers assessment must have four essential files:

### 3.1 Stream network
The feature class ``streams``, which contains the river network, with the essential attributes to conduct the ``FRA``. The stream network provides information for all 6 pressure indicators, the presence of waterfalls, as well as floodplain densities. For a description of the stream network and its attributes please see the technical documentation in the *figshare* repository at https://doi.org/10.6084/m9.figshare.7688801.

### 3.2 Barriers and reservoirs
The feature class ``barriers`` is a point feature class holding the dam locations and dam attribute information.

### 3.3 Benchmark rivers
The feature class ``bm_rivers`` includes rivers or river stretches that are used to benchmark (validate) the analysis. Benchmark rivers are rivers that were positively identified as free-flowing by a research article or by an expert. The global benchmarking dataset of reported FFRs was compiled from literature resources and expert input. Additional benchmark rivers may be added following the geodatabase template provided.

### 3.4 Lakes
The feature class ``lakes`` contains the location and potential sediment capture of global lakes.
