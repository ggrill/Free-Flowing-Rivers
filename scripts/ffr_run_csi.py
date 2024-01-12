import datetime
import logging
import os

#import arcpy
import pandas as pd
import geopandas as gpd

import indices.csi as csi
import indices.sta as sta
import numpy as np
import stats.backbone as bb
import stats.benchmarking as bench
import stats.benchmarking as bm
import indices.dom as dm
import stats.global_stats as sts
import stats.sensitivity as sns

import tools.helper as tools

# Creates an global object from the config class var. This holds the
# field and path names names. For example ffd.RIV_ORD returns the name of
# the field holding information about the discharge stream order
from config import config

fd = config.var


def run_csi(stamp, para, scenarios, st_flds, paths):
    """
    This is the main function to calculate the Connectivity Status Index (CSI); to calculate the river
    status, and to post-process the results into tables.

    The module is divided into three parts:

    1) Calculation of the CSI for each scenario.
    The fields that hold the CSI as well as the Dominance are added
    for each scenario and are named after the scenario name given
    in the config file

    2) The calculation of river status, in field ``CAT_FFR`` which
    determines the river or river stretch as either *'Free-flowing' (1)*,
    with *'Good connectivity status' (2)*, or *'impacted' (3)

    3) The calculation of benchmark, global and sensitivity statistics.

    :param stamp: Timestamp
    :param para: Parameters
    :param scenarios: Scenario set
    :param st_flds: list of fields
    :param paths: path settings
    :return:

    """

    # Looping through the individual scenarios
    for scenario in scenarios:

        sce_name = scenario[0]
        list_of_fields = scenario[1]
        list_of_weights = scenario[2]
        csi_threshold = scenario[3]
        flood_weight_damp = scenario[4]
        filter_thres = scenario[5]
        to_process = scenario[6]
        to_export = scenario[7]

        if to_process == 0:
            prt("Skipped: " + sce_name)
            continue
        else:
            prt("Processing: " + sce_name)

        # Define output CSI table
        csi_tb = paths["gpkg_full_path"]
        #"csi_tb"

        # Define output CSI fc
        csi_fc_name = "csi_fc_" + str(sce_name)

        stream_array = tools.load_stream_array(
            stream_feature_class=para["streams_fc"],
            stream_fields=st_flds
        )

        # Adding results fields to output table

        # Get the names of new csi fields to append
        sce_name, dom_field_name, ff_field_name, csi_field_names = tools.get_csi_field_names(name=sce_name)

        # Get the names of new ffr fields to append
        ffr_stat1_field, ffr_stat2_field, ffr_dis_field, ffr_field_names = tools.get_ffr_field_names(name=sce_name)

        prt("Adding results fields to stream array")
        stream_csi = tools.add_fields(
            stream_array,
            csi_field_names + ffr_field_names
        )

        prt("")
        prt("***********************")
        prt("PART 1: Calculating CSI")
        prt("***********************")
        prt("")

        prt(str(scenario))

        stream_csi = csi.calculate_csi(
            streams_array=stream_csi,
            csi_field_name=sce_name,
            dom_field_name=dom_field_name,
            ff_field_name=ff_field_name,
            fields=list_of_fields,
            weights=list_of_weights,
            flood_weight=flood_weight_damp,
            csi_threshold=csi_threshold,
            test_pickle_folder=paths["test_pickle_folder"])

        # Saving CSI slice to Pickle for later conducting sensitivity analysis
        # Each scenario result will have their own pickle. Sensitivity
        # analysis loads the pickles and processes them together
        tools.save_as_cpickle(
            pickle_object=stream_csi[sce_name],
            folder=paths["sta_csi_folder"],
            name=sce_name,
            file_extension=".csi"
        )

        # Assemble a results list that holds attributes of the scenario run_sed
        # Results from the global analysis will later be added. After each
        # scenario is run_sed, the list gets added to a list of lists. The list
        # of lists becomes the sheet "Global_stats" in the results excel
        result_list = [stamp]
        result_list += [sce_name]
        for f in list_of_fields:
            result_list += [f]
        for w in list_of_weights:
            result_list += [w]
        result_list += [csi_threshold] + [flood_weight_damp, to_process, filter_thres]

        prt("")
        prt("*************************************")
        prt("PART 2: Calculating global statistics")
        prt("*************************************")
        prt("")

        global_stats = sts.post_stats_global_single(stream_csi, sce_name, csi_threshold)

        # The results of the global global_stats.py analysis is appended to the
        # results list
        for item in global_stats:
            result_list.append(item)

        prt("")
        prt("***********************************************")
        prt("PART 3: Calculating global dominance statistics")
        prt("***********************************************")
        prt("")
        dom_stats = dm.post_stats_dom_single(stream_csi, sce_name, csi_threshold)

        dom_stats["Stamp"] = stamp
        dom_stats_sort = dom_stats[["Stamp", "SCE_NAME", "Pressure", "NUM"]]

        tools.export_excel(dom_stats_sort, "Global_dom", paths["writer"], False)

        prt("")
        prt("**********************************")
        prt("PART 4: Dissolving Backbone Rivers")
        prt("**********************************")
        prt("")

        # Filtering, dissolving and aggregating
        # This part is creates a copy of the stream array, applies filtering
        # and dissolving operations and then overwrites the FFR status fields,
        # and well as the river stretch IDs. The CSI value remains as is.

        # Make a copy of original results.
        stream_alt = stream_csi.copy()

        # Dissolve
        prt("Dissolving part 1 of %s: " % ff_field_name)
        stream_alt = sta.dissolve_rivers(stream_alt, ff_field_name,
                                         ffr_dis_field)

        # The spatial dissolving identifies river stretches that were both small and had a
        # disproportionally high impact on the CSI. This function conducts a spatial selected
        # of the river reaches that caused the sections with the high impact
        prt("Apply filter for %s: " % ff_field_name)
        bb_ids_to_filter = sta.apply_volume_filter(
            csi_fc=stream_alt,
            ff_field=ff_field_name,
            dis_id_field=ffr_dis_field,
            pct_aff_thres=filter_thres
        )

        # Spatial selection and overwrite with zeros
        stream_alt = sta.update_csi(stream_alt, bb_ids_to_filter, ffr_dis_field, sce_name, ff_field_name)

        # Dissolve again
        prt("Dissolving part 2 of %s: " % ff_field_name)
        stream_alt = sta.dissolve_rivers(stream_alt, ff_field_name, ffr_dis_field)

        prt("Updating array %s: " % ff_field_name)
        stream_csi = sta.update_streams_with_diss_id(stream_csi, stream_alt, ffr_dis_field)

        # Status calculations
        prt("Calculating Status of %s: " % ff_field_name)
        stream_csi = sta.calculate_sta(
            stream_csi,
            stream_alt,
            ff_field_name,
            ffr_stat1_field,
            ffr_stat2_field,
            ffr_dis_field
        )

        prt("")
        prt("*******************************************")
        prt("PART 5: Calculating benchmarking statistics")
        prt("*******************************************")
        prt("")

        if para["bench_fc"] is not None:

            bench_val, bench_dom = bm.post_stats_bench_single(
                stream_array_mod=stream_alt,
                scenario_name=sce_name,
                bench_fc=para["bench_fc"],
                csi_threshold=csi_threshold
            )

            # Adding the value for number of free-flowing rivers at the end of
            # the global results list
            result_list += [bench_val]

            sts.export_global_stats_results_to_excel(
                name_sheet="Global_stats",
                result_list=result_list,
                writer=paths["writer"]
            )

            bench.export_benchmarking_dom_results(
                bench_dom=bench_dom,
                stamp=stamp,
                writer=paths["writer"]
            )

        else:
            prt(" BENCHMARKING IS NONE")

        prt("")
        prt("***************************************")
        prt("PART 6: Calculating backbone statistics")
        prt("***************************************")
        prt("")

        bb.backbone_stats(
            stream_csi,
            sce_name,
            para["min_length"],
            paths["sta_pickle_folder"],
            paths["writer"]
        )

        prt("")
        prt("***************************************")
        prt("PART 7: Exporting results              ")
        prt("***************************************")
        prt("")

        if to_export == 1:

            # Reduce numpy array to only necessary fields, i.e
            # get the names of new csi fields to append to input streams
            # feature class
            distilled_fields = [
                fd.GOID,
                sce_name,
                dom_field_name,
                ff_field_name,
                ffr_stat1_field,
                ffr_stat2_field,
                ffr_dis_field
            ]

            distilled:gpd.GeoDataFrame = stream_csi[distilled_fields + ['geometry']]

            prt(f"Exporting table: {str(csi_tb)}/{sce_name}")

            distilled.to_file(csi_tb, driver="GPKG", layer = sce_name)

            #prt("Joining and exporting feature class")
            # output_fc = tools.export_joined(
            #     output_geodatabase_path=paths["gpkg_full_path"],
            #     output_table_name=csi_fc_name,
            #     table_to_join=csi_table,
            #     join_table=para["streams_fc"])
            # prt("Renaming fields")
            # tools.remove_csi_traces(output_fc, sce_name)
            # prt("Deleting join fields")
            # tools.delete_field(output_fc, ["OBJECTID_1", "GOID_1"])

        stream_array = None
        stream_csi = None

    prt("")
    prt("***************************************")
    prt("PART 8: Post processing sensitivity    ")
    prt("***************************************")
    prt("")

    sns.pst_csi_calculations(paths["sta_csi_folder"])

    # prt("")
    # prt("***************************************")
    # prt("PART 9: Open results Excel and end     ")
    # prt("***************************************")
    # prt("")

    #os.system("start " + paths["excel_file"])
    print(datetime.datetime.now())
    prt("Done")


def prt(txt):
    logging.info(txt)
    print(txt)