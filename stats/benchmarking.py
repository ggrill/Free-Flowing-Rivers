#import arcpy

import numpy as np
import pandas as pd

from config import config as conf
from tools import helper as tools

fd = conf.var


def post_stats_bench_single(
        stream_array_mod:pd.DataFrame,
        scenario_name,
        bench_fc,
        csi_threshold):

    """
    Calculates benchmarking statistics

    :param stream_array_mod:
    :param scenario_name:
    :param bench_fc:
    :param csi_threshold:
    :return:
    """

    bench = _loadBenchTable(bench_fc)
    bench = bench.set_index([fd.GOID])

    # Now preparing benchmark tables
    panda_df_for_bench = stream_array_mod.copy()
    panda_df_for_bench = panda_df_for_bench.set_index([fd.GOID])

    join_bench = pd.merge(
        panda_df_for_bench,
        bench,
        left_index=True,
        right_index=True
    )

    dom_field_name = scenario_name + str("_D")
    dom_bench = calculate_dominance_bench_rivers(
        join_bench,
        dom_field_name,
        scenario_name,
        csi_threshold
    )

    # Benchmarking stats requires two steps.
    number_matching_bench_rivers = benchmarking_rivers(
        join_bench,
        scenario_name,
        csi_threshold
    )

    return number_matching_bench_rivers, dom_bench


def calculate_dominance_bench_rivers(join:pd.DataFrame, domField, FieldName, thres):
    """
    Function to determine the DOM index for rivers that failed benchmarking

    :param join:
    :param domField:
    :param FieldName:
    :param thres:
    :return:
    """
    print("DOM: Calculating scenario {0} with threshold {1}".format(FieldName, thres))

    join.loc[:, "SCE_NAME"] = FieldName
    join.loc[:, "NUM"] = 1
    sel:pd.DataFrame = join[join[FieldName] < thres]

    if sel.empty == 0:
        return pd.DataFrame([])

    fun = {
        'NUM': np.sum,
        'SCE_NAME': 'first',
        'BENCH_SRC': 'first',
        "Name_Expert": 'first'}
    dom = sel.groupby(["FFRID", domField], as_index=False).agg(fun)
    dom.rename(columns={domField: 'Pressure'}, inplace=True)

    return dom


def benchmarking_rivers(join:pd.DataFrame, sce, threshold):
    """
    Calculates number of matching rivers

    :param join:
    :param sce:
    :param threshold:
    :return:
    """
    join.loc[:, "NUM"] = 1  # Helps in creating a count field

    # First I am creating statistics for each benchmark river (FFRID)
    # sce: np.min is getting the lowest CSI value for the river
    # Will be compared later with threshold in function
    f = {
        fd.BB_ID: 'first',
        fd.BAS_NAME: 'first',
        fd.BB_NAME: 'first',
        fd.LENGTH_KM: np.sum,
        fd.VOLUME_TCM: np.sum,
        fd.RIV_ORD: np.min,
        sce: np.min,
        fd.FFRID: 'first',
        fd.BENCH_SRC: 'first',
        fd.Name_Expert: 'first'}

    one:pd.DataFrame = join.groupby(fd.FFRID).agg(f)

    # Then calculating if above threshold
    print ("using threshold " + str(threshold))

    # Assigning value of 1 if entire river is above threshold, or 0 is not
    one.loc[:, "MATCH"] = one[sce].apply(_getLevelCode, var1=threshold)

    one.loc[:, "SCE_NAME"] = sce
    one.loc[:, "NUM"] = 1

    # Aggregating to number of free-flowing rivers / non-free flowing rivers
    fun = {'NUM': np.sum, "SCE_NAME": 'first'}
    two = one.groupby(["MATCH"], as_index=False).agg(fun)
    two.rename(columns={"NUM": 'COUNT'}, inplace=True)

    # Selecting only rows where Match = 1 (free-flowing rivers)
    two = two[two["MATCH"] == 1]
    number_matching_rivers = two.iloc[0]["COUNT"]

    return number_matching_rivers


def _getLevelCode(x, var1):
    if x < var1:
        r = 0
    else:
        r = 1
    return r


def _loadBenchTable(bench_table)->pd.DataFrame:
    """
    Load benchmarking table and return DataFrame

    :param bench_table:
    :return:
    """
    fields = [fd.GOID, fd.FFRID, fd.BENCH_SRC, fd.Name_Expert]
    input = bench_table[fields]

    return input


def export_benchmarking_dom_results(bench_dom:pd.DataFrame, stamp, writer):
    """
    Now export benchmarking dominance stats

    :param bench_dom:
    :param stamp:
    :param writer:
    :return:
    """

    dom_bench_list = []
    # Simply resorting the columns of the data frame
    try:
        bench_dom_sorted = bench_dom[[
            "SCE_NAME", "FFRID", "Name_Expert", "Pressure", "NUM",
            "BENCH_SRC"]]
        for row in bench_dom_sorted.values:
            dom_bench_list.append([stamp] + [val for val in row])
    except Exception as err:
        print ("Something went wrong with calculating"
               " the bench dom statistics")
        print (str(err))

    tools.export_excel(
        df=pd.DataFrame(dom_bench_list),
        name="Bench_dom",
        writer=writer)
