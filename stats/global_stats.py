import pandas as pd
import geopandas as gpd

from stats.benchmarking import fd
from tools import helper as tools


def post_stats_global_single(stream_array:gpd.GeoDataFrame, csi_name, csi_threshold):
    """
    Calculate a series of global statistics and write into Excel sheet as row

    :param stream_array:
    :param csi_name:
    :param csi_threshold:
    :return:
    """
    global threshold
    panda0 = stream_array.copy() #pd.DataFrame(stream_array)
    panda_df = panda0[panda0[fd.INC] == 1]

    # This do anything
    # glo = []
    # glo.append(["sce_name", "count_reaches", "count_reaches_affected",
    #             "perc_reaches_affected", "mean_csi", "count_nff", "perc_nff"])
    # headers = glo.pop(0)

    return aggregate_global_stats(panda_df, csi_name, csi_threshold)


def aggregate_global_stats(df:pd.DataFrame, sce:str, threshold:float):
    """
    Calculate global statistics

    :param df:
    :param sce:
    :param threshold:
    :return:
    """

    # "Global count reaches"
    reach_count = df[sce].count()

    # "Global count reaches impacted (CSI < 100)"
    glb = df[df[sce] < 99.99999999999999]
    imp_count = glb[sce].count()

    # "Percent of global impacted (CSI < 100)"
    pct_global_imp = round((100 * (imp_count / float(reach_count))), 1)

    # "Global Average (CSI < 100)"
    glb = df[df[sce] < 99.99999999999999]
    imp_mean = round(glb[sce].mean(), 1)

    # "Count affected reaches (CSI < thres)"
    glb2 = df[df[sce] < threshold]
    nff = glb2[sce].count()

    # "Percent NNF reaches (CSI < thres)"
    pct_nff = round((100 * (nff / float(reach_count))), 1)

    return [
        sce,
        reach_count,
        imp_count,
        pct_global_imp,
        imp_mean,
        nff,
        pct_nff]


def export_global_stats_results_to_excel(name_sheet, result_list:list, writer):
    """
    Now export global stats with benchmarking results

    :param name_sheet:
    :param result_list:
    :param writer:
    :return:
    """

    # Converting list to panda data frame
    # https://stackoverflow.com/a/53944118/344647

    results_pd = pd.DataFrame(result_list).T
    tools.export_excel(df=results_pd,
                       name=name_sheet,
                       writer=writer)


def write_bench_results_to_global_results(bench_val, result_list):
    result_list += bench_val
    return result_list
