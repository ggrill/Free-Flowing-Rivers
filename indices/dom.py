import numpy as np
import pandas as pd

from stats.benchmarking import fd


def post_stats_dom_single(stream_array:pd.DataFrame, csi_name:str, threshold:float):
    panda_df = stream_array.copy() # pd.DataFrame(stream_array)
    panda_df = panda_df[panda_df[fd.INC] == 1]

    dom_field_name = csi_name + str("_D")
    dom_all = calculate_dominance_all(
        panda_df, dom_field_name, csi_name, threshold)
    return dom_all


def calculate_dominance_all(panda_df:pd.DataFrame, dom_field_name, sce_name, thres):
    panda_df.loc[:, "SCE_NAME"] = sce_name
    panda_df.loc[:, "NUM"] = 1

    sel:pd.DataFrame = panda_df[panda_df[sce_name] < thres]

    fun = {'NUM': np.sum}
    dom:pd.DataFrame = sel.groupby([dom_field_name, 'SCE_NAME'], as_index=False).agg(fun)
    dom.rename(columns={dom_field_name: 'Pressure'}, inplace=True)
    return dom
