import pickle as cPickle
import os

#import arcpy

import numpy as np
import pandas as pd

import tools.helper as tools


def pst_csi_calculations(dir):
    """
    Conducts sensitivity analysis of CSI values

    :param dir: directory where pickles have been saved
    :return: saves a geodatabase table with CSI statistics
    """

    print ("Processing sensitivity analysis")

    mc_results = []

    x = 0
    for filename in os.listdir(dir):

        # NAME.csi files represent scenario run with CSI values of each river reach
        if filename.endswith(".csi"):
            x = x + 1
            print(filename)
            fil = os.path.join(dir, filename)
            # Load the pickle options back into model
            # https://stackoverflow.com/a/899199/344647
            with open(fil, 'rb') as fp:
                result_slice = cPickle.load(fp)

                if x == 1:
                    previous_sum = pd.Series(np.zeros_like(result_slice))
                    previous_diffsquared = pd.Series(
                        np.zeros_like(result_slice))
                    previous_min = pd.Series(result_slice)
                    previous_max = pd.Series(result_slice)

                sum = (result_slice + previous_sum)
                mean = sum / float(x)

                diffsquared = (result_slice - mean) ** 2
                sum_diffsquared = diffsquared + previous_diffsquared
                stddev = np.sqrt(sum_diffsquared / float(x))

                min = np.minimum(result_slice, previous_min)
                max = np.maximum(result_slice, previous_max)

            previous_sum = pd.Series(sum)
            previous_min = pd.Series(min)
            previous_max = pd.Series(max)
            previous_diffsquared = pd.Series(sum_diffsquared)

        else:
            pass

    mc_results.append(mean)
    mc_results.append(previous_min)
    mc_results.append(previous_max)
    mc_results.append(previous_max - previous_min)
    mc_results.append(stddev)

    concatentated = pd.concat(mc_results, axis=1)

    print("Percentile Stats")
    df = pd.DataFrame(concatentated)
    df.columns = ['CSI_AVG', 'CSI_MIN', 'CSI_MAX', 'CSI_RNG', 'CSI_STD']
    
    df.to_csv(dir + r"/stats_csi.csv")

    return df
