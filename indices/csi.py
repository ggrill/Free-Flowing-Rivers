import pickle as cPickle
import os

import numpy as np
import pandas as pd

import tools.helper as tools

def calculate_csi(
        streams_array,
        csi_field_name,
        dom_field_name,
        ff_field_name,
        fields,
        weights,
        flood_weight,
        csi_threshold,
        test_pickle_folder=''
    ):
    """
    Calculating the CSI and the DOM index

    :param streams_array: input stream feature class converted to numpy array
    :param csi_field_name: Field name that holds the CSI index
    :param dom_field_name: Field name that holds the DOM index
    :param ff_field_name: Field name that determines if CSI threshold is
    exceeded for the particular reach (0 = No or 1 = Yes)
    :param fields: List of fields to include in CSI calculations,
    as defined in Excel sheet. Fields must be present in the input stream file
    :param weights: list of weights (0-100%) corresponding to the
    list of fields
    :param flood_weight: Floodplain weight factor, see Excel sheet (=50%)
    :param csi_threshold: CSI threshold, defined in Excel sheet (=95%)
    :return:
    """

    # Extracting fields from numpy to individual panda series, makes for much
    # more efficient processing
    dof = pd.Series(streams_array[fields[0]])
    dor = pd.Series(streams_array[fields[1]])
    sed = pd.Series(streams_array[fields[2]])
    use = pd.Series(streams_array[fields[3]])
    rdd = pd.Series(streams_array[fields[4]])
    urb = pd.Series(streams_array[fields[5]])
    fld = pd.Series(streams_array["FLD"])

    fld = fld / 100.0

    """ Calculating CSI """

    # Adding floodplain weighting to road and urban only
    rdd = rdd + ((rdd * fld) * (flood_weight / 100))
    urb = urb + ((urb * fld) * (flood_weight / 100))

    #Percent to decimal
    urb = urb * 100

    # resetting to 100 in case weighting overshoots
    rdd[rdd > 100.0] = 100.0
    urb[urb > 100.0] = 100.0

    # Then regular weighting
    dof = dof * weights[0]
    dor = dor * weights[1]
    sed = sed * weights[2]
    use = use * weights[3]
    rdd = rdd * weights[4]
    urb = urb * weights[5]

    # very small values are set to zero
    dof[dof < 0.1] = 0
    dor[dor < 0.1] = 0
    sed[sed < 0.1] = 0
    use[use < 0.1] = 0
    rdd[rdd < 0.1] = 0
    urb[urb < 0.1] = 0

    # Weighted overlay operation
    csi = 100.0 - ((dof +
                    dor +
                    sed +
                    use +
                    rdd +
                    urb) / 100.0)

    # Rounding to 5 decimal places
    csi = np.around(csi, decimals=5)

    # Assigning CSI values to base array
    streams_array[csi_field_name] = csi

    # Determines if CSI is exceeded or not
    sel = np.where(streams_array[csi_field_name] < csi_threshold, 0, 1)
    streams_array[ff_field_name] = sel

    """ Calculating DOM """

    combine = pd.concat([dof, dor, sed, use, rdd, urb], axis=1)

    # Test if everything is zero (no clear dominant pressure), in which
    # case a '1' is assigned to the array, which leads to "NAN" in the later
    # step (at position 6)
    nan = np.where(np.sum(combine, axis=1) == 0, 1, 0)

    # Must be converted to DataFrame, same as others.
    nan = pd.Series(nan)

    # Combine again to get the index right
    combine2 = pd.concat([dof, dor, sed, use, rdd, urb, nan], axis=1)

    # Find index with highest value
    # Only the first index value will be returned. So if two indicators
    # have the same value, the first one in the list becomes the dominant
    # one. This slightly favours DOF instead of DOR and RDD instead of urb
    dom = combine2.idxmax(axis=1, skipna=True)

    dom[dom == 0] = "DOF"
    dom[dom == 1] = "DOR"
    dom[dom == 2] = "SED"
    dom[dom == 3] = "USE"
    dom[dom == 4] = "RDD"
    dom[dom == 5] = "URB"
    dom[dom == 6] = "NAN"

    streams_array[dom_field_name] = dom

    return streams_array
