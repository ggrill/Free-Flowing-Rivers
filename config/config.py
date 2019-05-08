class var:
    """
    Class variables to be used throughout the script.
    For description of fields see documentation file at https://doi.org/10.6084/m9.figshare.7688801

    """

    #################
    #  Barrier fields
    #################

    # Storage volume in million cubic meters (MCM)

    STOR_MCM = "STOR_MCM"

    # Discharge factor upstream (if used)
    DFU = "DFU"
    # Discharge factor downstream (if used)
    DFD = "DFD"

    #########################
    #  Benchmark table fields
    #########################

    # Free-flowing river benchmark id
    FFRID = "FFRID"

    # Benchmark source (e.g. study, expert)
    BENCH_SRC = "BENCH_SRC"
    Name_Expert = "Name_Expert"

    #####################
    # River reach fields
    #####################

    REACH_ID = "REACH_ID"

    GOID = "GOID"
    NOID = "NOID"
    NDOID = "NDOID"
    NUOID = "NUOID"

    # Continent ID
    CON_ID = "CON_ID"

    # Continent Name
    CONTINENT = "CONTINENT"

    # Country name
    COUNTRY = "COUNTRY"

    # Basin ID
    BAS_ID = "BAS_ID"

    # Basin Name
    BAS_NAME = "BAS_NAME"

    # Length of the river reach in kilometers
    LENGTH_KM = "LENGTH_KM"

    # Volume of river reach in thousand cubic meters
    VOLUME_TCM = "VOLUME_TCM"

    # Long-term average discharge in cubic meters per second
    DIS_AV_CMS = "DIS_AV_CMS"

    # Discharge river order of river reach (log10 of discharge)
    RIV_ORD = "RIV_ORD"

    # Field indicating if there is a waterfall present (1) or not (0)
    HYFALL = "HYFALL"

    # Upland area in square kilometers
    UPLAND_SKM = "UPLAND_SKM"

    # Percent of river reach catchment covered with floodplains
    FLD = "FLD"

    #######################
    # Backbone river fields
    #######################

    # The fields below define the river reach as part of a 'backbone river'
    # Backbone ID
    BB_ID = "BB_ID"

    # River Name
    BB_NAME = "BB_NAME"

    # River connected to ocean (1) or not (0)
    BB_OCEAN = "BB_OCEAN"

    # Total length and volume_mcm (sum) of the backbone river
    BB_LEN_KM = "BB_LEN_KM"
    BB_VOL_TCM = "BB_VOL_TCM"

    # Discharge river order of most downstream river reach of backbone river (log10 of discharge)
    BB_DIS_ORD = "BB_DIS_ORD"

    # used as a temporaray field for filter procedure
    CAT_FFR_FILTER = "CAT_FFR_FILTER"

    # Defines if river reach is included in assessment
    # 1 = yes
    # 0 = no
    INC = "INC"

    #

    ###############################
    # Sediment Index Calculations
    ###############################

    # Existing sediment yield of all pixels in reach catchment
    ERO_YLD_TON = "ERO_YLD_TON"

    ###############################
    # Field to create and calculate
    ###############################

    SED_LSS_LKS_OT_NAT = "SED_LSS_LKS_OT_NAT"

    SED_LSS_DMS_ANT = "SED_LSS_DMS_ANT"

    # Total accumulated sediment load taking into account lakes
    SED_NAT = "SED_NAT"
    SED_NAT_UP = "SED_NAT_UP"
    SED_LSS_LKS_IN_NAT = "SED_LSS_LKS_IN_NAT"

    # Total accumulated sediment load taking into account lakes and dams
    SED_ANT = "SED_ANT"
    SED_ANT_UP = "SED_ANT_UP"
    SED_LSS_LKS_OT_ANT = "SED_LSS_LKS_OT_ANT"
    SED_LSS_LKS_IN_ANT = "SED_LSS_LKS_IN_ANT"

    # Total sediment loss
    SED_LSS_TOT = "SED_LSS_TOT"

    # Sediment Trapping Index
    SED = "SED"

    def __init__(self):
        pass
