"""

Launching script that cycles through a set of tasks as defined in the sheet 'START' in the Excel
workbook 'confix.xlsx'

The script prepares an analysis environment by creating a output directory for the specific
model run with a name using the current date.

It backups the model code into the directory and creates output geodatabases, and sub-folders for
statistics and an excel sheet that stores results

"""

import datetime
import logging
import os
import shutil
import sys

import tools.helper as tools
from config import config

fd = config.var

from scripts import ffr_run_dof, ffr_run_sed, ffr_run_dor, ffr_run_csi


def start(stamp, sequence, para, scenarios, st_flds, paths):
    """
    Starts the assessment and executes the scripts indicated in the Excel file

    :param stamp: time stamp
    :param sequence: list of scripts and weather they should run
    :param para: dictionary of parameters from Excel file
    :param scenarios: List of scenarios from Excel file
    :param st_flds: required fields in stream database
    :param paths: dictionary of paths as defined by setup function
    :return:
    """

    if sequence == ["NO", "NO", "NO", "NO"]:
        print ("Nothing to run. Exit!")
        sys.exit(0)
    else:
        pass

    dof = 0
    dor = 0
    sed = 0
    csi = 0

    if sequence["run_dof"] == "YES":
        prt("*********")
        prt("RUN DOF")
        prt("*********" + '\n')

        dof = ffr_run_dof.run_dof(stamp, para, paths)

    if sequence["run_dor"] == "YES":
        prt("*********")
        prt("RUN DOR")
        prt("*********" + '\n')

        dor = ffr_run_dor.run_dor(stamp, para, paths)

    if sequence["run_sed"] == "YES":
        prt("*********")
        prt("RUN SED")
        prt("*********" + '\n')

        sed = ffr_run_sed.run_sed(para, paths)

    # Atualizando as colunas com os novos valores para o cálculo do CSI
        
    para['streams_fc']["DOF"] = dof
    para['streams_fc']["DOR"] = dor
    para['streams_fc']["SED"] = sed

    if sequence["run_csi"] == "YES":
        prt("*********")
        prt("RUN CSI")
        prt("*********" + '\n')

        ffr_run_csi.run_csi(stamp, para, scenarios, st_flds, paths)
    
    print("Fim")


def setup(base, out, xls_full, stamp, xls_file_name):
    """
    Setup assessment

    :param base: local path of assessment from where the script is launched
    :param out: output folder
    :param xls_full: full path and file name of config.xls file
    :param stamp: time stamp
    :param xls_file_name: file name of excel file
    :return:
    """
    # Create CSI geodatabase
    gpkg_name = "CSI"
    gpkg_full_path, gpkg_file_name = tools.create_gpkg(out, gpkg_name)

    # Make statistics folder, where excel files are stored
    sta_folder = os.path.join(out, "STAT")
    tools.create_path(sta_folder)

    # Make temp csi global_stats.py folder
    sta_csi_folder = os.path.join(out, "STATS_CSI")
    tools.create_path(sta_csi_folder)

    # Make temp csi global_stats.py folder
    sta_pickle_folder = os.path.join(out, "STATS_PICKLES")
    tools.create_path(sta_pickle_folder)

    # Make test_data pickle folder
    test_pickle_folder = os.path.join(out, "TEST_PICKLES")
    tools.create_path(test_pickle_folder)

    # Copy Excel-file into results folder for documentation
    shutil.copy(xls_full, out + "\\" + xls_file_name)

    ########### its realy necessary? I'm generate a .exe program

    # Copy entire code directory into output folder for reference
    # cde_folder = os.path.join(out, "CODE")
    # tools.create_path(cde_folder)
    # tools.copytree(src=base, dst=cde_folder)

    # Copy Python code into results folder for documentation
    # python_code = os.path.abspath(__file__)
    # tail = os.path.basename(python_code)
    # shutil.copy(xls_full, out + "\\" + tail)

    ############################################################

    # Setup logging
    tools.setup_logging(out)

    # Create Excel writer
    writer, excel_file = tools.get_writer(sta_folder, stamp)
    tools.create_results_sheet(writer)

    paths = {"writer": writer,
             "excel_file": excel_file,
             "gpkg_full_path": gpkg_full_path,
             "sta_csi_folder": sta_csi_folder,
             "sta_pickle_folder": sta_pickle_folder,
             "test_pickle_folder": test_pickle_folder}

    return paths


def prt(txt):
    logging.info(txt)
    print(txt)


def main(xlsx_file:str, path_process_files:str):
    time_stamp = tools.get_stamp()

    print("Starting model at {}".format(datetime.datetime.now()))

    path = os.path.realpath(__file__)
    basepath, filename = os.path.split(path)

    xls_path, xls_filename = os.path.split(xlsx_file)

    # Loading model parameters from EXCEL file
    sequence, para, scenarios, fields = tools.load_parameters(xlsx_file, path_process_files)

    # Create a time-stamped folder under "output_folder"
    output_folder = os.path.join(para["output_folder"], "Results_" + time_stamp)

    # These fields must be present in source dataset feature class
    st_flds = [fd.REACH_ID, fd.GOID, fd.NOID, fd.NUOID, fd.NDOID,
               fd.CON_ID, fd.BAS_ID, fd.BAS_NAME, fd.RIV_ORD,
               fd.DIS_AV_CMS, fd.LENGTH_KM,
               fd.VOLUME_TCM, fd.ERO_YLD_TON, fd.HYFALL, fd.BB_ID, fd.BB_NAME,
               fd.BB_OCEAN,
               fd.BB_LEN_KM, fd.BB_VOL_TCM, fd.BB_DIS_ORD, fd.INC
               ] + fields + [fd.FLD]

    ba_flds = [para["svol_field"]]

    tools.check_fields(para["streams_fc"], st_flds)
    tools.check_fields(para["dams_fc"], ba_flds)

    # 1 Setting up file structure
    paths = setup(base=basepath,
                  out=output_folder,
                  xls_full=xlsx_file,
                  stamp=time_stamp,
                  xls_file_name=xls_filename)

    prt("Results will be in: " + str(output_folder))

    start(time_stamp, sequence, para, scenarios, st_flds, paths)


# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##

if __name__ == '__main__':

    path = os.path.realpath(__file__)
    basepath, filename = os.path.split(path)

    # Pasta com os dados pré processados
    path_process_files = r"test/SAIDA.gpkg"

    # Arquivo Excel
    xlsx_file = r"F:\Projetos\Hidrobr\Free-Flowing-Rivers\config.xlsx" #os.path.join(basepath + r"\\config", "config.xlsx")

    main(xlsx_file, path_process_files)
