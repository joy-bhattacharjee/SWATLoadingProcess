# -*- coding: utf-8 -*-
# Importing all the required libraries
import numpy as np
import itertools
from math import sqrt,ceil
from skimage import feature
import pandas as pd
import fnmatch
import csv    
import shutil
import os
import matplotlib as plt

##################### Step-01: Loading Images, naming files #########################
## Function to list all the tif files in the input folder with path to folder as variable 
def ListFilesInFolder(pattern, folder):
    list_of_files = []
    pattern = pattern
    for path, subdirs, files in os.walk(folder):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                list_of_files.append(os.path.join(path, name))
    return list_of_files

input_folder=  r"#Provide input folder"

## Call the function ListFileInFolder and assign it to the variable Myfiles
Myfiles=ListFilesInFolder("*.rch", input_folder) # For sub and hru, need to use .sub or .hru
column_list = pd.read_csv(r"#Provide a csv file with all the column headers of an output rch file", header = None)
column_list_final = column_list[0]

clb_files = []
vld_files = []
for i in Myfiles:
    if i.split("\\")[-1].split("_")[-1] == "calib.rch":
        clb_files.append(i)
    else:
        vld_files.append(i)

out_folder = r"#Provide output folder"
os.chdir(out_folder)

# Creating a Function for Sed to extract load from the output.rch files for each rch. 
# Use chunksize for hru when calling the files from pd.read_fwf
def combination_sed(cal, vld, Reach_number):
    file_rch_clb = pd.read_fwf(cal, skiprows = 9, header =  None)
    file_rch_clb.columns = column_list_final
    file_rch_clb.index= file_rch_clb ["RCH"]
    file_rch_clb = file_rch_clb.drop(columns = ["Reach", "Extra"])
    
    file_rch_vld = pd.read_fwf(vld, skiprows = 9, header =  None)
    file_rch_vld.columns = column_list_final
    file_rch_vld.index= file_rch_vld ["RCH"]
    file_rch_vld = file_rch_vld.drop(columns = ["Reach", "Extra"])
    
    file_rch_clb_vld = file_rch_clb.append(file_rch_vld)
    
    Processed_col_sed = pd.DataFrame(file_rch_clb_vld, columns=['SED_OUTtons'])
                                                                
    Date = pd.date_range(start="1996/01/01", end="2015/12/31")
    
    Processed_col_sed_rch = Processed_col_sed[Processed_col_sed.index == Reach_number]
    Processed_col_sed_rch.index= Date
    
    Processed_col_sed_rch["Date"] = Date
    Processed_col_sed_rch ['Year'] = Processed_col_sed_rch.Date.dt.year
    Processed_col_sed_rch ['Month'] = Processed_col_sed_rch.Date.dt.month
    
    Processed_col_sed_rch_yearly_mean = Processed_col_sed_rch.groupby(['Year']).mean()
    Processed_col_sed_rch_yearly_mean.to_csv(cal.split("\\")[-1].split("_")[1] + "_" + str(Reach_number) +".csv")
    
    Processed_col_sed_rch_monthly_mean = Processed_col_sed_rch.groupby(['Month']).mean()
    Processed_col_sed_rch_monthly_mean.to_csv(cal.split("\\")[-1].split("_")[1] + "_" + str(Reach_number) +"_month.csv")

# Function for Org-N and TN
def combination_N(cal, vld, Reach_number):
    file_rch_clb = pd.read_fwf(cal, skiprows = 9, header =  None)
    file_rch_clb.columns = column_list_final
    file_rch_clb.index= file_rch_clb ["RCH"]
    file_rch_clb = file_rch_clb.drop(columns = ["Reach", "Extra"])
    
    file_rch_vld = pd.read_fwf(vld, skiprows = 9, header =  None)
    file_rch_vld.columns = column_list_final
    file_rch_vld.index= file_rch_vld ["RCH"]
    file_rch_vld = file_rch_vld.drop(columns = ["Reach", "Extra"])
    
    file_rch_clb_vld = file_rch_clb.append(file_rch_vld)
    
    Processed_col_N = pd.DataFrame(file_rch_clb_vld, columns=['ORGN_OUTkg','TOT Nkg'])
                                                                
    Date = pd.date_range(start="1996/01/01", end="2015/12/31")
       
    Processed_col_N_rch = Processed_col_N[Processed_col_N.index == Reach_number]
    Processed_col_N_rch.index= Date
    
    Processed_col_N_rch["Date"] = Date
    Processed_col_N_rch ['Year'] = Processed_col_N_rch.Date.dt.year
    Processed_col_N_rch ['Month'] = Processed_col_N_rch.Date.dt.month
    
    Processed_col_N_rch_yearly_mean = Processed_col_N_rch.groupby(['Year']).mean()
    Processed_col_N_rch_yearly_mean.to_csv(cal.split("\\")[-1].split("_")[1] + "_" + str(Reach_number) +".csv")
    
    Processed_col_N_rch_monthly_mean = Processed_col_N_rch.groupby(['Month']).mean()
    Processed_col_N_rch_monthly_mean.to_csv(cal.split("\\")[-1].split("_")[1] + "_" + str(Reach_number) +"_month.csv")
    
# Function for Org-P and TP
def combination_P(cal, vld, Reach_number):
    file_rch_clb = pd.read_fwf(cal, skiprows = 9, header =  None)
    file_rch_clb.columns = column_list_final
    file_rch_clb.index= file_rch_clb ["RCH"]
    file_rch_clb = file_rch_clb.drop(columns = ["Reach", "Extra"])
    
    file_rch_vld = pd.read_fwf(vld, skiprows = 9, header =  None)
    file_rch_vld.columns = column_list_final
    file_rch_vld.index= file_rch_vld ["RCH"]
    file_rch_vld = file_rch_vld.drop(columns = ["Reach", "Extra"])
    
    file_rch_clb_vld = file_rch_clb.append(file_rch_vld)
    
    Processed_col_P = pd.DataFrame(file_rch_clb_vld, columns=['ORGP_OUTkg','TOT Pkg'])
                                                                
    Date = pd.date_range(start="1996/01/01", end="2015/12/31")
       
    Processed_col_P_rch = Processed_col_P[Processed_col_P.index == Reach_number]
    Processed_col_P_rch.index= Date
    
    Processed_col_P_rch["Date"] = Date
    Processed_col_P_rch ['Year'] = Processed_col_P_rch.Date.dt.year
    Processed_col_P_rch ['Month'] = Processed_col_P_rch.Date.dt.month
    
    Processed_col_P_rch_yearly_mean = Processed_col_P_rch.groupby(['Year']).mean()
    Processed_col_P_rch_yearly_mean.to_csv(cal.split("\\")[-1].split("_")[1] + "_" + str(Reach_number) +".csv")
    
    Processed_col_P_rch_monthly_mean = Processed_col_P_rch.groupby(['Month']).mean()
    Processed_col_P_rch_monthly_mean.to_csv(cal.split("\\")[-1].split("_")[1] + "_" + str(Reach_number) +"_month.csv")
    
# Creating Reach list
Reach_list = list(range(1,112,1))

# Saving all the loading for each reach
for i in range(len(clb_files)):
    for j in range(len(vld_files)):
        if ((i == j) and (clb_files[i].split("\\")[-1].split("_")[1]== "sed")):
            for k in range(len(Reach_list)):
                combination_sed(clb_files[i],vld_files[j], Reach_list[k])
        if ((i == j) and (clb_files[i].split("\\")[-1].split("_")[1]== "TN")):
            for k in range(len(Reach_list)):
                combination_N(clb_files[i],vld_files[j], Reach_list[k])
        if ((i == j) and (clb_files[i].split("\\")[-1].split("_")[1]== "TP")):
            for k in range(len(Reach_list)):
                    combination_P(clb_files[i],vld_files[j], Reach_list[k])
