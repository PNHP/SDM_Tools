#-------------------------------------------------------------------------------
# Name:        1_NetworkGroupingPrep.py
# Purpose:     This purpose of this script is to prep aquatic point line and
#              polygon data into a set of points that can be fed into the '2_NetworkGrouping.py'
#              script to group observations.
#
# Author:      Christopher Tracey
# Created:     2018-11-21
# Updates:
#-------------------------------------------------------------------------------

# load packages
import arcpy, os
from arcpy.na import *

# get parameters from arc tool
training_pt = arcpy.GetParameterAsText(0) # point layer of aquatic species
training_ln = arcpy.GetParameterAsText(1) # line layer of aquatic species
training_py = arcpy.GetParameterAsText(2) # polygon layer of aquatic species
flowline = arcpy.GetParameterAsText(3) # NHD flowline layer to snap to

# set environment settings
arcpy.env.overwriteOutput = True

# convert multipart points to singlepart - just to prevent downstream
arcpy.MultipartToSinglepart_management(training_pt, "training_pt_singlepart")

# convert line and polygon data to vertices
arcpy.FeatureVerticesToPoints_management(training_ln,"training_ln_vert", "ALL")
arcpy.FeatureVerticesToPoints_management(training_py,"training_py_vert", "ALL")

# merge the three point layers together
arcpy.Merge_management(["training_pt_singlepart", "training_ln_vert","training_py_vert"], "training_merge")

# snap the points to the flowline
arcpy.Snap_edit("training_merge",[[flowline,"Edge","40 Meters"]])

# delete Identical features
arcpy.DeleteIdentical_management("training_merge", ["Shape"])

# delete temporary fields and datasets
arcpy.Delete_management("training_pt_singlepart")
arcpy.Delete_management("training_ln_vert")
arcpy.Delete_management("training_py_vert")



