#-------------------------------------------------------------------------------
# Name:        network_grouping.py
# Purpose:     This purpose of this script is to group aquatic observations that
#              are within a user-specified separation distance along a river
#              network.
#
# Author:      Molly Moore
#
# Created:     13/09/2018
#-------------------------------------------------------------------------------

#load packages
import arcpy, os
from arcpy.na import *

#get parameters from arc tool
species_pts = arcpy.GetParameterAsText(0)
network = arcpy.GetParameterAsText(1)
dams = arcpy.GetParameterAsText(2)
sep_dist = arcpy.GetParameterAsText(3)
scratchGeodatabase = "in_memory"

#set environment settings
arcpy.CheckOutExtension("Network")
arcpy.env.overwriteOutput = True

#calculate separation distance to be used in tools. use half of original minus
#1 to account for 1 meter buffer and overlapping buffers
sep_dist = int(sep_dist)
sep_dist = (sep_dist/2)-1

#create temporary unique id for use in join field later
i=1
fieldnames = [field.name for field in arcpy.ListFields(species_pts)]
if 'temp_join_id' not in fieldnames:
    arcpy.AddField_management(species_pts,"temp_join_id","LONG")
    with arcpy.da.UpdateCursor(species_pts,"temp_join_id") as cursor:
        for row in cursor:
            row[0] = i
            cursor.updateRow(row)
            i+=1

#create service area line layer
service_area_lyr = arcpy.na.MakeServiceAreaLayer(network,os.path.join(scratchGeodatabase,"service_area_temp"),"Length","TRAVEL_FROM",sep_dist,polygon_type="NO_POLYS",line_type="TRUE_LINES",overlap="OVERLAP")
service_area_lyr = service_area_lyr.getOutput(0)
subLayerNames = arcpy.na.GetNAClassNames(service_area_lyr)
facilitiesLayerName = subLayerNames["Facilities"]
serviceLayerName = subLayerNames["SALines"]
arcpy.na.AddLocations(service_area_lyr, facilitiesLayerName, species_pts, "", "")
arcpy.na.Solve(service_area_lyr)
lines = arcpy.mapping.ListLayers(service_area_lyr,serviceLayerName)[0]
flowline_clip = arcpy.CopyFeatures_management(lines,os.path.join(scratchGeodatabase,"service_area"))

#buffer clipped flowlines by 1 meter
flowline_buff = arcpy.Buffer_analysis(flowline_clip,os.path.join(scratchGeodatabase,"flowline_buff"),"1 Meter","FULL","ROUND")

#dissolve flowline buffers
flowline_diss = arcpy.Dissolve_management(flowline_buff,os.path.join(scratchGeodatabase,"flowline_diss"),multi_part="SINGLE_PART")

if dams:
    #buffer dams by 1.1 meters
    dam_buff = arcpy.Buffer_analysis(dams,os.path.join(scratchGeodatabase,"dam_buff"),"1.1 Meter","FULL","FLAT")
    #split flowline buffers at dam buffers by erasing area of dam
    flowline_erase = arcpy.Erase_analysis(flowline_diss,dam_buff,os.path.join(scratchGeodatabase,"flowline_erase"))
    multipart_input = flowline_erase
else:
    multipart_input = flowline_diss

#multi-part to single part to create unique polygons
single_part = arcpy.MultipartToSinglepart_management(multipart_input,os.path.join(scratchGeodatabase,"single_part"))


#create unique group id
arcpy.AddField_management(single_part,"groups","LONG")
num = 1
with arcpy.da.UpdateCursor(single_part,"groups") as cursor:
    for row in cursor:
        row[0] = num
        cursor.updateRow(row)
        num+=1

#join group id of buffered flowlines to closest points
s_join = arcpy.SpatialJoin_analysis(target_features=species_pts, join_features=single_part, out_feature_class=os.path.join(scratchGeodatabase,"s_join"), join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", match_option="CLOSEST", search_radius="5000 Meters", distance_field_name="")

#join field to original dataset
join_field = [field.name for field in arcpy.ListFields(s_join)]
join_field = join_field[-1]
arcpy.JoinField_management(species_pts,"temp_join_id",s_join,"temp_join_id",join_field)

#delete temporary fields and datasets
arcpy.DeleteField_management(species_pts,"temp_join_id")
arcpy.Delete_management("in_memory")

##delete_list = [os.path.join(scratchGeodatabase,"single_part"),os.path.join(scratchGeodatabase,"dam_buff"),os.path.join(scratchGeodatabase,"flowline_erase"),os.path.join(scratchGeodatabase,"flowline_diss"),os.path.join(scratchGeodatabase,"flowline_buff"),os.path.join(scratchGeodatabase,"flowline_clip"),os.path.join(scratchGeodatabase,"service_area"),os.path.join(scratchGeodatabase,"s_join")]
##for l in delete_list:
##    arcpy.Delete_management(l)