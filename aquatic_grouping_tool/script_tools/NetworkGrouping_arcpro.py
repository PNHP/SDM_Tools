#-------------------------------------------------------------------------------
# Name:        network_grouping.py
# Purpose:     This purpose of this script is to group aquatic observations that
#              are within a user-specified separation distance along a river
#              network.
#
# Author:      Molly Moore
# Created:     13/09/2018
# Updates:
#-------------------------------------------------------------------------------

#load packages
import arcpy, os
from arcpy.na import *

#get parameters from arc tool
species_pt = arcpy.GetParameterAsText(0) #point layer of aquatic species observations
species_ln = arcpy.GetParameterAsText(1) #line layer of aquatic species observations
species_py = arcpy.GetParameterAsText(2) #polygon layer of aquatic species observations
flowlines = arcpy.GetParameterAsText(3)
catchments = arcpy.GetParameterAsText(4)
network = arcpy.GetParameterAsText(5) #network dataset created from river flowlines
dams = arcpy.GetParameterAsText(6) #optional - point layer representing barriers (has to be snapped to network river flowlines)
sep_dist = arcpy.GetParameterAsText(7) #separation distance
snap_dist = arcpy.GetParameterAsText(8) #distance to flowline beyond which observations are not included
output_lines = arcpy.GetParameterAsText(9) #output presence flowlines
scratchGeodatabase = "in_memory" #set scratch database to in memory workspace

#set environment settings and check out extensions
arcpy.CheckOutExtension("Network")
arcpy.env.overwriteOutput = True
arcpy.env.qualifiedFieldNames = False

# create empty list to store converted point layers for future merge
species_lyrs = []

# convert multipart points to singlepart - just to prevent downstream
if species_pt:
    pts = arcpy.MultipartToSinglepart_management(species_pt, "pts")
    species_lyrs.append(pts)
else:
    pass

# convert line and polygon data to vertices
if species_ln:
    lns = arcpy.FeatureVerticesToPoints_management(species_ln,"lns", "ALL")
    species_lyrs.append(lns)
else:
    pass
if species_py:
    pys = arcpy.FeatureVerticesToPoints_management(species_py,"polys", "ALL")
    species_lyrs.append(pys)
else:
    pass

# merge the point layers together
species_pt = arcpy.Merge_management([pts,lns,pys],"species_pt")

### snap the points to the flowline if within 40 meters
##species_pt = arcpy.Snap_edit(species_pt,[network,"EDGE","40 Meters"])

# delete Identical features
species_pt = arcpy.DeleteIdentical_management(species_pt, ["Shape"])

#calculate separation distance to be used in tools. use half of original minus
#1 to account for 1 meter buffer and overlapping buffers
sep_dist = int(sep_dist)
sep_dist = (sep_dist/2)-1

#create temporary unique id for use in join field later
i=1
fieldnames = [field.name for field in arcpy.ListFields(species_pt)]
if 'temp_join_id' not in fieldnames:
    arcpy.AddField_management(species_pt,"temp_join_id","LONG")
    with arcpy.da.UpdateCursor(species_pt,"temp_join_id") as cursor:
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
arcpy.na.AddLocations(service_area_lyr, facilitiesLayerName, species_pt, "", snap_dist)
arcpy.na.Solve(service_area_lyr)
lines = service_area_lyr.listLayers(serviceLayerName)[0]
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
arcpy.AddField_management(single_part,"GROUP_ID","LONG")
num = 1
with arcpy.da.UpdateCursor(single_part,"GROUP_ID") as cursor:
    for row in cursor:
        row[0] = num
        cursor.updateRow(row)
        num+=1

#join group id of buffered flowlines to closest points
s_join = arcpy.SpatialJoin_analysis(target_features=species_pt, join_features=single_part, out_feature_class=os.path.join(scratchGeodatabase,"s_join"), join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", match_option="CLOSEST", search_radius="5000 Meters", distance_field_name="")

#join field to original dataset
join_field = [field.name for field in arcpy.ListFields(s_join)]
join_field = join_field[-1]
arcpy.JoinField_management(species_pt,"temp_join_id",s_join,"temp_join_id",join_field)

#delete null groups to get rid of observations that were beyond snap_dist
with arcpy.da.UpdateCursor(species_pt,join_field) as cursor:
    for row in cursor:
        if row[0] is None:
            cursor.deleteRow()

sp_join = arcpy.SpatialJoin_analysis(species_pt,catchments,"sp_join","JOIN_ONE_TO_ONE","KEEP_COMMON","","INTERSECT")
sp_join = arcpy.DeleteIdentical_management(sp_join,["GROUP_ID","COMID"])

with arcpy.da.SearchCursor(sp_join,"COMID") as cursor:
    comid = sorted({row[0] for row in cursor})

expression = 'COMID IN ({0})'.format(','.join(str(x) for x in comid))
flowlines_lyr = arcpy.MakeFeatureLayer_management(flowlines,"flowlines_lyr",expression)
arcpy.AddJoin_management(flowlines_lyr,"COMID",sp_join,"COMID")
arcpy.CopyFeatures_management(flowlines_lyr,output_lines)

#delete temporary fields and datasets
arcpy.DeleteField_management(species_pt,"temp_join_id")
arcpy.Delete_management("in_memory")