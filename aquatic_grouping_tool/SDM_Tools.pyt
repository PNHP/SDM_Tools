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

#import libraries
import arcpy, os
from arcpy.na import *

#set environment settings and check out extensions
arcpy.CheckOutExtension("Network")
arcpy.env.overwriteOutput = True
arcpy.env.qualifiedFieldNames = False
arcpy.env.workspace = "in_memory"

class Toolbox(object):
    def __init__(self):
        self.label = "SDM Tools"
        self.alias = "SDM Tools"

        # List of tool classes assocaited with this toolbox
        self.tools = [AquaticGrouping]

class AquaticGrouping(object):
    def __init__(self):
        self.label = "Aquatic Network Grouping"
        self.description = """The aquatic grouping tool assigns a unique group to observation points that fall within a user-specified separation distance from one another measured along a line."""
        self.canRunInBackground = False

    def getParameterInfo(self):
        software = arcpy.Parameter(
            displayName = "What ArcGIS software are you using?",
            name = "software",
            datatype = "GPString",
            parameterType = "Required",
            direction = "Input")
        software.filter.type = "ValueList"
        software.filter.list = ["ArcMap","ArcGIS Pro"]


        species_pt = arcpy.Parameter(
            displayName = "Point layer of aquatic species observations",
            name = "species_pt",
            datatype = "GPFeatureLayer",
            parameterType = "Optional",
            direction = "Input")

        species_ln = arcpy.Parameter(
            displayName = "Line layer of aquatic species observations",
            name = "species_ln",
            datatype = "GPFeatureLayer",
            parameterType = "Optional",
            direction = "Input")

        species_py = arcpy.Parameter(
            displayName = "Polygon layer of aquatic species observations",
            name = "species_py",
            datatype = "GPFeatureLayer",
            parameterType = "Optional",
            direction = "Input")

        flowlines = arcpy.Parameter(
            displayName = "NHD flowlines",
            name = "flowlines",
            datatype = "GPFeatureLayer",
            parameterType = "Required",
            direction = "Input")

        catchments = arcpy.Parameter(
            displayName = "NHD catchments",
            name = "catchments",
            datatype = "GPFeatureLayer",
            parameterType = "Required",
            direction = "Input")

        network = arcpy.Parameter(
            displayName = "Network dataset built on NHD flowlines",
            name = "network",
            datatype = "GPNetworkDatasetLayer",
            parameterType = "Required",
            direction = "Input")

        dams = arcpy.Parameter(
            displayName = "Dams/barrier points (must be snapped to NHD flowlines)",
            name = "dams",
            datatype = "GPFeatureLayer",
            parameterType = "Optional",
            direction = "Input")

        sep_dist = arcpy.Parameter(
            displayName = "Separation distance",
            name = "sep_dist",
            datatype = "GPString",
            parameterType = "Required",
            direction = "Input")

        snap_dist = arcpy.Parameter(
            displayName = "Snap distance in meters (distance to flowline beyond which observations are thrown out)",
            name = "snap_dist",
            datatype = "GPString",
            parameterType = "Required",
            direction = "Input")
        snap_dist.value = "100"

        output_lines = arcpy.Parameter(
            displayName = "Ouput presence flowlines for QC",
            name = "output_lines",
            datatype = "DEFeatureClass",
            parameterType = "Required",
            direction = "Output")

        params = [software,species_pt,species_ln,species_py,flowlines,catchments,network,dams,sep_dist,snap_dist,output_lines]
        return params

    def isLicensed(self):
        return True

    def updateParameters(self,params):
        return

    def updateMessages(self,params):
        return

    def execute(self,params,messages):
        software = params[0].valueAsText #what software is being used
        species_pt = params[1].valueAsText #point layer of aquatic species observations
        species_ln = params[2].valueAsText #line layer of aquatic species observations
        species_py = params[3].valueAsText #polygon layer of aquatic species observations
        flowlines = params[4].valueAsText #NHD flowlines
        catchments = params[5].valueAsText #NHD catchments
        network = params[6].valueAsText #network dataset created from river flowlines
        dams = params[7].valueAsText #optional - point layer representing barriers (has to be snapped to network river flowlines)
        sep_dist = params[8].valueAsText #separation distance
        snap_dist = params[9].valueAsText #distance to flowline beyond which observations are not included
        output_lines = params[10].valueAsText #output presence flowlines

        # create empty list to store converted point layers for future merge
        species_lyrs = []

        # convert multipart points to singlepart
        if species_pt:
            pts = arcpy.MultipartToSinglepart_management(species_pt, "pts")
            species_lyrs.append(pts)

        # convert line and polygon data to vertices
        if species_ln:
            lns = arcpy.FeatureVerticesToPoints_management(species_ln,"lns", "ALL")
            species_lyrs.append(lns)

        if species_py:
            pys = arcpy.FeatureVerticesToPoints_management(species_py,"polys", "ALL")
            species_lyrs.append(pys)

        # merge the point layers together
        species_pt = arcpy.Merge_management([pts,lns,pys],"species_pt")

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

        #delete identical points with tolerance to increase speed
        arcpy.DeleteIdentical_management(species_pt,"Shape","35 Meters")

        arcpy.AddMessage("Creating service area line layer")
        #create service area line layer
        service_area_lyr = arcpy.na.MakeServiceAreaLayer(network,"service_area_lyr","Length","TRAVEL_FROM",sep_dist,polygon_type="NO_POLYS",line_type="TRUE_LINES",overlap="OVERLAP")
        service_area_lyr = service_area_lyr.getOutput(0)
        subLayerNames = arcpy.na.GetNAClassNames(service_area_lyr)
        facilitiesLayerName = subLayerNames["Facilities"]
        serviceLayerName = subLayerNames["SALines"]
        arcpy.na.AddLocations(service_area_lyr, facilitiesLayerName, species_pt, "", snap_dist)
        arcpy.na.Solve(service_area_lyr)
        if software.lower() == "arcmap":
            lines = arcpy.mapping.ListLayers(service_area_lyr,serviceLayerName)[0]
        if software.lower() == "arcgis pro":
            lines = service_area_lyr.listLayers(serviceLayerName)[0]
        flowline_clip = arcpy.CopyFeatures_management(lines,"service_area")

        arcpy.AddMessage("Buffering service area flowlines")
        #buffer clipped service area flowlines by 1 meter
        flowline_buff = arcpy.Buffer_analysis(flowline_clip,"flowline_buff","1 Meter","FULL","ROUND")

        arcpy.AddMessage("Dissolving service area polygons")
        #dissolve flowline buffers
        flowline_diss = arcpy.Dissolve_management(flowline_buff,"flowline_diss",multi_part="SINGLE_PART")

        #separate buffered flowlines at dams
        if dams:
            #buffer dams by 1.1 meters
            dam_buff = arcpy.Buffer_analysis(dams,"dam_buff","1.1 Meter","FULL","FLAT")
            #split flowline buffers at dam buffers by erasing area of dam
            flowline_erase = arcpy.Erase_analysis(flowline_diss,dam_buff,"flowline_erase")
            multipart_input = flowline_erase
        else:
            multipart_input = flowline_diss

        #multi-part to single part to create unique polygons
        single_part = arcpy.MultipartToSinglepart_management(multipart_input,"single_part")


        #create unique group id
        arcpy.AddField_management(single_part,"GROUP_ID","LONG")
        num = 1
        with arcpy.da.UpdateCursor(single_part,"GROUP_ID") as cursor:
            for row in cursor:
                row[0] = num
                cursor.updateRow(row)
                num+=1

        #join group id of buffered flowlines to closest points
        s_join = arcpy.SpatialJoin_analysis(target_features=species_pt, join_features=single_part, out_feature_class="s_join", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", match_option="CLOSEST", search_radius=snap_dist, distance_field_name="")

        #join field to original dataset
        join_field = [field.name for field in arcpy.ListFields(s_join)]
        join_field = join_field[-1]
        arcpy.JoinField_management(species_pt,"temp_join_id",s_join,"temp_join_id",join_field)

        #delete null groups to get rid of observations that were beyond snap_dist
        with arcpy.da.UpdateCursor(species_pt,join_field) as cursor:
            for row in cursor:
                if row[0] is None:
                    cursor.deleteRow()

        arcpy.AddMessage("Joining COMID")
        #join species_pt layer with catchments to assign COMID
        sp_join = arcpy.SpatialJoin_analysis(species_pt,catchments,"sp_join","JOIN_ONE_TO_ONE","KEEP_COMMON","","INTERSECT")
        sp_join = arcpy.DeleteIdentical_management(sp_join,["GROUP_ID","COMID"])

        #obtain list of duplicate COMID because these are reaches assigned to multiple groups
        freq = arcpy.Frequency_analysis(sp_join,"freq","COMID")
        dup_comid = []
        with arcpy.da.SearchCursor(freq,["FREQUENCY","COMID"]) as cursor:
            for row in cursor:
                if row[0] > 1:
                    dup_comid.append(row[1])

        arcpy.AddMessage("Resolving same reaches assigned to different groups")
        #get all groups within duplicate reaches and assign them to a single group
        sp_join_lyr = arcpy.MakeFeatureLayer_management(sp_join,"sp_join_lyr")
        if dup_comid:
            for dup in dup_comid:
                arcpy.SelectLayerByAttribute_management(sp_join_lyr,"NEW_SELECTION","COMID = {0}".format(dup))
                combine_groups = []
                with arcpy.da.SearchCursor(sp_join_lyr,["GROUP_ID"]) as cursor:
                    for row in cursor:
                        combine_groups.append(row[0])
                arcpy.SelectLayerByAttribute_management(sp_join_lyr,"NEW_SELECTION","GROUP_ID IN ({0})".format(','.join(str(x) for x in combine_groups)))
                with arcpy.da.UpdateCursor(sp_join_lyr,["GROUP_ID"]) as cursor:
                    for row in cursor:
                        row[0] = num
                        cursor.updateRow(row)
                num += 1

        #clear selection on layer
        arcpy.SelectLayerByAttribute_management(sp_join_lyr,"CLEAR_SELECTION")

        #get list of COMID values for export of flowlines
        with arcpy.da.SearchCursor(sp_join_lyr,"COMID") as cursor:
            comid = sorted({row[0] for row in cursor})
        comid = list(set(comid))

        #join attributes to flowlines
        expression = 'COMID IN ({0})'.format(','.join(str(x) for x in comid))
        flowlines_lyr = arcpy.MakeFeatureLayer_management(flowlines,"flowlines_lyr",expression)
        arcpy.AddJoin_management(flowlines_lyr,"COMID",sp_join,"COMID")

        #export presence flowlines
        arcpy.CopyFeatures_management(flowlines_lyr,output_lines)

        #delete temporary fields and datasets
        arcpy.Delete_management("in_memory")
        return