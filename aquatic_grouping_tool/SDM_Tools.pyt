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
import arcpy, os, pandas
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
        self.tools = [AquaticGrouping,ExportCSV]

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
            displayName = "Input layers of aquatic species observations",
            name = "species_pt",
            datatype = "GPFeatureLayer",
            parameterType = "Optional",
            direction = "Input",
            multiValue = True)

        species_code = arcpy.Parameter(
            displayName = "Species identifier field used in input layers",
            name = "species_code",
            datatype = "Field",
            parameterType = "Required",
            direction = "Input")
        species_code.parameterDependencies = [species_pt.name]

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

        output_db = arcpy.Parameter(
            displayName = "Ouput database where presence flowlines will be written",
            name = "output_lines",
            datatype = "DEWorkspace",
            parameterType = "Required",
            direction = "Input")

        params = [software,species_pt,species_code,flowlines,catchments,network,dams,sep_dist,snap_dist,output_db]
        return params

    def isLicensed(self):
        return True

    def updateParameters(self,params):
        return

    def updateMessages(self,params):
        return

    def execute(self,params,messages):
        software = params[0].valueAsText #what software is being used
        species_in = params[1].valueAsText #point layer of aquatic species observations
        species_code = params[2].valueAsText
        flowlines = params[3].valueAsText #NHD flowlines
        catchments = params[4].valueAsText #NHD catchments
        network = params[5].valueAsText #network dataset created from river flowlines
        dams = params[6].valueAsText #optional - point layer representing barriers (has to be snapped to network river flowlines)
        sep_dist = params[7].valueAsText #separation distance
        snap_dist = params[8].valueAsText #distance to flowline beyond which observations are not included
        output_db = params[9].valueAsText #output presence flowlines

        data_in = species_in.split(';')
        data_out = []
        counter = 1
        for d in data_in:
            d_out = "out_"+str(counter)
            data_out.append(d_out)
            counter+=1

        join_id = 1
        for i,o in zip(data_in,data_out):
            if len(arcpy.ListFields(i,"temp_join_id")) == 0:
                arcpy.AddField_management(i,"temp_join_id","TEXT")
            with arcpy.da.UpdateCursor(i,"temp_join_id") as cursor:
                for row in cursor:
                    row[0]=str(join_id)
                    cursor.updateRow(row)
                    join_id+=1
            arcpy.FeatureVerticesToPoints_management(i,o,"ALL")

        # merge the point layers together
        species_pt = arcpy.Merge_management(data_out,os.path.join(output_db,"species_pt"))

        #calculate separation distance to be used in tools. use half of original minus
        #1 to account for 1 meter buffer and overlapping buffers
        sep_dist = int(sep_dist)
        sep_dist = (sep_dist/2)-2

        with arcpy.da.SearchCursor(species_pt,species_code) as cursor:
            species_list = sorted({row[0] for row in cursor})

        total_species = len(species_list)
        species_rep = 1
        group_id = 1

        for species in species_list:
            arcpy.AddMessage("Assigning groups based on separation distance for "+str(species_rep)+"/"+str(total_species)+": "+str(species))
            species_rep+=1
            s = arcpy.FeatureClassToFeatureClass_conversion(species_pt,output_db,"s","{}='{}'".format(species_code,species))

            #delete identical points with tolerance to increase speed
            arcpy.DeleteIdentical_management(s,"Shape","35 Meters")

            #arcpy.AddMessage("Creating service area line layer for " +str(species) + " separation distance grouping")
            service_area_lyr = arcpy.na.MakeServiceAreaLayer(network,"service_area_lyr","Length","TRAVEL_FROM",sep_dist,polygon_type="NO_POLYS",line_type="TRUE_LINES",overlap="OVERLAP")
            service_area_lyr = service_area_lyr.getOutput(0)
            subLayerNames = arcpy.na.GetNAClassNames(service_area_lyr)
            facilitiesLayerName = subLayerNames["Facilities"]
            serviceLayerName = subLayerNames["SALines"]
            arcpy.na.AddLocations(service_area_lyr, facilitiesLayerName, s, "", snap_dist)
            arcpy.na.Solve(service_area_lyr)
            if software.lower() == "arcmap":
                lines = arcpy.mapping.ListLayers(service_area_lyr,serviceLayerName)[0]
            if software.lower() == "arcgis pro":
                lines = service_area_lyr.listLayers(serviceLayerName)[0]
            flowline_clip = arcpy.CopyFeatures_management(lines,"service_area")

            #buffer clipped service area flowlines by 1 meter
            flowline_buff = arcpy.Buffer_analysis(flowline_clip,"flowline_buff","1 Meter","FULL","ROUND")
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
            single_part = arcpy.MultipartToSinglepart_management(multipart_input,os.path.join(output_db,"single_part"))

            #create unique group id
            arcpy.AddField_management(single_part,"group_id","LONG")
            num = 1
            with arcpy.da.UpdateCursor(single_part,"group_id") as cursor:
                for row in cursor:
                    row[0] = num
                    cursor.updateRow(row)
                    num+=1

            #join group id of buffered flowlines to closest points
            s_join = arcpy.SpatialJoin_analysis(target_features=s, join_features=single_part, out_feature_class=os.path.join(output_db,"s_join"), join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", match_option="CLOSEST", search_radius=snap_dist, distance_field_name="")

            #join species_pt layer with catchments to assign COMID
            sp_join = arcpy.SpatialJoin_analysis(s_join,catchments,os.path.join(output_db,"sp_join"),"JOIN_ONE_TO_ONE","KEEP_COMMON","","INTERSECT")
            arcpy.DeleteIdentical_management(sp_join,["group_id","FEATUREID"])
            if len(arcpy.ListFields(sp_join,"COMID")) == 0:
                arcpy.AddField_management(sp_join,"COMID","LONG")
                with arcpy.da.UpdateCursor(sp_join,["FEATUREID","COMID"]) as cursor:
                    for row in cursor:
                        row[1] = row[0]
                        cursor.updateRow(row)

            del_features = 0
            with arcpy.da.UpdateCursor(sp_join,"group_id") as cursor:
                for row in cursor:
                    if row[0] is None:
                        del_features+=1
                        cursor.deleteRow()
            arcpy.AddMessage(del_features+" features were not included because they were outside the snap_distance")

            #obtain list of duplicate COMID because these are reaches assigned to multiple groups
            freq = arcpy.Frequency_analysis(sp_join,"freq","COMID")
            dup_comid = []
            with arcpy.da.SearchCursor(freq,["FREQUENCY","COMID"]) as cursor:
                for row in cursor:
                    if row[0] > 1:
                        dup_comid.append(row[1])

            #get all groups within duplicate reaches and assign them to a single group
            sp_join_lyr = arcpy.MakeFeatureLayer_management(sp_join,"sp_join_lyr")
            if dup_comid:
                for dup in dup_comid:
                    arcpy.SelectLayerByAttribute_management(sp_join_lyr,"NEW_SELECTION","COMID = {0}".format(dup))
                    combine_groups = []
                    with arcpy.da.SearchCursor(sp_join_lyr,["group_id"]) as cursor:
                        for row in cursor:
                            combine_groups.append(row[0])
                arcpy.SelectLayerByAttribute_management(sp_join_lyr,"NEW_SELECTION","group_id IN ({0})".format(','.join(str(x) for x in combine_groups)))
                with arcpy.da.UpdateCursor(sp_join_lyr,["group_id"]) as cursor:
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

            with arcpy.da.UpdateCursor(s,[species_code]) as cursor:
                for row in cursor:
                    sp_code = (''.join([x[:4] for x in row[0].split()[0:2]])).lower()

            arcpy.env.qualifiedFieldNames = False
            #export presence flowlines
            arcpy.CopyFeatures_management(flowlines_lyr,os.path.join(output_db,sp_code+"_PresenceFlowlines"))

        #delete temporary fields and datasets
        arcpy.Delete_management("in_memory")
        delete_files = ["s_join","single_part","sp_join","species_pt","s"]
        for file in delete_files:
            f = os.path.join(output_db,file)
            arcpy.Delete_management(f)
        for i in data_in:
            arcpy.DeleteField_management(i,"temp_join_id")

        return

class ExportCSV(object):
    def __init__(self):
        self.label = "Export to CSV Format"
        self.description = """The Export to CSV Format tool exports the QCd network grouping results to a .csv file with standardized column names to be used as input into the random forest models."""
        self.canRunInBackground = False

    def getParameterInfo(self):
        presence_flowlines = arcpy.Parameter(
            displayName = "Presence Flowlines (QC'd result from the Aquatic Network Grouping Tool)",
            name = "presence_flowlines",
            datatype = "GPFeatureLayer",
            parameterType = "Required",
            direction = "Input")

        comid = arcpy.Parameter(
            displayName = "COMID Field",
            name = "comid",
            datatype = "Field",
            parameterType = "Required",
            direction = "Input")
        comid.value = "COMID"
        comid.parameterDependencies = [presence_flowlines.name]

        uid = arcpy.Parameter(
            displayName = "UID Field",
            name = "uid",
            datatype = "Field",
            parameterType = "Optional",
            direction = "Input")
        uid.parameterDependencies = [presence_flowlines.name]

        gname = arcpy.Parameter(
            displayName = "GNAME Field",
            name = "gname",
            datatype = "Field",
            parameterType = "Required",
            direction = "Input")
        gname.value = "GNAME"
        gname.parameterDependencies = [presence_flowlines.name]


        group_id = arcpy.Parameter(
            displayName = "group_id Field",
            name = "group_id",
            datatype = "Field",
            parameterType = "Required",
            direction = "Input")
        group_id.value = "group_id"
        group_id.parameterDependencies = [presence_flowlines.name]


        ra = arcpy.Parameter(
            displayName = "RA Field",
            name = "ra",
            datatype = "Field",
            parameterType = "Optional",
            direction = "Input")
        ra.parameterDependencies = [presence_flowlines.name]

        obsdate = arcpy.Parameter(
            displayName = "OBSDATE Field",
            name = "obsdate",
            datatype = "Field",
            parameterType = "Optional",
            direction = "Input")
        obsdate.parameterDependencies = [presence_flowlines.name]

        csv_folder = arcpy.Parameter(
            displayName = "Ouput Folder",
            name = "csv_folder",
            datatype = "DEFolder",
            parameterType = "Required",
            direction = "Input")

        params = [presence_flowlines,comid,uid,gname,group_id,ra,obsdate,csv_folder]
        return params

    def isLicensed(self):
        return True

    def updateParameters(self,params):
        return

    def updateMessages(self,params):
        return

    def execute(self,params,messages):
        presence_flowlines = params[0].valueAsText #QCd presence flowlines
        comid = params[1].valueAsText
        uid = params[2].valueAsText #comid field
        gname = params[3].valueAsText #species_code field
        group_id = params[4].valueAsText #group_id field
        ra = params[5].valueAsText #ra field
        obsdate = params[6].valueAsText #obsdate field
        csv_folder = params[7].valueAsText

        arcpy.AddField_management(presence_flowlines,"species_code","TEXT")
        with arcpy.da.UpdateCursor(presence_flowlines,[gname,"species_code"]) as cursor:
            for row in cursor:
                sp_code = (''.join([x[:4] for x in row[0].split()[0:2]])).lower()
                row[1] = sp_code
                cursor.updateRow(row)

        # dictionary - string in old name defines new name
        fldsNamesDict={comid:'COMID','species_code':'SPECIES_CD',group_id:'GROUP_ID'}
        if uid:
            fldsNamesDict[uid] = 'UID'

        if ra:
            fldsNamesDict[ra] = 'RA'

        if obsdate:
            fldsNamesDict[obsdate] = 'OBSDATE'

        # list of strings
        fldsNames=list(fldsNamesDict.keys())

        # new fieldmappings object
        fieldmappings=arcpy.FieldMappings()
        # load input fc to fieldmappings object
        fieldmappings.addTable(presence_flowlines)

        #remove fieldmaps for those fields that are not needed in the output joined fc
        fields_to_delete = [f.name for f in fieldmappings.fields if f.name not in fldsNames]
        for field in fields_to_delete:
            fieldmappings.removeFieldMap(fieldmappings.findFieldMapIndex(field))

        # fields of input fc
        flds=fieldmappings.fieldMappings
        # INDEX
        i=0
        # loop over fields
        for fld in flds:
            # loop - which string from dictionary is in old field name? what will be new name?
            for fldName in fldsNames:
                if fldName in fld.getInputFieldName(0):
                    # SET NEW FIELD NAME
                    of=fld.outputField
                    of.name=fldsNamesDict[fldName]
                    fld.outputField=of
            # REPLACE FIELDMAP
            fieldmappings.replaceFieldMap (i, fld)
            # INCREASING INDEX
            i=i+1
        # export fc to shp using field mapping with new fields names
        arcpy.TableToTable_conversion(presence_flowlines,csv_folder,sp_code+'.csv',"",fieldmappings)

        field_order = ["COMID","SPECIES_CD","GROUP_ID"]
        if uid:
            field_order.append("UID")
        if ra:
            field_order.append("RA")
        if obsdate:
            field_order.append("OBSDATE")

        csv = os.path.join(csv_folder,sp_code+'.csv')
        df = pandas.read_csv(csv)
        df_reorder = df[field_order]
        df_reorder.to_csv(csv,index=False)

        if os.path.exists(os.path.join(csv_folder,sp_code+'.txt.xml')):
            os.remove(os.path.join(csv_folder,sp_code+'.txt.xml'))
        if os.path.exists(os.path.join(csv_folder,sp_code+'.xml')):
            os.remove(os.path.join(csv_folder,sp_code+'.xml'))
        if os.path.exists(os.path.join(csv_folder,sp_code+'.csv.xml')):
            os.remove(os.path.join(csv_folder,sp_code+'.csv.xml'))

        return