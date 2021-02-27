import os
import sys
import time
import datetime
import gdal
import rasterio
import zipfile
import ntpath
import csv

from rasterio.warp import reproject, Resampling, calculate_default_transform
from rasterio import Affine

# GLOBAL DIRECTORIES
INPUT_DIR = ""
OUTPUT_LOC = ""
TEMP_LOC = ""

# Geographic: "wgs84", "hartebeesthoek" or
# Projected: "web" (Web-mercator), "albers_africa", "albers_south_africa" or
# LO-system: "lo15", "lo19", "lo21", "lo23", "lo25", "lo27", "lo29", "lo31", "lo33" or
# UTM-system: "utm_33s", "utm_34s", "utm_35s" or "utm_36s".
COORDINATE_SYSTEM = "lo19"

# New stack: For raster with bands [1, 2, 3, 4]: [3, 2, 1]
# will produce a new raster where band 3 is first, band 2 second, and band 1 will be third
# Band 4 is excluded in this example.
BAND_RESTACK = [3, 2, 1]  # Used for band restacking

RESAMPLE = True  # Will perform resampling to the provided spatial resolution if necessary
SPATIAL_RES = 10  # If a does not consist of this spatial resolution, it is resampled
RESAMPLING = "nearest"  # "nearest", "bilinear" or "cubic". Used for resampling and/or projecting

FORMAT = 'Gtiff'  # Will add more formats at a later stage
OVERWRITE = False  # "True": existing output files will be deleted. "False": If the file exists, it is skipped


# Prints a message. Adds the time and date to the string
def write_message(message):
    time_sec = time.time()
    timestamp = datetime.datetime.fromtimestamp(time_sec).strftime('%Y-%m-%d %H:%M:%S')
    message = "[" + str(timestamp) + "] " + str(message)

    print(message)


# Checks whether a file extension is supported/allowed
def check_extension(cur_file, list_extensions):
    filename = ntpath.basename(cur_file)
    file_extension = (os.path.splitext(filename)[1]).replace(".", "")

    for extension in list_extensions:
        if extension == file_extension:
            return True

    return False


# Unzips a list of zip files
def unzip_files(list_zip_files, extract_dir):
    for zip_file in list_zip_files:
        zip_ref = zipfile.ZipFile(zip_file)  # ZipeFile object
        list_zip_contents = zip_ref.namelist()  # List all contents
        extraction_folder = extract_dir + list_zip_contents[0]  # The first folder of the zip file

        if not os.path.exists(extraction_folder):  # If the extracted folder exists, skip
            writeMessage("Extracting zip file: " + zip_file)
            zip_ref.extractall(extract_dir)


# Searches for zip files
# Recursive
def search_files(cur_dir):
    list_contents = os.listdir(cur_dir)

    for content in list_contents:
        content_dir = cur_dir + content
        if os.path.isdir(content_dir):  # Subfolder found
            search_files(content_dir + "/")
        elif checkExtension(content, ["tif", "img"]):  # Zip file found
            writeMessage("Example")
        elif content.endswith(".zip"):  # Zip file found
            writeMessage("Example")


# Removes unwanted characters from a string, such as spaces, tabs and newlines
def remove_unwanted_txt(string):
    string = string.replace(" ", "")
    string = string.replace("\n", "")
    string = string.replace("\t", "")

    return string


# Reads raw sensor band directories from the metadata
# Currently only works with Sentinel-2 metadata, will update as more sensors will be added
def read_raster_sentinel2_metadata(metadata, raw_folder):
    sensor = ""
    date = ""
    tile = ""
    list_rasters = []

    if os.path.exists(metadata):
        metadata_file = open(metadata)
        list_lines = metadata_file.readlines()
        for file_line in list_lines:
            if "<PRODUCT_URI>" in file_line:
                product_info = (remove_unwanted_txt(file_line)
                                .replace("<PRODUCT_URI>", "")).replace(".SAFE</PRODUCT_URI>", "")
                list_split_line = product_info.split("_")
                sensor = list_split_line[0]
                date = (list_split_line[2])[:8]
                tile = (list_split_line[5])[1:]
            if "<IMAGE_FILE>" in file_line:  # Band directories
                raster_dir = raw_folder + (remove_unwanted_txt(file_line)
                                           .replace("<IMAGE_FILE>", "")).replace("</IMAGE_FILE>", ".jp2")
                list_rasters.append(raster_dir)
    else:
        writeMessage("ERROR: Metadata " + metadata + " does not exist!")

    return sensor, date, tile, list_rasters


# Gets the raster bit-depth/dtype, e.g. float, 16bit unsigned
def get_raster_dtype(raster):
    raster_obj = gdal.Open(raster)
    band_dtype = raster_obj.GetRasterBand(1).DataType
    dtype_name = gdal.GetDataTypeName(band_dtype)

    return dtype_name.lower()  # All characters has to be lowercase for rasterio


# Stacks the list of raster directories
# Provide the output directory
# Geotiff is the output format (*.tiff)
def stack_rasters(list_bands, output_stacked_raster):
    for band in list_bands:  # Checks whether all of the provided rasters/bands exist
        if not os.path.exists(band):
            writeMessage("ERROR: Stacking cannot be performed because one of the rasters/bands does not exist: " + band)
            return

    # If the raster exist or overwrite is disabled, the stacking is not performed
    if not os.path.exists(output_stacked_raster) or OVERWRITE:
        delete_raster(output_stacked_raster)  # Deletes the raster if it exists

        band_count = len(list_bands)
        writeMessage("Stacking " + str(band_count) + " bands...")

        band_obj1 = rasterio.open(list_bands[0])
        dtype = get_raster_dtype(list_bands[0])
        writeMessage("Raster dtype: " + dtype)

        # Creates the stacked Geotiff raster and sets the raster properties
        with rasterio.open(output_stacked_raster, 'w', driver='Gtiff', width=band_obj1.width, height=band_obj1.height,
                           count=band_count, crs=band_obj1.crs, transform=band_obj1.transform,
                           dtype=dtype) as stacked_raster:
            band_id = 1
            for band in list_bands:
                band_obj = rasterio.open(band)
                stacked_raster.write(band_obj.read(1), band_id)

                band_id = band_id + 1


# Restacks the bands of the provided input raster
# Provide the output file
# New stack: For raster with bands [1, 2, 3, 4]: [2, 1, 3], second,
# first and third bands. Band 4 is excluded in this example.
# Geotiff is the output format (*.tiff)
def restack_bands(input_raster, output_restacked_raster, new_stack):
    if os.path.exists(input_raster):  # If the input raster does not exist, the restack is not performed
        # If the output raster exists and overwrite is disabled, the restack is not performed
        if not os.path.exists(output_restacked_raster) or OVERWRITE:
            delete_raster(output_restacked_raster)  # Deletes the output raster if it exists

            new_stack_len = len(new_stack)
            if new_stack_len > 0:  # Check if atleast one element is provided
                orig_raster = rasterio.open(input_raster)
                dtype = get_raster_dtype(input_raster)
                band_count = orig_raster.count

                # Checks if all provided bands are valid
                writeMessage("Restacking: " + input_raster)
                writeMessage("Original raster band count: " + str(band_count))
                writeMessage("New raster band count: " + str(new_stack_len))
                for band in new_stack:
                    # If a restack band is outside the band range of the original raster
                    if band <= 0 or band > band_count:
                        writeMessage("ERROR: Restack band " + str(band) + " is outside the possible band range")
                        return

                writeMessage("New stack: " + str(new_stack))

                # Creates the restacked raster and sets the raster properties
                with rasterio.open(output_restacked_raster, 'w', driver='Gtiff',
                                   width=orig_raster.width, height=orig_raster.height,
                                   count=new_stack_len, crs=orig_raster.crs, transform=orig_raster.transform,
                                   dtype=dtype) as new_stacked_raster:
                    new_band_id = 1
                    for band in new_stack:  # Restacks the bands
                        new_stacked_raster.write(orig_raster.read(band), new_band_id)
                        new_band_id = new_band_id + 1
            else:  # The band stack is empty and restacking can therefore not be performed
                writeMessage("ERROR: Not performing restack because the new band stack is empty: " + str(new_stack))
    else:  # No input raster found
        writeMessage("ERROR: Input raster does not exist: " + input_raster)


# Get the resamling method for modules using the "Resampling" module
# Resampling has to be a string, "nearest", "bilinear" or "cubic". Both uppercase and lowercase characters accepted
# If the provided resampling method is identified, nearest will be applied
def get_resampling(resampling_str):
    if resampling_str == "nearest":  # Nearest neighbour
        resampling = Resampling.nearest
    elif resampling_str == "bilinear":  # Bilinear
        resampling = Resampling.bilinear
    elif resampling_str == "cubic":  # Cubic convolution
        resampling = Resampling.cubic
    else:  # Resampling method not identified, the default (nearest neighbour) method will be used
        writeMessage("WARNING: Unknown resampling methods (" + str(resampling_str) +
                     "), nearest resampling will be applied.")
        resampling = Resampling.nearest

    return resampling


# Returns either the EPSG code or projection xml text
# Geographic: "wgs84", "hartebeesthoek"
# Projected: "web" (Web-mercator), "albers_africa", "albers_south_africa"
# LO-system: "lo15", "lo19", "lo21", "lo23", "lo25", "lo27", "lo29", "lo31", "lo33"
# UTM-system: "utm_33s", "utm_34s", "utm_35s" or "utm_36s"
# If the coordinate system could not be identified, an error message is returned
def get_epsg_projection_code(projection_name):
    if "wgs84" == projection_name:  # Geographic: WGS84
        return "EPSG:4326"
    elif "hartebeesthoek" == projection_name:  # Geographic: Hartebeesthoek94
        return "EPSG:4148"
    elif "lo15" == projection_name:  # Projected: Lo15, hartebeesthoek94
        return "PROJCS[\"Lo15\",GEOGCS[\"Hartebeesthoek94\",DATUM[\"D_Hartebeesthoek_1994\"," \
               "SPHEROID[\"WGS_1984\",6378137,298.257223563]],PRIMEM[\"Greenwich\",0],UNIT[\"Degree\"," \
               "0.017453292519943295]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"latitude_of_origin\",0]," \
               "PARAMETER[\"central_meridian\",15],PARAMETER[\"scale_factor\",1],PARAMETER[\"false_easting\",0]," \
               "PARAMETER[\"false_northing\",0],UNIT[\"Meter\",1]]"
    elif "lo17" == projection_name:  # Projected: Lo17, hartebeesthoek94
        return "PROJCS[\"Lo17\",GEOGCS[\"Hartebeesthoek94\",DATUM[\"D_Hartebeesthoek_1994\"," \
               "SPHEROID[\"WGS_1984\",6378137,298.257223563]],PRIMEM[\"Greenwich\",0],UNIT[\"Degree\"," \
               "0.017453292519943295]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"latitude_of_origin\",0]," \
               "PARAMETER[\"central_meridian\",17],PARAMETER[\"scale_factor\",1],PARAMETER[\"false_easting\",0]," \
               "PARAMETER[\"false_northing\",0],UNIT[\"Meter\",1]]"
    elif "lo19" == projection_name:  # Projected: Lo19, hartebeesthoek94
        return "PROJCS[\"Lo19\",GEOGCS[\"Hartebeesthoek94\",DATUM[\"D_Hartebeesthoek_1994\"," \
               "SPHEROID[\"WGS_1984\",6378137,298.257223563]],PRIMEM[\"Greenwich\",0],UNIT[\"Degree\"," \
               "0.017453292519943295]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"latitude_of_origin\",0]," \
               "PARAMETER[\"central_meridian\",19],PARAMETER[\"scale_factor\",1],PARAMETER[\"false_easting\",0]," \
               "PARAMETER[\"false_northing\",0],UNIT[\"Meter\",1]]"
    elif "lo21" == projection_name:  # Projected: Lo21, hartebeesthoek94
        return "PROJCS[\"Lo21\",GEOGCS[\"Hartebeesthoek94\",DATUM[\"D_Hartebeesthoek_1994\"," \
               "SPHEROID[\"WGS_1984\",6378137,298.257223563]],PRIMEM[\"Greenwich\",0],UNIT[\"Degree\"," \
               "0.017453292519943295]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"latitude_of_origin\",0]," \
               "PARAMETER[\"central_meridian\",21],PARAMETER[\"scale_factor\",1],PARAMETER[\"false_easting\",0]," \
               "PARAMETER[\"false_northing\",0],UNIT[\"Meter\",1]]"
    elif "lo23" == projection_name:  # Projected: Lo23, hartebeesthoek94
        return "PROJCS[\"Lo23\",GEOGCS[\"Hartebeesthoek94\",DATUM[\"D_Hartebeesthoek_1994\"," \
               "SPHEROID[\"WGS_1984\",6378137,298.257223563]],PRIMEM[\"Greenwich\",0],UNIT[\"Degree\"," \
               "0.017453292519943295]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"latitude_of_origin\",0]," \
               "PARAMETER[\"central_meridian\",23],PARAMETER[\"scale_factor\",1],PARAMETER[\"false_easting\",0]," \
               "PARAMETER[\"false_northing\",0],UNIT[\"Meter\",1]]"
    elif "lo25" == projection_name:  # Projected: Lo25, hartebeesthoek94
        return "PROJCS[\"Lo25\",GEOGCS[\"Hartebeesthoek94\",DATUM[\"D_Hartebeesthoek_1994\"," \
               "SPHEROID[\"WGS_1984\",6378137,298.257223563]],PRIMEM[\"Greenwich\",0],UNIT[\"Degree\"," \
               "0.017453292519943295]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"latitude_of_origin\",0]," \
               "PARAMETER[\"central_meridian\",25],PARAMETER[\"scale_factor\",1],PARAMETER[\"false_easting\",0]," \
               "PARAMETER[\"false_northing\",0],UNIT[\"Meter\",1]]"
    elif "lo27" == projection_name:  # Projected: Lo27, hartebeesthoek94
        return "PROJCS[\"Lo27\",GEOGCS[\"Hartebeesthoek94\",DATUM[\"D_Hartebeesthoek_1994\"," \
               "SPHEROID[\"WGS_1984\",6378137,298.257223563]],PRIMEM[\"Greenwich\",0],UNIT[\"Degree\"," \
               "0.017453292519943295]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"latitude_of_origin\",0]," \
               "PARAMETER[\"central_meridian\",27],PARAMETER[\"scale_factor\",1],PARAMETER[\"false_easting\",0]," \
               "PARAMETER[\"false_northing\",0],UNIT[\"Meter\",1]]"
    elif "lo29" == projection_name:  # Projected: Lo29, hartebeesthoek94
        return "PROJCS[\"Lo29\",GEOGCS[\"Hartebeesthoek94\",DATUM[\"D_Hartebeesthoek_1994\"," \
               "SPHEROID[\"WGS_1984\",6378137,298.257223563]],PRIMEM[\"Greenwich\",0],UNIT[\"Degree\"," \
               "0.017453292519943295]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"latitude_of_origin\",0]," \
               "PARAMETER[\"central_meridian\",29],PARAMETER[\"scale_factor\",1],PARAMETER[\"false_easting\",0]," \
               "PARAMETER[\"false_northing\",0],UNIT[\"Meter\",1]]"
    elif "lo31" == projection_name:  # Projected: Lo31, hartebeesthoek94
        return "PROJCS[\"Lo31\",GEOGCS[\"Hartebeesthoek94\",DATUM[\"D_Hartebeesthoek_1994\"," \
               "SPHEROID[\"WGS_1984\",6378137,298.257223563]],PRIMEM[\"Greenwich\",0],UNIT[\"Degree\"," \
               "0.017453292519943295]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"latitude_of_origin\",0]," \
               "PARAMETER[\"central_meridian\",31],PARAMETER[\"scale_factor\",1],PARAMETER[\"false_easting\",0]," \
               "PARAMETER[\"false_northing\",0],UNIT[\"Meter\",1]]"
    elif "lo33" == projection_name:  # Projected: Lo33, hartebeesthoek94
        return "PROJCS[\"Lo33\",GEOGCS[\"Hartebeesthoek94\",DATUM[\"D_Hartebeesthoek_1994\"," \
               "SPHEROID[\"WGS_1984\",6378137,298.257223563]],PRIMEM[\"Greenwich\",0],UNIT[\"Degree\"," \
               "0.017453292519943295]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"latitude_of_origin\",0]," \
               "PARAMETER[\"central_meridian\",33],PARAMETER[\"scale_factor\",1],PARAMETER[\"false_easting\",0]," \
               "PARAMETER[\"false_northing\",0],UNIT[\"Meter\",1]]"
    elif "utm33s" == projection_name:  # Projected: UTM33S, wgs94
        return "PROJCS[\"WGS_1984_UTM_Zone_33S\",GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\"," \
               "SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0]," \
               "UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"]," \
               "PARAMETER[\"False_Easting\",500000.0],PARAMETER[\"False_Northing\",10000000.0]," \
               "PARAMETER[\"Central_Meridian\",15.0],PARAMETER[\"Scale_Factor\",0.9996]," \
               "PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0],AUTHORITY[\"EPSG\",32733]]"
    elif "utm34s" == projection_name:  # Projected: UTM34S, wgs94
        return "PROJCS[\"WGS_1984_UTM_Zone_34S\",GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\"," \
               "SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0]," \
               "UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"]," \
               "PARAMETER[\"False_Easting\",500000.0],PARAMETER[\"False_Northing\",10000000.0]," \
               "PARAMETER[\"Central_Meridian\",21.0],PARAMETER[\"Scale_Factor\",0.9996]," \
               "PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0],AUTHORITY[\"EPSG\",32734]]"
    elif "utm35s" == projection_name:  # Projected: UTM35S, wgs94
        return "PROJCS[\"WGS_1984_UTM_Zone_35S\",GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\"," \
               "SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\"," \
               "0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"False_Easting\",500000.0]," \
               "PARAMETER[\"False_Northing\",10000000.0],PARAMETER[\"Central_Meridian\",27.0]," \
               "PARAMETER[\"Scale_Factor\",0.9996]," \
               "PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0],AUTHORITY[\"EPSG\",32735]]"
    elif "utm36s" == projection_name:  # Projected: UTM36S, wgs94
        return "PROJCS[\"WGS_1984_UTM_Zone_36S\",GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\"," \
               "SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0]," \
               "UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"]," \
               "PARAMETER[\"False_Easting\",500000.0],PARAMETER[\"False_Northing\",10000000.0]," \
               "PARAMETER[\"Central_Meridian\",33.0],PARAMETER[\"Scale_Factor\",0.9996]," \
               "PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0],AUTHORITY[\"EPSG\",32736]]"
    elif "albers_africa" == projection_name:  # Projected: Albers equal area conic for Africa, wgs84
        return "PROJCS[\"Africa_Albers_Equal_Area_Conic\",GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\"," \
               "SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\"," \
               "0.0174532925199433]],PROJECTION[\"Albers\"],PARAMETER[\"False_Easting\",0.0]," \
               "PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",25.0]," \
               "PARAMETER[\"Standard_Parallel_1\",20.0],PARAMETER[\"Standard_Parallel_2\",-23.0]," \
               "PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0]]"
    # Projected: Albers equal area conic for South Africa, hartebeesthoek94
    elif "albers_south_africa" == projection_name:
        return "PROJCS[\"South_Africa_Albers_Equal_Area_Conic\",GEOGCS[\"GCS_Hartebeesthoek_1994\"," \
               "DATUM[\"D_Hartebeesthoek_1994\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]]," \
               "PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Albers\"]," \
               "PARAMETER[\"False_Easting\",0.0],PARAMETER[\"False_Northing\",0.0]," \
               "PARAMETER[\"Central_Meridian\",25.0],PARAMETER[\"Standard_Parallel_1\",-33.5]," \
               "PARAMETER[\"Standard_Parallel_2\",-34.5],PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0]]"
    elif "web" == projection_name:  # Projected: Web-mercator, WGS84
        return "EPSG:3857"
    else:  # Unknown projection, error message returned
        return "ERROR: Projection not found: " + projection_name


# Projects the provided raster
# Resampling has to be a string, "nearest", "bilinear" or "cubic". Both uppercase and lowercase characters accepted
# If the provided resampling method is identified, nearest will be applied
# See the "get_epsg_projection_code" for the list of possible chooses which can be selected from the method
# If the required projection is not provided in the function, the required EPSG code or xml text can be used
# Spatial reference (EPSG codes) list: https://spatialreference.org/ref/
# Geotiff is the output format (*.tiff)
def project_raster(input_raster, output_raster, resampling_str, epsg_code):
    if os.path.exists(input_raster):
        # Skips projecting if the output raster already exists or the overwrite is disabled
        if not os.path.exists(output_raster) or OVERWRITE:
            raster_name = ntpath.basename(input_raster)
            resampling_str = resampling_str.lower()
            resampling = get_resampling(resampling_str)

            writeMessage("Project raster: " + raster_name)
            writeMessage("Resampling: " + resampling_str)
            writeMessage("EPSG code: " + epsg_code)

            delete_raster(output_raster)  # If the output raster exists, it is deleted
            with rasterio.open(input_raster) as source_raster:

                transform, width, height = calculate_default_transform(source_raster.crs,
                                                                       epsg_code, source_raster.width,
                                                                       source_raster.height, *source_raster.bounds)
                kwargs = source_raster.meta.copy()
                kwargs.update({
                    'crs': epsg_code,
                    'transform': transform,
                    'width': width,
                    'height': height
                })

                with rasterio.open(output_raster, 'w', **kwargs) as projected_raster:
                    band_count = source_raster.count
                    for band in range(1, band_count):  # Projects each band and writes it to the projected raster file
                        source_band = rasterio.band(source_raster, band)
                        source_trans = source_raster.transform
                        source_prj = source_raster.crs

                        prj_band = rasterio.band(projected_raster, band)
                        prj_trans = projected_raster.transform

                        reproject(source=source_band, destination=prj_band, src_transform=source_trans,
                                  src_crs=source_prj, dst_transform=prj_trans, dst_crs=epsg_code, resampling=resampling)
    else:  # If the input raster does not exist, projecting is skipped
        writeMessage("ERROR: Input raster (" + input_raster + ") not found, projecting not performed.")


# Deletes the provided raster
# Will also delete additional files ("tfw", "aux.xml", "ovr" and "xml")
# Should work with any raster format
# Deletion is not executed if the raster does not exist
def delete_raster(raster_to_delete):
    raster_extensions = ["tfw", "aux.xml", "ovr", "xml"]
    if os.path.exists(raster_to_delete):
        writeMessage("Deleting: " + raster_to_delete)
        os.remove(raster_to_delete)

        base_name = ntpath.basename(raster_to_delete)
        base_extension = os.path.splitext(base_name)[1]
        base_path = ntpath.dirname(raster_to_delete) + "/"
        # Loops through possible additional files of a raster (pyramid layers, etc.)
        for extension in raster_extensions:
            if 'tfw' in extension:
                filename = base_name.replace(base_extension, "." + extension)
            else:
                filename = base_name + "." + extension

            file = base_path + filename
            if os.path.exists(file):
                os.remove(file)


# Deletes a single file
# Deletion is not executed if the file does not exist
def delete_file(file_to_delete):
    if os.path.exists(file_to_delete):
        writeMessage("Deleting: " + file_to_delete)
        os.remove(file_to_delete)


# Copies a raster
# Can be used to change the format of the raster
def copy_raster(input_raster, output_raster, overwrite):
    # If the output raster already exists and overwrite is disabled, it is skipped
    if not os.path.exists(output_raster) or overwrite:
        delete_raster(output_raster)  # Deletes the raster if it exists

        writeMessage("Copying raster: " + input_raster)
        writeMessage("Output raster: " + output_raster)

        input_raster_obj = rasterio.open(input_raster)
        dtype = get_raster_dtype(input_raster)
        band_count = input_raster_obj.count

        # Creates the new raster
        with rasterio.open(output_raster, 'w', driver=FORMAT, width=input_raster_obj.width,
                           height=input_raster_obj.height,
                           count=band_count, crs=input_raster_obj.crs, transform=input_raster_obj.transform,
                           dtype=dtype) as new_raster:

            index_band = 1
            while index_band <= band_count:  # Writes each band to the new raster
                input_raster_band = input_raster_obj.read(index_band)
                new_raster.write(input_raster_band, index_band)

                index_band = index_band + 1


# Creates the output folder if it does not exist
def create_output_folder(output_folder):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    return output_folder


# Sentinel-2: Searches for 10m, 20m and 60m bands from a provided list
def s2_get_raster_stack_bands(list_rasters, s2_level):
    list_10m_bands = []  # Stack for 10m bands
    list_20m_bands = []  # Stack for 20m bands
    list_60m_bands = []  # Stack for 60m bands

    if s2_level == "L2A":  # Sentinel-2 Level 2A
        for raster in list_rasters:
            filename = ntpath.basename(raster)  # Gets the raster name from the path
            if "10m" in filename and ("B02" in filename or "B03" in filename or "B04" in filename or "B08" in filename):
                list_10m_bands.append(raster)
            elif "20m" in filename and ("B05" in filename or "B06" in filename or "B07" in filename
                                        or "B8A" in filename or "B11" in filename or "B12" in filename):
                list_20m_bands.append(raster)
            elif "60m" in filename and ("B01" in filename or "B09" in filename or "WVP" in filename):  # WVP = Band 10
                list_60m_bands.append(raster)
    elif s2_level == "L1C":  # Sentinel-2 Level 1C
        for raster in list_rasters:
            filename = ntpath.basename(raster)  # Gets the raster name from the path
            if "B02" in filename or "B03" in filename or "B04" in filename or "B08" in filename:
                list_10m_bands.append(raster)
            elif "B05" in filename or "B06" in filename or "B07" in filename or "B8A" in filename or "B11" in filename \
                    or "B12" in filename:
                list_20m_bands.append(raster)
            elif "B01" in filename or "B09" in filename or "B11" in filename:
                list_60m_bands.append(raster)
    else:  # The Sentinel-2 data level could not be identified
        writeMessage("ERROR: Unknown Sentinel-2 level: " + s2_level)

    return list_10m_bands, list_20m_bands, list_60m_bands


# Creates a csv metadata file based on provided info
# list_info: [[raster, sensor, capture_date, tile, bands, spatial_res, projection],
# [raster, sensor, capture_date, tile, bands, spatial_res, projection], ...]
# Provide "" for data info if the table element should be left empty
# output_metadata: directory + "metadata.csv"
def create_metadata(list_info, output_metadata):
    if len(list_info) > 0:  # Checks if any info is provided to print to the csv file
        if not os.path.exists(output_metadata) or OVERWRITE:  # Skips if the metadata file exists
            delete_file(output_metadata)

            writeMessage("Metadata: " + ntpath.basename(output_metadata))

            with open(output_metadata, 'w', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)
                # Columns which will be written to the csv file
                columns = ["Raster", "Data", "Capture date", "Tile", "Bands", "Spatial resolution", "Projection"]
                csv_writer.writerow(columns)

                for file_info in list_info:  # Loops through each raster metadata
                    # WARNING: Remember to make changes below if changes are made to metadata
                    if len(file_info) == len(columns):
                        raster = file_info[0]
                        sensor = file_info[1]
                        date = file_info[2]
                        tile = file_info[3]
                        bands = file_info[4]
                        spatial_res = file_info[5]
                        prj = file_info[6]

                        csv_writer.writerow([raster, sensor, str(date), tile, str(bands), str(spatial_res), prj])
                    # If the length is incorrect, not enough info is provided. An error is written to the csv file
                    else:
                        csv_writer.writerow(["ERROR: Not enough info provided for the raster: " + str(file_info)])
    else:  # No metadata data info is provided
        writeMessage("ERROR: No metadata data info provided: " + output_metadata)
