0.  If needed to switch location, copy the collocation_v1 folder 
    and all its contents

1.  Install latest python dependencies:
    h5py, pandas, numpy, xarray, netCDF4

2.  Complete config.json File
    The key "single-satellite-file" is for the option to combine 
    multiple satellite swaths files or use one single file. When this
    option is True, the program will only read the first file of 
    "satellite-files" array otherwise it will read all files in 
    "satellite-files" and combine its data for collocation.

    "satellite-file-type" currently only supports .h5 and .nc files
    use "h5" for .h5 files and "ncdf" for .nc files

    "folder-output-path" is the location where the collocated folders
    are created. It is a folder containing each site where collocation
    is preformed. Regardless of whether there are collocated data, 
    for the output each site will contain one satellite csv file and 
    one snotel csv file

    pickling and folder output options can be used together or 
    separately

    Constants for collocation:
    "BOX-LEN-KM" is the bounding box square side length where the snotel
    station is centered at the middle of the box.
    "TIME-DELAY" is the time gap after the satellite observation

3.  cd into collocation folder

4.  Run collocation.py
    python collocation.py

5.  Check produced output
    output should be produced immediately after program stops

Note: Program should finish in seconds unless using large number of
satellite files




    