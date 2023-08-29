Steps to Run:
0.  If needed to switch location, copy the collocation_v3 folder 
    and all its contents

1.  Install latest python dependencies:
    h5py, pandas, numpy, xarray, netCDF4

    Install following if using Grapher class:
    matplotlib, cartopy

2.  Complete config.json File
    All path is absolute paths

    There can be no empty fields.

    Constants for collocation:
    "BOX-LEN-KM" is the bounding box square side length where the snotel
    station is centered at the middle of the box.
    "TIME-DELAY" is the time gap after the satellite observation

3.  cd into collocation folder

4.  Run collocation.py
    python collocation.py

5.  Check produced output
    output should be produced immediately after program stops in the /out folder
