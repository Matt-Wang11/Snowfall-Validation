from lib import pd, np, time, os, pickle, json
from lib.snotel_data import *
from lib.utilities import hourly_swe_to_rate
from lib.file_reader import FileReader
from lib.statistics import Statistics

"""
Input: 
    Satellite folder/files
    All Snotel hourly data (pickled)
    Box Dimension
    Time Delay

Output:
    Data that is spatially and temporally collocated between satellite and 
    snotel ground observations

Idea: 
    Based on satellite time frame to narrow down snotel data set since the 
    snotel data set is much larger, then spatially localize onto the bounding
    area around each snotel station
"""


def collocate_multiple(
    sntl: SnotelDataset, sats: list[pd.DataFrame], coll_data: CollocatedDataset=None
) -> CollocatedDataset:
    """ 
    Collocated multiple swaths overa defined area each sats element is a swath DataFrame
    """
    
    if coll_data is None:
        coll_data = CollocatedDataset()

    for sat in sats:
        coll_data = collocate(sntl=sntl, sat=sat, coll_data=coll_data)    
 
    return coll_data


def collocate(
    sntl: SnotelDataset, sat: pd.DataFrame, coll_data: CollocatedDataset = None
) -> CollocatedDataset:
    """
    Performs collocation. If the site is not created create it. Can add new
    CollocatedSiteData to previous CollocatedDataset or create new one
    """

    if coll_data is None:
        coll_data = CollocatedDataset()

    for code, site in sntl:
        coll_sat = spatial_collocation(site.lon, site.lat, sat)
        coll_sntl = temporal_colloacation(site.hourly, coll_sat["datetime"].max())

        if not coll_sat.empty and not coll_sntl.empty:
            if not coll_data.has_site(code):
                coll_data.add_site(CollocatedSiteData(code, site.lon, site.lat))

            coll_data.add_collocated_data(code, coll_sat, coll_sntl)

    return coll_data


# Find satellite data that is in the site bounds
# ! Neglects boundary issues ... no snotel stations are near bonudaries
def spatial_collocation(lon: int, lat: int, sat: pd.DataFrame) -> pd.DataFrame:
    lat_bounds = km_to_deg(sat["latitude"], BOX_LEN_KM / 2)
    lon_bounds = km_to_deg(0, BOX_LEN_KM / 2)

    return sat[
        (sat["longitude"] < lon + lat_bounds)
        & (sat["longitude"] > lon - lat_bounds)
        & (sat["latitude"] < lat + lon_bounds)
        & (sat["latitude"] > lat - lon_bounds)
    ]


# Find snotel data within satellite time frame
# ! Collocate with daily data?
def temporal_colloacation(sntl: pd.DataFrame, t_max: np.datetime64) -> pd.DataFrame:
    delta_s = np.timedelta64(TIME_DELAY[0], "m")
    delta_e = np.timedelta64(TIME_DELAY[1], "m")
    # metric could be switch to more effective onces
    
    return sntl[
        (sntl["Date_Time"] > t_max + delta_s) & (sntl["Date_Time"] < t_max + delta_e)
    ]


def accumulate_daily(sats: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Reads a collection of satellite collocated data and returns a list of daily
    accumulated data. Accumulated daily data is calculated by taking the mean 
    of non-zero values and multiplying by 24
    """
    
    sats = pd.concat(sats, ignore_index=True, axis=0)

    dates = []
    sf = []
    t = sats.datetime.min().to_numpy().astype("datetime64[D]")
    days = (sats.datetime.max() - t).to_numpy().astype("timedelta64[D]").astype(int)+1

    for _ in range(days):
        dates.append(t)
        sf.append(sats[(sats.datetime >= t) & 
                       (sats.datetime <= t + np.timedelta64(1, "D")) & 
                       (sats.sfr > 0)].sfr.mean() * 24)
    
        t += np.timedelta64(1, "D")
    
    df = pd.DataFrame({"datetime": dates, "sfr" : sf})
    df.dropna(inplace=True)

    return df


def km_to_deg(lat, km):
    MEAN_EARTH_RADIUS = 6371
    r2 = MEAN_EARTH_RADIUS * np.cos(lat * (np.pi / 180))

    return (km / r2) * (180 / np.pi)


def main():
    print("Starting...")

    with open("config.json") as f:
        config = json.load(f)

    print("config file read...")

    # =========================================================================

    # Set global constants
    global BOX_LEN_KM
    BOX_LEN_KM = config["BOX-LEN-KM"]

    global TIME_DELAY
    TIME_DELAY = config["TIME-DELAY-RANGE"]

    center = config["TARGET-CENTER"]
    
    # =========================================================================

    # Read data
    sntl = FileReader.read_sntl_data(config["path-to-sntl-hourly"])
    hourly_swe_to_rate(sntl, config["sntl-target-data"], "hourly", True)

    print("Snotel data read...")

    sat = {}
    sat_dirs = config["satellite-directories"]

    for sat_name in config["sat-to-run"]:
        print(f"Reading {sat_name}")
        sat[sat_name] = FileReader.read_all(sat_dirs[sat_name], center, config["date-to-run"])

    print("Satellite data read...")

    # =========================================================================

    # Collocation
    collocated_data = CollocatedDataset()

    for sat_name, sat_dfs in sat.items():
        collocated_data = collocate_multiple(sntl=sntl, sats=sat_dfs, coll_data=collocated_data)

    print("Collocation complete...")

    # =========================================================================

    print("Saving data...")

    # Save data
    folder = config["folder-output-path"]

    if folder != "":
        data_folder = os.path.join(folder, config["folder-output-name"])
        os.makedirs(data_folder)

        if config["folder-csv-output"]:
            # Folder output
            collocated_data.to_csv(folder, config["folder-output-name"])    
       
        # config file output
        with open(os.path.join(data_folder, "config.json"), "w") as f:    
            json.dump(config, f)

        # pickle output
        with open(os.path.join(data_folder, config["pickle-output-file-name"]), "wb") as f:
            pickle.dump(collocated_data, f)

        # pickle qc output
        stats = Statistics("hourly_" + config["sntl-target-data"], config["sntl-temp-data"])
        stats.process_collocated_data(collocated_data)
            
        # with open(os.path.join(data_folder, "qc_" + config["pickle-output-file-name"]), "wb") as f:
        #     pickle.dump(collocated_data, f)

        if config["calculate_stats"] and len(collocated_data) != 0:
            # Calculate statistics

            print("Calculating and saving statistics...")
            site_stats = {}

            for site_code, site in collocated_data:
                cm = stats.site_confusion_matrix(site)
                site_stats[site_code] = (stats.site_bias(site), stats.site_corr(site), 
                                         stats.site_rmse(site), len(site.sat),
                                         stats.POD(cm), stats.FAR(cm), stats.HSS(cm))

            bias = stats.bias(collocated_data)
            corr = stats.correlation(collocated_data)
            rmse = stats.rmse(collocated_data)
            
            cm = stats.confusion_matrix(collocated_data)
            pod = stats.POD(cm)
            far = stats.FAR(cm)
            hss = stats.HSS(cm)

            # statistics output
            with open(os.path.join(data_folder, "statistics.txt"), "w") as f:
                f.write("Global Stats:\n")
                f.write(f"Bias: {bias}\n")
                f.write(f"Corr: {corr}\n")
                f.write(f"Rmse: {rmse}\n")
                f.write(f"POD: {pod}\n")
                f.write(f"FARate: {far}\n")
                f.write(f"HSS: {hss}\n")

                for site, stat in site_stats.items():
                    f.write(f"Site: {site}, {stat[3]} Collocated sets\n")
                    f.write(f"Bias: {stat[0]}\n")
                    f.write(f"Corr: {stat[1]}\n")
                    f.write(f"Rmse: {stat[2]}\n")
                    f.write(f"POD: {stat[3]}\n")
                    f.write(f"FARate: {stat[4]}\n")
                    f.write(f"HSS: {stat[5]}\n\n")

        else:
            print("No collocation, can not calculate statistics")


if __name__ == "__main__":
    start_t = time.time()
    main()
    print(f"Runtime: {time.time() - start_t}")

