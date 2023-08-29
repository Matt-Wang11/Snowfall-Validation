from lib import xr, np, pd, h5py, os, pickle
from lib.utilities import is_in_bounds


class FileReader:

    @classmethod
    def __parse_time(cls, year, month, day, hour, minute, sec):
        if year <= 0 or month <= 0 or day <= 0 or hour < 0 or minute < 0 or sec < 0:
            return np.datetime64('NaT')

        return "{}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}".format(
            year, month, day, hour, minute, sec,
        )

    @classmethod
    def __create_clusters(cls, indices: list[int], centers: bool=False) -> list[int]:
        """
        Perform clustering on a list of integers (indices) and separate them into k
        clusters
        """
        clusters = []

        eps = 100
        curr_index = indices[0]
        curr_cluster = [curr_index]

        for index in indices[1:]:
            if index <= curr_index + eps:
                curr_cluster.append(index)
            else:
                clusters.append(curr_cluster)
                curr_cluster = [index]
            curr_index = index

        clusters.append(curr_cluster)

        if centers: 
            clusters = [min(cluster, key=lambda x: abs(x - np.mean(cluster))) for cluster in clusters] 

        return clusters
    
    @classmethod
    def get_all_files(cls, dir_path: str, extension: str=None):
        """
        Gets all file names of all files in a directory that has file extension
        Returns the path to each file in a sorted array
        """
        return sorted([
            os.path.join(dir_path, f)
            for f in os.listdir(dir_path)
            if extension is None or f.endswith(extension) or f.endswith(f".{extension}")
        ])

    @classmethod
    def select_sat(cls, center: tuple[float | int], files: list[str] | tuple[str]) -> list[list[str]]:
        """
        This function selects satellite swaths from files that contains the center
        point. Then it uses the found swaths to build larger swaths that cover 
        the whole of a region. For n20, npp

        center: (lon, lat)
        files: MUST BE SORTED
        """
        indices = []
        selections = []

        for index, f in enumerate(files):
            ds = xr.open_dataset(f)
            attributes = ds.attrs
            bounds = (  # Last scanline has -999 so use first
                attributes["geospatial_first_scanline_first_fov_lon"],
                attributes["geospatial_first_scanline_last_fov_lon"],
                attributes["geospatial_first_scanline_first_fov_lat"],
                attributes["geospatial_first_scanline_last_fov_lat"],
            )

            if is_in_bounds(bounds, center):
                indices.append(index) 

        if len(indices) != 0:
            clusters = cls.__create_clusters(indices, centers=True)
            
            centers = []
            for i in clusters:
                selections.append(files[max(0, i - 8) : min(i + 8, len(files) - 1)])

        return selections

    @classmethod
    def localize_sat(cls, center: tuple[float | int], file: str | xr.Dataset) -> list: 
        """
        This function filters large swaths of satellite data to find parts
        of the swaths that cover locations around the center. For n19, moc, mob
        
        center: (lon, lat)
        """
        ROW_PER_SWATH = 192
        
        # Filter lon 
        closest_indices = [] 
        dfs = []
    
        if type(file) == str:
            ds = xr.open_dataset(file)
        elif type(file) == xr.Dataset:
            ds = file
        else:
            raise ValueError("Invalid parameter values")

        shape = ds.Longitude.data.shape    
     
        # Find all row indices that has mean approx equal to the center lon 
        for i, row in enumerate(ds.Latitude.data):
            if round(np.mean(row)) == round(center[1]):
                closest_indices.append(i)
        
        if len(closest_indices) != 0: 
            centers = cls.__create_clusters(closest_indices, centers=True)
            
            if len(centers) != 0:
                # Filter lat
                df = cls.read_satellite_ncdf(file) 
                
                for index in centers:
                    lon_row = ds.Longitude.data[index]
                    
                    if is_in_bounds([lon_row[0], lon_row[-1], center[1] + 10, center[1] - 10], center):
                        dfs.append(df.iloc[max(0, index-int(ROW_PER_SWATH/2)) * shape[1]
                        : min(index+int(ROW_PER_SWATH/2), len(ds.Latitude.data)) * shape[1]].copy(deep=True))

        return dfs


    @classmethod
    def read_all(cls, folder: str, center: list | tuple, date_range: tuple=None): 
        """
        Can have single swaths or big swaths in one folder but small swaths
        that required to be built up needs to be in its own separate folder
        Each folder can only contain either folder or files of same type
        """

        dfs = []

        if os.path.exists(folder):
            files = cls.get_all_files(folder)   # ! Hinges on sorted file date

            if len(files) != 0:
                for file in files:
                    if os.path.isfile(file):
                        try:
                            ds = xr.open_dataset(file)
                            rows = ds.SFR.data.shape[0]
                        except Exception as ex:
                            # This could still let some error by incase this folder contains rows>700
                            print(f"Error '{ex}' occured reading file: '{file}' This file is skipped")
                            continue
                        
                        try:
                            if rows >= 20 and rows < 700: # Swaths that cover the region
                                if is_in_bounds((ds.attrs["geospatial_first_scanline_first_fov_lon"], 
                                                 ds.attrs["geospatial_first_scanline_last_fov_lon"],
                                                 ds.attrs["geospatial_first_scanline_first_fov_lat"],
                                                 ds.attrs["geospatial_first_scanline_last_fov_lat"]), center):
                                    dfs.append(cls.read_satellite_ncdf(ds))
                            elif rows > 700:     # Big swaths need to be filtered
                                dfs.extend(cls.localize_sat(center, ds)) 
                            elif rows < 21:   # Tiny swaths need to be built up
                                selected_files = cls.select_sat(center, files)

                                for swath in selected_files:
                                    dfs.append(cls.read_multiple_ncdf(swath))
                                break
                        except Exception as ex:
                            print(f"Error '{ex}' occured processing file data of '{file}' This file is skipped")
                    elif os.path.isdir(file):
                        if date_range != None:
                            file_t = file.split("/")[-1] 
                            start = np.datetime64(date_range[0])
                            end = np.datetime64(date_range[1])

                            if len(file_t) == 4:    # Convert to year so comparison works
                                file_date = np.datetime64(file_t)

                                start = start.astype("datetime64[Y]")
                                end = end.astype("datetime64[Y]")
                            elif len(file_t) == 8:
                                file_date = np.datetime64(f"{file_t[:4]}-{file_t[4:6]}-{file_t[6:8]}")
                            else:
                                raise ValueError(f"'{file_t}' Does not recognize folder name time format") 

                            if file_date >= start and file_date <= end:
                                dfs.extend(cls.read_all(file, center, date_range))
                            elif file_date > end:  # ! WORKS ONLY BECAUSE OF 8 char date format for sorting
                                break   # Past date range we don't need to continue
                        else:
                            dfs.extend(cls.read_all(file, center, date_range))
        else:    
            raise ValueError("Invalid folder path")
        
        return dfs

    @classmethod
    def read_sntl_data(cls, file: str):
        with open(file, "rb") as f:
            return pickle.load(f)

    @classmethod
    def read_satellite_h5(cls, file: str | h5py.File): 
        if type(file) == str:
            f = h5py.File(file, "r")
        elif type(file) == h5py.File:
            f = file
        else:
            raise ValueError("Incorrect file parameters")
        
        data = f["ATMS_Swath"]["Data Fields"]
        geo = f["ATMS_Swath"]["Geolocation Fields"]

        lon = np.array(geo["Longitude"]).byteswap().newbyteorder()
        lat = np.array(geo["Latitude"]).byteswap().newbyteorder()
        sfr = np.array(data["SFR"]).byteswap().newbyteorder() / 100
        dim = lon.shape

        # Create datetime data that corresponds to lon lat sfr shape
        datetime = []

        for i in range(dim[0]):
            datetime.append(
                [
                    "{}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}".format(
                        geo["ScanTime_year"][i][0],
                        geo["ScanTime_month"][i][0],
                        geo["ScanTime_dom"][i][0],
                        geo["ScanTime_hour"][i][0],
                        geo["ScanTime_minute"][i][0],
                        geo["ScanTime_second"][i][0],
                    )
                ]
                * dim[1]
            )

        datetime = np.array(datetime, dtype=np.datetime64)

        df = pd.DataFrame(
            {
                "datetime": datetime.reshape((dim[0] * dim[1],)),
                "longitude": lon.reshape((dim[0] * dim[1],)),
                "latitude": lat.reshape((dim[0] * dim[1],)),
                "sfr": sfr.reshape((dim[0] * dim[1],)),
            },
            columns=["datetime", "longitude", "latitude", "sfr"],
        )

        return df[(df["longitude"] > -180) & (df["latitude"] > -90) & (df["sfr"] >= 0.0)]

    @classmethod
    def read_multiple_ncdf(cls, files: list[str] | list[xr.Dataset]):
        """
        This function is only for reading ncdf files that are tiny swaths.
        NOT for reading multiple swaths.
        """

        df = cls.read_satellite_ncdf(files[0])

        for f in files[1:]:
            df_copy_df = cls.read_satellite_ncdf(f)
            df = pd.concat([df, df_copy_df])

        df.sort_values(by=["datetime"])

        return df

    @classmethod
    def read_satellite_ncdf(cls, file: str | xr.Dataset):
        if type(file) == str:
            ds = xr.open_dataset(file)
        elif type(file) == xr.Dataset:
            ds = file
        else:
            raise ValueError("Incorrect file parameters")

        sfr = ds["SFR"].data
        lon = ds["Longitude"].data
        lat = ds["Latitude"].data
        dim = sfr.shape

        dom = ds["ScanTime_dom"].data.astype("timedelta64[D]").astype(int)
        hour = ds["ScanTime_hour"].data.astype("timedelta64[h]").astype(int)
        minute = ds["ScanTime_minute"].data.astype("timedelta64[m]").astype(int)
        sec = ds["ScanTime_second"].data.astype("timedelta64[s]").astype(int)

        datetime = []

        for i in range(dim[0]):
            # ! Will give error when parsing -999 current fix sucks
            datetime.append([cls.__parse_time(ds["ScanTime_year"].data[i],
                                      ds["ScanTime_month"].data[i],
                                      dom[i], hour[i], minute[i], sec[i])] * dim[1])

        datetime = np.array(datetime, dtype=np.datetime64)

        df = pd.DataFrame(  # Flatten all
            {
                "datetime": datetime.reshape((dim[0] * dim[1],)),
                "longitude": lon.reshape((dim[0] * dim[1],)),
                "latitude": lat.reshape((dim[0] * dim[1],)),
                "sfr": sfr.reshape((dim[0] * dim[1],)),
            },
            columns=["datetime", "longitude", "latitude", "sfr"],
        )

        return df[(df["longitude"] > -180) & (df["latitude"] > -90) & (df["sfr"] >= 0.0)]

