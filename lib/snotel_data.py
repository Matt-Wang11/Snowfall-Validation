from lib import pd, os


class SnotelSiteData:
    def __init__(
        self,
        site_code: int | str,
        site_name: str,
        state: str,
        lon: float,
        lat: float,
        elev: float,
    ) -> None:
        self.site_code = site_code
        self.site_name = site_name
        self.state = state
        self.lon = lon
        self.lat = lat
        self.elev = elev

        self.daily = None  # pd.DataFrame
        self.hourly = None  # pd.DataFrame

    def set_daily_data(self, data: pd.DataFrame):
        self.daily = data

    def set_hourly_data(self, data: pd.DataFrame):
        self.hourly = data

    def has_hourly_data(self) -> bool:
        return self.hourly is not None

    def has_daily_data(self) -> bool:
        return self.daily is not None

    def __repr__(self) -> str:
        return f"Site code: {self.site_code}\nSite name: {self.site_name}\nState: {self.state}\nLongitude: {self.lon}\nLatitude: {self.lat}\nElevation: {self.elev}\n{self.daily}\n{self.hourly}"

    def __len__(self) -> int:
        return len(self.hourly), len(self.daily)


class SnotelDataset:
    def __init__(self) -> None:
        self.data_sets = dict()  # dict of int|str:SnotelSiteData
        self.__index = -1

    def add_site(self, sntl_site: int | str, data: SnotelSiteData) -> None:
        if sntl_site not in self.data_sets:
            self.data_sets[sntl_site] = data

    def add_hourly_data(self, sntl_site: int | str, data: pd.DataFrame):
        self.data_sets[sntl_site].set_hourly_data(data)

    def add_daily_data(self, sntl_site: int | str, data: pd.DataFrame):
        self.data_sets[sntl_site].set_daily_data(data)

    def append_daily_column(
        self, sntl_site: int | str, column: pd.DataFrame, time="Date_Time"
    ):
        daily_df = self.data_sets[sntl_site].daily
        self.add_daily_data(sntl_site, pd.merge(daily_df, column, on=time, how="outer"))

    def has_site(self, sntl_site: int | str) -> bool:
        return sntl_site in self.data_sets.keys()  # O(1)

    def to_ncdf(self, folder: str):
        for code, site in self.data_sets.items():
            site.to_ncdf(folder, f"SNTL{str(code)}.nc")

    def __getitem__(self, __key):
        return self.data_sets.__getitem__(__key)

    def __setitem__(self, __key, __value):
        self.data_sets.__setitem__(__key, __value)

    def __repr__(self) -> str:
        return str(self.data_sets)

    def __len__(self) -> int:
        return len(self.data_sets)

    def __iter__(self):
        return self

    def __next__(self):
        self.__index += 1
        if self.__index >= len(self.data_sets):
            self.__index = -1
            raise StopIteration
        else:  # Return key value pair
            return list(self.data_sets.items())[self.__index]


class CollocatedSiteData:
    def __init__(
        self,
        site_code: int | str,
        lon: float,
        lat: float,
    ) -> None:
        self.site_code = site_code
        self.lon = lon
        self.lat = lat
        self.sat = []
        self.sntl = []
        self.__index = -1

    def add_data(self, sat: pd.DataFrame, sntl: pd.DataFrame):
        if sat is None or sntl is None or sat.empty or sntl.empty:
            raise ValueError("Satellite and Snotel DataFrames can not be None or empty")

        self.sat.append(sat)
        self.sntl.append(sntl)

    def remove_data(self, index: int):
        self.sat.pop(index)
        self.sntl.pop(index)

    def to_csv(self, path: str):
        path = os.path.join(path, str(self.site_code))
        os.makedirs(path)

        digits = len(str(len(self.sat)))

        for i, s in enumerate(self.sat):
            num = str(i).zfill(digits)

            self.sat[i].to_csv(os.path.join(path, f"satellite_{num}.csv"), index=False)
            self.sntl[i].to_csv(os.path.join(path, f"sntl_{num}.csv"), index=False)

    def get_sat_column(self, column: str) -> list:
        elements = []

        if column in self.sat[0].columns:
            elements = self.sat[0][column].tolist()

            for sat in self.sat[1:]:
                elements.extend(sat[column].tolist())

        return elements

    def get_sntl_column(self, column: str) -> list:
        elements = []

        if column in self.sntl[0].columns:
            elements = self.sntl[0][column].tolist()

            for sntl in self.sntl[1:]:
                elements.extend(sntl[column].tolist())

        return elements
    
    def get_within_timeframe(self, timeframe) -> list:
        dfs = []

        for i, df in enumerate(self.sat):
            if (df.datetime >= timeframe[0]).any() and (self.sntl[i].Date_Time <= timeframe[1]).any():
                dfs.append([df, self.sntl[i]])

        return dfs

    def __len__(self) -> int:
        return len(self.sat)

    def __getitem__(self, __i):
        return [self.sntl.__getitem__(__i), self.sat.__getitem__(__i)]

    def __repr__(self) -> str:
        return f"Site code: {self.site_code}\nLongitude: {self.lon}\nLatitude: {self.lat}\nSnotel:\n{self.sntl}\nSatellite:\n{self.sat}\n"

    def __iter__(self):
        return self

    def __next__(self):
        self.__index += 1
        if self.__index >= len(self.sat):
            self.__index = -1
            raise StopIteration
        else:
            return self.sat[self.__index], self.sntl[self.__index]

class CollocatedDataset:
    def __init__(self) -> None:
        self.data = {}
        self.__index = -1

    def add_site(self, site: CollocatedSiteData):
        if type(site) == CollocatedSiteData:
            self.data[site.site_code] = site

    def remove_site(self, site: int):
        self.data.pop(site)

    def add_collocated_data(self, site: int, sat: pd.DataFrame, sntl: pd.DataFrame):
        if site in self.data:
            self.data[site].add_data(sat, sntl)
        else:
            raise ValueError("Site does not exist, you need to create it first")
    
    def has_site(self, site: int):
        return site in self.data

    def to_csv(self, path: str, package_name: str):
        package_path = os.path.join(path, package_name)
        os.makedirs(package_path)

        for _, coll_site in self.data.items():
            coll_site.to_csv(package_path)

    def __len__(self) -> int:
        return len(self.data)

    def __getitem__(self, __key):
        return self.data.__getitem__(__key)

    def __repr__(self) -> str:
        return str(self.data)

    def __iter__(self):
        return self

    def __next__(self):
        self.__index += 1
        if self.__index >= len(self.data):
            self.__index = -1
            raise StopIteration
        else:  # Return key value pair
            return list(self.data.items())[self.__index]
