from lib import np, pd
from lib.snotel_data import CollocatedSiteData

def reformat_long(long: float | int):
    """
    Convert the negative to positive boundary to only contain positive values
    """

    if (isinstance(long, float)
        or isinstance(long, int)
        or isinstance(long, np.floating)):
        if long < 0:
            long = 360 + long
    else:
        for row in long:
            for i, element in enumerate(row):
                if element < 0:
                    row[i] = 360 + element

    return long

# ! We are choosing only the acute angles that the two longitude bounds make
# ! There are cases where there are obtuse angles but those exists on the poles
def is_in_bounds(bounds: tuple | list, coords: tuple | list):
    """
    Check if a given set of coordinates are in a defined bound. The coords are
    defined (lon, lat) The bounds is a tuple of (start lon, end lon, start lat, end lat)
    """
    is_in = True  # True until proven false
    lon_bounds = (reformat_long(bounds[0]), reformat_long(bounds[1]))
    lat_bounds = bounds[2:]
    reformatted_lon = reformat_long(coords[0])

    angle = abs(lon_bounds[1] - lon_bounds[0])  # Angle to 0
    if angle <= 180:  # ! Need to take care of case where 0 is involved
        if reformatted_lon < min(lon_bounds) or reformatted_lon > max(lon_bounds):
            is_in = False
    else:
        if reformatted_lon > min(lon_bounds) and reformatted_lon < max(lon_bounds):
            is_in = False

    # TODO: Case where coords is a list of coordinates

    # Lat bounds
    if coords[1] < min(lat_bounds) or coords[1] > max(lat_bounds):
        is_in = False

    return is_in

def hourly_swe_to_rate(sntl, target_data: str, type: str, new_col: bool=False):
    for _, site in sntl:
        if site.has_hourly_data() and target_data in site.hourly.columns:
            convert_swe_to_sfr(type, site.hourly, target_data, new_col=new_col, inplace=True)

# ! Does not support daily conversion as of now
def convert_swe_to_sfr(type: str, df: pd.DataFrame, swe: str, new_col: bool=False, inplace=False) -> pd.DataFrame:
    """
    This function converts the daily or hourly swe to sfr to be comparable with
    satellite data. Sets the first value to be 0, originally NaN after df.diff()
    Sets all negative values to 0
    """

    if not inplace:
        df = df.copy(deep=True)

    if type == "hourly":
        if new_col:
            target = "hourly_" + swe
        else:
            target = swe

        # Start at 1 index
        df[target] = df[swe].diff()
        # Deal with 0 index and negatives
        df.at[0, target] = 0
        df.loc[df[target] < 0, target] = 0
    elif type == "daily":
        ...
    else:
        raise ValueError("type parameter can only be 'hourly' or 'daily'")
    
    return df

def select_closest(df: pd.DataFrame, lon: float, lat: float) -> pd.DataFrame:
    """
    Reduces sat data points of one station down to one data point which is chosen
    with the closest to the station.
    """
    re_site_lon = reformat_long(lon)

    if not df.empty:
        closest_point = None
        closest_dist = np.inf

        for index in df.index:
            re_sat_lon = reformat_long(df["longitude"][index])

            dist = (
                (re_site_lon - re_sat_lon) ** 2 + (lat - df["latitude"][index]) ** 2
            ) ** 0.5

            if dist < closest_dist:
                closest_point = index
                closest_dist = dist

        return df[df.index == closest_point]


def reduce_sat_df(df: pd.DataFrame, lon: float, lat: float):
    """
    Reduces sat data points that overlap in date time. Chooses the data point
    that is closes to the station. Takes a datafame and the lon lat for the
    station. Directly modifies inputted dataframe because we pd.drop
    """
    re_site_lon = reformat_long(lon)

    if not df.empty:
        for _, indices in df.groupby(["datetime"]).groups.items():
            closest_point = None
            closest_dist = np.inf

            for index in indices:
                re_sat_lon = reformat_long(df["longitude"][index])
                dist = (
                    (re_site_lon - re_sat_lon) ** 2 + (lat - df["latitude"][index]) ** 2
                ) ** 0.5

                if dist < closest_dist:
                    closest_point = index
                    closest_dist = dist

            for index in indices:
                if index != closest_point:
                    df.drop(index, inplace=True)

def select_closest_collocated_data(site: CollocatedSiteData):
    """
    Iteratively select one data point for every satellite DataFrame at station
    parameter is a CollocatedSiteData
    """

    for i, data in enumerate(site.sat):
        site.sat[i] = select_closest(data, site.lon, site.lat)

def reduce_sat_collocated_data(site: CollocatedSiteData):
    """
    Iteratively reduce every satellite DataFrame at station parameter is a
    CollocatedSiteData
    """
    for data in site.sat:
        reduce_sat_df(data, site.lon, site.lat)



