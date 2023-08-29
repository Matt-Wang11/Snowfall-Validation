from matplotlib.colors import LinearSegmentedColormap
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from lib.utilities import reformat_long
from lib import xr, np, pd

class Grapher:

    @classmethod
    def __proper_bounds(cls, left_long, right_long, down_lat, top_lat, margin=10) -> list:
        """
        Takes lat, lon points and output a rectangular area around that properly
        displays the area If increasing left to right get min on left and max on right
        For matplotlib graphing purposes
        """

        if margin <= 0:
            raise ValueError()

        if left_long - margin <= -180:
            left_long = 2 * 180 - margin + left_long
        else:
            left_long -= margin

        if right_long + margin > 180:
            right_long = 2 * -180 + margin + right_long
        else:
            right_long += margin

        top_lat = min(90, top_lat + margin / 2)
        down_lat = max(-90, down_lat - margin)

        return [reformat_long(left_long), reformat_long(right_long), down_lat, top_lat]

    @classmethod
    def graph_sfr(cls, long, lat, sfr, projection, extent=None, title="SFR"):
        """
        Takes in 2d array long, lat, sfr and graphs the satellite data on a world map
        """
        alpha = 0.7
        color_scl = (
            [0, (255 / 256, 255 / 256, 255 / 256, alpha)],
            [0.1, (50 / 256, 95 / 256, 153 / 256, alpha)],
            [0.2, (99 / 256, 215 / 256, 90 / 256, alpha)],
            [0.4, (255 / 256, 255 / 256, 84 / 256, alpha)],
            [0.6, (234 / 256, 51 / 256, 35 / 256, alpha)],
            [0.8, (159 / 256, 32 / 256, 21 / 256, alpha)],
            [1.0, (82 / 256, 12 / 256, 6 / 256, alpha)],
        )

        sfr = sfr[:-1, :-1]

        long = reformat_long(long)

        ax = plt.axes(projection=projection)
        transform = ccrs.PlateCarree()

        cmap = LinearSegmentedColormap.from_list("cmap", color_scl)

        mesh = plt.pcolormesh(
            long, lat, sfr, cmap=cmap, vmin=np.min(sfr), vmax=np.max(sfr),
            transform=transform,
        )

        if extent is not None:
            ax.set_extent(extent, transform)

        ax.add_feature(cfeature.OCEAN, facecolor="turquoise", alpha=0.4)
        ax.add_feature(cfeature.LAND, facecolor="olivedrab", alpha=0.4)
        ax.add_feature(cfeature.BORDERS, edgecolor="black")
        ax.coastlines()

        gl = ax.gridlines(
            crs=transform, draw_labels=True, x_inline=False, y_inline=False,
            linewidth=0.33, color="k", alpha=0.5,
        )
        gl.right_labels = False
        gl.top_labels = False

        plt.colorbar(mesh, ax=ax)
        plt.title(title)
        plt.show()

    @classmethod
    def graph_df(cls, df: pd.DataFrame, projection, row_len: int=90, extent: list=None, title: str="SFR"):
        """
        n20, npp : 96
        moc, mob, n19 : 90
        """
        
        shape = (int(len(df) / row_len), row_len)
        
        sfr = df.sfr.to_numpy().reshape(shape)
        lon = df.longitude.to_numpy().reshape(shape)
        lat = df.latitude.to_numpy().reshape(shape)
        
        sfr[sfr == -999] = 0
        cls.graph_sfr(lon, lat, sfr, projection=projection, extent=extent, title=title)

    @classmethod
    def graph_nc(cls, data_path, projection, extent=None, title="SFR"):
        ds = xr.open_dataset(data_path)
        sfr = ds["SFR"].data
        lon = ds["Longitude"].data
        lat = ds["Latitude"].data

        sfr[sfr == -999] = 0
        cls.graph_sfr(lon, lat, sfr, projection=projection, extent=extent, title=title)

    @classmethod
    def graph_multiple_nc(cls, files, projection, extent=None, title="SFR"):
        """
        Combines multiple satellite swaths and then graph them together as one
        calls on graph_sfr to perform graphing.
        """

        ds = xr.open_dataset(files[0])
        sfr = ds["SFR"].data
        lon = ds["Longitude"].data
        lat = ds["Latitude"].data

        for f in files[1:]:
            ds = xr.open_dataset(f)

            sfr = np.concatenate((sfr, ds["SFR"].data), axis=0)
            lon = np.concatenate((lon, ds["Longitude"].data), axis=0)
            lat = np.concatenate((lat, ds["Latitude"].data), axis=0)

        sfr[sfr == -999] = 0
        cls.graph_sfr(lon, lat, sfr, projection=projection, extent=extent, title=title)

