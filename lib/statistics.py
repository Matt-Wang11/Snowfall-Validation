from lib import np

class Statistics:
    """
    Statistics needs to know the target data of the snotel data set since we 
    could be using various columns. The sfr is always sfr so direct reference ok
    """

    def __init__(self, target_data: str, temp_data) -> None:
        self.swe = target_data
        self.temp = temp_data
    
    def __totals(self, site):
        """
        Returns sfr total, swe total
        """
    
        sfr_total = swe_total = 0.0
    
        for sat, sntl in site:
            if self.swe in sntl.columns:
                swe_total += len(sntl[self.swe].notna())
            sfr_total += len(sat.sfr.notna())
    
        return sfr_total, swe_total 

    def __counts(self, site):
        """
        Returns sfr count, swe count
        """
    
        sfr_total = swe_total = 0.0
        
        for sat, sntl in site:
            if self.swe in sntl.columns:
                swe_total += sntl[self.swe].sum()
            sfr_total += sat.sfr.sum()
    
        return sfr_total, swe_total 
    
    def __zscore(self, series):
        """
        Calculates the zscore of a column
        """
        return (series - series.mean()) / series.std()

    def remove_site_invalid_temp(self, site):
        for i in range(len(site)):
            site.sntl[i] = site.sntl[i][site.sntl[i][self.temp] < 0]

    def remove_dataset_invalid_temp(self, coll):
        for _, site in coll:
            self.remove_site_invalid_temp(site)

    # ! Change this function into two like above one for site one for dataset
    def process_collocated_data(self, coll, remove_zero=True):
        """
        This function preforms the quality control and prepares the data for 
        statistical calculations. 
        1. Removes all NaNs
        2. Removes all 0s 
        """
        sites_remove = []

        for code, site in coll:

            i = 0
            while i < len(site):    # Could be changed into for loop by changing the deletion to iterate the index list
                if remove_zero:
                    site.sat[i] = site.sat[i][site.sat[i].sfr != 0]
                site.sat[i] = site.sat[i][site.sat[i].sfr.notna()]
                
                if self.swe in site.sntl[i].columns:
                    if remove_zero:
                        site.sntl[i] = site.sntl[i][site.sntl[i][self.swe] > 0]
                    site.sntl[i] = site.sntl[i][site.sntl[i][self.swe].notna()]
                    
                    # Make sure swe is snow not rain
                    site.sntl[i] = site.sntl[i][site.sntl[i][self.temp] < 0]
                else:
                    site.sntl[i] = site.sntl[i][0:0]

                if site.sat[i].empty or site.sntl[i].empty:
                    site.remove_data(i)
                    i -= 1
                else:
                    z_score = self.__zscore(site.sat[i].sfr)
                    if not np.isnan(z_score).any():
                        site.sat[i] = site.sat[i][(z_score < 3) & (z_score > -3)]
                    
                    z_score = self.__zscore(site.sntl[i][self.swe])
                    if not np.isnan(z_score).any():
                        site.sntl[i] = site.sntl[i][(z_score < 3) & (z_score > -3)]
                
                i += 1
            if len(site.sat) == 0 or len(site.sntl) == 0:
                sites_remove.append(code)

        for site in sites_remove:
            coll.remove_site(site)

    def np_site_mean(self, site):
        sfr = np.array(site.get_sat_column("sfr"))
        swe = np.array(site.get_sntl_column(self.swe))

        return sfr.mean(), swe.mean()

    def np_dataset_mean(self, data):
        sfr = []
        swe = []

        for _, site in data:
            sfr.extend(site.get_sat_column("sfr"))
            swe.extend(site.get_sntl_column(self.swe))

        sfr = np.array(sfr)
        swe = np.array(swe)

        return sfr.mean(), swe.mean()

    def site_mean(self, coll_site):
        sfr_count, swe_count = self.__counts(coll_site)
        sfr_total, swe_total = self.__totals(coll_site)

        if sfr_total == 0:
            sfr_mean = np.NaN
        else:
            sfr_mean = sfr_count / sfr_total

        if swe_total == 0:
            swe_mean = np.NaN
        else:
            swe_mean = swe_count / swe_total 

        return sfr_mean, swe_mean

    def dataset_mean(self, coll_data):

        sat_sum = sat_count = sntl_sum = sntl_count = 0.0 

        for _, site in coll_data:
            sntl_sum += self.__counts(site)[1]
            sntl_count += self.__totals(site)[1]
            sat_sum += self.__counts(site)[0]
            sat_count += self.__totals(site)[0]
        
        return sat_sum/sat_count, sntl_sum/sntl_count
   
    def np_site_bias(self, site) -> float:
        sat_mean, sntl_mean = self.np_site_mean(site)

        return sat_mean - sntl_mean

    def np_bias(self, data) -> float:
        sat_mean, sntl_mean = self.np_dataset_mean(data)

        return sat_mean - sntl_mean

    def site_bias(self, coll_site) -> float:
        sat_mean, sntl_mean = self.site_mean(coll_site)

        return sat_mean - sntl_mean

    def bias(self, coll_data) -> float:
        """
        For each site we calculate the bias between the snotel and satellite data
        bias is calculated by either calculating the mean or selecting one data 
        the statistic is calculated with all of the data in each site at once
        NaNs are not included in the statistic
        
        Returns sat mean - sntl mean
        """
        sat_mean, sntl_mean = self.dataset_mean(coll_data)    

        return sat_mean - sntl_mean
   
    def np_site_corr(self, site, round=False) -> float:
        sfr = []
        swe = []

        for sat, sntl in site:
            sfr.append(sat.sfr.mean())
            swe.append(sntl[self.swe].mean())

        sfr = np.array(sfr)
        swe = np.array(swe)
        if round:
            swe = np.round(swe, decimals=8)

        return np.corrcoef(sfr, swe)[0][1]

    def np_corr(self, data, round=False) -> float:
        sfr = []
        swe = []

        for _, site in data:
            for sat, sntl in site:
                sfr.append(sat.sfr.mean())
                swe.append(sntl[self.swe].mean())

        sfr = np.array(sfr)
        swe = np.array(swe)
        if round:
            swe = np.round(swe, decimals=8)

        return np.corrcoef(sfr, swe)[0][1]

    def site_corr(self, coll_site) -> float:
        top = x_diff = y_diff = 0.0

        mean = self.site_mean(coll_site) 

        for sat, sntl in coll_site:
            if self.swe in sntl.columns \
            and sntl[self.swe].isna().sum() != len(sntl[self.swe]):
                x = np.nanmean(sat.sfr.to_numpy())
                y = np.nanmean(sntl[self.swe].to_numpy())
                top += (x - mean[0]) * (y - mean[1])
                x_diff += (x - mean[0])**2 
                y_diff += (y - mean[1])**2
        
        bottom = (x_diff * y_diff)**0.5 
        if bottom == 0:
            return np.NaN
        return top / bottom

    def correlation(self, coll_data) -> float:
        top = x_diff = y_diff = 0.0

        mean = self.dataset_mean(coll_data) 
        
        if mean == np.NaN:
            return np.NaN

        for _, site in coll_data:
            for sat, sntl in site:
                if self.swe in sntl.columns \
                and sntl[self.swe].isna().sum() != len(sntl[self.swe]):
                    x = np.nanmean(sat.sfr.to_numpy())
                    y = np.nanmean(sntl[self.swe].to_numpy())
                    top += (x - mean[0]) * (y - mean[1])
                    x_diff += (x - mean[0])**2 
                    y_diff += (y - mean[1])**2
        
        bottom = (x_diff * y_diff)**0.5 
        if bottom == 0:
            return np.NaN
        return top / bottom


    def np_site_rmse(self, site) -> float:
        sfr = []
        swe = []

        for sat, sntl in site:
            sfr.append(sat.sfr.mean())
            swe.append(sntl[self.swe].mean())

        sfr = np.array(sfr)
        swe = np.array(swe)

        return np.sqrt(((sfr - swe)**2).sum() / len(sfr))

    def np_rmse(self, data) -> float:
        sfr = []
        swe = []

        for _, site in data:
            for sat, sntl in site:
                sfr.append(sat.sfr.mean())
                swe.append(sntl[self.swe].mean())

        sfr = np.array(sfr)
        swe = np.array(swe)

        return np.sqrt(((sfr - swe)**2).sum() / len(sfr))
    
    def site_rmse(self, coll_site) -> float:
        mse = 0.0 
        count = 0
        
        for sat, sntl in coll_site: 
            if self.swe in sntl.columns \
            and sntl[self.swe].isna().sum() != len(sntl[self.swe]):
                count += 1
                x = np.nanmean(sat.sfr.to_numpy())
                y = np.nanmean(sntl[self.swe].to_numpy())
                mse += (x - y)**2
        
        if count == 0:
            return np.NaN
        return (mse / count)**0.5

    def rmse(self, coll_data) -> float:
        mse = 0.0 
        count = 0
        
        for _, site in coll_data:
            for sat, sntl in site: 
                if self.swe in sntl.columns \
                and sntl[self.swe].isna().sum() != len(sntl[self.swe]):
                    count += 1
                    x = np.nanmean(sat.sfr.to_numpy())
                    y = np.nanmean(sntl[self.swe].to_numpy())
                    mse += (x - y)**2
        
        if count == 0:
            return np.NaN
        return (mse / count)**0.5

    def POD(self, cm) -> float:
        return cm[0][0] / (cm[0][0] + cm[1][0])

    def FAR(self, cm) -> float:
        """
        False Alarm Rate
        """
        return cm[0][1] / (cm[0][1] + cm[1][1])

    def HSS(self, cm) -> float:
        return 2 * (cm[0][0] * cm[1][1] - cm[1][0] * cm[0][1]) / ((cm[0][0] + cm[1][0]) * (cm[1][0] + cm[1][1]) + (cm[0][0] + cm[0][1]) * (cm[0][1] + cm[1][1]))

    def site_confusion_matrix(self, site, sat_thres=0.2, sntl_thres=2.54) -> list[list[int]]:
        cm = [[0, 0],
              [0, 0]]
        is_detected = lambda df, t, th: (df[t] > th).any()
        
        for sat, sntl in site:
            sat_detected = is_detected(sat, "sfr", sat_thres)
            sntl_detected = is_detected(sntl, self.swe, sntl_thres)

            if sat_detected and sntl_detected:
                cm[0][0] += 1
            elif sat_detected and not sntl_detected:
                cm[0][1] += 1
            elif not sat_detected and sntl_detected:
                cm[1][0] += 1
            elif not sat_detected and not sntl_detected:
                cm[1][1] += 1
        
        return cm

    def confusion_matrix(self, coll, sat_thres=0.2, sntl_thres=2.54) -> list[list[int]]:
        """
        Produces a confusion matrix of the expected (snotel) values and the
        predicted (satellite) values. 

        The collocated dataset CAN NOT be already processed.
        Returns a 2 x 2 matrix 
        """
        cm = [[0, 0], 
              [0, 0]]

        for _, site in coll:
            temp_cm = self.site_confusion_matrix(site, sat_thres, sntl_thres)
            cm[0][0] += temp_cm[0][0]
            cm[0][1] += temp_cm[0][1]
            cm[1][0] += temp_cm[1][0]
            cm[1][1] += temp_cm[1][1]

        return cm

