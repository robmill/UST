library(forecast);
library(fpp);
library(xts)
library(tseries)
library(CosmosToR)
library(zoo)
library(plyr)
library(dplyr)
library(reshape2)
library(lubridate)
library(sqldf)

# Connect to CSDFENG COSMOS VC
#
vc <- vc_connection('https://cosmos11.osdinfra.net/cosmos/CSDFENG/')
SETO_Compute_RAW<-submit_iscope(vc, 'SELECT * FROM 
        (SSTREAM "/my/SubIDInvestigation/SETO/SETO_ComputeUsage.ss"); 
        OUTPUT TO CONSOLE;')

# translate to ts class data

# adf.test()

# use window to generate train date set

# use window to generate test set


# forecast to generate forecast class data


# use accuracy on forecast class data

# minimize AIC


# Modify DateTime variable to correct date format
SETO_Compute<-data.frame(SETO_Compute_RAW,stringsAsFactors = FALSE);
SETO_Compute<-SETO_Compute[SETO_Compute$ClusterType=="COMPUTE",]
SETO_Compute$DateAndTime<-as.Date(SETO_Compute$DateAndTime,format="%m/%d/%Y");

# Subset for FY16
FY16_Start<-as.Date("2015-07-01")
FY16_End<-as.Date("2016-06-30")
SETO_Compute_FY16_daily<-SETO_Compute[SETO_Compute$DateAndTime >= FY16_Start 
                                      & SETO_Compute$DateAndTime <= FY16_End,]
SETO_Compute_FY16_daily_sorted<-
        SETO_Compute_FY16_daily[order(SETO_Compute_FY16_daily$DateAndTime),]

write.csv(SETO_Compute_FY16_daily,"c:/temp/SETO_Compute_Daily.csv")


# Aggregate to monthly time grain
# Hadley Wickham 
# http://stackoverflow.com/questions/6052631/aggregate-daily-data-to-month-year-in-r
SETO_Compute_FY16_daily_sorted_temp<-SETO_Compute_FY16_daily_sorted
SETO_Compute_FY16_daily_sorted_temp$my<-
        floor_date(SETO_Compute_FY16_daily_sorted_temp$DateAndTime,"month")


# Compute average and peak monthly usage
SETO_Compute_avg_monthly<-sqldf('select my as DateAndTime,
                                SubscriptionGUID,
                                Customer,
                                GeographyName,
                                RegionName,
                                ModelSegment,
                                ClusterType,
                                VMType,
                                VMSize,
                                VMSeries,
                                avg(TotalQuantity) as AvgMonthlyQuantity
                                FROM SETO_Compute_FY16_daily_sorted_temp
                                GROUP BY my,SubscriptionGUID,Customer,GeographyName,RegionName,ModelSegment,ClusterType,VMType,VMSize')

SETO_Compute_peak_monthly<-sqldf('select my as DateAndTime,
                                SubscriptionGUID,
                                Customer,
                                GeographyName,
                                RegionName,
                                ModelSegment,
                                ClusterType,
                                VMType,
                                VMSize,
                                VMSeries,
                                max(TotalQuantity) as PeakMonthlyQuantity
                                FROM SETO_Compute_FY16_daily_sorted_temp
                                GROUP BY my,SubscriptionGUID,Customer,GeographyName,RegionName,ModelSegment,ClusterType,VMType,VMSize')

SETO_Compute_monthly_Detailed<-sqldf('select my as DateAndTime,
                                SubscriptionGUID,
                                 Customer,
                                 GeographyName,
                                 RegionName,
                                 ModelSegment,
                                 ClusterType,
                                 VMType,
                                 VMSize,
                                 VMSeries,
                                 avg(TotalQuantity) as AvgMonthlyQuantity,
                                 max(TotalQuantity) as PeakMonthlyQuantity,
                                sum(TotalQuantity)/30 as Avg30Day
                                 FROM SETO_Compute_FY16_daily_sorted_temp
                                 GROUP BY my,SubscriptionGUID,Customer,GeographyName,RegionName,ModelSegment,ClusterType,VMType,VMSize')


SETO_Compute_monthly_TopLevel<-sqldf('select my as DateAndTime,
                                SubscriptionGUID,
                                 avg(TotalQuantity) as AvgMonthlyQuantity,
                                 max(TotalQuantity) as PeakMonthlyQuantity,
                                sum(TotalQuantity) as MonthlyAgg,
                                sum(TotalQuantity)/30 as Avg30Day
                                 FROM SETO_Compute_FY16_daily_sorted_temp
                                 GROUP BY my,SubscriptionGUID')


# Validate that average value snaps to monthly compute hours found in CSDF Forecast Comparison
April16_Validation_TopLevel<-sum(SETO_Compute_monthly_TopLevel$Avg30Day[SETO_Compute_monthly_TopLevel$DateAndTime=="2016-04-01"]);
April16_Validation_Detailed<-sum(SETO_Compute_monthly_Detailed$Avg30Day[SETO_Compute_monthly_Detailed$DateAndTime=="2016-04-01"]);

SETO_Compute_monthly_TotalValidation<-sqldf('select my as DateAndTime,
                                     avg(TotalQuantity) as AvgMonthlyQuantity,
                                     max(TotalQuantity) as PeakMonthlyQuantity,
                                     sum(TotalQuantity) as MonthlyAgg,
                                    sum(TotalQuantity)/30 as Avg30Day
                                     FROM SETO_Compute_FY16_daily_sorted_temp
                                     GROUP BY my'))

count_SubIDs<-unique(SETO_Compute_monthly_Detailed$SubscriptionGUID);

# Write SETO Monthly Compute Detailed and TopLevel to CSV
write.csv(SETO_Compute_monthly_TopLevel,"c:/temp/SETO_SubID_InvestigationCOMPUTE_TopLevel.csv")
write.csv(SETO_Compute_monthly_Detailed,"c:/temp/SETO_SubID_InvestigationCOMPUTE_Detailed.csv")

SETO_SubID_List<-data.frame(unique(SETO_Compute_monthly_TopLevel$SubscriptionGUID))




plot(SETO_Compute_monthly_TopLevel[SETO_Compute_monthly_TopLevel$SubscriptionGUID==SETO_SubID_List[3,],]$AvgMonthlyQuantity, type="l")

SETO_Compute_monthly_TopLevel_BoxPlot<-SETO_Compute_monthly_TopLevel;
SETO_Compute_monthly_TopLevel_BoxPlot$DateAndTime<-as.yearmon(SETO_Compute_monthly_TopLevel_BoxPlot$DateAndTime);

boxplot(Avg30Day ~ DateAndTime, data=SETO_Compute_monthly_TopLevel_BoxPlot, xlab="Month", ylab="Azure Compute Units");

# validate against Excel results
test<-sqldf('SELECT 
        DateAndTime,
        Customer,
        GeographyName,
            RegionName,
            ModelSegment,
            ClusterType,
            VMType,
            VMSize,
            VMSeries,
            AvgMonthlyQuantity,
            SubscriptionGUID
            FROM SETO_Compute_avg_monthly
            WHERE SubscriptionGUID=="002b06d5-140b-4518-ab8c-67bb8e174d68"')