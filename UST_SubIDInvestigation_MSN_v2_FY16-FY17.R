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
MSN_Compute_RAW<-submit_iscope(vc, 'SELECT * FROM 
                                (SSTREAM "/my/SubIDInvestigation/MSN/MSN_ComputeUsage.ss"); 
                                OUTPUT TO CONSOLE;')





# Modify DateTime variable to correct date format
MSN_Compute<-data.frame(MSN_Compute_RAW,stringsAsFactors = FALSE);
MSN_Compute<-MSN_Compute[MSN_Compute$ClusterType=="COMPUTE",]
MSN_Compute$DateAndTime<-as.Date(MSN_Compute$DateAndTime,format="%m/%d/%Y");

# Subset for FY16
FY16_Start<-as.Date("2015-08-01")
FY16_End<-as.Date("2016-07-30")
MSN_Compute_FY16_daily<-MSN_Compute[MSN_Compute$DateAndTime >= FY16_Start 
                                      & MSN_Compute$DateAndTime <= FY16_End,]
MSN_Compute_FY16_daily_sorted<-
        MSN_Compute_FY16_daily[order(MSN_Compute_FY16_daily$DateAndTime),]

write.csv(MSN_Compute_FY16_daily,"c:/temp/MSN_Compute_Daily.csv")


# Aggregate to monthly time grain
# Hadley Wickham 
# http://stackoverflow.com/questions/6052631/aggregate-daily-data-to-month-year-in-r
MSN_Compute_FY16_daily_sorted_temp<-MSN_Compute_FY16_daily_sorted
MSN_Compute_FY16_daily_sorted_temp$my<-
        floor_date(MSN_Compute_FY16_daily_sorted_temp$DateAndTime,"month")


# Compute average and peak monthly usage
MSN_Compute_avg_monthly<-sqldf('select my as DateAndTime,
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
                                FROM MSN_Compute_FY16_daily_sorted_temp
                                GROUP BY my,SubscriptionGUID,Customer,GeographyName,RegionName,ModelSegment,ClusterType,VMType,VMSize')

MSN_Compute_peak_monthly<-sqldf('select my as DateAndTime,
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
                                 FROM MSN_Compute_FY16_daily_sorted_temp
                                 GROUP BY my,SubscriptionGUID,Customer,GeographyName,RegionName,ModelSegment,ClusterType,VMType,VMSize')

MSN_Compute_monthly_Detailed<-sqldf('select my as DateAndTime,
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
                                     FROM MSN_Compute_FY16_daily_sorted_temp
                                     GROUP BY my,SubscriptionGUID,Customer,GeographyName,RegionName,ModelSegment,ClusterType,VMType,VMSize')


MSN_Compute_monthly_TopLevel<-sqldf('select my as DateAndTime,
                                     SubscriptionGUID,
                                     avg(TotalQuantity) as AvgMonthlyQuantity,
                                     max(TotalQuantity) as PeakMonthlyQuantity,
                                     sum(TotalQuantity) as MonthlyAgg,
                                     sum(TotalQuantity)/30 as Avg30Day
                                     FROM MSN_Compute_FY16_daily_sorted_temp
                                     GROUP BY my,SubscriptionGUID')


# Validate that average value snaps to monthly compute hours found in CSDF Forecast Comparison
April16_Validation_TopLevel<-sum(MSN_Compute_monthly_TopLevel$Avg30Day[MSN_Compute_monthly_TopLevel$DateAndTime=="2016-04-01"]);
April16_Validation_Detailed<-sum(MSN_Compute_monthly_Detailed$Avg30Day[MSN_Compute_monthly_Detailed$DateAndTime=="2016-04-01"]);

MSN_Compute_monthly_TotalValidation<-sqldf('select my as DateAndTime,
                                            avg(TotalQuantity) as AvgMonthlyQuantity,
                                            max(TotalQuantity) as PeakMonthlyQuantity,
                                            sum(TotalQuantity) as MonthlyAgg,
                                            sum(TotalQuantity)/30 as Avg30Day
                                            FROM MSN_Compute_FY16_daily_sorted_temp
                                            GROUP BY my'))

count_SubIDs<-unique(MSN_Compute_monthly_Detailed$SubscriptionGUID);

# Write MSN Monthly Compute Detailed and TopLevel to CSV
write.csv(MSN_Compute_monthly_TopLevel,"c:/temp/MSN_SubID_InvestigationCOMPUTE_TopLevel.csv")
write.csv(MSN_Compute_monthly_Detailed,"c:/temp/MSN_SubID_InvestigationCOMPUTE_Detailed.csv")

MSN_SubID_List<-data.frame(unique(MSN_Compute_monthly_TopLevel$SubscriptionGUID))




plot(MSN_Compute_monthly_TopLevel[MSN_Compute_monthly_TopLevel$SubscriptionGUID==MSN_SubID_List[3,],]$AvgMonthlyQuantity, type="l")

MSN_Compute_monthly_TopLevel_BoxPlot<-MSN_Compute_monthly_TopLevel;
MSN_Compute_monthly_TopLevel_BoxPlot$DateAndTime<-as.yearmon(MSN_Compute_monthly_TopLevel_BoxPlot$DateAndTime);

boxplot(Avg30Day ~ DateAndTime, data=MSN_Compute_monthly_TopLevel_BoxPlot, xlab="Month", ylab="Azure Compute Units");
