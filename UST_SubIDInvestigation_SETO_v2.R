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
SETO_Compute_RAW<-submit_iscope(vc, 'SELECT * FROM (SSTREAM "/my/SubIDInvestigation/SETO/SETO_ComputeUsage.ss"); OUTPUT TO CONSOLE;')

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
FY16_Start<-as.Date("2015-06-01")
FY16_End<-as.Date("2016-06-30")
SETO_Compute_FY16_daily<-SETO_Compute[SETO_Compute$DateAndTime >= FY16_Start & SETO_Compute$DateAndTime <= FY16_End,]
SETO_Compute_FY16_daily_sorted<-SETO_Compute_FY16_daily[order(SETO_Compute_FY16_daily$DateAndTime),]

write.csv(SETO_Compute_FY16_daily,"c:/temp/SETO_Compute_Daily.csv")


# Aggregate to monthly time grain
# Hadley Wickham 
# http://stackoverflow.com/questions/6052631/aggregate-daily-data-to-month-year-in-r
SETO_Compute_FY16_daily_sorted_temp<-SETO_Compute_FY16_daily_sorted
SETO_Compute_FY16_daily_sorted_temp$my<-floor_date(SETO_Compute_FY16_daily_sorted_temp$DateAndTime,"month")


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
                                     GROUP BY my')

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


#
#
#headSETOust_compute<-read.csv("C:/UST_Usage/UstOnlyComputeUsage.ss.csv")
#msn_compute<-read.csv("C:/UST_Usage/MSN_Template_ComputeUsage.ss.csv")
#ust_compute$DateAndTime<-as.Date(ust_compute$DateAndTime,format="%m/%d/%Y")
#msn_compute$DateAndTime<-as.Date(msn_compute$DateAndTime,format="%m/%d/%Y");

#yr<-strftime(ust_compute$DateAndTime,"%Y")
#mo<-strftime(ust_compute$DateAndTime,"%m")

# 
#msn_275_compute<-read.csv("C:/UST_Usage/MSNComputeUsage.ss.csv");

#nrow(msn_275_compute)
#msn_275_compute$DateAndTime<-as.Date(msn_275_compute$DateAndTime,format="%m/%d/%Y");
#msn_275_compute<-msn_275_compute[msn_275_compute$ClusterType=="COMPUTE",]
#msn_275_compute<-msn_275_compute[msn_275_compute$Customer=="MSN",]

#nrow(msn_275_compute)

#msn_275_compute<-msn_275_compute[grep("D",msn_275_compute$VMSize),]$TotalQuantity


#msn_275_compute[grep("D",msn_275_compute$VMSize),]$TotalQuantity<-msn_275_compute[grep("D",msn_275_compute$VMSize),]$TotalQuantity/1.625

#msn_275_daily<-aggregate(msn_275_compute$TotalQuantity,by=list(msn_275_compute$DateAndTime),FUN=sum)
#names(msn_275_daily)<-c("Date","ACU")
#plot(msn_275_daily,type="l", main="MSN Azure Compute Daily Usage - Filtered",ylab="Cores")
#lines

#msn_compute<-msn_compute[msn_compute$VMSize!="UNKNOWN",]
#msn_compute<-msn_compute[msn_compute$Customer=="MSN",]

#msn_compute<-msn_compute[msn_compute$ClusterType=="COMPUTE",]
#msn_compute[grep("D",msn_compute$VMSize),]$TotalQuantity<-msn_compute[grep("D",msn_compute$VMSize),]$TotalQuantity/1.625

#msn_daily<-aggregate(msn_compute$TotalQuantity,by=list(msn_compute$DateAndTime),FUN=sum)
#names(msn_daily)<-c("Date","ACU")
#plot(msn_daily, type="l")

#max(msn_daily$ACU)

#msn_275_region<-unique(msn_275_compute$RegionName)
#msn_275_region<-as.data.frame(msn_275_region)
#names(msn_275_region)<-c("RegionName")

#msn_275_region<-sort(msn_275_region$RegionName)
#msn_275_region<-as.data.frame(msn_275_region)
#names(msn_275_region)<-c("RegionName")


#msn_275_daily_region<-aggregate(msn_275_compute$TotalQuantity,by=list(msn_275_compute$DateAndTime,msn_275_compute$RegionName),FUN=sum)
#names(msn_275_daily_region)<-c("Date","REGION","ACU")
#plot(msn_275_daily_region["REGION"=="APAC EAST",]$ACU,type="l", main="MSN Azure Compute Daily Usage - 275 SubIDs")

#plot(msn_275_daily_region[msn_275_daily_region$REGION=="APAC EAST",]$ACU,type="l",ylab="ACU",xlab="Date",main="APAC EAST")

#plot(msn_275_daily_region[msn_275_daily_region$REGION=="APAC SOUTHEAST",]$ACU,type="l",ylab="ACU",xlab="Date",main="APAC SOUTHEAST")
#plot(msn_275_daily_region[msn_275_daily_region$REGION=="CHINA EAST",]$ACU,type="l",ylab="ACU",xlab="Date",main="CHINA EAST")
#plot(msn_275_daily_region[msn_275_daily_region$REGION=="CHINA NORTH",]$ACU,type="l",ylab="ACU",xlab="Date",main="CHINA NORTH")
#plot(msn_275_daily_region[msn_275_daily_region$REGION=="EUROPE NORTH",]$ACU,type="l",ylab="ACU",xlab="Date",main="EUROPE NORTH")
#plot(msn_275_daily_region[msn_275_daily_region$REGION=="EUROPE WEST",]$ACU,type="l",ylab="ACU",xlab="Date",main="EUROPE WEST")
#plot(msn_275_daily_region[msn_275_daily_region$REGION=="JAPAN EAST",]$ACU,type="l",ylab="ACU",xlab="Date",main="JAPAN EAST")
#plot(msn_275_daily_region[msn_275_daily_region$REGION=="US CENTRAL",]$ACU,type="l",ylab="ACU",xlab="Date",main="US CENTRAL")
#plot(msn_275_daily_region[msn_275_daily_region$REGION=="US EAST",]$ACU,type="l",ylab="ACU",xlab="Date",main="US EAST")
#plot(msn_275_daily_region[msn_275_daily_region$REGION=="US EAST 2",]$ACU,type="l",ylab="ACU",xlab="Date",main="US EAST 2")
#plot(msn_275_daily_region[msn_275_daily_region$REGION=="US NORTH CENTRAL",]$ACU,type="l",ylab="ACU",xlab="Date",main="US NORTH CENTRAL")
#plot(msn_275_daily_region[msn_275_daily_region$REGION=="US SOUTH CENTRAL",]$ACU,type="l",ylab="ACU",xlab="Date",main="US SOUTH CENTRAL")
#plot(msn_275_daily_region[msn_275_daily_region$REGION=="US WEST",]$ACU,type="l",ylab="ACU",xlab="Date",main="US WEST")
#US NORTH CENTRAL
#yr_

#write.csv(msn_275_daily_region,"c:/UST_Usage/msn_regionView.csv")

#yr<-unique(yr)
#mo<-unique(mo)

#ust_compute_ts<-ust_compute[,c("DateAndTime","TotalQuantity")]

#ust_daily<-aggregate(ust_compute$TotalQuantity,by=list(ust_compute$DateAndTime),FUN=sum)
#names(ust_daily)<-c("Date","ACU")
#plot(ust_daily,type="l")

#msn_daily<-aggregate(msn_compute$TotalQuantity,by=list(msn_compute$DateAndTime),FUN=sum)
#names(msn_daily)<-c("Date","ACU")

#ust_yearmon<-as.yearmon(ust_compute_ts$DateAndTime)
#ust_monthly<-aggregate(ust_compute$TotalQuantity,by=list(ust_yearmon),FUN=sum)

#test$Group.1<-as.Date(test$Group.1)

#plot(test[1:nrow(test)-1,])


#daily_arima<-auto.arima(daily$x)
#plot(forecast(daily_arima,h=100),type="l")

#daily_ets<-ets(daily$x)

#plot(forecast(daily_ets,h=730))
#plot(forecast(daily_arima,h=730),type="l")

#daily_arima_forecast<-forecast(daily_arima,h=730)


#daily_window<-window(daily,)
#ust_monthly_arima<-auto.arima(ust_monthly[5:40,]$x)
#forecast(ust_monthly_arima)

#forecast(ust_monthly_arima)
#ust_monthly_arima<-auto.arima(ust_monthly[10:30,]$x)
#forecast(ust_monthly_arima)
#plot(forecast(ust_monthly_arima,h=24))

#plots
plot(ust_daily,type="l",main="Universal Store Azure Compute Daily Usage")
plot(ust_daily,type="l",main="Universal Store Azure Compute Daily Usage",col="dark blue")

#Auto.Arima
ust_daily_arima<-auto.arima(ust_daily[200:600,]$ACU)

ust_daily$Month<-cut(ust_daily$Date,breaks = "month")
plot(msn_daily, type="l", main="MSN Azure Compute Daily Usage")

msn_region<-unique(msn_compute$RegionName)
msn_region<-as.data.frame(msn_region)
names(msn_region)<-c("RegionName")
msn_region<-sort(msn_region$RegionName)
msn_region<-as.data.frame(msn_region)
names(msn_region)<-c("RegionName")




msn_275_region<-unique(msn_275_compute$RegionName)
msn_275_region<-as.data.frame(msn_275_region)
names(msn_275_region)<-c("RegionName")

msn_275_region<-sort(msn_275_region$RegionName)
msn_275_region<-as.data.frame(msn_275_region)
names(msn_275_region)<-c("RegionName")



ust_region<-unique(ust_compute$RegionName)
ust_region<-as.data.frame(ust_region)
names(ust_region)<-c("RegionName")

ust_region<-sort(ust_region$RegionName)
ust_region<-as.data.frame(ust_region)
names(ust_region)<-c("RegionName")


msn_daily_<-aggregate(msn_compute$TotalQuantity,by=list(msn_compute$DateAndTime),FUN=sum)
names(msn_daily)<-c("Date","ACU")
