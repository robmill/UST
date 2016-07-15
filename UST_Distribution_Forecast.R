library(forecast)
library(iterators)
library(showtext)
library(ggplot2)
library(scales)
library(Mcomp)
library(tidyr)
library(dplyr)


source("https://raw.githubusercontent.com/ellisp/forecast/dev/R/hybridf.R")


ust<-read.csv("c:/temp/UST.csv")
ust.ts<-ts(ust)
ust.ts<-ust.ts[,2]

ust.train5<-window(ust.ts,end=5)

ust.train<-window(ust.ts,end=6)
#ust.ets<-ets(ust.train,lambda = TRUE,bounds = c("usual"),restrict = TRUE, upper=c(0.9999,0.9999,0.9999,0.9),lower=c(0.0001,0.0001,0.0001,0.8))
ust.ets<-ets(ust.train,lambda = TRUE,bounds = c("usual"),restrict = TRUE)
ust.ets5<-ets(ust.train5, bounds = c("usual"))

ust.bats<-bats(ust.train)

ust.arima<-auto.arima(ust.train)



ust.ets$aic
ust.bats$AIC

ust.forecast.bats<-forecast.bats(ust.bats,bootstrap = TRUE,simulate = TRUE,h = 10,npaths = 100000,level=c(50,80,85,95,99))

ust.forecast.ets5<-forecast(ust.ets5,h=1,level=c(50,80,85,95,99));
ust.forecast.ets<-forecast(ust.ets,bootstrap = TRUE,simulate = TRUE,h = 20,npaths = 100000,level=c(50,80,85,95,99))
ust.forecast.ets2<-forecast(ust.ets,bootstrap = FALSE,simulate = FALSE,h = 20,level=c(50,80,85,95,99))

ust.forecast.arima<-forecast(ust.arima,bootstrap = TRUE,h = 10,npaths = 10000,level=c(50,80,85,95,99))

ust.forecast.ensemble.point<-(ust.forecast.ets$mean+ust.forecast.arima$mean)/2

write.csv(ust.forecast.ets5,"c:/temp/ust_forecast_USCentral_5.csv")
write.csv(ust.forecast.ets,"c:/temp/ust_forecast_Sim_USCentral.csv")
write.csv(ust.forecast.ets,"c:/temp/ust_forecast_noSim_USCentral.csv")
write.csv(ust.forecast.bats$upper,"c:/temp/ust_forecast_bats5_APAC.csv")
write.csv(ust.forecast.arima$upper,"c:/temp/ust_forecast_arima5_APAC.csv")

par(mfrow=c(1,2))
plot(ust.forecast.bats)
plot(ust.forecast.ets)


accuracy(ust.forecast.bats,ust.ts)
accuracy(ust.forecast.ets,ust.ts)
accuracy(ust.forecast.arima,ust.ts)
accuracy(ust.forecast.ensemble.point)

#pdf()
#contour()
#dev.off()

