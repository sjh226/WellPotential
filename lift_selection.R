####RUN CLUSTER ANALYSIS TO ISOLATE WELLS BASED ON GEOGRAPHIC LOCATION AND GENERAL AGE

library(RJDBC)
library(ggplot2)
library(dplyr)
library(sp)
library(cluster)
library(factoextra)
library(magrittr)
library(mclust)
library(spdep)
library(dismo)
library(rgeos)
library(geosphere)
library(RODBC)


myconn = odbcDriverConnect('driver={SQL Server};server=10.75.6.160;database=OperationsDataMart;trusted_connection=true')

###QUERY FOR NEW DATASET

test.set1=as.data.frame(sqlQuery(myconn,
        "SELECT DISTINCT L.WELLNAME,
                p.API,
                p.ASSET,
                p.AREA,
                ARTIFICIALLIFTTYPE,
                artificialliftsubtype,
                AGE,
                LATITUDE,
                LONGITUDE,
                TARGET_FORMATION,
                MAX(AVGONLINELINEPRESSUREPAST120DAYS) OVER (PARTITION BY L.API) AS AVGONLINELINEPRESSUREPAST120DAYS,
                MAX(AVGONLINETBGPRESSUREPAST120DAYS) OVER (PARTITION BY L.API) AS AVGONLINETBGPRESSUREPAST120DAYS,
                MAX(AVGONLINECSGPRESSUREPAST120DAYS) OVER (PARTITION BY L.API) AS AVGONLINECSGPRESSUREPAST120DAYS,
                TUBINGID
            FROM TeamOptimizationEngineering.dbo.ArtificialLiftHistory l
        INNER JOIN
            (SELECT MAX(estimatedinstalldate) OVER (partition by api) AS LastInstallDate,
                    api
               FROM TeamOptimizationEngineering.dbo.ArtificialLiftHistory) kl
            ON l.api = kl.api
            AND kl.lastinstalldate = l.estimatedinstalldate
        LEFT JOIN
            (SELECT WELLNAME,
                    ASSET,
                    API,
                    WELLKEY,
                    AREA,
                    LATITUDE,
                    LONGITUDE,
                    DATEDIFF(DD, spuddate, GETDATE()) AS AGE,
                    DRILLYEAR
               FROM OPERATIONSDATAMART.DIMENSIONS.WELLS) P
            ON P.API=L.API
        LEFT JOIN
            (SELECT WELL_ID,
                    API_NO,
                    TARGET_FORMATION
            FROM EDW.OPENWELLS.CD_WELL
            WHERE IS_OFFSHORE LIKE 'N') K
            ON LEFT(K.API_NO, 10) = P.API
        LEFT JOIN
            (SELECT DISTINCT WELLKEY,
                    API,
                    AVG(LINEPRESSURE) OVER (PARTITION BY WELLKEY) AS AVGONLINELINEPRESSUREPAST120DAYS,
                    AVG(TUBINGPRESSURE) OVER (PARTITION BY WELLKEY) AS AVGONLINETBGPRESSUREPAST120DAYS,
                    AVG(CASINGPRESSURE) OVER (PARTITION BY WELLKEY) AS AVGONLINECSGPRESSUREPAST120DAYS
            FROM OPERATIONSDATAMART.REPORTING.AllData
            WHERE CLEANDAY LIKE 'YES'
            AND DATEKEY >= DATEADD(DD, -120, GETDATE())) J
            ON J.API=L.API
        LEFT JOIN
            (SELECT DISTINCT api,
                    well1_wellname AS WELLNAME,
                    MAX(meter1_mp_tubeid) OVER (partition by api) AS TUBINGID
            FROM   edw.rtr.datadailyhistory pr
            INNER JOIN (SELECT  DISTINCT id,
                                LEFT(well1_api, 10) AS api,
                                well1_asset
                        FROM   edw.rtr.datacurrentsnapshot) k
            ON k.id = pr.id) JL
        ON JL.API=L.API
        "))

test.set1=subset(test.set1, ASSET=='Arkoma')

####INITIAL CLUSTERING OF WELL SUBSET BY AGE SINCE FIRST PRODUCTION, AND GEOGRAPHIC LOCATION

test.set=test.set1[,c('WELLNAME', 'API','AREA', 'AGE','LONGITUDE', 'LATITUDE', 'AVGONLINELINEPRESSUREPAST120DAYS')]

test.set=na.omit(test.set)

data=test.set[,c('WELLNAME', 'API', 'AGE', 'AVGONLINELINEPRESSUREPAST120DAYS', 'AREA')]

coords=test.set[,c("LONGITUDE","LATITUDE")]

df=SpatialPointsDataFrame(coords, data, coords.nrs = numeric(0),
                          proj4string = CRS(as.character(NA)), match.ID = TRUE)

proj4string(df) <- CRS("+proj=longlat +datum=WGS84")

mdist <- distm(df)

# cluster all points using a hierarchical clustering approach
hc <- hclust(as.dist(mdist), method="complete")

d=50000

# define clusters based on a tree "height" cutoff "d" and add them to the SpDataFrame
df$LocationCluster=cutree(hc, h=d)

xy=df

# expand the extent of plotting frame
xy@bbox[] <- as.matrix(extend(extent(xy),0.001))

# get the centroid coords for each cluster
cent <- matrix(ncol=2, nrow=max(xy$LocationCluster))
for (i in 1:max(xy$LocationCluster))
  # gCentroid from the rgeos package
  cent[i,] <- gCentroid(subset(xy, LocationCluster == i))@coords

# compute circles around the centroid coords using a 40m radius
# from the dismo package
ci <- circles(cent, d=d, lonlat=T)

# plot
plot(ci@polygons, axes=T)
plot(xy, col=rainbow(4)[factor(xy$LocationCluster)], add=T)

df=data.frame(df)

df2=df[,c("AGE", "AREA")]

df2$AREA=as.numeric(as.factor(df2$AREA))

fviz_nbclust(df2, kmeans, method = "gap_stat")

gg=fviz_nbclust(df2, kmeans, method = "gap_stat")

#value=as.data.frame(gg$data)

#for (i in 2:(nrow(value)-1)){
#  if(value[i,"gap"]>value[i-1,"gap"] & value[i,"gap"]>value[i+1,"gap"]){
#    value[i,"new"]=1
#  } else {
#    value[i,"new"]=0
#  }
#}

#values=subset(value, new==1)

#totalnumberclusters=ifelse(nrow(values)==0, 10, as.numeric(rownames(values))[1])

df3=kmeans(df2, 9, nstart=20)

totalnumberclusters=9

df2$cluster=as.factor(df3$cluster)

df=merge(df2,df["WELLNAME"], by="row.names", all.x=TRUE)

df=merge(df,test.set1[c("WELLNAME","API", "AVGONLINETBGPRESSUREPAST120DAYS", "AVGONLINELINEPRESSUREPAST120DAYS","AVGONLINECSGPRESSUREPAST120DAYS", "ARTIFICIALLIFTTYPE")], by="WELLNAME", all.x=TRUE)

#SUBSETTING BY ARTIFICIAL LIFT TYPE AND PREVIOUSLY DEFINED WELL CLUSTER, THEN PULLING IN PRODUCTION DATA TO DETERMINE TOP PERFORMERS IN EACH CLUSTER

artificialliftlist=c("FreeFlowing", "GasLift", "RodPump", "Plunger", "ChemicalInjection")

#loop through different artificial lift types here

for (name1 in artificialliftlist){

  df1=subset(df, ARTIFICIALLIFTTYPE==name1)

  cmd=paste0("select distinct p.WELLNAME, p.API, p.asset, avg(p.gas) over (partition by p.api) as avgdailyproductionclean from operationsdatamart.reporting.alldata p inner join (select distinct JL.* from teamoptimizationengineering.dbo.artificiallifthistory jl inner join (select max(estimatedinstalldate) over (partition by api) as LastInstallDate, api from TeamOptimizationEngineering.dbo.ArtificialLiftHistory) kl on jl.api=kl.api and kl.lastinstalldate=jl.estimatedinstalldate) l on p.api=l.api where p.asset like 'arkoma' and p.datekey>=dateadd(dd, -120, getdate()) and cleandaygas like 'yes' and ArtificialLiftType like '", name1,"' order by wellname")

  productiondata=as.data.frame(sqlQuery(myconn, cmd))

  productiondata[is.na(productiondata)] <- 0

  df3=merge(productiondata, df1[,c("API","cluster", "AGE",'AVGONLINETBGPRESSUREPAST120DAYS', 'AVGONLINELINEPRESSUREPAST120DAYS', 'AVGONLINECSGPRESSUREPAST120DAYS', 'ARTIFICIALLIFTTYPE')], by="API",all.x=TRUE)

  totalnumberclusters=max(as.numeric(df3$cluster))

  mybiglist=list()

  for(i in 1:9){

    assign(paste0('cluster',i),subset(df3, cluster==i))
    tmp=subset(df3, cluster==i)
    tmp2=paste0('cluster', i)
    mybiglist[[tmp2]] <- tmp

  }

  for(i in 1:9){

    productiondata=subset(df3, cluster==i)

    mybiglist1=list()

    for(name in names(mybiglist)){

      productiondata=mybiglist[[name]]

      sd.production=sd(productiondata$avgdailyproductionclean)

      mean.production=mean(productiondata$avgdailyproductionclean)

      productiondata$sd.productionperday=(productiondata$avgdailyproductionclean-mean.production)/sd.production

      productiondata$WeightedTotalValue=productiondata$sd.productionperday

      sd.totalweighted=sd(productiondata$WeightedTotalValue)

      mean.totalweighted=sd(productiondata$WeightedTotalValue)

      productiondata$HalfSDBelow=ifelse((mean.totalweighted-.5*sd.totalweighted)>=productiondata$WeightedTotalValue, 1, 0)

      tmp2=paste0(name, "1")
      mybiglist1[[tmp2]] <- tmp
      assign(paste0(name, "1"), productiondata)

      tmp=productiondata

    }

  }

  assign(paste0(name1, "dataset"), Reduce(function(x,y) merge(x,y, all=TRUE), mybiglist1))

}

##BINDING OF ENTIRE DATSET TOGETHER

final=rbind(rbind(rbind(rbind(FreeFlowingdataset, GasLiftdataset), RodPumpdataset), Plungerdataset), ChemicalInjectiondataset)

final=final[!duplicated(final), ]


###FUNCTIONS FOR DECISION TREE LOGIC

CriticalVelocity <- function(LinePressure) {
  if(is.finite(LinePressure)){
    1.92 * ((60 ^ (1 / 4)) * ((67 - (2.699 * 0.6 * LinePressure / (560 * 0.998))) ^ (1 / 4))) / ((2.699 * 0.6 * LinePressure / (560 * 0.998)) ^ (1 / 2))
  } else {
    NA
  }
}

CriticalRate=function(CriticalVelocity, TubingID, LinePressure){
  if(TubingID>0 & !is.na(TubingID)){
    0.7855 * (( TubingID/ 12) ^ 2) * CriticalVelocity * LinePressure * 520 * 3600 * 24 / (0.998 * 14.7 * 560 * 1000)
  }  else {
    0.7855 * ((1.995 / 12) ^ 2) * CriticalVelocity * LinePressure* 520 * 3600 * 24 / (0.998 * 14.7 * 560 * 1000)
  }
}


GLR=function(GasFlowRate, WaterFlowRate, OilFlowRate){

  if (WaterFlowRate>0| OilFlowRate>0){

    GasFlowRate/(WaterFlowRate+OilFlowRate)

  } else {
    1000
  }


}

##PULL DATA THAT WILL GO INTO DECISION TREE LOGIC

decisiontreedata=as.data.frame(sqlQuery(myconn, "
                                        SELECT DISTINCT m.WELLNAME,
                                        m.asset,
                                        m.API,
                                        m.wellkey,
                                        m.area,
                                        latitude,
                                        longitude,
                                        Max(Abs(w.md))
                                        OVER (
                                        partition BY M.api) AS CURRENTMD,
                                        Max(Abs(w.tvd))
                                        OVER (
                                        partition BY M.api) AS CURRENTTVD,
                                        L.well_id,
                                        Max(Abs(bh_md))
                                        OVER (
                                        partition BY m.api) AS BH_MD,
                                        Max(Abs(bh_tvd))
                                        OVER (
                                        partition BY M.api) AS BH_TVD,
                                        target_formation,
                                        Max(planned_azimuth)
                                        OVER (
                                        partition BY M.api) AS AZIMUTH,
                                        Avg(oil)
                                        OVER (
                                        partition BY M.api) AS AVGOIL,
                                        Avg(gas)
                                        OVER (
                                        partition BY M.api) AS AVGGAS,
                                        Avg(mcfe)
                                        OVER (
                                        partition BY M.api) AS AVGMCFE,
                                        Avg(water)
                                        OVER (
                                        partition BY M.api) AS WATER,
                                        volume,
                                        Max(tubingid)
                                        OVER (
                                        partition BY M.api) AS TUBINGID,
                                        casingid,
                                        temperature,
                                        flowrate,
                                        shutincasingpressure,
                                        sandpresent
                                        FROM   operationsdatamart.dimensions.wells m
                                        LEFT JOIN (SELECT well_id,
                                        api_no,
                                        target_formation
                                        FROM   edw.openwells.cd_well
                                        WHERE  is_offshore LIKE 'N') l
                                        ON LEFT(L.api_no, 10) = LEFT(M.api, 10)
                                        LEFT JOIN (SELECT well_id,
                                        wellbore_id,
                                        bh_md,
                                        bh_tvd,
                                        planned_azimuth,
                                        ko_md,
                                        ko_tvd,
                                        average_dogleg,
                                        tortuosity
                                        FROM   edw.openwells.cd_definitive_survey_header
                                        WHERE  phase LIKE 'ACTUAL') P
                                        ON P.well_id = L.well_id
                                        LEFT JOIN (SELECT DISTINCT L.well_id,
                                        L.wellbore_id,
                                        Max(md_assembly_base)
                                        OVER (
                                        partition BY L.well_id) AS MD,
                                        Max(tvd_assembly_base)
                                        OVER (
                                        partition BY L.well_id) AS TVD,
                                        create_date
                                        FROM   edw.openwells.cd_assembly L
                                        INNER JOIN (SELECT well_id,
                                        Max(create_date)
                                        OVER (
                                        partition BY well_id) AS MAXDATE
                                        FROM   edw.openwells.cd_assembly) P
                                        ON L.create_date = P.maxdate
                                        AND L.well_id = P.well_id) W
                                        ON W.well_id = L.well_id
                                        LEFT JOIN (SELECT DISTINCT L.well_id,
                                        L.wellbore_id,
                                        string_type,
                                        assembly_size,
                                        hole_size,
                                        Min(assembly_size)
                                        OVER (
                                        partition BY L.well_id) AS CASINGID,
                                        Max(md_assembly_base)
                                        OVER (
                                        partition BY L.well_id) AS MD,
                                        Max(tvd_assembly_base)
                                        OVER (
                                        partition BY L.well_id) AS TVD,
                                        create_date
                                        FROM   edw.openwells.cd_assembly L
                                        INNER JOIN (SELECT well_id,
                                        Max(create_date)
                                        OVER (
                                        partition BY well_id) AS MAXDATE
                                        FROM   edw.openwells.cd_assembly
                                        WHERE  string_type LIKE 'CASING') P
                                        ON L.create_date = P.maxdate
                                        AND L.well_id = P.well_id) tp
                                        ON tp.well_id = L.well_id
                                        ---DAILY PRESSURES, GAS AND WATER RATES
                                        LEFT JOIN (SELECT DISTINCT wellname,
                                        asset,
                                        api,
                                        wellkey,
                                        area,
                                        datekey,
                                        oil,
                                        gas,
                                        mcfe,
                                        water
                                        FROM   operationsdatamart.reporting.alldata
                                        WHERE  asset LIKE 'arkoma'
                                        AND cleanday LIKE 'yes'
                                        AND datekey > Dateadd(dd, -120, Getdate())) PO
                                        ON PO.api = M.api
                                        LEFT JOIN (
                                        --TUBING ID, FLOWRATES, TEMPERATURE AT SURFACE, VOLUMES
                                        SELECT DISTINCT pr.id,
                                        api,
                                        well1_wellname        AS WELLNAME,
                                        Avg(meter1_staticpresspdayavg)
                                        OVER (
                                        partition BY api) AS STATICPRESSURE,
                                        Avg(meter1_volumepday)
                                        OVER (
                                        partition BY api) AS VOLUME,
                                        Avg(meter1_diffpresspdayavg)
                                        OVER (
                                        partition BY api) AS DIFFPRESSURE,
                                        meter1_mp_tubeid      AS TUBINGID,
                                        Avg(meter1_temperature)
                                        OVER (
                                        partition BY api) AS TEMPERATURE,
                                        Avg(meter1_flowrate)
                                        OVER (
                                        partition BY api) AS FLOWRATE
                                        FROM   edw.rtr.datadailyhistory pr
                                        INNER JOIN (SELECT DISTINCT id,
                                        LEFT(well1_api, 10) AS api,
                                        well1_asset
                                        FROM   edw.rtr.datacurrentsnapshot) k
                                        ON k.id = pr.id
                                        WHERE  recorddate >= Dateadd(dd, -60, Getdate())) RY
                                        ON ry.api = m.api
                                        LEFT JOIN (SELECT DISTINCT WELL1_WELLNAME,
                                        ID,
                                        MAX(shutincasingpressure) over (partition by WELL1_Wellname) as shutincasingpressure
                                        from (SELECT DISTINCT well1_wellname,
                                        id,
                                        Avg (well1_casingpress)
                                        OVER (partition BY id) AS shutincasingpressure
                                        FROM   edw.rtr.datahourlyhistory
                                        WHERE  well1_asset LIKE 'ARK'
                                        AND recorddate > Dateadd(dd, -30, Getdate())
                                        AND meter1_flowrate = 0)P) KL
                                        ON KL.id = RY.id
                                        LEFT JOIN (--CHECK FOR PRESENCE OF SAND
                                        SELECT DISTINCT J.wellname,
                                        J.api,
                                        CASE
                                        WHEN k.sandpresent IS NULL
                                        AND p.sandpresent IS NULL THEN 0
                                        WHEN k.sandpresent = 1
                                        AND p.sandpresent IS NULL THEN 1
                                        WHEN k.sandpresent IS NULL
                                        AND p.sandpresent = 1 THEN 1
                                        WHEN k.sandpresent = 1
                                        AND p.sandpresent = 1 THEN 1
                                        END AS SANDPRESENT
                                        FROM   operationsdatamart.reporting.alldata J
                                        LEFT JOIN (SELECT DISTINCT wellname,
                                        api,
                                        asset,
                                        1 AS SANDPRESENT
                                        FROM   operationsdatamart.reporting.alldata
                                        WHERE  lastchokecomment LIKE '% sand %'
                                        AND lastchokecomment NOT LIKE
                                        '%NO SIGN OF SAND%'
                                        AND lastchokecomment NOT LIKE
                                        '%NO SAND%'
                                        AND datekey >= Dateadd(dd, -180,
                                        Getdate())
                                        AND asset LIKE 'ARKOMA') P
                                        ON J.api = P.api
                                        LEFT JOIN (SELECT assetname          AS WELLNAME,
                                        RIGHT(assetid, 10) AS api,
                                        1                  AS sandpresent
                                        FROM   edw.enbase.surfacefailureaction
                                        WHERE  surfacefailuredamages LIKE '% SAND %'
                                        AND createddate >= Dateadd(dd, -180,
                                        Getdate
                                        ())) k
                                        ON j.api = k.api
                                        WHERE  J.asset LIKE 'ARKOMA') LK
                                        ON LK.api = M.api
                                        WHERE  m.asset LIKE 'arkoma'
                                        "))

final2=merge(final, decisiontreedata, by=c("WELLNAME","API", "asset"), all.x=TRUE)

for(i in 1:nrow(final2)){
  final2[i,"CriticalVelocity"]=CriticalVelocity(final2[i,"AVGONLINELINEPRESSUREPAST120DAYS"])

  final2[i,"CriticalRate"]=CriticalRate(final2[i,"CriticalVelocity"], final2[i,"TUBINGID"], final2[i,"AVGONLINELINEPRESSUREPAST120DAYS"])

  final2[i,"GLR"]=GLR(final2[i,"AVGGAS"], final2[i,"WATER"], final2[i,"AVGOIL"])

}

final2$BH_TVD=ifelse(is.na(final2$BH_TVD), 10000, final2$BH_TVD)

final2$BH_MD=ifelse(is.na(final2$BH_MD), 10000, final2$BH_MD)

final2$casingid=ifelse(is.na(final2$casingid), 2.375, final2$casingid)

final2$TUBINGID=ifelse(is.na(final2$TUBINGID), 1.995, final2$TUBINGID)

final2$CriticalRate=ifelse(is.finite(final2$CriticalRate), final2$CriticalRate, mean(final2$CriticalRate))

final2$GLR=ifelse(is.finite(final2$GLR), final2$GLR, mean(final2$GLR))

final2$shutincasingpressure=ifelse(is.na(final2$shutincasingpressure), mean(final2$shutincasingpressure), final2$shutincasingpressure)

final3 <- final2[!(is.na(final2$shutincasingpressure)),]

final3 <- final3[!(is.na(final3$CriticalRate)),]

final3 <- final3[!(is.nan(final3$CriticalRate)),]

final3 <- final3[!(is.na(final3$AVGMCFE)),]

##DECISION TREE LOGIC

decisiontree=function(CriticalRate, MD, TVD, OilFlowRate, GasFlowRate, WaterFlowRate, shutincasingpressure, LinePressure, CasingID, SandPresent, GLR){

  if (GasFlowRate<=CriticalRate & WaterFlowRate<25 & GLR>400 & OilFlowRate<10 & shutincasingpressure>1.5*LinePressure & GasFlowRate>.8*CriticalRate){
    "Continuous Plunger"
  } else if (GasFlowRate<=CriticalRate & WaterFlowRate<25 & GLR>400 & OilFlowRate<10 & shutincasingpressure>1.5*LinePressure & GasFlowRate<=.8*CriticalRate){
    "Conventional Plunger"
  } else if (GasFlowRate<=CriticalRate & (WaterFlowRate>=25 | OilFlowRate>=10| shutincasingpressure<=1.5*LinePressure) & (OilFlowRate>50 | WaterFlowRate>200)){
    "ESP"
  } else if (GasFlowRate<=CriticalRate & SandPresent==0 & (WaterFlowRate>=25 | OilFlowRate>=10) & (OilFlowRate<=50 | WaterFlowRate<=200) & TVD<13500){
    "Rod Pump"
  } else if (GasFlowRate<=CriticalRate & (WaterFlowRate>=25 | OilFlowRate>=10 | shutincasingpressure<=1.5*LinePressure) & (OilFlowRate<=50 | WaterFlowRate<=200) & TVD>13500){
    "Gas Lift, Plunger, Velocity string or Foamer"
  } else if (GasFlowRate>CriticalRate & (OilFlowRate>50 | WaterFlowRate>200)){
    "Freeflowing or ESP (only if ESP is economic)"
    #} else if (GasFlowRate>CriticalRate & (OilFlowRate<=50 | WaterFlowRate<=200)){
    #  "Free Flowing or Rod Pump (only if rod pump is economic)"
  } else if (GasFlowRate>CriticalRate & WaterFlowRate<=50){
    "FreeFlowing"
  } else {
    "Gas Lift, Foamer, other"
  }
}

CostofOperationPlunger=function(ExpectedLifespan, SuggestedALType, AVERAGEINSTALLCOSTPLUNGER,
                                AVERAGECOSTRIGJOBPLUNGER, AVERAGEDAYSBETWEENRIGJOBSPLUNGER,
                                AVERAGECOSTWIRELINEJOBPLUNGER, AVERAGETIMEBETWEENWIRELINEJOBSPLUNGER, AVERAGETIMEBETWEENCHANGEOUTSPLUNGER){
  AVERAGEINSTALLCOSTPLUNGER+AVERAGECOSTRIGJOBPLUNGER*ceiling(ExpectedLifespan/AVERAGEDAYSBETWEENRIGJOBSPLUNGER)+AVERAGECOSTWIRELINEJOBPLUNGER*ceiling(ExpectedLifespan/AVERAGETIMEBETWEENWIRELINEJOBSPLUNGER)+100*ceiling(ExpectedLifespan/AVERAGETIMEBETWEENCHANGEOUTSPLUNGER)
}

CostofOperationRodPump=function(ExpectedLifespan, SuggestedALType, AVERAGEINSTALLCOSTRODPUMP,AVERAGECOSTRIGJOBRODPUMP,
                                AVERAGEDAYSBETWEENRIGJOBSRODPUMP){
  AVERAGEINSTALLCOSTRODPUMP+AVERAGECOSTRIGJOBRODPUMP*ceiling(ExpectedLifespan/AVERAGEDAYSBETWEENRIGJOBSRODPUMP)

}

CostofOperationGasLift=function(ExpectedLifespan, SuggestedALType, AVERAGEINSTALLCOSTGASLIFT,
                                AVERAGECOSTRIGJOBGASLIFT, AVERAGEDAYSBETWEENRIGJOBSGASLIFT){
  AVERAGEINSTALLCOSTGASLIFT+AVERAGECOSTRIGJOBGASLIFT*ceiling(ExpectedLifespan/AVERAGEDAYSBETWEENRIGJOBSGASLIFT)

}

needs.optimized=subset(final3, HalfSDBelow==1)

for (i in 1:nrow(needs.optimized)){
  needs.optimized[i,"SuggestedALType"]=decisiontree(needs.optimized[i,"CriticalRate"], needs.optimized[i,"BH_MD"], needs.optimized[i,"BH_TVD"],
                                                    needs.optimized[i,"AVGOIL"], needs.optimized[i,"AVGMCFE"], needs.optimized[i,"WATER"],
                                                    needs.optimized[i,"shutincasingpressure"],
                                                    needs.optimized[i,"AVGONLINELINEPRESSUREPAST120DAYS"], needs.optimized[i,"casingid"], needs.optimized[i,"sandpresent"], needs.optimized[i, "GLR"])
}

#needs.optimized=merge(needs.optimized, na.omit(unique(well_eurs[,c("EUR_Gas_w_def","TimeToLimit_Gas_w_def", "API", "EstimatedReserves")])), by="API")

needs.optimized$ExpectedLifespan=needs.optimized$TimeToLimit_Gas_w_def

needs.optimized=needs.optimized[!duplicated(needs.optimized),]

myconn = odbcDriverConnect('driver={SQL Server};server=SQLDW-L48.BP.COM;database=OperationsDataMart;trusted_connection=true')

values=paste("(", "'", needs.optimized$WELLNAME, "'",",","'",needs.optimized$API,"'",",","'",needs.optimized$asset,"'",",","'",needs.optimized$area,"'",",","'",needs.optimized$EVALUATEDARTIFICIALLIFTTYPE,"'", ",","'", needs.optimized$SuggestedALType, "'",")",sep="", collapse=",")

cmd=paste("delete from TeamOptimizationEngineering.dbo.ARTIFICIALLIFTSELECTIONARKOMA; INSERT INTO TEAMOPTIMIZATIONENGINEERING.dbo.ARTIFICIALLIFTSELECTIONARKOMA(WELLNAME, API, ASSET, AREA, CURRENTARTIFICIALLIFTTYPE, RECOMMENDEDARTIFICIALLIFTTYPE) VALUES ", values)

#sqlQuery(myconn, cmd)
