source('./startup.R')
library(WindR)
w.start(showmenu = FALSE)

outpath <- "D:/Axioma folder/temp Axioma ws"
# preposition.path <- "//nas/交易/交易计划/StandardTradingPlanGenerator/PrePosition/历史数据/陈晓"

prc_dt <- '20180731'
trade_dt <- '20180801'

# for preposition, no longer needed
# strategy_code <- 'EN300C_W'
# strategy_code <- 'EN300C_W_H'
# strategy_code <- 'EN500A'
# strategy_code <- 'RF500C'
# strategy_code <- 'SM500'
# strategy_code <- 'RF500C_A'


# update NT list for Axioma
updateNT1list <- function() {
    code <- "select trade_dt, concat(substr(s_info_windcode,1,6), '-CN') tickerMap, 1 flag
    from ashareeodprices
    where s_dq_tradestatus = '停牌'
    and trade_dt >='20050101' order by 1, 2"
    NTlist <- execQuery(code)
    fwrite(NTlist, file = sprintf("%s/NT.csv", outpath), col.names = FALSE, row.names = FALSE, quote = FALSE, sep = ",")
    cat("NT1 updated!\n")
}

updateNT1list()



# update NT2 list for Axioma
updateNT2list <- function(dt) {
    if (nchar(as.character(dt))==10) {dt <- format(as.Date(dt), '%Y%m%d')}
    NT <- w.wset('tradesuspend', sprintf('startdate=%s;enddate=%s', dt, dt))$Data
    NT <- data.frame(tickerMap = paste0(substr(NT$wind_code, 1, 6), '-CN'), flag = 1)
    fwrite(NT, file = sprintf("%s/NT2.csv", outpath), row.names = FALSE, col.names = FALSE, sep = ",")
    cat("NT2 updated!\n")
    cat(sprintf("Number of NT names: %d\n", nrow(NT)))
}
updateNT2list(dt = trade_dt)



# update ST list for Axioma
updateSTlist <- function(){
    code <- sprintf("select distinct concat(substr(st.s_info_windcode,1,6), '-CN') tickerMap, 1 flag
                    from asharest st, asharedescription a
                    where st.s_info_windcode = a.s_info_windcode
                    and a.s_info_delistdate is null
                    and st.remove_dt is null
                    order by 1")
    STlist <- execQuery(code)
    fwrite(STlist, file = sprintf("%s/ST.csv", outpath), col.names = FALSE, row.names = FALSE, quote = FALSE, sep = ",")
    cat("ST updated!\n")
}

updateSTlist()


# update Citics Industry Classification
updateCiticsClass <- function(){
    IndClassMapping <- read.csv("./Citics Industry Classification Mapping.csv", header = TRUE)
    
    code <- "select concat(substr(a.s_info_windcode, 1, 6), '-CN') s_info_windcode, b.Industriesname
    from AShareIndustriesClassCITICS a,
    AShareIndustriesCode  b
    where substr(a.citics_ind_code, 1, 4) = substr(b.IndustriesCode, 1, 4)
    and b.levelnum = '2'
    and a.cur_sign = '1'
    order by 1"
    CiticsIndClass <- execQuery(code)
    CiticsIndClass <- sqldf("select a.s_info_windcode, b.Code from CiticsIndClass a, IndClassMapping b
                            where a.industriesname = b.Name
                            order by a.s_info_windcode")
    fileConn<-file(sprintf("%s/Citics_Industry.csv", outpath))
    writeLines(c("METAGROUP NAME,Citics Sector",
                 "METAGROUP DESC,Citics Classification",
                 "NAME PREFIX,Citics.",
                 ",",
                 ","), fileConn)
    close(fileConn)
    
    fwrite(CiticsIndClass, file = sprintf("%s/Citics_Industry.csv", outpath), col.names = FALSE, row.names = FALSE, quote = FALSE, append = TRUE, sep = ",")
    cat("Citics Classification updated!\n")
}

updateCiticsClass()

updateCiticsClassII <- function(){
    IndClassMapping <- read.csv("./Citics Industry Classification II Mapping.csv", header = TRUE)
    
    code <- "select concat(substr(a.s_info_windcode, 1, 6), '-CN') s_info_windcode, b.Industriesname
    from AShareIndustriesClassCITICS a,
    AShareIndustriesCode  b
    where substr(a.citics_ind_code, 1, 6) = substr(b.IndustriesCode, 1, 6)
    and b.levelnum = '3'
    and a.cur_sign = '1'
    order by 1"
    CiticsIndClass <- execQuery(code)
    CiticsIndClass <- sqldf("select a.s_info_windcode, b.Code from CiticsIndClass a, IndClassMapping b
                            where a.industriesname = b.Name
                            order by a.s_info_windcode")
    fileConn<-file(sprintf("%s/Citics_Industry_II.csv", outpath))
    writeLines(c("METAGROUP NAME,Citics Sector II",
                 "METAGROUP DESC,Citics Classification II",
                 "NAME PREFIX,Citics.II.",
                 ",",
                 ","), fileConn)
    close(fileConn)
    
    fwrite(CiticsIndClass, file = sprintf("%s/Citics_Industry_II.csv", outpath), col.names = FALSE, row.names = FALSE, quote = FALSE, append = TRUE, sep = ",")
    cat("Citics Classification II updated!\n")
}

updateCiticsClassII()



# update SW Industry Classification
updateSWClass <- function(){
    IndClassMapping <- read.csv("./SW Industry Classification Mapping.csv", header = TRUE)
    
    ASHAREALL <- w.wset('sectorconstituent', paste0('date=', as.character(Sys.Date())), 'sectorid=a001010100000000')$Data
    ASHARESWIndustry <- w.wss(paste(ASHAREALL$wind_code, collapse = ","),'industry_sw, ipo_date','industryType=1')$Data
    ASHARESWIndustry <- ASHARESWIndustry[complete.cases(ASHARESWIndustry), ]
    ASHARESWIndustry$IPO_DATE <- as.Date(w.asDateTime(ASHARESWIndustry$IPO_DATE))
    ASHARESWIndustry$CODE <- paste0(substr(ASHARESWIndustry$CODE, 1, 6), '-CN')
    
    ASHARESWIndustry <- sqldf("select a.CODE tickerMap, b.Code from ASHARESWIndustry a, IndClassMapping b
                              where a.INDUSTRY_SW = b.Name
                              order by a.CODE")
    fileConn<-file(sprintf("%s/SW_Industry.csv", outpath))
    writeLines(c("METAGROUP NAME,SW Sector",
                 "METAGROUP DESC,SW Classification",
                 "NAME PREFIX,SW.",
                 ",",
                 ","), fileConn)
    close(fileConn)
    fwrite(ASHARESWIndustry, file = sprintf("%s/SW_Industry.csv", outpath), col.names = FALSE, row.names = FALSE, quote = FALSE, append = TRUE, sep = ",")
    cat("SW Classification updated!\n")
}

updateSWClass()


# convert CJD's blacklist to Axioma format
updateBlackList <- function(dt) {
    blacklist <- fread(sprintf("//nas/交易/投研生产数据共享/黑名单/blacklist_%s.csv", dt), header = TRUE, data.table = FALSE)
    code <- sprintf("select s_info_windcode from asharedescription where s_info_listdate is not null and s_info_delistdate is null")
    stk_list <- execQuery(code, db = 'wind')[, 1]
    blacklist <- filter(blacklist, ticker %in% stk_list) %>% 
        transmute(ticker = sprintf("%s-CN", substr(ticker, 1, 6)), 
                  event_type =1) %>% 
        arrange(ticker)
    fwrite(blacklist, file = sprintf("%s/jiejin.csv", outpath), col.names = FALSE, row.names = FALSE, quote = FALSE, sep = ",")
    cat("Black List updated!\n")
}

updateBlackList(dt = prc_dt)


# update the universe excluding 中国平安 601318.SH
updateMaster_excl_601318 <- function() {
    code <- "select concat(substr(s_info_windcode, 1, 6), '-CN') ticker, 1 flag
    from asharedescription
    where s_info_delistdate is null 
    and s_info_listdate is not null
    and s_info_windcode != '601318.SH'
    order by 1 "
    all_ashare_stkcd <- execQuery(code, db = 'wind')
    # all_ashare_stkcd$flag[all_ashare_stkcd$ticker == '601318-CN'] <- 0
    fwrite(all_ashare_stkcd, file = sprintf("%s/Master_excl_601318.csv", outpath), col.names = FALSE, row.names = FALSE, quote = FALSE, sep = ",")
    cat("Master_excl_601318 updated!\n")
}
updateMaster_excl_601318()

# createPreposition <- function(preposition_dt, trade_dt, stg, to_path) {
#     code <- sprintf("select p.product_name, t.secu_code ticker, t.share_qty position 
#                   from trade.ts_clear_bal_secu t, TS_ACC_PRODUCT p
#                   where t.stg_kind = '%s'
#                   and t.share_date = '%s'
#                   and t.fund_account = p.fund_account_spot", stg, as.Date(preposition_dt, "%Y%m%d"))
#     preposition <- execQuery(code, db = 'trade_guest')
#     
#     
#     preposition$ticker <- to_windcode(preposition$ticker)
#     preposition <- preposition[preposition$position!=0, ]
#     
#     products <- sort(unique(preposition$product_name))
#     
#     if(!dir.exists(sprintf("%s/%s/%s", to_path, trade_dt, stg))) {
#         dir.create(sprintf("%s/%s/%s", to_path, trade_dt, stg), recursive = TRUE)
#     }
#     
#     for (p in products){
#         temp <- preposition[preposition$product_name == p, c(2, 3)]
#         fwrite(temp, file = sprintf("%s/%s/%s/%s-preposition.csv", to_path, trade_dt, stg, p), col.names = TRUE, row.names = FALSE, quote = FALSE, sep = ",")
#     }
#     cat("Presposition updated!\n")
# }
# 
# createPreposition(preposition_dt = prc_dt, trade_dt = trade_dt, stg = strategy_code, to_path = preposition.path)


