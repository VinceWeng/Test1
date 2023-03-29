# loading libraries -----------------
library(readtext)
library(data.table)
library(jiebaR)
library(quanteda)
library(quanteda.textstats)
library(readxl) # 读industry_data_2.xlsx
#123
#789
# install.packages("dplyr") # 删除包含半年报的信息
library(dplyr)

# combining the firm-level data and the text metadata -----------------

# a list that store the processed output
dt_ls <- list() # 生成新的存储信息表

# 1. 合并公司所属行业信息以及年报信息

for (year in 2007: 2019) {
  # garbage collection
  gc()

  # 1.1 导入各公司所属行业信息
  print(year)
  # read the firm-level industry data
  industry_dt <- read_xlsx("./Data/Input/Stock market firms/industry_data_2.xlsx") # 导入公司及其所属行业的信息
  # make each stock_id and time pair unique
  industry_dt <- unique(industry_dt, by=c("stock_id", "date")) # 去掉重复行   #############################
  # rename the columns
  # industry_dt <- industry_dt[, .(stock_id=Stkcd, date=Accper, industry_code=Indcd,
  #                                industry_name=Indnme)] # 修改列名
  # make the dates the correct format
  industry_dt <- data.table(industry_dt)
  industry_dt$date <- as.Date(industry_dt$date) # 将时间转换为时间格式
  
  # 1.2 处理每年的各公司年报信息
  # paths that store the text reprots
  file_path <- paste0("./Data/Input/Annual Reports Brief/txt/txt_", year, "/*") # 每年年报txt文档数据的路径
  # read the text reports from the path
  reports <- readtext(file_path, encoding = "utf-8") # 读每年的年报txt文档数据,輸出文檔名doc_id,以及文檔内容
  # data.tables are easier to work with
  reports <- data.table(reports) # 将数据转换为table格式
  # keep only cn characters
  reports[, text:=gsub("[^\u4e00-\u9fa5]", "", text)] # 仅保留中文
  # count the number of chinese characters
  reports[, num_char:=nchar(text)] # 计算每个公司年报的中文字数
  # no longer need the text column
  reports[, text:=NULL] # 删掉年报原文，仅保留中文字数
  
  # garbage collection
  gc()
  
  # 1.3 生成公司id、年报发布日期信息
  reports[, stock_id:= as.numeric(substr(doc_id, 1, 6))] # 公司id
  # select the substring after the second + 
  reports[, declare_date:= substr(doc_id, 8, 17)] # 年报发布日期
  reports <- filter(reports, !grepl('-06-30', declare_date)) # 刪掉半年报相关内容
  # reports$declare_date <- substr(reports$declare_date, 1, 10) # 年报发布日期
  reports$declare_date <- as.Date(reports$declare_date) # 转换为日期格式
  
  # 1.4 合并公司所属行业信息和公司年报信息
  setkey(industry_dt, stock_id, date) # 设置公司所属行业信息表中的每行信息的标识
  setkey(reports, stock_id, declare_date) # 设置公司年报信息表中的每行信息的标识
  # keep the earliest published report for each firm
  reports <- unique(reports, by="stock_id") # 去掉有重复id的行
 
  # roll join the industry data to the report data
  output_dt <- industry_dt[reports, roll=TRUE] # 合并两个信息表
  # output
  dt_ls[[as.character(year)]] <- output_dt # 将每年的信息存入list中
}

# 2. 分年导出数据至excel
fwrite(dt_ls[["2007"]], "./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2007link brief.csv", bom=TRUE)
fwrite(dt_ls[["2008"]], "./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2008link brief.csv", bom=TRUE)
fwrite(dt_ls[["2009"]], "./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2009link brief.csv", bom=TRUE)
fwrite(dt_ls[["2010"]], "./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2010link brief.csv", bom=TRUE)
fwrite(dt_ls[["2011"]], "./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2011link brief.csv", bom=TRUE)
fwrite(dt_ls[["2012"]], "./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2012link brief.csv", bom=TRUE)
fwrite(dt_ls[["2013"]], "./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2013link brief.csv", bom=TRUE)
fwrite(dt_ls[["2014"]], "./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2014link brief.csv", bom=TRUE)
fwrite(dt_ls[["2015"]], "./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2015link brief.csv", bom=TRUE)
fwrite(dt_ls[["2016"]], "./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2016link brief.csv", bom=TRUE)
fwrite(dt_ls[["2017"]], "./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2017link brief.csv", bom=TRUE)
fwrite(dt_ls[["2018"]], "./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2018link brief.csv", bom=TRUE)
fwrite(dt_ls[["2019"]], "./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2019link brief.csv", bom=TRUE)

