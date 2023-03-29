# loading libraries -----------------
gc()
library(readtext)
library(data.table)
library(jiebaR)
library(quanteda)
library(quanteda.textstats)
library(doParallel)
source("./Auxiliary functions - adjusted.R")

# -------------
# 1. 读取每年各公司年报中各关键词的tfidf信息
# loading firm tf idf data
firm_tf_idf_07_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Firm tf-idf/2007tfidf_firm.csv", encoding="UTF-8")
firm_tf_idf_08_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Firm tf-idf/2008tfidf_firm.csv", encoding="UTF-8")
firm_tf_idf_09_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Firm tf-idf/2009tfidf_firm.csv", encoding="UTF-8")
firm_tf_idf_10_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Firm tf-idf/2010tfidf_firm.csv", encoding="UTF-8")
firm_tf_idf_11_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Firm tf-idf/2011tfidf_firm.csv", encoding="UTF-8")
firm_tf_idf_12_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Firm tf-idf/2012tfidf_firm.csv", encoding="UTF-8")
firm_tf_idf_13_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Firm tf-idf/2013tfidf_firm.csv", encoding="UTF-8")
firm_tf_idf_14_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Firm tf-idf/2014tfidf_firm.csv", encoding="UTF-8")
firm_tf_idf_15_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Firm tf-idf/2015tfidf_firm.csv", encoding="UTF-8")
firm_tf_idf_16_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Firm tf-idf/2016tfidf_firm.csv", encoding="UTF-8")
firm_tf_idf_17_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Firm tf-idf/2017tfidf_firm.csv", encoding="UTF-8")
firm_tf_idf_18_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Firm tf-idf/2018tfidf_firm.csv", encoding="UTF-8")
firm_tf_idf_19_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Firm tf-idf/2019tfidf_firm.csv", encoding="UTF-8")


# 2. 读取政府政策文件中的各关键词tf
# loading fyp tf data
fyp11_tf_dt <- fread("./Data/Output/Five-year plans term frequency/fyp11_dt.csv", encoding="UTF-8")
fyp11_tf_dt <- fyp11_tf_dt[label == 1, .(feature, tf_fyp=tf)] #读取５年规划各关键词tf

fyp12_tf_dt <- fread("./Data/Output/Five-year plans term frequency/fyp12_dt.csv", encoding="UTF-8")
fyp12_tf_dt <- fyp12_tf_dt[label == 1, .(feature, tf_fyp=tf)]

fyp13_tf_dt <- fread("./Data/Output/Five-year plans term frequency/fyp13_dt.csv", encoding="UTF-8")
fyp13_tf_dt <- fyp13_tf_dt[label == 1, .(feature, tf_fyp=tf)]


# 3. 读取每年每家公司的所属行业、年报日期等基本信息
# loading annual report declare date
declare07_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2007link brief.csv", encoding="UTF-8")
declare07_dt <- declare07_dt[, .(stock_id, declare_date=date, industry_name, industry_code)]
declare08_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2008link brief.csv", encoding="UTF-8")
declare08_dt <- declare08_dt[, .(stock_id, declare_date=date, industry_name, industry_code)]
declare09_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2009link brief.csv", encoding="UTF-8")
declare09_dt <- declare09_dt[, .(stock_id, declare_date=date, industry_name, industry_code)]
declare10_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2010link brief.csv", encoding="UTF-8")
declare10_dt <- declare10_dt[, .(stock_id, declare_date=date, industry_name, industry_code)]
declare11_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2011link brief.csv", encoding="UTF-8")
declare11_dt <- declare11_dt[, .(stock_id, declare_date=date, industry_name, industry_code)]
declare12_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2012link brief.csv", encoding="UTF-8")
declare12_dt <- declare12_dt[, .(stock_id, declare_date=date, industry_name, industry_code)]
declare13_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2013link brief.csv", encoding="UTF-8")
declare13_dt <- declare13_dt[, .(stock_id, declare_date=date, industry_name, industry_code)]
declare14_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2014link brief.csv", encoding="UTF-8")
declare14_dt <- declare14_dt[, .(stock_id, declare_date=date, industry_name, industry_code)]
declare15_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2015link brief.csv", encoding="UTF-8")
declare15_dt <- declare15_dt[, .(stock_id, declare_date=date, industry_name, industry_code)]
declare16_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2016link brief.csv", encoding="UTF-8")
declare16_dt <- declare16_dt[, .(stock_id, declare_date=date, industry_name, industry_code)]
declare17_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2017link brief.csv", encoding="UTF-8")
declare17_dt <- declare17_dt[, .(stock_id, declare_date=date, industry_name, industry_code)]
declare18_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2018link brief.csv", encoding="UTF-8")
declare18_dt <- declare18_dt[, .(stock_id, declare_date=date, industry_name, industry_code)]
declare19_dt <- fread("./Data/Output/Test/Test 1 管理层讨论/Linking firms with industry brief/2019link brief.csv", encoding="UTF-8")
declare19_dt <- declare19_dt[, .(stock_id, declare_date=date, industry_name, industry_code)]


# 4. 设置生成measure的函数
# function to facilitate the merging
get_measure_dt <- function(firm_dt, fyp_dt, year_id) {
  
  # 4.1 求去年各公司所有关键词tf*tf_idf之和
  merged_dt <- merge(firm_dt, fyp_dt, by="feature") # 每个关键词在每家公司的tfidf和对应的政策报告的tf
  
  merged_dt[, prod:=tf_idf*tf_fyp] # 增加1列二者的乘积
  
  merged_sum_dt <- merged_dt[order(stock_id, -prod), .(measure=sum(prod),  # 以每家公司stock id排序，统计每家公司每个关键词tf*tf_idf乘积之和
                                                        w1=fifelse(prod[1]>0, feature[1], "NA"), # 增加5列，统计每家公司对measure贡献前5的关键词
                                                        w2=fifelse(prod[2]>0, feature[2], "NA"),
                                                        w3=fifelse(prod[3]>0, feature[3], "NA"),
                                                        w4=fifelse(prod[4]>0, feature[4], "NA"),
                                                        w5=fifelse(prod[5]>0, feature[5], "NA")), 
                              by=stock_id]
  
  merged_sum_dt[, terms:=paste(w1, w2, w3, w4, w5, sep=", ")] # 增加1列，囊括所有关键词
  merged_sum_dt[, ":="(w1=NULL, # 去掉5列贡献前5的关键词
                        w2=NULL,
                        w3=NULL,
                        w4=NULL,
                        w5=NULL)]
  
  # 4.2 返回结果
  merged_sum_dt$year = year_id
  return(merged_sum_dt)
}


# 5. 对各年文件运行步骤4中的函数
# 5.1 将年报年份对应政府政策文件名称
corresp_dt <- data.table(a_year=7:19,  # 将年份对应五年规划
                         fyp_year=c(11, 11, 11, 11, 12, 12, 12, 12, 12,
                                    13, 13, 13, 13))

output_ls <- list() # 设置空的list

# 5.2 对各年文件运行函数
for (x in 7:19) {
  
  # 5.2.1 确定各年的文件名称
  temp_num_char <- convert_to_two_digit_char(x)  # 生成本年

  firm_dt_temp <- paste0("firm_tf_idf_", temp_num_char, "_dt") # 定位firm dt

  fyp_num_temp <- convert_to_two_digit_char(corresp_dt[a_year==x, fyp_year])

  fyp_dt_temp <- paste0("fyp", fyp_num_temp, "_tf_dt") # 定位fyp dt

  # 5.2.2 运行函数得到measure
  temp_dt <- get_measure_dt(get(firm_dt_temp), get(fyp_dt_temp),
                            ifelse(nchar(x)==1, paste0("200", x), paste0("20",x)))
  
  declare_dt_temp <- paste0("declare", temp_num_char, "_dt")
  
  temp_dt <- merge(temp_dt, get(declare_dt_temp), by="stock_id") # 新增3列，为每家公司的行业及年报发布日期信
  
  print(paste(firm_dt_temp, fyp_dt_temp, ifelse(nchar(x)==1, paste0("200", x), paste0("20",x)), declare_dt_temp), sep=", ")
  
  output_ls[[convert_to_two_digit_char(x)]] <- temp_dt # 写入list中
}


# 6. 输出结果到excel
# output the results
fwrite(output_ls[["07"]], "./Data/Output/Test/Test 1 管理层讨论/MeasureV2/measure07 brief.csv", bom=TRUE)
fwrite(output_ls[["08"]], "./Data/Output/Test/Test 1 管理层讨论/MeasureV2/measure08 brief.csv", bom=TRUE)
fwrite(output_ls[["09"]], "./Data/Output/Test/Test 1 管理层讨论/MeasureV2/measure09 brief.csv", bom=TRUE)
fwrite(output_ls[["10"]], "./Data/Output/Test/Test 1 管理层讨论/MeasureV2/measure10 brief.csv", bom=TRUE)
fwrite(output_ls[["11"]], "./Data/Output/Test/Test 1 管理层讨论/MeasureV2/measure11 brief.csv", bom=TRUE)
fwrite(output_ls[["12"]], "./Data/Output/Test/Test 1 管理层讨论/MeasureV2/measure12 brief.csv", bom=TRUE)
fwrite(output_ls[["13"]], "./Data/Output/Test/Test 1 管理层讨论/MeasureV2/measure13 brief.csv", bom=TRUE)
fwrite(output_ls[["14"]], "./Data/Output/Test/Test 1 管理层讨论/MeasureV2/measure14 brief.csv", bom=TRUE)
fwrite(output_ls[["15"]], "./Data/Output/Test/Test 1 管理层讨论/MeasureV2/measure15 brief.csv", bom=TRUE)
fwrite(output_ls[["16"]], "./Data/Output/Test/Test 1 管理层讨论/MeasureV2/measure16 brief.csv", bom=TRUE)
fwrite(output_ls[["17"]], "./Data/Output/Test/Test 1 管理层讨论/MeasureV2/measure17 brief.csv", bom=TRUE)
fwrite(output_ls[["18"]], "./Data/Output/Test/Test 1 管理层讨论/MeasureV2/measure18 brief.csv", bom=TRUE)
fwrite(output_ls[["19"]], "./Data/Output/Test/Test 1 管理层讨论/MeasureV2/measure19 brief.csv", bom=TRUE)

measure_merged <- rbindlist(output_ls)
measure_merged[, measure:=measure*10^6]
fwrite(measure_merged, "./Data/Output/Test/Test 1 管理层讨论/MeasureV2/measures_rbinded brief .csv", bom=TRUE)

# drafts below, ignore --------------------------

measure08_dt <- get_measure_dt(firm_tf_idf_07_dt, fyp11_tf_dt,
                               firm_tf_idf_08_dt, fyp11_tf_dt,
                               2008)
measure08_dt <- merge(measure08_dt, declare08_dt, by="stock_id")

measure09_dt <- get_measure_dt(firm_tf_idf_08_dt, fyp11_tf_dt,
                               firm_tf_idf_09_dt, fyp11_tf_dt,
                               2009)
measure09_dt <- merge(measure09_dt, declare09_dt, by="stock_id")

measure10_dt <- get_measure_dt(firm_tf_idf_07_dt, fyp11_tf_dt,
                               firm_tf_idf_08_dt, fyp11_tf_dt,
                               2008)
measure10_dt <- merge(measure10_dt, declare10_dt, by="stock_id")

measure11_dt <- get_measure_dt(firm_tf_idf_07_dt, fyp11_tf_dt,
                               firm_tf_idf_08_dt, fyp11_tf_dt,
                               2008)
measure11_dt <- merge(measure11_dt, declare11_dt, by="stock_id")

measure12_dt <- get_measure_dt(firm_tf_idf_07_dt, fyp11_tf_dt,
                               firm_tf_idf_08_dt, fyp11_tf_dt,
                               2008)
measure12_dt <- merge(measure12_dt, declare12_dt, by="stock_id")

measure13_dt <- get_measure_dt(firm_tf_idf_07_dt, fyp11_tf_dt,
                               firm_tf_idf_08_dt, fyp11_tf_dt,
                               2008)
measure13_dt <- merge(measure13_dt, declare13_dt, by="stock_id")

measure14_dt <- get_measure_dt(firm_tf_idf_07_dt, fyp11_tf_dt,
                               firm_tf_idf_08_dt, fyp11_tf_dt,
                               2008)
measure14_dt <- merge(measure14_dt, declare14_dt, by="stock_id")

measure15_dt <- get_measure_dt(firm_tf_idf_07_dt, fyp11_tf_dt,
                               firm_tf_idf_08_dt, fyp11_tf_dt,
                               2008)
measure15_dt <- merge(measure15_dt, declare15_dt, by="stock_id")

measure16_dt <- get_measure_dt(firm_tf_idf_07_dt, fyp11_tf_dt,
                               firm_tf_idf_08_dt, fyp11_tf_dt,
                               2008)
measure16_dt <- merge(measure16_dt, declare16_dt, by="stock_id")

measure17_dt <- get_measure_dt(firm_tf_idf_07_dt, fyp11_tf_dt,
                               firm_tf_idf_08_dt, fyp11_tf_dt,
                               2008)
measure17_dt <- merge(measure17_dt, declare17_dt, by="stock_id")

measure18_dt <- get_measure_dt(firm_tf_idf_07_dt, fyp11_tf_dt,
                               firm_tf_idf_08_dt, fyp11_tf_dt,
                               2008)
measure18_dt <- merge(measure18_dt, declare18_dt, by="stock_id")

measure19_dt <- get_measure_dt(firm_tf_idf_18_dt, fyp13_tf_dt,
                               firm_tf_idf_19_dt, fyp13_tf_dt,
                               2019)
measure19_dt <- merge(measure19_dt, declare19_dt, by="stock_id")

