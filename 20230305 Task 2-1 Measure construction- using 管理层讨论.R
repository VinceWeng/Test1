# loading libraries -----------------
gc()
library(readtext)
library(data.table)
library(jiebaR)
library(quanteda)
library(quanteda.textstats)
library(doParallel)
# 567
# 111213
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


# 2. 读取政府政策文件中的各关键词tf之差
# loading fyp tf data
fyp11_tf_dt <- fread("Data/Output/fyp11_diff_dt.csv", encoding="UTF-8")
fyp11_tf_dt <- fyp11_tf_dt[label == 1, .(feature, diff_tf)]  #　读取两个５年规划各关键词tf之差

fyp12_tf_dt <- fread("Data/Output/fyp12_diff_dt.csv", encoding="UTF-8")
fyp12_tf_dt <- fyp12_tf_dt[label == 1, .(feature, diff_tf)]

fyp13_tf_dt <- fread("Data/Output/fyp13_diff_dt.csv", encoding="UTF-8")
fyp13_tf_dt <- fyp13_tf_dt[label == 1, .(feature, diff_tf)]


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
get_measure_dt <- function(old_dt, old_fyp, new_dt, new_fyp, year_id) {
  
  # 4.1 求去年各公司所有关键词diff_tf*tf_idf之和
  merged_dt1 <- merge(old_fyp, old_dt[, .(stock_id, feature, tf_idf)], 
                      by=c("feature")) # 每个关键词在每家公司的tfidf和对应的政策报告的diff_tf
  
  merged_dt1[, prod:=diff_tf*tf_idf] # 增加1列二者的乘积
  
  merged_sum_dt1 <- merged_dt1[order(stock_id, -prod), .(measure1=sum(prod), # 以每家公司stock id排序，统计每家公司每个关键词diff_tf*tf_idf乘积之和
                                                         w1=fifelse(prod[1]>0, feature[1], "NA"), # 增加5列，统计每家公司对measure贡献前5的关键词
                                                         w2=fifelse(prod[2]>0, feature[2], "NA"),
                                                         w3=fifelse(prod[3]>0, feature[3], "NA"),
                                                         w4=fifelse(prod[4]>0, feature[4], "NA"),
                                                         w5=fifelse(prod[5]>0, feature[5], "NA")), 
                               by=stock_id]
  
  merged_sum_dt1[, terms_old:=paste(w1, w2, w3, w4, w5, sep=", ")] # 增加1列，囊括所有关键词
  merged_sum_dt1[, ":="(w1=NULL, # 去掉5列贡献前5的关键词
                        w2=NULL,
                        w3=NULL,
                        w4=NULL,
                        w5=NULL)]
  
  # 4.2 对本年各公司所有关键词diff_tf*tf_idf之和
  merged_dt2 <- merge(new_fyp, new_dt[, .(stock_id, feature, tf_idf)], 
                      by=c("feature"))
  
  merged_dt2[, prod:=diff_tf*tf_idf]
  
  merged_sum_dt2 <- merged_dt2[order(stock_id, -prod), .(measure2=sum(prod), 
                                                         w1=fifelse(prod[1]>0, feature[1], "NA"),
                                                         w2=fifelse(prod[2]>0, feature[2], "NA"),
                                                         w3=fifelse(prod[3]>0, feature[3], "NA"),
                                                         w4=fifelse(prod[4]>0, feature[4], "NA"),
                                                         w5=fifelse(prod[5]>0, feature[5], "NA")), 
                               by=stock_id]
  
  merged_sum_dt2[, terms_new:=paste(w1, w2, w3, w4, w5, sep=", ")]
  merged_sum_dt2[, ":="(w1=NULL,
                        w2=NULL,
                        w3=NULL,
                        w4=NULL,
                        w5=NULL)]
  
  # 4.3 计算两年各公司所有关键词diff_tf*tf_idf之和的差作为measure
  output_dt <- merge(merged_sum_dt1, merged_sum_dt2, by="stock_id")
  output_dt[, measure:=(measure2-measure1)*10^6] # 计算measure
  output_dt[, year:=year_id] # 统计年份
  
  # 4.4 返回结果
  return(output_dt[, .(stock_id, measure, year, terms_old, terms_new)]) #　输出
}


# 5. 对各年文件运行步骤4中的函数
# 5.1 将年报年份对应政府政策文件名称
corresp_dt <- data.table(a_year=7:19, # 将年份对应五年规划
                         fyp_year=c(11, 11, 11, 11, 12, 12, 12, 12, 12,
                                    13, 13, 13, 13))

output_ls <- list() # 设置空的list

# 5.2 对各年文件运行函数
for (x in 8:19) {
  
  # 5.2.1 确定各年的文件名称
  temp_num_char1 <- convert_to_two_digit_char(x-1) # 生成去年
  temp_num_char2 <- convert_to_two_digit_char(x) # 生成本年
  
  old_firm_dt_temp <- paste0("firm_tf_idf_", temp_num_char1, "_dt") # 定位old firm dt
  new_firm_dt_temp <- paste0("firm_tf_idf_", temp_num_char2, "_dt") #定位new firm dt
  
  # old calculation method
  #fyp_num_temp1 <- convert_to_two_digit_char(corresp_dt[a_year==x-1, fyp_year])
  # new method
  fyp_num_temp1 <- convert_to_two_digit_char(corresp_dt[a_year==x, fyp_year])
  fyp_num_temp2 <- convert_to_two_digit_char(corresp_dt[a_year==x, fyp_year])
  
  old_fyp_dt_temp <- paste0("fyp", fyp_num_temp1, "_tf_dt") # 定位old fyp dt
  new_fyp_dt_temp <- paste0("fyp", fyp_num_temp2, "_tf_dt") # 定位new fyp dt
  
  # 5.2.2 运行函数得到measure
  temp_dt <- get_measure_dt(get(old_firm_dt_temp), get(old_fyp_dt_temp),
                            get(new_firm_dt_temp), get(new_fyp_dt_temp),
                            ifelse(nchar(x)==1, paste0("200", x), paste0("20",x)))
  
  declare_dt_temp <- paste0("declare", temp_num_char2, "_dt")
  
  temp_dt <- merge(temp_dt, get(declare_dt_temp), by="stock_id") # 新增3列，为每家公司的行业及年报发布日期信息
  
  output_ls[[convert_to_two_digit_char(x)]] <- temp_dt # 写入list中
}


# 6. 输出结果到excel
# output the results
fwrite(output_ls[["08"]], "Data/Output/Test/Test 1 管理层讨论//MeasureV1/measure08.csv", bom=TRUE)
fwrite(output_ls[["09"]], "Data/Output/Test/Test 1 管理层讨论//MeasureV1/measure09.csv", bom=TRUE)
fwrite(output_ls[["10"]], "Data/Output/Test/Test 1 管理层讨论//MeasureV1/measure10.csv", bom=TRUE)
fwrite(output_ls[["11"]], "Data/Output/Test/Test 1 管理层讨论//MeasureV1/measure11.csv", bom=TRUE)
fwrite(output_ls[["12"]], "Data/Output/Test/Test 1 管理层讨论//MeasureV1/measure12.csv", bom=TRUE)
fwrite(output_ls[["13"]], "Data/Output/Test/Test 1 管理层讨论//MeasureV1/measure13.csv", bom=TRUE)
fwrite(output_ls[["14"]], "Data/Output/Test/Test 1 管理层讨论//MeasureV1/measure14.csv", bom=TRUE)
fwrite(output_ls[["15"]], "Data/Output/Test/Test 1 管理层讨论//MeasureV1/measure15.csv", bom=TRUE)
fwrite(output_ls[["16"]], "Data/Output/Test/Test 1 管理层讨论//MeasureV1/measure16.csv", bom=TRUE)
fwrite(output_ls[["17"]], "Data/Output/Test/Test 1 管理层讨论//MeasureV1/measure17.csv", bom=TRUE)
fwrite(output_ls[["18"]], "Data/Output/Test/Test 1 管理层讨论//MeasureV1/measure18.csv", bom=TRUE)
fwrite(output_ls[["19"]], "Data/Output/Test/Test 1 管理层讨论//MeasureV1/measure19.csv", bom=TRUE)

measure_merged <- rbindlist(output_ls)
# measure_merged[, measure:=measure*10^6]
fwrite(measure_merged, "Data/Output/Test/Test 1 管理层讨论//MeasureV1/measures_rbinded.csv", bom=TRUE)


# drafts below --------------------------

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

