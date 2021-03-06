select 
store_group     	,
division_nbr     	,
item_nbr     	,
model_nbr     	,
current_store_nbr     	,
ha_non_ha     	,
region     	,
district     	,
CONCAT('"',location_city_desc,'"')   as location_city_desc  	,
store_type     	,
sku_nbr     	,
if(max(sho_cost) is null,'-',max(sho_cost))	as sho_cost,
snc_uid_cd     	,
IMA_brand_name     	,
CONCAT('"',ecom_item_desc,'"')     	,
market_data_brand_name     	,
CONCAT('"',market_item_desc,'"')     	,
Current_Price_amt	,
regular_price_amt	,
min_market_price	,
avg_market_price	,
price_comp_amazon_or_other     	,
if(max(sho_cost) is null,'-',price_after_rules - max(sho_cost)) as price_diff_SHO_vs_market	,
percent_off_from_market_price	,
price_change_recommendation     	,
price_after_rules 	,
if(loss_value  is null,'-',loss_value) as loss_value	,
if(max(sho_cost) is null,'-',price_after_rules - max(sho_cost))	as adj_price_vs_cost,  
if(comments is null,'',comments)    as comments 	,
price_match_avail,
price_match_incart,
price_match_url,
price_match_competetor     	,
item_condition,
if(abt_electronics_us_price     is null,'-',abt_electronics_us_price)    as abt_electronics_us_price    	,
if(amazon_marketplace_us_price  is null,'-',amazon_marketplace_us_price) as amazon_marketplace_us_price ,
if(amazon_us_price              is null,'-',amazon_us_price)			 as amazon_us_price             ,
if(bestbuy_us_price             is null,'-',bestbuy_us_price)            as bestbuy_us_price            ,
if(costco_us_price             is null,'-',costco_us_price)            as costco_us_price            ,
if(home_depot_us_price          is null,'-',home_depot_us_price)         as home_depot_us_price         	,
if(jc_penney_us_price           is null,'-',jc_penney_us_price)          as jc_penney_us_price          	,
if(lowes_us_price               is null,'-',lowes_us_price)              as lowes_us_price              	,
if(menards_us_price               is null,'-',menards_us_price)              as menards_us_price              	,
if(sears_us_price               is null,'-',sears_us_price)              as sears_us_price              	,
if(cadence_level_flg            is null,'-',cadence_level_flg)			 as cadence_level_flg           	
from test.final_price_scrape
where price_change_recommendation	='Y' and store_group IN ('1','2','test') and loss_value >= 5 

GROUP BY
store_group     	,
division_nbr     	,
item_nbr     	,
model_nbr     	,
current_store_nbr     	,
ha_non_ha     	,
region     	,
district     	,
location_city_desc  	,
store_type     	,
sku_nbr     	,
snc_uid_cd     	,
IMA_brand_name     	,
CONCAT('"',ecom_item_desc,'"')     	,
market_data_brand_name     	,
CONCAT('"',market_item_desc,'"')     	,
Current_Price_amt	,
regular_price_amt	,
min_market_price	,
avg_market_price	,
price_comp_amazon_or_other     	,
percent_off_from_market_price	,
price_change_recommendation     	,
price_after_rules 	,
loss_value	,
comments 	,
price_match_incart,
price_match_avail,
price_match_url,
price_match_competetor     	,
item_condition,
abt_electronics_us_price    	,
amazon_marketplace_us_price ,
amazon_us_price             ,
bestbuy_us_price            ,
costco_us_price            ,
home_depot_us_price         	,
jc_penney_us_price          	,
lowes_us_price              	,
menards_us_price              	,
sears_us_price              	,
cadence_level_flg           

