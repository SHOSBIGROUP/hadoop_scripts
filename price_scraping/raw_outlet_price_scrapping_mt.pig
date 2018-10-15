REGISTER /appl/common/local_lib/sho_udfs.jar;

-- LOAD ecom data, which is base for the price scrapping - This data is current active inventory snapshot for outelt website

ecom_data =  LOAD 'fact.ecommerce_outlet_inventory_uid_current' using org.apache.hcatalog.pig.HCatLoader();
filter_ecom_data = FILTER ecom_data by (active_flg=='1' and store_unit_type_cd=='4' and sku_nbr=='991' and (division_nbr=='022' OR division_nbr=='026' OR division_nbr=='046' OR division_nbr=='025') and cadence_level_flg!='0' and current_price_amt < regular_price_amt and NOT(LOWER(item_desc) matches '.*ped.*'));

--filter_ecom_data = LOAD '/prod/test/ecom_data' using PigStorage('') as (division_nbr:chararray,item_nbr:chararray,line_nbr:chararray,subline_nbr:chararray,store_nbr:chararray,sku_nbr:chararray,transaction_nbr:chararray,snc_uid_cd:chararray,item_desc:chararray,current_price_amt:double,regular_price_amt:double,cadence_level_flg:chararray);

gen_1 = FOREACH filter_ecom_data GENERATE
division_nbr,
item_nbr,
line_nbr,
subline_nbr,
store_nbr,
sku_nbr,
transaction_nbr,
snc_uid_cd,
item_desc,  
(current_price_amt/100) as Current_Price_amt,
(regular_price_amt/100) as regular_price_amt,
cadence_level_flg;

-- LOAD manager special data to remove those items from price scarapping

manager_data = LOAD 'raw.manager_special' using org.apache.hcatalog.pig.HCatLoader();
filt_manager = FILTER manager_data by (approval_flag == 'Y' and year_nbr == '2018');

gen_filt_manager = FOREACH filt_manager GENERATE
sho_udfs.LPAD( division_number, '0', 3 ) AS division_nbr,
sho_udfs.LPAD(item_number, '0', 5 ) AS item_nbr,
item_uid as uid_cd,
sho_udfs.LPAD(plus_4_number, '0', 4 ) as plus_4_nbr;

-- JOIN with eocm data to remove manager special items

join_manager_ecom = JOIN gen_1 by (division_nbr,item_nbr,transaction_nbr,snc_uid_cd) LEFT OUTER, gen_filt_manager by (division_nbr,item_nbr,plus_4_nbr,uid_cd);

filt_manager_ecom = FILTER join_manager_ecom by gen_filt_manager::uid_cd is null;

gen_1 = FOREACH  filt_manager_ecom GENERATE
gen_1::division_nbr	as	division_nbr	,
gen_1::item_nbr	as	item_nbr	,
gen_1::line_nbr	as	line_nbr	,
gen_1::subline_nbr	as	subline_nbr	,
gen_1::store_nbr	as	store_nbr	,
gen_1::sku_nbr	as	sku_nbr	,
gen_1::transaction_nbr	as	transaction_nbr	,
gen_1::snc_uid_cd	as	snc_uid_cd	,
gen_1::item_desc	as	item_desc	,
gen_1::Current_Price_amt	as	Current_Price_amt	,
gen_1::regular_price_amt	as	regular_price_amt	,
gen_1::cadence_level_flg	as	cadence_level_flg	;


-- LOAD item table to get proper model numbers for each item - this is a STATIC table
item_data = LOAD 'test.item_table_for_pricing' using org.apache.hcatalog.pig.HCatLoader();

-- JOIN ecom and item data to get model numbers 
join_ecom_item = JOIN gen_1 by (division_nbr,item_nbr),item_data by (division_nbr,item_nbr);

gen_2 = FOREACH join_ecom_item GENERATE
item_data::division_nbr	as 	division_nbr	,
gen_1::line_nbr	as 	line_nbr	,
gen_1::subline_nbr	as 	subline_nbr	,
item_data::item_nbr	as 	item_nbr	,
item_data::model_nbr	as 	model_nbr	,
gen_1::store_nbr	as 	store_nbr	,
gen_1::sku_nbr	as 	sku_nbr	,
gen_1::transaction_nbr	as 	transaction_nbr	,
gen_1::snc_uid_cd	as 	snc_uid_cd	,
gen_1::item_desc	as 	item_desc	,
gen_1::Current_Price_amt	as 	Current_Price_amt	,
gen_1::regular_price_amt	as 	regular_price_amt	,
gen_1::cadence_level_flg	as 	cadence_level_flg	;

-- LOAD IMA table to get the item-desc for each items
ima_data = LOAD 'fact.ima_item_hierarchy' using org.apache.hcatalog.pig.HCatLoader();

gen_item = FOREACH ima_data GENERATE
division_nbr,
item_nbr,
brand_nm;

-- JOIN gen_2 with ima to add item_desc

join_gen_2_ima = JOIN gen_2 by (division_nbr,item_nbr) left, gen_item by (division_nbr,item_nbr);

gen_3 = FOREACH join_gen_2_ima GENERATE
gen_2::division_nbr	as	division_nbr	,
gen_2::line_nbr	as	line_nbr	,
gen_2::subline_nbr	as	subline_nbr	,
gen_2::item_nbr	as	item_nbr	,
gen_2::model_nbr	as	model_nbr	,
gen_2::store_nbr	as	store_nbr	,
'HA' as ha_non_ha,
gen_2::sku_nbr	as	sku_nbr	,
gen_2::transaction_nbr	as	transaction_nbr	,
gen_2::snc_uid_cd	as	snc_uid_cd	,
gen_item::brand_nm	as	brand_nm	,
gen_2::item_desc	as	item_desc	,
gen_2::Current_Price_amt	as	Current_Price_amt	,
gen_2::regular_price_amt	as	regular_price_amt	,
gen_2::cadence_level_flg	as	cadence_level_flg	;


-- LOAD current SHO location list - This is a STATIC table
location_data = LOAD 'test.current_sho_location' using org.apache.hcatalog.pig.HCatLoader();

-- JOIN location with gen_3 to get region, district, location name etc

join_gen_3_location = JOIN gen_3 by store_nbr left, location_data by store_nbr;


gen_4 = FOREACH join_gen_3_location GENERATE
gen_3::division_nbr	as	division_nbr	,
gen_3::line_nbr	as	line_nbr	,
gen_3::subline_nbr	as	subline_nbr	,
gen_3::item_nbr	as	item_nbr	,
gen_3::model_nbr	as	model_nbr	,
gen_3::store_nbr	as	store_nbr	,
gen_3::ha_non_ha	as	ha_non_ha	,
gen_3::sku_nbr	as	sku_nbr	,
gen_3::transaction_nbr	as	transaction_nbr	,
gen_3::snc_uid_cd	as	snc_uid_cd	,
gen_3::brand_nm	as	brand_nm	,
gen_3::item_desc	as	item_desc	,
gen_3::Current_Price_amt	as	Current_Price_amt	,
gen_3::regular_price_amt	as	regular_price_amt	,
gen_3::cadence_level_flg	as	cadence_level_flg	,
location_data::region	as	region	,
location_data::district	as	district	,
location_data::location_city_desc	as	location_city_desc	,
location_data::store_format	as	store_format	,
location_data::store_group as store_group;



-- LOAD core cost for reference - This is a STATIC reference table
core_cost_data =  LOAD 'test.sho_core_cost'  using org.apache.hcatalog.pig.HCatLoader();

-- JOIN with gen_4 and core cost to get cost amount

join_gen_4_core_cost = JOIN gen_4 by (division_nbr,item_nbr) left, core_cost_data by (division_nbr,item_nbr);

gen_5 = FOREACH join_gen_4_core_cost GENERATE
gen_4::store_group as store_group,
gen_4::division_nbr	as	division_nbr	,
gen_4::line_nbr	as	line_nbr	,
gen_4::subline_nbr	as	subline_nbr	,
gen_4::item_nbr	as	item_nbr	,
gen_4::model_nbr	as	model_nbr	,
gen_4::store_nbr	as	store_nbr	,
gen_4::ha_non_ha	as	ha_non_ha	,
gen_4::sku_nbr	as	sku_nbr	,
gen_4::transaction_nbr	as	transaction_nbr	,
gen_4::snc_uid_cd	as	snc_uid_cd	,
gen_4::brand_nm	as	brand_nm	,
gen_4::item_desc	as	item_desc	,
gen_4::Current_Price_amt	as	Current_Price_amt	,
gen_4::regular_price_amt	as	regular_price_amt	,
gen_4::cadence_level_flg	as	cadence_level_flg	,
gen_4::region	as	region	,
gen_4::district	as	district	,
gen_4::location_city_desc	as	location_city_desc	,
gen_4::store_format	as	store_format	,
core_cost_data::core_cost	as	core_cost	;


-- LOAD Wiser Market data 

wiser_data = LOAD 'test.sho_wiser_data' using org.apache.hcatalog.pig.HCatLoader();

gen_wiser = FOREACH wiser_data GENERATE
division_nbr,
item_nbr,
div_item,
manufacturer,
product_name as market_item_desc,
price_match_all_items,
final_all,
amazon_price_match as amazon_or_other,
avg_price,
abt_electronics_us_price        ,
amazon_marketplace_us_price     ,
amazon_us_price ,
bestbuy_us_price        ,
costco_us_price	,
home_depot_us_price     ,
jc_penney_us_price      ,
lowes_us_price	,
menards_us_price	,
sears_us_price,
price_match_url,
price_match_avail,
item_condition,
price_match_incart;



-- JOIN gen_5 ( ecom cleaned data) with wiser for further analysis

join_gen_5_wiser = JOIN gen_5 by (division_nbr,item_nbr),gen_wiser by (division_nbr,item_nbr);



gen_6 = FOREACH join_gen_5_wiser GENERATE
gen_5::store_group as store_group,
gen_5::division_nbr	as	division_nbr	,
gen_5::line_nbr	as	line_nbr	,
gen_5::subline_nbr	as	subline_nbr	,
gen_5::item_nbr	as	item_nbr	,
gen_5::model_nbr	as	model_nbr	,
gen_5::store_nbr	as	store_nbr	,
gen_5::ha_non_ha	as	ha_non_ha	,
gen_5::sku_nbr	as	sku_nbr	,
gen_5::transaction_nbr	as	transaction_nbr	,
gen_5::snc_uid_cd	as	snc_uid_cd	,
gen_5::brand_nm	as	brand_nm	,
gen_5::item_desc	as	item_desc	,
gen_5::Current_Price_amt	as	Current_Price_amt	,
gen_5::regular_price_amt	as	regular_price_amt	,
gen_5::cadence_level_flg	as	cadence_level_flg	,
gen_5::region	as	region	,
gen_5::district	as	district	,
gen_5::location_city_desc	as	location_city_desc	,
gen_5::store_format	as	store_format	,
gen_5::core_cost	as	core_cost	,
gen_wiser::manufacturer	as	manufacturer	,
gen_wiser::market_item_desc	as	market_item_desc	,
gen_wiser::price_match_all_items	as	price_match_all_items	,
gen_wiser::final_all	as	min_market_price	,
gen_wiser::avg_price	as	avg_market_price	,
gen_wiser::amazon_or_other as price_comp_amazon_or_other,
gen_wiser::abt_electronics_us_price	as	abt_electronics_us_price	,
gen_wiser::amazon_marketplace_us_price	as	amazon_marketplace_us_price	,
gen_wiser::amazon_us_price	as	amazon_us_price	,
gen_wiser::bestbuy_us_price	as	bestbuy_us_price	,
gen_wiser::costco_us_price as costco_us_price ,
gen_wiser::home_depot_us_price	as	home_depot_us_price	,
gen_wiser::jc_penney_us_price	as	jc_penney_us_price	,
gen_wiser::lowes_us_price	as	lowes_us_price	,
gen_wiser::menards_us_price as menards_us_price ,
gen_wiser::sears_us_price	as	sears_us_price	,


(gen_wiser::final_all is not null ? (gen_5::Current_Price_amt - gen_wiser::final_all) : 0 ) as price_diff_SHO_vs_market,
(gen_wiser::final_all is not null ? ((gen_5::Current_Price_amt - gen_wiser::final_all )/gen_5::Current_Price_amt) : 0 ) as percent_off_from_market_price,
(((gen_wiser::final_all is null OR gen_wiser::final_all < 10) OR ( gen_5::Current_Price_amt > 100 and gen_wiser::final_all < 30 and gen_wiser::avg_price < 30 )) ? 1 : 0 )  as outlier1,
((gen_wiser::final_all < 100 and gen_wiser::avg_price < 100 and gen_5::Current_Price_amt > 1000) ? 1 : 0 ) as outlier2,
((gen_5::Current_Price_amt >(gen_wiser::final_all - (gen_wiser::final_all * 0.1))  ) ? 0 : 1 ) as outlier4,
(((gen_5::Current_Price_amt - gen_wiser::final_all) > 900 ) ? 1 : 0 ) as outlier5,
'' as comments,
gen_wiser::price_match_url as price_match_url,
gen_wiser::price_match_avail as price_match_avail,
gen_wiser::item_condition as item_condition,
gen_wiser::price_match_incart as price_match_incart;


gen_7 = FOREACH gen_6 generate
store_group,
division_nbr	,
line_nbr	,
subline_nbr	,
item_nbr	,
model_nbr	,
store_nbr as current_store_nbr	,
ha_non_ha	,
sku_nbr	,
snc_uid_cd	,
brand_nm as IMA_brand_name	,
item_desc as ecom_item_desc	,
Current_Price_amt	,
regular_price_amt	,
cadence_level_flg	,
region	,
district	,
location_city_desc	,
store_format as store_type	,
core_cost as sho_cost	,
manufacturer as market_data_brand_name	,
market_item_desc	,
price_match_all_items as price_match_competetor	,
min_market_price	,
avg_market_price	,
price_comp_amazon_or_other	,
abt_electronics_us_price	,
amazon_marketplace_us_price	,
amazon_us_price	,
bestbuy_us_price	,
costco_us_price,
home_depot_us_price	,
jc_penney_us_price	,
lowes_us_price	,
menards_us_price,
sears_us_price	,

price_diff_SHO_vs_market	,
percent_off_from_market_price	,
outlier1	,
outlier2	,
((min_market_price < 400 and percent_off_from_market_price > 0.50 ) ? 1 : 0 ) as outlier3,
outlier4	,
(((store_group=='1' OR store_group=='2') AND outlier4 == 0 and (division_nbr=='026' OR division_nbr=='022' OR division_nbr=='046')) ? (min_market_price * 0.9 )  : (store_group=='test' and min_market_price < 1000 ? min_market_price * 0.9 : ((store_group=='test' and (min_market_price >= 1000 and min_market_price <= 2000) ? min_market_price - 100 : min_market_price - 200) ))) as price_after_rules,
outlier5	,
comments,
price_match_url,
price_match_avail,
item_condition,
price_match_incart;

gen_8 = FOREACH  gen_7 GENERATE
store_group,
division_nbr	,
line_nbr	,
subline_nbr	,
item_nbr	,
model_nbr	,
current_store_nbr	,
ha_non_ha	,
sku_nbr	,
snc_uid_cd	,
IMA_brand_name	,
ecom_item_desc	,
Current_Price_amt	,
regular_price_amt	,
cadence_level_flg	,
region	,
district	,
location_city_desc	,
store_type	,
sho_cost	,
market_data_brand_name	,
market_item_desc	,
price_match_competetor	,
min_market_price	,
avg_market_price	,
price_comp_amazon_or_other	,
abt_electronics_us_price	,
amazon_marketplace_us_price	,
amazon_us_price	,
bestbuy_us_price	,
costco_us_price,
home_depot_us_price	,
jc_penney_us_price	,
lowes_us_price	,
menards_us_price,
sears_us_price	,
price_diff_SHO_vs_market	,
percent_off_from_market_price	,
price_after_rules,
(Current_Price_amt - price_after_rules) as loss_value,
(price_after_rules - sho_cost) as adjusted_price_vs_cost,
outlier1	,
outlier2	,
outlier3	,
outlier4	,
outlier5	,
(((outlier1 + outlier2 + outlier3 + outlier4 + outlier5)==0 and price_after_rules > 0 ) ? 'Y' : 'N' ) as price_change_recommendation,
comments,
price_match_url,
price_match_avail,
item_condition,
price_match_incart;

final_output = FOREACH gen_8 GENERATE
store_group,
division_nbr	,
item_nbr	,
model_nbr	,
current_store_nbr	,
ha_non_ha	,
region	,
district	,
location_city_desc	,
store_type	,
sku_nbr	,
sho_cost	,
snc_uid_cd	,
IMA_brand_name	,
ecom_item_desc	,
market_data_brand_name	,
market_item_desc	,
Current_Price_amt	,
regular_price_amt	,
min_market_price	,
avg_market_price	,
price_comp_amazon_or_other	,
price_diff_SHO_vs_market	,
percent_off_from_market_price	,
price_change_recommendation	,
price_after_rules	,
loss_value	,
adjusted_price_vs_cost	,
comments	,
price_match_incart,
price_match_avail,
price_match_url,
price_match_competetor	,
item_condition,
abt_electronics_us_price	,
amazon_marketplace_us_price	,
amazon_us_price	,
bestbuy_us_price	,
costco_us_price,
home_depot_us_price	,
jc_penney_us_price	,
lowes_us_price	,
menards_us_price,
sears_us_price	,
cadence_level_flg	,
outlier1	,
outlier2	,
outlier3	,
outlier4	,
outlier5	;

final_output = DISTINCT final_output;

rmf /prod/test/sho/final_price_scrape/;
STORE final_output INTO '/prod/test/sho/final_price_scrape' using PigStorage('');
