DROP TABLE IF EXISTS  test.sho_wiser_data;

CREATE EXTERNAL TABLE IF NOT EXISTS test.sho_wiser_data
(
division_nbr	string,	
item_nbr	string,	
div_item	string,	
Manufacturer	string,	
Product_Name	string,	
sku	string,	
Price_Match_All_Items	string,	
price_match_url	string,	
price_match_avail string,
price_match_incart string,
min_max_diff_pct float,	
Amazon_Price_Match	float,	
Final_All	float,	
item_condition	string,	
min2_min1_delta_pct	float,	
Avg_Price	float,	
Final1	float,	
final_Price	float,	
Min1	float,	
Min2	float,	
Min2_Min1	float,	
blank2	string,	
Max_Price	float,	
count	float,	
avg_min_min1	float,	
avg_div_min1	float,	
Abt_Electronics_US_Price	float,	
Amazon_Marketplace_US_Price	float,	
Amazon_US_Price	float,	
Ashley_furniture	float,	
BestBuy_US_Price	float,	
Costco_US_Price	float,	
Home_Depot_US_Price	float,	
JC_Penney_US_Price	float,	
Lowes_US_Price	float,	
Menards_US_Price	float,	
Sears_US_Price	float
)
comment 'This table contains lates SHO wiser market data'
STORED AS TEXTFILE
LOCATION '/prod/test/sho/wiser_data'
TBLPROPERTIES ('serialization.null.format' = '');

