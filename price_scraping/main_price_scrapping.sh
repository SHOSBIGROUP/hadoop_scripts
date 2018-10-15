#!/bin/bash

#POG_PROPERTY_PATH=/appl/project/pog/conf
PRICING_PATH="/appl/dev/pojha/new_tables/price_scrapping"
#POG_PROPERTY_PATH=/appl/dev/mmisra/pog/conf
#. ${PRICING_PATH}/conf/pricing_properties
. /appl/common/conf/global_properties
export QUERY_PATH=$PRICING_PATH/query
export LOGS_PATH="/logs/test/price_scrapping_logs_$CURR_DT"
#export LOGS_PATH="/logs/test/pog_logs_$CURR_DT"

export SCRIPT_DATE=$(date -d "$1" '+%Y-%m-%d')
export CURR_DT=$SCRIPT_DATE
export YESTER_DT=$(date -d "${SCRIPT_DATE} 1 day ago" '+%Y-%m-%d')

export WISER_FILE=$PRICING_PATH/data/wiser_market_data.txt
export RECOMMENDED_FILE=$PRICING_PATH/data/price_recommendations.csv
export SCHEMA="store_group	division_nbr	item_nbr	model_nbr	current_store_nbr	ha_non_ha	region	district	location_city_desc	store_type	sku_nbr	sho_cost	snc_uid_cd	IMA_brand_name	ecom_item_desc	market_data_brand_name	market_item_desc	Current_Price_amt	regular_price_amt	min_market_price	avg_market_price	price_comp_amazon_or_other	price_diff_SHO_vs_market	percent_off_from_market_price	price_change_recommendation	price_after_rules	loss_value	adj_price_vs_cost	comments	price_match_avail	price_match_incart	price_match_url	price_match_competetor	item_condition	abt_electronics_us_price	amazon_marketplace_us_price	amazon_us_price	bestbuy_us_price	costco_us_price	home_depot_us_price	jc_penney_us_price	lowes_us_price	menards_us_price	sears_us_price	cadence_level_flg"

while [[ "${PROCESSED}" -eq 0 ]];
do
	hadoop fs -rm /prod/test/sho/wiser_data/*
	hadoop fs -put $WISER_FILE /prod/test/sho/wiser_data/	
    if ( check_hdfs_file_exists.sh "/fact/ecommerce/outlet/inventory/uid_current" );
	then


		pig -useHCatalog $PRICING_PATH/pig/raw_outlet_price_scrapping_mt.pig

				echo $SCHEMA > ${RECOMMENDED_FILE}
        hive -f "$QUERY_PATH/price_recommendation.sql" | sed "s/|/	/g"  >> $RECOMMENDED_FILE
	
        PROCESSED=1

    else

        log_entry.sh "Fact ecomm table is not upadted yet, going to sleep !"

    fi



  COUNT=$(( $COUNT + 1 ))

  

done

