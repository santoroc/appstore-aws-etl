CREATE TABLE app_store.sales
(
	"provider" VARCHAR(50)
	,provider_country VARCHAR(10)
	,sku VARCHAR(255)
	,developer VARCHAR(255)
	,title VARCHAR(255)
	,version VARCHAR(10)
	,product_type_identifier VARCHAR(50)
	,units INTEGER
	,developer_proceeds NUMERIC(18,2)
	,begin_date DATE sortkey
	,end_date DATE
	,customer_currency VARCHAR(10)
	,country_code VARCHAR(10)
	,currency_of_proceeds VARCHAR(10)
	,apple_identifier VARCHAR(50)
	,customer_price NUMERIC(18,2)
	,promo_code VARCHAR(100)
	,parent_identifier VARCHAR(50)
	,subscription VARCHAR(50)
	,period VARCHAR(50)
	,category VARCHAR(50)
	,cmb VARCHAR(50)
	,device VARCHAR(50)
	,supported_platforms VARCHAR(50)
	,proceeds_reason VARCHAR(50)
	,preserved_pricing VARCHAR(50)
	,client VARCHAR(50)
	,order_type VARCHAR(50)
	,contingency_app_name VARCHAR(50)
	,api_date date
	,etl_timestamp timestamp
)
;