
-- Segement Data Schema (production)
-- Created by : Jerry Cheng
-- Last Updated Date : 04/07/2017


DROP TABLE IF EXISTS "added_address";
CREATE TABLE "added_address" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"customer_id" int8 NULL,
	"event" varchar(512) NULL,
	"event_text" varchar(512) NULL,
	"original_timestamp" timestamp NULL,
	"sent_at" timestamp NULL,
	"store_id" int8 NULL,
	"timestamp" timestamp NULL,
	"user_id" varchar(512) NULL,
	"__segment_object_id" varchar(512) NULL,
	"segment_object_id" varchar(512) NULL,
	"session_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "added_payment_method";
CREATE TABLE "added_payment_method" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"customer_id" int8 NULL,
	"event" varchar(512) NULL,
	"event_text" varchar(512) NULL,
	"original_timestamp" timestamp NULL,
	"sent_at" timestamp NULL,
	"store_id" int8 NULL,
	"timestamp" timestamp NULL,
	"user_id" varchar(512) NULL,
	"__segment_object_id" varchar(512) NULL,
	"segment_object_id" varchar(512) NULL,
	"session_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "added_product";
CREATE TABLE "added_product" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"anonymous_id" varchar(512) NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"event" varchar(512) NULL,
	"event_text" varchar(512) NULL,
	"_id" int8 NULL,
	"name" varchar(512) NULL,
	"original_timestamp" timestamp NULL,
	"price" float8 NULL,
	"quantity" int8 NULL,
	"sent_at" timestamp NULL,
	"sku" varchar(512),
	"store_id" int8 NULL,
	"timestamp" timestamp NULL,
	"type" varchar(512) NULL,
	"user_id" varchar(512) NULL,
	"__segment_object_id" varchar(512) NULL,
	"segment_object_id" varchar(512) NULL,
	"session_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "aliases";
CREATE TABLE "aliases" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"original_timestamp" timestamp NULL,
	"previous_id" varchar(512) NULL,
	"sent_at" timestamp NULL,
	"timestamp" timestamp NULL,
	"user_id" varchar(512) NULL
);


DROP TABLE IF EXISTS "cancel_migrated_order";
CREATE TABLE "cancel_migrated_order" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"context_library_name" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "cancel_reship";
CREATE TABLE "cancel_reship" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"agent_id" int8 NULL,
	"cancel_reship_notes" varchar(512) NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"customer_id" int8 NULL,
	"event" varchar(512) NULL,
	"event_text" varchar(512) NULL,
	"order_id" int8 NULL,
	"order_status_id" int8 NULL,
	"original_timestamp" timestamp NULL,
	"reship_refund_amount" int8 NULL,
	"sent_at" timestamp NULL,
	"timestamp" timestamp NULL,
	"user_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "changed_shipping_address";
CREATE TABLE "changed_shipping_address" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"event_text" varchar(512) NULL,
	"user_id" varchar(512) NULL,
	"override_default_action_id" int8 NULL,
	"order_status_id" int8 NULL,
	"original_address_id" int8 NULL,
	"agent_id" int8 NULL,
	"change_notes" varchar(512) NULL,
	"context_library_name" varchar(512) NULL,
	"event" varchar(512) NULL,
	"order_id" int8 NULL,
	"original_timestamp" timestamp NULL,
	"sent_at" timestamp NULL,
	"timestamp" timestamp NULL,
	"customer_id" int8 NULL,
	"tax_difference" int8 NULL,
	"ignore_tax_charges" bool NULL,
	"new_address_id" int8 NULL,
	"context_library_version" varchar(512) NULL,
	"order_total" float8 NULL,
	"segment_object_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "completed_order";
CREATE TABLE "completed_order" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"customer_id" int8 NULL,
	"discount" int8 NULL,
	"event" varchar(512) NULL,
	"event_text" varchar(512) NULL,
	"ip_address" varchar(512) NULL,
	"order_id" int8 NULL,
	"original_timestamp" timestamp NULL,
	"passed_compliance" bool NULL,
	"products" varchar(5120) NULL,
	"sent_at" timestamp NULL,
	"shipping" float8 NULL,
	"store_id" int8 NULL,
	"subtotal" float8 NULL,
	"tax" float8 NULL,
	"timestamp" timestamp NULL,
	"total" float8 NULL,
	"user_id" varchar(512) NULL,
	"vouchers" varchar(512) NULL,
	"campaign_id" int8 NULL,
	"partner_id" int8 NULL,
	"promotion_id" int8 NULL,
	"sub_id" varchar(512) NULL,
	"__segment_object_id" varchar(512) NULL,
	"segment_object_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "failed_payment_attempt";
CREATE TABLE "failed_payment_attempt" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"event" varchar(512) NULL,
	"user_id" varchar(512) NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"event_text" varchar(512) NULL,
	"original_timestamp" timestamp NULL,
	"timestamp" timestamp NULL,
	"amount" float8 NULL,
	"customer_id" int8 NULL,
	"sent_at" timestamp NULL,
	"store_id" int8 NULL,
	"response_message" varchar(512) NULL,
	"__segment_object_id" varchar(512) NULL,
	"segment_object_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "identifies";
CREATE TABLE "identifies" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"anonymous_id" varchar(512) NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"customer_id" int8 NULL,
	"email" varchar(512) NULL,
	"first_name" varchar(512) NULL,
	"last_name" varchar(512) NULL,
	"original_timestamp" timestamp NULL,
	"sent_at" timestamp NULL,
	"store_id" int8 NULL,
	"timestamp" timestamp NULL,
	"uid" varchar(512) NULL,
	"user_id" varchar(512) NULL,
	"context_campaign_medium" varchar(512) NULL,
	"context_campaign_name" varchar(512) NULL,
	"context_campaign_source" varchar(2048) NULL,
	"context_ip" varchar(512) NULL,
	"context_page_path" varchar(1024) NULL,
	"context_page_referrer" varchar(2048) NULL,
	"context_page_search" varchar(512) NULL,
	"context_page_title" varchar(512) NULL,
	"context_page_url" varchar(2048) NULL,
	"context_user_agent" varchar(512) NULL,
	"_uuid" varchar(512) NULL,
	"context_campaign_content" varchar(512) NULL,
	"context_campaign_souce" varchar(512) NULL,
	"__segment_object_id" varchar(512) NULL,
	"segment_object_id" varchar(512) NULL,
	"src_file_name" varchar(2048) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "issued_refund";
CREATE TABLE "issued_refund" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"agent_id" int8 NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"customer_id" int8 NULL,
	"event" varchar(512) NULL,
	"event_text" varchar(512) NULL,
	"is_full_refund" bool NULL,
	"is_recallable" bool NULL,
	"order_id" int8 NULL,
	"order_status_id" int8 NULL,
	"order_total" float8 NULL,
	"original_timestamp" timestamp NULL,
	"penalty_fees" int8 NULL,
	"refund_amount_items" float8 NULL,
	"refund_amount_shipping" float8 NULL,
	"refund_amount_tax" float8 NULL,
	"refund_amount_total" float8 NULL,
	"refund_reason_id" int8 NULL,
	"sent_at" timestamp NULL,
	"timestamp" timestamp NULL,
	"user_id" varchar(512) NULL,
	"refund_notes" varchar(512) NULL,
	"segment_object_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "logged_in";
CREATE TABLE "logged_in" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"customer_id" int8 NULL,
	"event" varchar(512) NULL,
	"event_text" varchar(512) NULL,
	"original_timestamp" timestamp NULL,
	"sent_at" timestamp NULL,
	"store_id" int8 NULL,
	"timestamp" timestamp NULL,
	"user_id" varchar(512) NULL,
	"__segment_object_id" varchar(512) NULL,
	"segment_object_id" varchar(512) NULL,
	"session_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "logged_out";
CREATE TABLE "logged_out" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"customer_id" int8 NULL,
	"event" varchar(512) NULL,
	"event_text" varchar(512) NULL,
	"original_timestamp" timestamp NULL,
	"sent_at" timestamp NULL,
	"timestamp" timestamp NULL,
	"user_id" varchar(512) NULL,
	"__segment_object_id" varchar(512) NULL,
	"segment_object_id" varchar(512) NULL,
	"session_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "pages";
CREATE TABLE "pages" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"anonymous_id" varchar(512) NULL,
	"context_ip" varchar(512) NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"context_page_path" varchar(2048) NULL,
	"context_page_referrer" varchar(2048) NULL,
	"context_page_search" varchar(512) NULL,
	"context_page_title" varchar(512) NULL,
	"context_page_url" varchar(2048) NULL,
	"context_user_agent" varchar(512) NULL,
	"original_timestamp" timestamp NULL,
	"path" varchar(2048) NULL,
	"referrer" varchar(2048) NULL,
	"search" varchar(512) NULL,
	"sent_at" timestamp NULL,
	"timestamp" timestamp NULL,
	"title" varchar(512) NULL,
	"url" varchar(2048) NULL,
	"user_id" varchar(512) NULL,
	"context_campaign_name" varchar(512) NULL,
	"context_campaign_source" varchar(512) NULL,
	"context_campaign_medium" varchar(512) NULL,
	"context_campaign_term" varchar(512) NULL,
	"context_campaign_campyahooaign" varchar(512) NULL,
	"context_campaign_medq_ium" varchar(512) NULL,
	"context_campaign_soure" varchar(512) NULL,
	"context_campaign_campaigevite" varchar(512) NULL,
	"context_campaign_content" varchar(512) NULL,
	"context_campaign_campaign" varchar(512) NULL,
	"context_campaign_sourse" varchar(512) NULL,
	"context_campaign_sorce" varchar(512) NULL,
	"context_campaign_souce" varchar(512) NULL,
	"context_campaign_low" varchar(512) NULL,
	"__segment_object_id" varchar(512) NULL,
	"segment_object_id" varchar(512) NULL,
	"context_campaign_ca_mpaign" varchar(512) NULL,
	"src_file_name" varchar(2048) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "registered";
CREATE TABLE "registered" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"customer_id" int8 NULL,
	"dob" timestamp NULL,
	"event" varchar(512) NULL,
	"event_text" varchar(512) NULL,
	"original_timestamp" timestamp NULL,
	"sent_at" timestamp NULL,
	"store_id" int8 NULL,
	"timestamp" timestamp NULL,
	"user_id" varchar(512) NULL,
	"__segment_object_id" varchar(512) NULL,
	"segment_object_id" varchar(512) NULL,
	"session_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "removed_product";
CREATE TABLE "removed_product" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"anonymous_id" varchar(512) NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"event" varchar(512) NULL,
	"event_text" varchar(512) NULL,
	"_id" int8 NULL,
	"name" varchar(512) NULL,
	"original_timestamp" timestamp NULL,
	"sent_at" timestamp NULL,
	"sku" varchar(512) NULL,
	"store_id" int8 NULL,
	"timestamp" timestamp NULL,
	"type" varchar(512) NULL,
	"user_id" varchar(512) NULL,
	"__segment_object_id" varchar(512) NULL,
	"segment_object_id" varchar(512) NULL,
	"session_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "replaced_items";
CREATE TABLE "replaced_items" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"agent_id" int8 NULL,
	"customer_id" int8 NULL,
	"event" varchar(512) NULL,
	"items_replaced" varchar(512) NULL,
	"order_id" int8 NULL,
	"original_timestamp" timestamp NULL,
	"override_default_action_id" int8 NULL,
	"replacement_notes" varchar(512) NULL,
	"sent_at" timestamp NULL,
	"timestamp" timestamp NULL,
	"user_id" varchar(512) NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"event_text" varchar(512) NULL,
	"order_status_id" int8 NULL,
	"order_total" float8 NULL,
	"segment_object_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "rerouted_order";
CREATE TABLE "rerouted_order" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"agent_id" int8 NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"customer_id" int8 NULL,
	"event" varchar(512) NULL,
	"event_text" varchar(512) NULL,
	"order_id" int8 NULL,
	"order_status_id" int8 NULL,
	"original_timestamp" timestamp NULL,
	"reroute_charge" float8 NULL,
	"reroute_charge_payment_id" int8 NULL,
	"reroute_notes" varchar(512) NULL,
	"reroute_shipping_address_id" int8 NULL,
	"sent_at" timestamp NULL,
	"timestamp" timestamp NULL,
	"user_id" varchar(512) NULL,
	"compliance_action_orderride_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "reshipped_order";
CREATE TABLE "reshipped_order" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"sent_at" timestamp NULL,
	"event_text" varchar(512) NULL,
	"shipping_address_id" int8 NULL,
	"reship_charge_payment_method_id" int8 NULL,
	"agent_id" int8 NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"customer_id" int8 NULL,
	"event" varchar(512) NULL,
	"items_reshipped" varchar(1024) NULL,
	"order_id" int8 NULL,
	"order_status_id" int8 NULL,
	"original_order_id" int8 NULL,
	"original_timestamp" timestamp NULL,
	"reship_reason_id" int8 NULL,
	"timestamp" timestamp NULL,
	"user_id" varchar(512) NULL,
	"reship_charge" varchar(512) NULL,
	"reship_notes" varchar(512) NULL,
	"segment_object_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "selected_offer";
CREATE TABLE "selected_offer" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"anonymous_id" varchar(512) NULL,
	"context_ip" varchar(512) NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"context_page_path" varchar(1024) NULL,
	"context_page_title" varchar(512) NULL,
	"context_page_url" varchar(512) NULL,
	"context_user_agent" varchar(512) NULL,
	"event" varchar(512) NULL,
	"event_text" varchar(512) NULL,
	"_id" varchar(512) NULL,
	"original_timestamp" timestamp NULL,
	"price" varchar(512) NULL,
	"properties_id" varchar(512) NULL,
	"properties_price" int8 NULL,
	"properties_store_id" int8 NULL,
	"properties_timestamp" int8 NULL,
	"sent_at" timestamp NULL,
	"store_id" int8 NULL,
	"timestamp" timestamp NULL,
	"action" varchar(512) NULL,
	"context_campaign_medium" varchar(512) NULL,
	"context_campaign_name" varchar(512) NULL,
	"context_campaign_source" varchar(512) NULL,
	"context_page_referrer" varchar(512) NULL,
	"context_page_search" varchar(512) NULL,
	"customer_id" int8 NULL,
	"shipping" varchar(512) NULL,
	"user_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "set_payment_method";
CREATE TABLE "set_payment_method" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"customer_id" int8 NULL,
	"event" varchar(512) NULL,
	"event_text" varchar(512) NULL,
	"original_timestamp" timestamp NULL,
	"sent_at" timestamp NULL,
	"store_id" int8 NULL,
	"timestamp" timestamp NULL,
	"user_id" varchar(512) NULL,
	"__segment_object_id" varchar(512) NULL,
	"segment_object_id" varchar(512) NULL,
	"session_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "set_shipping_address";
CREATE TABLE "set_shipping_address" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"customer_id" int8 NULL,
	"event" varchar(512) NULL,
	"event_text" varchar(512) NULL,
	"original_timestamp" timestamp NULL,
	"sent_at" timestamp NULL,
	"state" varchar(512) NULL,
	"store_id" int8 NULL,
	"timestamp" timestamp NULL,
	"user_id" varchar(512) NULL,
	"__segment_object_id" varchar(512) NULL,
	"segment_object_id" varchar(512) NULL,
	"session_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "tracks";
CREATE TABLE "tracks" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"anonymous_id" varchar(512) NULL,
	"context_ip" varchar(512) NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"context_page_path" varchar(2048) NULL,
	"context_page_referrer" varchar(2048) NULL,
	"context_page_title" varchar(512) NULL,
	"context_page_url" varchar(512) NULL,
	"context_user_agent" varchar(1024) NULL,
	"event" varchar(512) NULL,
	"event_text" varchar(512) NULL,
	"original_timestamp" timestamp NULL,
	"sent_at" timestamp NULL,
	"timestamp" timestamp NULL,
	"user_id" varchar(512) NULL,
	"context_page_search" varchar(512) NULL,
	"context_campaign_medium" varchar(512) NULL,
	"context_campaign_source" varchar(512) NULL,
	"__segment_object_id" varchar(512) NULL,
	"segment_object_id" varchar(512) NULL,
	"context_campaign_name" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "users";
CREATE TABLE "users" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"customer_id" int8 NULL,
	"email" varchar(512) NULL,
	"first_name" varchar(512) NULL,
	"last_name" varchar(512) NULL,
	"store_id" int8 NULL,
	"uid" varchar(512) NULL,
	"context_campaign_medium" varchar(512) NULL,
	"context_campaign_name" varchar(512) NULL,
	"context_campaign_source" varchar(2048) NULL,
	"context_ip" varchar(512) NULL,
	"context_page_path" varchar(2048) NULL,
	"context_page_referrer" varchar(2048) NULL,
	"context_page_search" varchar(2048) NULL,
	"context_page_title" varchar(512) NULL,
	"context_page_url" varchar(2048) NULL,
	"context_user_agent" varchar(2048) NULL,
	"_uuid" varchar(512) NULL,
	"context_campaign_content" varchar(2048) NULL,
	"context_campaign_souce" varchar(2048) NULL,
	"__segment_object_id" varchar(512) NULL,
	"segment_object_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp Null
);


DROP TABLE IF EXISTS "verified_customer_age";
CREATE TABLE "verified_customer_age" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"customer_id" int8 NULL,
	"event" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"event_text" varchar(512) NULL,
	"user_id" varchar(512) NULL,
	"original_timestamp" timestamp NULL,
	"agent_id" int8 NULL,
	"order_id" int8 NULL,
	"order_status_id" int8 NULL,
	"sent_at" timestamp NULL,
	"verification_method" varchar(512) NULL,
	"context_library_name" varchar(512) NULL,
	"timestamp" timestamp NULL,
	"verification_notes" varchar(512) NULL,
	"segment_object_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "viewed_pack";
CREATE TABLE "viewed_pack" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"anonymous_id" varchar(512) NULL,
	"context_ip" varchar(512) NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"context_page_path" varchar(1024) NULL,
	"context_page_referrer" varchar(2048) NULL,
	"context_page_title" varchar(512) NULL,
	"context_page_url" varchar(1024) NULL,
	"context_user_agent" varchar(1024) NULL,
	"customer_id" int8 NULL,
	"event" varchar(512) NULL,
	"event_text" varchar(512) NULL,
	"_id" int8 NULL,
	"name" varchar(512) NULL,
	"original_timestamp" timestamp NULL,
	"sent_at" timestamp NULL,
	"store_id" int8 NULL,
	"timestamp" timestamp NULL,
	"user_id" varchar(512) NULL,
	"price" varchar(512) NULL,
	"__segment_object_id" varchar(512) NULL,
	"segment_object_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "viewed_product";
CREATE TABLE "viewed_product" (
	"id" varchar(512) NOT NULL UNIQUE PRIMARY KEY,
	"received_at" timestamp NULL,
	"uuid" int8 NULL,
	"uuid_ts" timestamp NULL,
	"anonymous_id" varchar(512) NULL,
	"context_ip" varchar(512) NULL,
	"context_library_name" varchar(512) NULL,
	"context_library_version" varchar(512) NULL,
	"context_page_path" varchar(1024) NULL,
	"context_page_referrer" varchar(2048) NULL,
	"context_page_title" varchar(512) NULL,
	"context_page_url" varchar(1024) NULL,
	"context_user_agent" varchar(1024) NULL,
	"customer_id" int8 NULL,
	"event" varchar(512) NULL,
	"event_text" varchar(512) NULL,
	"_id" int8 NULL,
	"name" varchar(512) NULL,
	"original_timestamp" timestamp NULL,
	"price" float8 NULL,
	"sent_at" timestamp NULL,
	"store_id" int8 NULL,
	"timestamp" timestamp NULL,
	"user_id" varchar(512) NULL,
	"context_page_search" varchar(512) NULL,
	"context_campaign_medium" varchar(512) NULL,
	"context_campaign_source" varchar(512) NULL,
	"__segment_object_id" varchar(512) NULL,
	"segment_object_id" varchar(512) NULL,
	"src_file_name" varchar(1024) NULL,
	"data_loaded_at" timestamp default current_timestamp
);


DROP TABLE IF EXISTS "loading_log";
CREATE TABLE "loading_log" (
	"id" serial primary key,
	"file_name" varchar(1024) NOT NULL,
	"records" int8 NULL,
	"last_modified_at" timestamp NULL,
	"data_loaded_at" timestamp default current_timestamp
);

DROP TABLE IF EXISTS "loading_to_hdfs_log";
CREATE TABLE "loading_log" (
	"id" serial primary key,
	"file_name" varchar(1024) NOT NULL,
	"last_modified_at" timestamp NULL,
	"data_loaded_at" timestamp default current_timestamp
);

DROP TABLE IF EXISTS "selected_offer";
CREATE TABLE "selected_offer" (
	"id" varchar(512) NOT NULL,
	"received_at" timestamp NULL,
	"uuid" int8,
	"uuid_ts" timestamp NULL,
	"anonymous_id" varchar(512),
	"context_ip" varchar(512),
	"context_library_name" varchar(512),
	"context_library_version" varchar(512),
	"context_page_path" varchar(2048),
	"context_page_title" varchar(512),
	"context_page_url" varchar(2048),
	"context_user_agent" varchar(512),
	"event" varchar(512),
	"event_text" varchar(512),
	"_id" varchar(512),
	"original_timestamp" timestamp NULL,
	"price" varchar(512),
	"properties_id" varchar(512),
	"properties_price" int8,
	"properties_store_id" int8,
	"properties_timestamp" int8,
	"sent_at" timestamp NULL,
	"store_id" int8,
	"timestamp" timestamp NULL,
	"action" varchar(512),
	"context_campaign_medium" varchar(512),
	"context_campaign_name" varchar(512),
	"context_campaign_source" varchar(512),
	"context_page_referrer" varchar(2048),
	"context_page_search" varchar(2048),
	"customer_id" int8,
	"shipping" varchar(512),
	"user_id" varchar(512),
	"src_file_name" varchar(1024),
	"data_loaded_at" timestamp default current_timestamp
);






