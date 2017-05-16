import smart_open
import sys
import json
from pyspark import SparkConf, SparkContext
from operator import add
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
reload(sys)
sys.setdefaultencoding('utf-8')


class EventFile:
    def __init__(self, object_name, schema):
        print 'File_Name -- ' + object_name + '--'
        self.object_name = object_name
        self.schema_name = schema
        #self.object_name = '1489762706936.9b8efad78e96.1.10.5.8ed08c5b-6100-49f4-ae7c-0c994a231623'
        self.process()

    def process(self):
        #with smart_open.smart_open(self.raw_data) as fin:
        completed_order_str = ''
        completed_order_t_str = ''
        identifies_str = ''
        added_prod_str = ''
        added_prod_t_str = ''
        added_addr_str = ''
        added_addr_t_str = ''
        added_pay_str = ''
        added_pay_t_str = ''
        set_pay_str = ''
        set_pay_t_str = ''
        reg_str = ''
        reg_t_str = ''
        s_shipping_str = ''
        s_shipping_t_str = ''
        log_in_str = ''
        log_in_t_str = ''
        removed_prod_str = ''
        removed_prod_t_str = ''
        issue_refund_str = ''
        issue_refund_t_str = ''
        logged_out_str = ''
        logged_out_t_str = ''
        reshipped_order_str = ''
        reshipped_order_t_str = ''
        chg_ship_addr_str = ''
        chg_ship_addr_t_str = ''
        failed_pay_att_str = ''
        failed_pay_att_t_str = ''
        replaced_items_str = ''
        replaced_items_t_str = ''
        verified_c_age_str = ''
        verified_c_age_t_str = ''
        cancel_reship_str = ''
        cancel_reship_t_str = ''
        viewed_product_str = ''
        viewed_product_t_str = ''
        viewed_pack_str = ''
        viewed_pack_t_str = ''
        page_str = ''
        m_identifies_str = ''
        select_offer_str = ''
        select_offer_t_str = ''
        loaded_overlay_str = ''
        loaded_overlay_t_str = ''
        clicked_subnav_str = ''
        clicked_subnav_t_str = ''
        watched_video_str = ''
        watched_video_t_str = ''
        selected_gift_type_str = ''
        selected_gift_type_t_str = ''
        started_video_str = ''
        started_video_t_str = ''
        paused_video_str = ''
        paused_video_t_str = ''

        self.records = 0
	if self.schema_name == 'ho_production':
	  hdfs_prefix = 'hdfs://segment/ho_production/'
        elif self.schema_name == 'atocw_production':
	  hdfs_prefix = 'hdfs://segment/atocw_production/'
        elif self.schema_name == 'chw_production':
	  hdfs_prefix = 'hdfs://segment/chw_production/'
	elif self.schema_name == 'adw_production':
	  hdfs_prefix = 'hdfs://segment/adw_production/'
	elif self.schema_name == 'bw_production':
	  hdfs_prefix = 'hdfs://segment/bw_production/'
	
        for line in smart_open.smart_open(hdfs_prefix + self.object_name):
            self.records += 1
            try:
                if '{"_metadata"' in line:
                    data = json.loads(str(line))
                    m_id = data['messageId']
                    if '"event":' in line:
                        event_text = self.is_null('str', data['event'])
                        type_text = self.is_null('str', data['type'])

                        if event_text == 'Viewed Product':#and self.is_key_duplicate(event_text, m_id):
                            print str(self.records) + '-Viewed Product-' + line
                            event = 'viewed_product'
                            p_price = str(data['properties']['price'])
                            receivedAt = str(data['receivedAt'])
                            anonymousId = self.is_null('str', data['anonymousId'])
                            context_ip = str(data['context']['ip'])
                            context_library_name = str(data['context']['library']['name'])
                            context_library_version = str(data['context']['library']['version'])
                            context_page_path = str(data['context']['page']['path'])

                            context_page_referrer = self.is_null('str',
                                                                 data['context']['page']['referrer']).replace("'",
                                                                                                              "''")

                            context_page_title = str(data['context']['page']['title'])
                            context_page_url = str(data['context']['page']['url'])
                            context_user_agent = str(data['context']['userAgent'])
                            p_id = str(data['properties']['id'])
                            p_name = self.is_null('str', data['properties']['name']).replace("'", "''")
                            originalTimestamp = str(data['originalTimestamp'])
                            sentAt = str(data['sentAt'])
                            store_id = str(data['properties']['store_id'])
                            timestamp = str(data['timestamp'])
                            user_id = self.is_null('int', data['userId'])

                            viewed_product_str = viewed_product_str + "('" + m_id + "','" + receivedAt + "','" \
                                                + anonymousId + "','" + context_ip + "','" + context_library_name + "','" \
                                                + context_library_version + "','" + context_page_path + "','" \
                                                + context_page_referrer + "','" + context_page_title + "','" \
                                                + context_page_url + "','" + context_user_agent + "','" + event + "','" \
                                                + event_text + "','" + p_id + "','" + p_name + "','" + originalTimestamp + "','" \
                                                + p_price + "','" + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                                + self.object_name + "'),"

                            viewed_product_t_str = viewed_product_t_str + "('" + m_id + "','" + receivedAt + "','" \
                                                  + anonymousId + "','" + context_ip + "','" \
                                                  + context_library_name + "','" + context_library_version + "','" \
                                                  + context_page_path + "','" + context_page_referrer + "','" \
                                                  + context_page_title + "','" \
                                                  + context_page_url + "','" + context_user_agent + "','" \
                                                  + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                                  + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                                  + self.object_name + "'),"

                        elif event_text == 'Viewed Pack':# and self.is_key_duplicate(event_text, m_id):
                            print str(self.records) + '-Viewed Pack-' + line
                            event = 'viewed_pack'
                            receivedAt = str(data['receivedAt'])
                            anonymousId = self.is_null('str', data['anonymousId'])
                            context_ip = str(data['context']['ip'])
                            context_library_name = str(data['context']['library']['name'])
                            context_library_version = str(data['context']['library']['version'])
                            context_page_path = str(data['context']['page']['path'])
                            context_page_referrer = self.is_null('str',
                                                                 data['context']['page']['referrer']).replace("'",
                                                                                                              "''")
                            context_page_title = str(data['context']['page']['title'])
                            context_page_url = str(data['context']['page']['url'])
                            context_user_agent = str(data['context']['userAgent'])
                            p_id = str(data['properties']['id'])
                            p_name = self.is_null('str', data['properties']['name']).replace("'", "''")
                            originalTimestamp = str(data['originalTimestamp'])
                            sentAt = str(data['sentAt'])
                            store_id = str(data['properties']['store_id'])
                            timestamp = str(data['timestamp'])
                            user_id = self.is_null('int', data['userId'])

                            viewed_pack_str = viewed_pack_str + "('" + m_id + "','" + receivedAt + "','" \
                                                 + anonymousId + "','" + context_ip + "','" + context_library_name + "','" \
                                                 + context_library_version + "','" + context_page_path + "','" \
                                                 + context_page_referrer + "','" + context_page_title + "','" \
                                                 + context_page_url + "','" + context_user_agent + "','" + event + "','" \
                                                 + event_text + "','" + p_id + "','" + p_name + "','" + originalTimestamp + "','" \
                                                 + sentAt + "','" + store_id + "','" + timestamp + "','" + user_id + "','" \
                                                 + self.object_name + "'),"

                            viewed_pack_t_str = viewed_pack_t_str + "('" + m_id + "','" + receivedAt + "','" \
                                                   + anonymousId + "','" + context_ip + "','" \
                                                   + context_library_name + "','" + context_library_version + "','" \
                                                   + context_page_path + "','" + context_page_referrer + "','" \
                                                   + context_page_title + "','" \
                                                   + context_page_url + "','" + context_user_agent + "','" \
                                                   + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                                   + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                                   + self.object_name + "'),"

                        elif event_text == 'Selected Offer':# and self.is_key_duplicate(event_text, m_id):
                            print str(self.records) + '-Selected Offer-' + line
                            receivedAt = str(data['receivedAt'])
                            anonymousId = self.is_null('str', data['anonymousId'])
                            context_ip = str(data['context']['ip'])
                            context_library_name = str(data['context']['library']['name'])
                            context_library_version = str(data['context']['library']['version'])
                            context_page_path = str(data['context']['page']['path'])
                            context_page_title = str(data['context']['page']['title'])
                            context_page_url = str(data['context']['page']['url'])
                            context_user_agent = str(data['context']['userAgent'])
                            event = 'selected_offer'
                            p_id = str(data['properties']['id'])
                            originalTimestamp = str(data['originalTimestamp'])
                            price = str(data['properties']['price'])
                            sentAt = str(data['sentAt'])
                            store_id = str(data['properties']['store_id'])
                            timestamp = str(data['timestamp'])
                            action = str(data['properties']['action'])

                            if data['context'].get('campaign') and data['context']['campaign'].get('medium'):
                                context_campaign_medium = self.is_null('str', data['context']['campaign']['medium'])
                            else:
                                context_campaign_medium = ''

                            if data['context'].get('campaign') and data['context']['campaign'].get('name'):
                                context_campaign_name = self.is_null('str', data['context']['campaign']['name'])
                            else:
                                context_campaign_name = ''

                            if data['context'].get('campaign') and data['context']['campaign'].get('source'):
                                context_campaign_source = self.is_null('str', data['context']['campaign']['source'])
                            else:
                                context_campaign_source = ''

                            context_page_referrer = self.is_null('str',
                                                                 data['context']['page']['referrer']).replace("'",
                                                                                                              "''")

                            context_page_search = self.is_null('str', data['context']['page']['search'])

                            if data['properties'].get('customer_id'):
                                customer_id = str(data['properties']['customer_id'])
                            else:
                                customer_id = 'null'

                            shipping = str(data['properties']['shipping'])
                            user_id = self.is_null('int', data['userId'])

                            select_offer_str = select_offer_str + "('" + m_id + "','" + receivedAt + "','" \
                                              + anonymousId + "','" + context_ip + "','" + context_library_name + "','" \
                                              + context_library_version + "','" + context_page_path + "','" \
                                              + context_page_title + "','" + context_page_url + "','" \
                                              + context_user_agent + "','" + event + "','" + event_text + "','" \
                                              + p_id + "','" + originalTimestamp + "'," + price + ",'" \
                                              + sentAt + "','" + store_id + "','" + timestamp + "','" + action + "','" \
                                              + context_campaign_medium + "','" + context_campaign_name + "','" \
                                              + context_campaign_source + "','" + context_page_referrer + "','" \
                                              + context_page_search + "'," + customer_id + "," + shipping + ",'" \
                                              + user_id + "','" + self.object_name + "'),"

                            select_offer_t_str = select_offer_t_str + "('" + m_id + "','" + receivedAt + "','" \
                                                + anonymousId + "','" + context_ip + "','" \
                                                + context_library_name + "','" + context_library_version + "','" \
                                                + context_page_path + "','" + context_page_referrer + "','" \
                                                + context_page_title + "','" \
                                                + context_page_url + "','" + context_user_agent + "','" \
                                                + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                                + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                                + self.object_name + "'),"


                        elif event_text == 'Loaded Overlay':  # and self.is_key_duplicate(event_text, m_id):
                            print str(self.records) + '-Loaded Overlay-' + line
                            receivedAt = str(data['receivedAt'])
                            action = str(data['properties']['action'])
                            anonymousId = self.is_null('str', data['anonymousId'])
                            context_ip = str(data['context']['ip'])
                            context_library_name = str(data['context']['library']['name'])
                            context_library_version = str(data['context']['library']['version'])
                            context_page_path = str(data['context']['page']['path'])
                            context_page_referrer = self.is_null('str',
                                                                 data['context']['page']['referrer']).replace("'",
                                                                                                              "''")
                            context_page_title = str(data['context']['page']['title'])
                            context_page_url = str(data['context']['page']['url'])
                            context_user_agent = str(data['context']['userAgent'])
                            event = 'loaded_overlay'
                            inventory_unit_id = str(data['properties']['inventory_unit_id'])
                            originalTimestamp = str(data['originalTimestamp'])
                            sentAt = str(data['sentAt'])
                            store_id = str(data['properties']['store_id'])
                            timestamp = str(data['timestamp'])
                            context_page_search = self.is_null('str', data['context']['page']['search'])
                            if data['properties'].get('customer_id'):
                                customer_id = str(data['properties']['customer_id'])
                            else:
                                customer_id = 'null'
                            user_id = self.is_null('int', data['userId'])

                            loaded_overlay_str = loaded_overlay_str + "('" + m_id + "','" + receivedAt + "','" \
                                              + action + "','" + anonymousId + "','" + context_ip + "','" + context_library_name + "','" \
                                              + context_library_version + "','" + context_page_path + "','" \
                                              + context_page_referrer + "','" + context_page_title + "','" \
                                              + context_page_url + "','" + context_user_agent + "','" + event + "','" \
                                              + event_text + "','" + inventory_unit_id + "','" + originalTimestamp + "','" \
                                              + sentAt + "','" + store_id + "','" + timestamp + "','" + context_page_search + "'," \
                                              + customer_id + ",'" + user_id + "','" \
                                              + self.object_name + "'),"

                            loaded_overlay_t_str = loaded_overlay_t_str + "('" + m_id + "','" + receivedAt + "','" \
                                                + anonymousId + "','" + context_ip + "','" \
                                                + context_library_name + "','" + context_library_version + "','" \
                                                + context_page_path + "','" + context_page_referrer + "','" \
                                                + context_page_title + "','" \
                                                + context_page_url + "','" + context_user_agent + "','" \
                                                + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                                + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                                + self.object_name + "'),"

                        elif event_text == 'Clicked Subnav':  # and self.is_key_duplicate(event_text, m_id):
                            print str(self.records) + '-Clicked Subnav-' + line
                            receivedAt = str(data['receivedAt'])
                            action = str(data['properties']['action'])
                            anonymousId = self.is_null('str', data['anonymousId'])
                            context_ip = str(data['context']['ip'])
                            context_library_name = str(data['context']['library']['name'])
                            context_library_version = str(data['context']['library']['version'])
                            context_page_path = str(data['context']['page']['path'])
                            context_page_referrer = self.is_null('str',
                                                                 data['context']['page']['referrer']).replace("'",
                                                                                                              "''")
                            context_page_title = str(data['context']['page']['title'])
                            context_page_url = str(data['context']['page']['url'])
                            context_user_agent = str(data['context']['userAgent'])
                            event = 'clicked_subnav'
                            label = str(data['properties']['label'])
                            originalTimestamp = str(data['originalTimestamp'])
                            sentAt = str(data['sentAt'])
                            store_id = str(data['properties']['store_id'])
                            timestamp = str(data['timestamp'])
                            context_page_search = self.is_null('str', data['context']['page']['search'])
                            user_id = self.is_null('int', data['userId'])
                            if data['properties'].get('customer_id'):
                                customer_id = str(data['properties']['customer_id'])
                            else:
                                customer_id = 'null'

                            if data['context'].get('campaign') and data['context']['campaign'].get('medium'):
                                context_campaign_medium = self.is_null('str', data['context']['campaign']['medium'])
                            else:
                                context_campaign_medium = ''

                            if data['context'].get('campaign') and data['context']['campaign'].get('name'):
                                context_campaign_name = self.is_null('str', data['context']['campaign']['name'])
                            else:
                                context_campaign_name = ''

                            if data['context'].get('campaign') and data['context']['campaign'].get('source'):
                                context_campaign_source = self.is_null('str', data['context']['campaign']['source'])
                            else:
                                context_campaign_source = ''

                            if data['context'].get('campaign') and data['context']['campaign'].get('term'):
                                context_campaign_term = self.is_null('str', data['context']['campaign']['term'])
                            else:
                                context_campaign_term = ''

                            clicked_subnav_str = clicked_subnav_str + "('" + m_id + "','" + receivedAt + "','" \
                                                + action + "','" + anonymousId + "','" + context_ip + "','" + context_library_name + "','" \
                                                + context_library_version + "','" + context_page_path + "','" \
                                                + context_page_referrer + "','" + context_page_title + "','" \
                                                + context_page_url + "','" + context_user_agent + "','" + event + "','" \
                                                + event_text + "','" + label + "','" + originalTimestamp + "','" \
                                                + sentAt + "','" + store_id + "','" + timestamp + "','" + context_page_search + "'," \
                                                + user_id + "," + customer_id + ",'" \
                                                + context_campaign_medium + "','" + context_campaign_name + "','" \
                                                + context_campaign_source + "','" + context_campaign_term + "','" \
                                                + self.object_name + "'),"

                            clicked_subnav_t_str = clicked_subnav_t_str + "('" + m_id + "','" + receivedAt + "','" \
                                                 + anonymousId + "','" + context_ip + "','" \
                                                 + context_library_name + "','" + context_library_version + "','" \
                                                 + context_page_path + "','" + context_page_referrer + "','" \
                                                 + context_page_title + "','" \
                                                 + context_page_url + "','" + context_user_agent + "','" \
                                                 + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                                 + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                                 + self.object_name + "'),"

                        elif event_text in ('Watched Video', 'Started Video', 'Paused Video'):  # and self.is_key_duplicate(event_text, m_id):
                            receivedAt = str(data['receivedAt'])
                            action = str(data['properties']['action'])
                            anonymousId = self.is_null('str', data['anonymousId'])
                            context_ip = str(data['context']['ip'])
                            context_library_name = str(data['context']['library']['name'])
                            context_library_version = str(data['context']['library']['version'])
                            context_page_path = str(data['context']['page']['path'])
                            context_page_title = str(data['context']['page']['title'])
                            context_page_url = str(data['context']['page']['url'])
                            context_user_agent = str(data['context']['userAgent'])
                            duration = str(data['properties']['duration'])
                            properties_id = str(data['properties']['id'])
                            originalTimestamp = str(data['originalTimestamp'])
                            properties_percent = str(data['properties']['percent'])
                            seconds = str(data['properties']['seconds'])
                            sentAt = str(data['sentAt'])
                            store_id = str(data['properties']['store_id'])
                            timestamp = str(data['timestamp'])
                            context_page_referrer = self.is_null('str',
                                                                 data['context']['page']['referrer']).replace("'",
                                                                                                              "''")
                            if data['context'].get('campaign') and data['context']['campaign'].get('medium'):
                                context_campaign_medium = self.is_null('str', data['context']['campaign']['medium'])
                            else:
                                context_campaign_medium = ''

                            if data['context'].get('campaign') and data['context']['campaign'].get('name'):
                                context_campaign_name = self.is_null('str', data['context']['campaign']['name'])
                            else:
                                context_campaign_name = ''

                            if data['context'].get('campaign') and data['context']['campaign'].get('source'):
                                context_campaign_source = self.is_null('str', data['context']['campaign']['source'])
                            else:
                                context_campaign_source = ''

                            context_page_search = self.is_null('str', data['context']['page']['search'])


                            if data['properties'].get('customer_id'):
                                customer_id = str(data['properties']['customer_id'])
                            else:
                                customer_id = 'null'

                            user_id = self.is_null('int', data['userId'])

                            if data['context'].get('campaign') and data['context']['campaign'].get('term'):
                                context_campaign_term = self.is_null('str', data['context']['campaign']['term'])
                            else:
                                context_campaign_term = ''

                            if event_text == 'Watched Video':
                                print str(self.records) + '-Watched Video-' + line
                                event = 'watched_video'
                                watched_video_str = watched_video_str + "('" + m_id + "','" + receivedAt + "','" \
                                                    + action + "','" + anonymousId + "','" + context_ip + "','" + context_library_name + "','" \
                                                    + context_library_version + "','" + context_page_path + "','" \
                                                    + context_page_title + "','" \
                                                    + context_page_url + "','" + context_user_agent + "'," + duration + ",'" \
                                                    + event + "','" + event_text + "','" + properties_id + "','" + originalTimestamp + "'," \
                                                    + properties_percent + "," + seconds + ",'" \
                                                    + sentAt + "','" + store_id + "','" + timestamp + "','" + context_page_referrer + "','" \
                                                    + context_campaign_medium + "','" + context_campaign_name + "','" \
                                                    + context_campaign_source + "','" + context_page_search + "'," \
                                                    + customer_id + "," + user_id + ",'" + context_campaign_term + "','" \
                                                    + self.object_name + "'),"

                                watched_video_t_str = watched_video_t_str + "('" + m_id + "','" + receivedAt + "','" \
                                                      + anonymousId + "','" + context_ip + "','" \
                                                      + context_library_name + "','" + context_library_version + "','" \
                                                      + context_page_path + "','" + context_page_referrer + "','" \
                                                      + context_page_title + "','" \
                                                      + context_page_url + "','" + context_user_agent + "','" \
                                                      + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                                      + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                                      + self.object_name + "'),"


                            elif event_text == 'Started Video':
                                print str(self.records) + '-Started Video-' + line
                                event = 'started_video'
                                started_video_str = started_video_str + "('" + m_id + "','" + receivedAt + "','" \
                                                    + action + "','" + anonymousId + "','" + context_ip + "','" + context_library_name + "','" \
                                                    + context_library_version + "','" + context_page_path + "','" \
                                                    + context_page_title + "','" \
                                                    + context_page_url + "','" + context_user_agent + "'," + duration + ",'" \
                                                    + event + "','" + event_text + "','" + properties_id + "','" + originalTimestamp + "'," \
                                                    + properties_percent + "," + seconds + ",'" \
                                                    + sentAt + "','" + store_id + "','" + timestamp + "','" + context_page_referrer + "','" \
                                                    + context_campaign_medium + "','" + context_campaign_name + "','" \
                                                    + context_campaign_source + "','" + context_page_search + "'," \
                                                    + customer_id + "," + user_id + ",'" + context_campaign_term + "','" \
                                                    + self.object_name + "'),"

                                started_video_t_str = started_video_t_str + "('" + m_id + "','" + receivedAt + "','" \
                                                      + anonymousId + "','" + context_ip + "','" \
                                                      + context_library_name + "','" + context_library_version + "','" \
                                                      + context_page_path + "','" + context_page_referrer + "','" \
                                                      + context_page_title + "','" \
                                                      + context_page_url + "','" + context_user_agent + "','" \
                                                      + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                                      + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                                      + self.object_name + "'),"

                            elif event_text == 'Paused Video':
                                print str(self.records) + '-Paused Video-' + line
                                event = 'paused_video'
                                paused_video_str = paused_video_str + "('" + m_id + "','" + receivedAt + "','" \
                                                    + action + "','" + anonymousId + "','" + context_ip + "','" + context_library_name + "','" \
                                                    + context_library_version + "','" + context_page_path + "','" \
                                                    + context_page_title + "','" \
                                                    + context_page_url + "','" + context_user_agent + "'," + duration + ",'" \
                                                    + event + "','" + event_text + "','" + properties_id + "','" + originalTimestamp + "'," \
                                                    + properties_percent + "," + seconds + ",'" \
                                                    + sentAt + "','" + store_id + "','" + timestamp + "','" + context_page_referrer + "','" \
                                                    + context_campaign_medium + "','" + context_campaign_name + "','" \
                                                    + context_campaign_source + "','" + context_page_search + "'," \
                                                    + customer_id + "," + user_id + ",'" + context_campaign_term + "','" \
                                                    + self.object_name + "'),"

                                paused_video_t_str = paused_video_t_str + "('" + m_id + "','" + receivedAt + "','" \
                                                      + anonymousId + "','" + context_ip + "','" \
                                                      + context_library_name + "','" + context_library_version + "','" \
                                                      + context_page_path + "','" + context_page_referrer + "','" \
                                                      + context_page_title + "','" \
                                                      + context_page_url + "','" + context_user_agent + "','" \
                                                      + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                                      + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                                      + self.object_name + "'),"


                        elif event_text == 'Selected Gift Type':  # and self.is_key_duplicate(event_text, m_id):
                            print str(self.records) + '-Selected Gift Type-' + line
                            receivedAt = str(data['receivedAt'])
                            action = str(data['properties']['action'])
                            anonymousId = self.is_null('str', data['anonymousId'])
                            context_ip = str(data['context']['ip'])
                            context_library_name = str(data['context']['library']['name'])
                            context_library_version = str(data['context']['library']['version'])
                            context_page_path = str(data['context']['page']['path'])
                            context_page_referrer = self.is_null('str',
                                                                 data['context']['page']['referrer']).replace("'",
                                                                                                              "''")
                            context_page_title = str(data['context']['page']['title'])
                            context_page_url = str(data['context']['page']['url'])
                            context_user_agent = str(data['context']['userAgent'])
                            event = 'selected_gift_type'
                            gift_label = str(data['properties']['gift_label'])
                            properties_id = str(data['properties']['id'])
                            originalTimestamp = str(data['originalTimestamp'])
                            sentAt = str(data['sentAt'])
                            store_id = str(data['properties']['store_id'])
                            timestamp = str(data['timestamp'])
                            context_page_search = self.is_null('str', data['context']['page']['search'])

                            if data['properties'].get('customer_id'):
                                customer_id = str(data['properties']['customer_id'])
                            else:
                                customer_id = 'null'

                            user_id = self.is_null('int', data['userId'])

                            if data['context'].get('campaign') and data['context']['campaign'].get('medium'):
                                context_campaign_medium = self.is_null('str', data['context']['campaign']['medium'])
                            else:
                                context_campaign_medium = ''

                            if data['context'].get('campaign') and data['context']['campaign'].get('name'):
                                context_campaign_name = self.is_null('str', data['context']['campaign']['name'])
                            else:
                                context_campaign_name = ''

                            if data['context'].get('campaign') and data['context']['campaign'].get('source'):
                                context_campaign_source = self.is_null('str', data['context']['campaign']['source'])
                            else:
                                context_campaign_source = ''

                            selected_gift_type_str = selected_gift_type_str + "('" + m_id + "','" + receivedAt + "','" \
                                                + action + "','" + anonymousId + "','" + context_ip + "','" + context_library_name + "','" \
                                                + context_library_version + "','" + context_page_path + "','" \
                                                + context_page_referrer + "','" + context_page_title + "','" \
                                                + context_page_url + "','" + context_user_agent + "','" \
                                                + event + "','" + event_text + "','" + gift_label + "','" \
                                                + properties_id + "','" + originalTimestamp + "','" \
                                                + sentAt + "','" + store_id + "','" + timestamp + "','" + context_page_search + "'," \
                                                + customer_id + "," + user_id + ",'" \
                                                + context_campaign_medium + "','" + context_campaign_name + "','" \
                                                + context_campaign_source + "','" \
                                                + self.object_name + "'),"

                            selected_gift_type_t_str = selected_gift_type_t_str + "('" + m_id + "','" + receivedAt + "','" \
                                                  + anonymousId + "','" + context_ip + "','" \
                                                  + context_library_name + "','" + context_library_version + "','" \
                                                  + context_page_path + "','" + context_page_referrer + "','" \
                                                  + context_page_title + "','" \
                                                  + context_page_url + "','" + context_user_agent + "','" \
                                                  + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                                  + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                                  + self.object_name + "'),"

                        else:
                            print 'meta_other' + event_text + ' - ' + type_text + '---'
                            print str(self.records) + '-' + line
                            #sys.exit()

                    else:
                        type_text = self.is_null('str', data['type'])
                        if type_text == 'page':# and self.is_key_duplicate(type_text, m_id):
                            print str(self.records) + '-page-' + line
                            receivedAt = str(data['receivedAt'])
                            anonymousId = self.is_null('str', data['anonymousId'])
                            context_ip = str(data['context']['ip'])
                            context_library_name = str(data['context']['library']['name'])
                            context_library_version = str(data['context']['library']['version'])
                            context_page_path = str(data['context']['page']['path'])
                            context_page_referrer = self.is_null('str', data['context']['page']['referrer']).replace("'", "''")
                            context_page_search = self.is_null('str', data['context']['page']['search'])
                            context_page_title = str(data['context']['page']['title'])
                            context_page_url = str(data['context']['page']['url'])
                            context_user_agent = str(data['context']['userAgent'])
                            originalTimestamp = str(data['originalTimestamp'])
                            p_path = str(data['properties']['path'])
                            #p_referrer = self.is_null('str', data['properties']['referrer'])
                            p_referrer = self.is_null('str', data['properties']['referrer']).replace("'","''")
                            p_search = self.is_null('str', data['properties']['search'])
                            sentAt = str(data['sentAt'])
                            timestamp = str(data['timestamp'])
                            p_title = self.is_null('str', data['properties']['title'])
                            p_url = self.is_null('str', data['properties']['url'])
                            user_id = self.is_null('int', data['userId'])

                            if data['context'].get('campaign') and data['context']['campaign'].get('name'):
                                context_campaign_name = self.is_null('str', data['context']['campaign']['name'])
                            else:
                                context_campaign_name = ''

                            if data['context'].get('campaign') and data['context']['campaign'].get('source'):
                                context_campaign_source = self.is_null('str', data['context']['campaign']['source'])
                            else:
                                context_campaign_source = ''

                            if data['context'].get('campaign') and data['context']['campaign'].get('medium'):
                                context_campaign_medium = self.is_null('str', data['context']['campaign']['medium'])
                            else:
                                context_campaign_medium = ''

                            page_str = page_str + "('" + m_id + "','" + receivedAt + "','" \
                                                    + anonymousId + "','" + context_ip + "','" + context_library_name + "','" \
                                                    + context_library_version + "','" + context_page_path + "','" \
                                                    + context_page_referrer + "','" + context_page_search + "','" \
                                                    + context_page_title + "','" + context_page_url + "','" + context_user_agent + "','" \
                                                    + originalTimestamp + "','" + p_path + "','" + p_referrer + "','" + p_search + "','" \
                                                    + sentAt + "','" + timestamp + "','" + p_title + "','" + p_url + "','" \
                                                    + user_id + "','" + context_campaign_name + "','" + context_campaign_source + "','" \
                                                    + context_campaign_medium + "','" + self.object_name + "'),"

                        elif type_text == 'identify':# and self.is_key_duplicate(type_text, m_id):
                            print str(self.records) + '-identify(type)-' + line
                            receivedAt = str(data['receivedAt'])
                            anonymousId = str(data['anonymousId'])
                            context_library_name = str(data['context']['library']['name'])
                            context_library_version = str(data['context']['library']['version'])

                            if '"traits":{"customer_id":' in line:

                                if data['traits'].get('customer_id'):
                                    customer_id = str(data['traits']['customer_id'])
                                else:
                                    customer_id = 'null'
				print '111'
                                if data['traits'].get('email'):
                                    #email = str(data['traits']['email'])
                                    email = self.is_null('str', data['traits']['email']).replace("'", "''")
                                else:
                                    email = ''
				    
                                if data['traits'].get('first_name'):
                                    #first_name = str(data['traits']['first_name'])
                                    first_name = self.is_null('str', data['traits']['first_name']).replace("'", "''")
                                else:
                                    first_name = ''

                                if data['traits'].get('last_name'):
                                    #last_name = str(data['traits']['last_name'])
                                    last_name = self.is_null('str', data['traits']['last_name']).replace("'", "''")
                                else:
                                    last_name = ''

                                if data['traits'].get('store_id'):
                                    store_id = str(data['traits']['store_id'])
                                else:
                                    store_id = 'null'

                                if data['traits'].get('uuid'):
                                    uuid = str(data['traits']['uuid'])
                                else:
                                    uuid = ''
                            else:
                                customer_id = 'null'
				email = ''
				first_name = ''                             
				last_name = ''
				store_id = 'null'
				uuid = ''

                            if data['context'].get('campaign') and data['context']['campaign'].get('name'):
                                context_campaign_name = self.is_null('str', data['context']['campaign']['name'])
                            else:
                                context_campaign_name = ''

                            if data['context'].get('campaign') and data['context']['campaign'].get('source'):
                                context_campaign_source = self.is_null('str', data['context']['campaign']['source'])
                            else:
                                context_campaign_source = ''

                            if data['context'].get('campaign') and data['context']['campaign'].get('medium'):
                                context_campaign_medium = self.is_null('str', data['context']['campaign']['medium'])
                            else:
                                context_campaign_medium = ''

                            if data['context'].get('campaign') and data['context']['campaign'].get('content'):
                                context_campaign_content = self.is_null('str', data['context']['campaign']['content'])
                            else:
                                context_campaign_content = ''


                                originalTimestamp = str(data['originalTimestamp'])
                                sentAt = str(data['sentAt'])
                                timestamp = str(data['timestamp'])
                                user_id = str(data['userId'])
                                context_ip = str(data['context']['ip'])
                                context_page_path = str(data['context']['page']['path'])
                                #context_page_referrer = self.is_null('str', data['context']['page']['referrer'])
                                context_page_referrer = self.is_null('str', data['context']['page']['referrer']).replace("'", "''")
                                context_page_search = self.is_null('str', data['context']['page']['search'])
                                context_page_title = str(data['context']['page']['title'])
                                context_page_url = str(data['context']['page']['url'])
                                context_user_agent = str(data['context']['userAgent'])

                                m_identifies_str = m_identifies_str + "('" + m_id + "','" + receivedAt + "','" \
                                                   + anonymousId + "','" + context_library_name + "','" + context_library_version + "'," \
                                                   + customer_id + ",'" + email + "','" + first_name + "','" \
                                                   + last_name + "','" + originalTimestamp + "','" + sentAt + "'," + store_id + ",'" \
                                                   + timestamp + "','','" + user_id + "','" + context_campaign_medium + "','" \
                                                   + context_campaign_name + "','" + context_campaign_source + "','" \
                                                   + context_ip + "','" + context_page_path + "','" + context_page_referrer + "','" \
                                                   + context_page_search + "','" + context_page_title + "','" + context_page_url + "','" \
                                                   + context_user_agent + "','" + uuid + "','" + context_campaign_content + "','" \
                                                   + self.object_name + "'),"


                        else:
                            print 'else - ' + str(self.records) + '-' + type_text + line
                            #sys.exit()


                elif '{"anonymousId"' in line:
                    data = json.loads(str(line))
                    m_id = data['messageId']

                    if data['type'] == 'identify':
                        event_text = ''
                    else:
                        event_text = self.is_null('str', data['event'])

                    if event_text == 'Completed Order':# and self.is_key_duplicate(event_text, m_id):
                        print str(self.records) + '-Completed Order-' + line
                        receivedAt = str(data['receivedAt'])
                        context_library_name = str(data['context']['library']['name'])
                        context_library_version = str(data['context']['library']['version'])
                        customer_id = str(data['properties']['customer_id'])
                        discount = str(data['properties']['discount'])
                        event = 'completed_order'
                        ip_address = str(data['properties']['ip_address'])
                        order_id = str(data['properties']['order_id'])
                        originalTimestamp = str(data['originalTimestamp'])
                        passed_compliance = str(data['properties']['passed_compliance'])
                        products = str(data['properties']['products']).replace("'", "''").replace("u'", "'")
                        sentAt = str(data['sentAt'])
                        shipping = str(data['properties']['shipping'])
                        store_id = str(data['properties']['store_id'])
                        subtotal = str(data['properties']['subtotal'])
                        tax = str(data['properties']['tax'])
                        timestamp = str(data['timestamp'])
                        total = str(data['properties']['total'])
                        user_id = str(data['userId'])
                        vouchers = str(data['properties']['vouchers'])
                        camp_id = self.is_null('int', data['properties']['campaign_id'])
                        par_id = self.is_null('int', data['properties']['partner_id'])
                        prom_id = self.is_null('int', data['properties']['promotion_id'])
                        sub_id = self.is_null('str', data['properties']['sub_id'])

                        completed_order_str = completed_order_str + "('" + m_id + "','" + receivedAt + "','" \
                                              + context_library_name + "','" + context_library_version + "','" \
                                              + customer_id + "'," + discount + ",'" + event + "','" \
                                              + event_text + "','" + ip_address + "','" + order_id + "','" \
                                              + originalTimestamp + "','" + passed_compliance + "','" \
                                              + products + "','" + sentAt + "'," + shipping + ",'" \
                                              + store_id + "'," + subtotal + "," + tax + ",'" + timestamp + "'," \
                                              + total + ",'" + user_id + "','" + vouchers + "'," \
                                              + camp_id + "," + par_id + "," + prom_id + ",'" \
                                              + self.object_name + "','" \
                                              + sub_id + "'),"

                        completed_order_t_str = completed_order_t_str + "('" + m_id + "','" + receivedAt + "','','" \
                                                + context_library_name + "','" + context_library_version + "','" \
                                                + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                                + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                                + self.object_name + "'),"

                    elif event_text == 'Cancel Reship':# and self.is_key_duplicate(event_text, m_id):
                        print str(self.records) + '-Cancel Reship-' + line #45
                        receivedAt = str(data['receivedAt'])
                        agent_id = str(data['properties']['agent_id'])
                        cancel_reship_notes = self.is_null('str', data['properties']['cancel_reship_notes'])\
                            .replace("'", "''")
                        context_library_name = str(data['context']['library']['name'])
                        context_library_version = str(data['context']['library']['version'])
                        customer_id = str(data['properties']['customer_id'])
                        event = 'cancel_reship'
                        order_id = str(data['properties']['order_id'])
                        order_status_id = str(data['properties']['order_status_id'])
                        originalTimestamp = str(data['originalTimestamp'])
                        reship_refund_amount = str(data['properties']['reship_refund_amount'])
                        sentAt = str(data['sentAt'])
                        timestamp = str(data['timestamp'])
                        user_id = self.is_null('int', data['userId'])

                        cancel_reship_str = cancel_reship_str + "('" + m_id + "','" + receivedAt + "','" \
                                             + agent_id + "','" + cancel_reship_notes + "','" + context_library_name + "','" \
                                             + context_library_version + "','" + customer_id + "','" \
                                             + event + "','" + event_text + "','" \
                                             + order_id + "','" + order_status_id + "','" + originalTimestamp + "','" \
                                             + reship_refund_amount + "','" + sentAt + "','" \
                                             + timestamp + "','" + user_id + "','" \
                                             + self.object_name + "'),"

                        cancel_reship_t_str = cancel_reship_t_str + "('" + m_id + "','" + receivedAt + "','','" \
                                               + context_library_name + "','" + context_library_version + "','" \
                                               + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                               + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                               + self.object_name + "'),"


                    elif event_text == 'Replaced Items':# and self.is_key_duplicate(event_text, m_id):
                        print str(self.records) + '-Replaced Items-' + line  #130
                        receivedAt = str(data['receivedAt'])
                        agent_id = str(data['properties']['agent_id'])
                        customer_id = str(data['properties']['customer_id'])
                        event = 'replaced_item'
                        items_replaced = str(data['properties']['items_replaced']).replace("'", "''").replace("u'", "'")
                        order_id = str(data['properties']['order_id'])
                        originalTimestamp = str(data['originalTimestamp'])
                        override_default_action_id = str(data['properties']['override_default_action_id'])
                        replacement_notes = self.is_null('str', data['properties']['replacement_notes']).replace("'", "''")
                        sentAt = str(data['sentAt'])
                        timestamp = str(data['timestamp'])
                        user_id = self.is_null('int', data['userId'])
                        context_library_name = str(data['context']['library']['name'])
                        context_library_version = str(data['context']['library']['version'])
                        order_status_id = str(data['properties']['order_status_id'])
                        order_total = str(data['properties']['order_total'])

                        replaced_items_str = replaced_items_str + "('" + m_id + "','" + receivedAt + "','" \
                                             + agent_id + "','" + customer_id + "','" + event + "','" \
                                             + items_replaced + "','" + order_id + "','" \
                                             + originalTimestamp + "','" + override_default_action_id + "','" \
                                             + replacement_notes + "','" + sentAt + "','" + timestamp + "','" \
                                             + user_id + "','" + context_library_name + "','" \
                                             + context_library_version + "','" + event_text + "','" \
                                             + order_status_id + "'," + order_total + ",'" \
                                             + self.object_name + "'),"

                        replaced_items_t_str = replaced_items_t_str + "('" + m_id + "','" + receivedAt + "','','" \
                                               + context_library_name + "','" + context_library_version + "','" \
                                               + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                               + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                               + self.object_name + "'),"

                    elif event_text == 'Verified Customer Age':# and self.is_key_duplicate(event_text, m_id):
                        print str(self.records) + '-Verified Customer Age-' + line  #134
                        receivedAt = str(data['receivedAt'])
                        customer_id = str(data['properties']['customer_id'])
                        event = 'verified_customer_age'
                        context_library_version = str(data['context']['library']['version'])
                        user_id = self.is_null('int', data['userId'])
                        originalTimestamp = str(data['originalTimestamp'])
                        agent_id = str(data['properties']['agent_id'])
                        order_id = str(data['properties']['order_id'])
                        order_status_id = str(data['properties']['order_status_id'])
                        sentAt = str(data['sentAt'])
                        verification_method = str(data['properties']['verification_method'])
                        context_library_name = str(data['context']['library']['name'])
                        timestamp = str(data['timestamp'])
                        verification_notes = self.is_null('str', data['properties']['verification_notes']).replace("'", "''")

                        verified_c_age_str = verified_c_age_str + "('" + m_id + "','" + receivedAt + "','" \
                                         + customer_id + "','" + event + "','" + context_library_version + "','" \
                                         + event_text + "','" + user_id + "','" + originalTimestamp + "','" \
                                         + agent_id + "','" + order_id + "','" + order_status_id + "','" \
                                         + sentAt + "','" + verification_method + "','" \
                                         + context_library_name + "','" + timestamp + "','" \
                                         + verification_notes + "','" + self.object_name + "'),"

                        verified_c_age_t_str = verified_c_age_t_str + "('" + m_id + "','" + receivedAt + "','','" \
                                           + context_library_name + "','" + context_library_version + "','" \
                                           + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                           + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                           + self.object_name + "'),"


                    elif event_text == 'Failed payment attempt':# and self.is_key_duplicate(event_text, m_id):
                        print str(self.records) + '-Failed payment attempt-' + line  #233
                        receivedAt = str(data['receivedAt'])
                        event = 'failed_payment_attempt'
                        user_id = self.is_null('int', data['userId'])
                        context_library_name = str(data['context']['library']['name'])
                        context_library_version = str(data['context']['library']['version'])
                        originalTimestamp = str(data['originalTimestamp'])
                        timestamp = str(data['timestamp'])
                        amount = str(data['properties']['amount'])
                        customer_id = str(data['properties']['customer_id'])
                        sentAt = str(data['sentAt'])
                        store_id = str(data['properties']['store_id'])

                        failed_pay_att_str = failed_pay_att_str + "('" + m_id + "','" + receivedAt + "','" \
                                         + event + "','" + user_id + "','" + context_library_name + "','" \
                                         + context_library_version + "','" + event_text + "','" \
                                         + originalTimestamp + "','" + timestamp + "'," + amount + ",'" \
                                         + customer_id + "','" + sentAt + "','" + store_id + "','" \
                                         + self.object_name + "'),"

                        failed_pay_att_t_str = failed_pay_att_t_str + "('" + m_id + "','" + receivedAt + "','','" \
                                           + context_library_name + "','" + context_library_version + "','" \
                                           + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                           + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                           + self.object_name + "'),"

                    elif event_text == 'Changed Shipping Address':# and self.is_key_duplicate(event_text, m_id):
                        print str(self.records) + '-Changed Shipping Address-' + line #72
                        receivedAt = str(data['receivedAt'])
                        user_id = self.is_null('int', data['userId'])
                        override_default_action_id = str(data['properties']['override_default_action_id'])
                        order_status_id = str(data['properties']['order_status_id'])
                        original_address_id = str(data['properties']['original_address_id'])
                        agent_id = str(data['properties']['agent_id'])
                        change_notes = self.is_null('str', data['properties']['change_notes']).replace("'", "''")
                        context_library_name = str(data['context']['library']['name'])
                        event = 'changed_shipping_address'
                        order_id = str(data['properties']['order_id'])
                        originalTimestamp = str(data['originalTimestamp'])
                        sentAt = str(data['sentAt'])
                        timestamp = str(data['timestamp'])
                        customer_id = str(data['properties']['customer_id'])
                        tax_difference = str(data['properties']['tax_difference'])
                        ignore_tax_charges = str(data['properties']['ignore_tax_charges'])
                        new_address_id = str(data['properties']['new_address_id'])
                        context_library_version = str(data['context']['library']['version'])
                        order_total = str(data['properties']['order_total'])

                        chg_ship_addr_str = chg_ship_addr_str + "('" + m_id + "','" + receivedAt + "','" \
                                         + event_text + "','" + user_id + "','" \
                                         + override_default_action_id + "','" + order_status_id + "','" \
                                         + original_address_id + "','" + agent_id + "','" \
                                         + change_notes + "','" + context_library_name + "','" + event + "','" \
                                         + order_id + "','" + originalTimestamp + "','" + sentAt + "','" \
                                         + timestamp + "','" + customer_id + "'," + tax_difference + ",'" \
                                         + ignore_tax_charges + "','" + new_address_id + "','" \
                                         + context_library_version + "'," + order_total + ",'" \
                                         + self.object_name + "'),"

                        chg_ship_addr_t_str = chg_ship_addr_t_str + "('" + m_id + "','" + receivedAt + "','','" \
                                           + context_library_name + "','" + context_library_version + "','" \
                                           + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                           + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                           + self.object_name + "'),"

                    elif event_text == 'Reshipped Order':# and self.is_key_duplicate(event_text, m_id):
                        print str(self.records) + '-Reshipped Order-' + line
                        receivedAt = str(data['receivedAt'])
                        sentAt = str(data['sentAt'])
                        shipping_address_id = str(data['properties']['shipping_address_id'])
                        reship_charge_payment_method_id = str(data['properties']['reship_charge_payment_method_id'])
                        agent_id = str(data['properties']['agent_id'])
                        context_library_name = str(data['context']['library']['name'])
                        context_library_version = str(data['context']['library']['version'])
                        customer_id = str(data['properties']['customer_id'])
                        event = 'reshipped_order'
                        items_reshipped = str(data['properties']['items_reshipped']).replace("'", "''").replace("u'", "'")
                        order_id = str(data['properties']['order_id'])
                        order_status_id = str(data['properties']['order_status_id'])
                        original_order_id = str(data['properties']['original_order_id'])
                        originalTimestamp = str(data['originalTimestamp'])
                        reship_reason_id = self.is_null('int', data['properties']['reship_reason_id'])
                        timestamp = str(data['timestamp'])
                        user_id = str(data['userId'])
                        reship_charge = self.is_null('int', data['properties']['reship_charge'])
                        reship_notes = self.is_null('str', data['properties']['reship_notes']).replace("'", "''")

                        reshipped_order_str = reshipped_order_str + "('" + m_id + "','" + receivedAt + "','" \
                                              + sentAt + "','" + event_text + "','" + shipping_address_id + "','" \
                                              + reship_charge_payment_method_id + "','" \
                                              + agent_id + "','" + context_library_name + "','" + context_library_version + "','" \
                                              + customer_id + "','" + event + "','" \
                                              + items_reshipped + "','" + order_id + "','" + order_status_id + "','" \
                                              + original_order_id + "','" + originalTimestamp + "'," \
                                              + reship_reason_id + ",'" + timestamp + "','" + user_id + "'," \
                                              + reship_charge + ",'" + reship_notes + "','" \
                                              + self.object_name + "'),"

                        reshipped_order_t_str = reshipped_order_t_str + "('" + m_id + "','" + receivedAt + "','','" \
                                                + context_library_name + "','" + context_library_version + "','" \
                                                + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                                + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                                + self.object_name + "'),"

                    elif event_text == 'Logged Out':# and self.is_key_duplicate(event_text, m_id):
                        print str(self.records) + '-Logged Out-' + line
                        event = 'logged_out'
                        receivedAt = str(data['receivedAt'])
                        context_library_name = str(data['context']['library']['name'])
                        context_library_version = str(data['context']['library']['version'])
                        customer_id = str(data['properties']['customer_id'])
                        originalTimestamp = str(data['originalTimestamp'])
                        sentAt = str(data['sentAt'])
                        timestamp = str(data['timestamp'])
                        user_id = self.is_null('int', data['userId'])
                        logged_out_str = logged_out_str + "('" + m_id + "','" + receivedAt + "','" \
                                        + context_library_name + "','" + context_library_version + "','" \
                                        + customer_id + "','" + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                        + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                        + self.object_name + "'),"

                        logged_out_t_str = logged_out_t_str + "('" + m_id + "','" + receivedAt + "','','" \
                                        + context_library_name + "','" + context_library_version + "','" \
                                        + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                        + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                        + self.object_name + "'),"

                    elif event_text  == 'Added Product':# and self.is_key_duplicate(event_text, m_id):
                        print str(self.records) + '-Added Product-' + line
                        receivedAt = str(data['receivedAt'])
                        anonymousId = str(data['anonymousId'])
                        context_library_name = str(data['context']['library']['name'])
                        context_library_version = str(data['context']['library']['version'])
                        event = 'added_product'
                        properties_id = str(data['properties']['id'])
                        name = str(data['properties']['name']).replace("'", "''")
                        originalTimestamp = str(data['originalTimestamp'])
                        price = str(data['properties']['price'])
                        quantity = str(data['properties']['quantity'])
                        sentAt = str(data['sentAt'])
                        sku = self.is_null('str', data['properties']['sku'])
                        store_id = str(data['properties']['store_id'])
                        timestamp = str(data['timestamp'])
                        properties_type = str(data['properties']['type'])
                        user_id = self.is_null('int', data['userId'])
                        added_prod_str = added_prod_str + "('" + m_id + "','" + receivedAt + "','" \
                                            + anonymousId + "','" + context_library_name + "','" + context_library_version + "','" \
                                            + event + "','" + event_text + "','" + properties_id + "','" \
                                            + name + "','" + originalTimestamp + "'," + price + "," + quantity + ",'" \
                                            + sentAt + "','" + sku + "','" + store_id + "','" \
                                            + timestamp + "','" + properties_type + "','" + user_id + "','" + self.object_name + "'),"

                        added_prod_t_str = added_prod_t_str + "('" + m_id + "','" + receivedAt + "','','" \
                                                + context_library_name + "','" + context_library_version + "','" \
                                                + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                                + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                                + self.object_name + "'),"

                    elif event_text == 'Removed Product':# and self.is_key_duplicate(event_text, m_id):
                        print str(self.records) + '-Removed Product-' + line
                        receivedAt = str(data['receivedAt'])
                        anonymousId = self.is_null('str', str(data['anonymousId']))
                        context_library_name = str(data['context']['library']['name'])
                        context_library_version = str(data['context']['library']['version'])
                        event = 'removed_product'
                        properties_id = str(data['properties']['id'])
                        name = str(data['properties']['name']).replace("'", "''")
                        originalTimestamp = str(data['originalTimestamp'])
                        sentAt = str(data['sentAt'])
                        sku = self.is_null('str', data['properties']['sku'])
                        store_id = str(data['properties']['store_id'])
                        timestamp = str(data['timestamp'])
                        properties_type = str(data['properties']['type'])
                        user_id = self.is_null('int', data['userId'])

                        removed_prod_str = removed_prod_str + "('" + m_id + "','" + receivedAt + "','" \
                                            + anonymousId + "','" + context_library_name + "','" + context_library_version + "','" \
                                            + event + "','" + event_text + "','" + properties_id + "','" \
                                            + name + "','" + originalTimestamp + "','" + sentAt + "','" \
                                            + sku + "','" + store_id + "','" + timestamp + "','" \
                                            + properties_type + "','" + user_id + "','" + self.object_name + "'),"

                        removed_prod_t_str = removed_prod_t_str + "('" + m_id + "','" + receivedAt + "','','" \
                                            + context_library_name + "','" + context_library_version + "','" \
                                            + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                            + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                            + self.object_name + "'),"

                    elif event_text == 'Issued Refund':# and self.is_key_duplicate(event_text, m_id):
                        print str(self.records) + '-Issued Refund-' + line
                        receivedAt = str(data['receivedAt'])
                        agent_id = str(data['properties']['agent_id'])
                        context_library_name = str(data['context']['library']['name'])
                        context_library_version = str(data['context']['library']['version'])
                        customer_id = str(data['properties']['customer_id'])
                        event = 'issued_refund'
                        is_full_refund = str(data['properties']['is_full_refund'])
                        is_recallable = str(data['properties']['is_recallable'])
                        order_id = str(data['properties']['order_id'])
                        order_status_id = str(data['properties']['order_status_id'])
                        order_total = str(data['properties']['order_total'])
                        originalTimestamp = str(data['originalTimestamp'])
                        penalty_fees = str(data['properties']['penalty_fees'])
                        refund_amount_items = str(data['properties']['refund_amount_items'])
                        refund_amount_shipping = str(data['properties']['refund_amount_shipping'])
                        refund_amount_tax = str(data['properties']['refund_amount_tax'])
                        refund_amount_total = str(data['properties']['refund_amount_total'])
                        refund_reason_id = str(data['properties']['refund_reason_id'])
                        sentAt = str(data['sentAt'])
                        timestamp = str(data['timestamp'])
                        user_id = str(data['userId'])
                        refund_notes = self.is_null('str', data['properties']['refund_notes']).replace("'", "''")

                        issue_refund_str = issue_refund_str + "('" + m_id + "','" + receivedAt + "','" \
                                              + agent_id + "','" + context_library_name + "','" + context_library_version + "','" \
                                              + customer_id + "','" + event + "','" + event_text + "','" \
                                              + is_full_refund + "','" + is_recallable + "','" + order_id + "','" \
                                              + order_status_id + "'," + order_total + ",'" + originalTimestamp + "'," \
                                              + penalty_fees + "," + refund_amount_items + "," \
                                              + refund_amount_shipping + "," + refund_amount_tax + "," + refund_amount_total + ",'" \
                                              + refund_reason_id + "','" + sentAt + "','" + timestamp + "','" \
                                              + user_id + "','" + refund_notes + "','" + self.object_name + "'),"

                        issue_refund_t_str = issue_refund_t_str + "('" + m_id + "','" + receivedAt + "','','" \
                                                + context_library_name + "','" + context_library_version + "','" \
                                                + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                                + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                                + self.object_name + "'),"

                    elif event_text == 'Set Shipping Address':# and self.is_key_duplicate(event_text, m_id):
                        print str(self.records) + '-Set Shipping Address-' + line
                        receivedAt = str(data['receivedAt'])
                        context_library_name = str(data['context']['library']['name'])
                        context_library_version = str(data['context']['library']['version'])
                        event = 'set_shipping_address'
                        customer_id = str(data['properties']['customer_id'])
                        originalTimestamp = str(data['originalTimestamp'])
                        sentAt = str(data['sentAt'])
                        state = str(data['properties']['state'])
                        store_id = str(data['properties']['store_id'])
                        timestamp = str(data['timestamp'])
                        user_id = self.is_null('int', data['userId'])

                        s_shipping_str = s_shipping_str + "('" + m_id + "','" + receivedAt + "','" \
                                  + context_library_name + "','" + context_library_version + "','" \
                                  + customer_id + "','" + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                  + sentAt + "','" + state + "','" + store_id + "','" \
                                  + timestamp + "','" + user_id + "','" + self.object_name + "'),"

                        s_shipping_t_str = s_shipping_t_str + "('" + m_id + "','" + receivedAt + "','','" \
                                    + context_library_name + "','" + context_library_version + "','" \
                                    + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                    + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                    + self.object_name + "'),"

                    elif event_text == 'Registered':# and self.is_key_duplicate(event_text, m_id):
                        print str(self.records) + '-Registered-' + line
                        receivedAt = str(data['receivedAt'])
                        context_library_name = str(data['context']['library']['name'])
                        context_library_version = str(data['context']['library']['version'])
                        event = 'registered'
                        customer_id = str(data['properties']['customer_id'])
                        dob = str(data['properties']['dob'])
                        originalTimestamp = str(data['originalTimestamp'])
                        sentAt = str(data['sentAt'])
                        store_id = str(data['properties']['store_id'])
                        timestamp = str(data['timestamp'])
                        user_id = self.is_null('int', data['userId'])
                        reg_str = reg_str + "('" + m_id + "','" + receivedAt + "','" \
                                         + context_library_name + "','" + context_library_version + "','" \
                                         + customer_id + "','" + dob + "','" + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                         + sentAt + "','" + store_id + "','" \
                                         + timestamp + "','" + user_id + "','" + self.object_name + "'),"

                        reg_t_str = reg_t_str + "('" + m_id + "','" + receivedAt + "','','" \
                                           + context_library_name + "','" + context_library_version + "','" \
                                           + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                           + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                           + self.object_name + "'),"

                    elif event_text == 'Rerouted Order':# and self.is_key_duplicate(event_text, m_id):
                        print str(self.records) + '-Rerouted Order-' + line
                        #sys.exit()

                    elif event_text in ('Added Address', 'Added Payment Method', 'Set Payment Method', 'Logged In'):
                        receivedAt = str(data['receivedAt'])
                        context_library_name = str(data['context']['library']['name'])
                        context_library_version = str(data['context']['library']['version'])
                        customer_id = str(data['properties']['customer_id'])
                        originalTimestamp = str(data['originalTimestamp'])
                        sentAt = str(data['sentAt'])
                        store_id = str(data['properties']['store_id'])
                        timestamp = str(data['timestamp'])
                        user_id = self.is_null('int', data['userId'])
                        if event_text == 'Added Address':# and self.is_key_duplicate(event_text, m_id):
                            print str(self.records) + '-Added Address-' + line
                            event = 'added_address'
                            added_addr_str = added_addr_str + "('" + m_id + "','" + receivedAt + "','" \
                                             + context_library_name + "','" + context_library_version + "','" \
                                             + customer_id + "','" + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                             + sentAt + "','" + store_id + "','" \
                                             + timestamp + "','" + user_id + "','" + self.object_name + "'),"

                            added_addr_t_str = added_addr_t_str + "('" + m_id + "','" + receivedAt + "','','" \
                                               + context_library_name + "','" + context_library_version + "','" \
                                               + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                               + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                               + self.object_name + "'),"

                        elif event_text == 'Added Payment Method':# and self.is_key_duplicate(event_text, m_id):
                            print str(self.records) + '-Added Payment Method-' + line
                            event = 'added_payment_method'
                            added_pay_str = added_pay_str + "('" + m_id + "','" + receivedAt + "','" \
                                            + context_library_name + "','" + context_library_version + "','" \
                                            + customer_id + "','" + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                            + sentAt + "','" + store_id + "','" \
                                            + timestamp + "','" + user_id + "','" + self.object_name + "'),"

                            added_pay_t_str = added_pay_t_str + "('" + m_id + "','" + receivedAt + "','','" \
                                              + context_library_name + "','" + context_library_version + "','" \
                                              + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                              + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                              + self.object_name + "'),"

                        elif event_text == 'Set Payment Method':# and self.is_key_duplicate(event_text, m_id):
                            print str(self.records) + '-Set Payment Method-' + line
                            event = 'set_payment_method'
                            set_pay_str = set_pay_str + "('" + m_id + "','" + receivedAt + "','" \
                                            + context_library_name + "','" + context_library_version + "','" \
                                            + customer_id + "','" + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                            + sentAt + "','" + store_id + "','" \
                                            + timestamp + "','" + user_id + "','" + self.object_name + "'),"

                            set_pay_t_str = set_pay_t_str + "('" + m_id + "','" + receivedAt + "','','" \
                                              + context_library_name + "','" + context_library_version + "','" \
                                              + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                              + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                              + self.object_name + "'),"

                        elif event_text == 'Logged In':# and self.is_key_duplicate(event_text, m_id):
                            print str(self.records) + '-Logged In-' + line
                            event = 'logged_in'
                            log_in_str = log_in_str + "('" + m_id + "','" + receivedAt + "','" \
                                            + context_library_name + "','" + context_library_version + "','" \
                                            + customer_id + "','" + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                            + sentAt + "','" + store_id + "','" \
                                            + timestamp + "','" + user_id + "','" + self.object_name + "'),"

                            log_in_t_str = log_in_t_str + "('" + m_id + "','" + receivedAt + "','','" \
                                              + context_library_name + "','" + context_library_version + "','" \
                                              + event + "','" + event_text + "','" + originalTimestamp + "','" \
                                              + sentAt + "','" + timestamp + "','" + user_id + "','" \
                                              + self.object_name + "'),"

                    elif data['type'] == 'identify':# and self.is_key_duplicate(event_text, m_id):
                        print str(self.records) + '-Identify sssss-' + line
                        receivedAt = str(data['receivedAt'])
                        anonymousId = str(data['anonymousId'])
                        context_library_name = str(data['context']['library']['name'])
                        context_library_version = str(data['context']['library']['version'])
                        customer_id = str(data['traits']['customer_id'])
                        #email = str(data['traits']['email'])
			print '66666'
                        email = self.is_null('str', data['traits']['email']).replace("'", "''")
			print '77777'
                        #first_name = str(data['traits']['first_name'])
                        first_name = self.is_null('str', data['traits']['first_name']).replace("'", "''")
                        last_name = self.is_null('str', data['traits']['last_name']).replace("'", "''")
                        #last_name = str(data['traits']['last_name'])
                        originalTimestamp = str(data['originalTimestamp'])
                        sentAt = str(data['sentAt'])
                        store_id = str(data['traits']['store_id'])
                        timestamp = str(data['timestamp'])
                        uid = str(data['traits']['uid'])
                        user_id = str(data['userId'])

                        identifies_str = identifies_str + "('" + m_id + "','" + receivedAt + "','" \
                                              + anonymousId + "','" + context_library_name + "','" + context_library_version + "','" \
                                              + customer_id + "','" + email + "','" + first_name + "','" \
                                              + last_name + "','" + originalTimestamp + "','" + sentAt + "','" + store_id + "','" \
                                              + timestamp + "','" + uid + "','" + user_id + "','" + self.object_name + "'),"

                    else:
                        print str(self.records) + '-xxxxxxxxxxxxxxx-' + line
                        #sys.exit()

                elif '{"context":' in line:
                    print str(self.records) + '-context-' + line

                else:
                    print 'others' + line
                    #sys.exit()

            except Exception as e:
                print (e)
                #sys.exit()

        if completed_order_str != '' and completed_order_t_str != '':
            c_o_str = (completed_order_str.rstrip(','), completed_order_t_str.rstrip(','))
            self.insert_data_to_db('Completed Order', *c_o_str)

        if identifies_str != '':
            i_str = (identifies_str.rstrip(','))
            self.insert_data_to_db('Identifies', i_str)

        if added_prod_str != '' and added_prod_t_str != '':
            a_prod_str = (added_prod_str.rstrip(','), added_prod_t_str.rstrip(','))
            self.insert_data_to_db('Added Product', *a_prod_str)

        if added_addr_str != '' and added_addr_t_str != '':
            a_a_str = (added_addr_str.rstrip(','), added_addr_t_str.rstrip(','))
            self.insert_data_to_db('Added Address', *a_a_str)

        if added_pay_str != '' and added_pay_t_str != '':
            a_pay_str = (added_pay_str.rstrip(','), added_pay_t_str.rstrip(','))
            self.insert_data_to_db('Added Payment Method', *a_pay_str)

        if reg_str != '' and reg_t_str != '':
            registered_str = (reg_str.rstrip(','), reg_t_str.rstrip(','))
            self.insert_data_to_db('Registered', *registered_str)

        if s_shipping_str != '' and s_shipping_t_str != '':
            s_s_str = (s_shipping_str.rstrip(','), s_shipping_t_str.rstrip(','))
            self.insert_data_to_db('Set Shipping Address', *s_s_str)

        if log_in_str != '' and log_in_t_str != '':
            l_i_str = (log_in_str.rstrip(','), log_in_t_str.rstrip(','))
            self.insert_data_to_db('Logged In', *l_i_str)

        if removed_prod_str != '' and removed_prod_t_str != '':
            r_p_str = (removed_prod_str.rstrip(','), removed_prod_t_str.rstrip(','))
            self.insert_data_to_db('Removed Product', *r_p_str)

        if issue_refund_str != '' and issue_refund_t_str != '':
            i_r_str = (issue_refund_str.rstrip(','), issue_refund_t_str.rstrip(','))
            self.insert_data_to_db('Issued Refund', *i_r_str)

        if logged_out_str != '' and logged_out_t_str != '':
            l_o_str = (logged_out_str.rstrip(','), logged_out_t_str.rstrip(','))
            self.insert_data_to_db('Logged Out', *l_o_str)

        if reshipped_order_str != '' and reshipped_order_t_str != '':
            r_o_str = (reshipped_order_str.rstrip(','), reshipped_order_t_str.rstrip(','))
            self.insert_data_to_db('Reshipped Order', *r_o_str)

        if chg_ship_addr_str != '' and chg_ship_addr_t_str != '':
            c_s_str = (chg_ship_addr_str.rstrip(','), chg_ship_addr_t_str.rstrip(','))
            self.insert_data_to_db('Changed Shipping Address', *c_s_str)

        if failed_pay_att_str != '' and failed_pay_att_t_str != '':
            f_p_a_str = (failed_pay_att_str.rstrip(','), failed_pay_att_t_str.rstrip(','))
            self.insert_data_to_db('Failed payment attempt', *f_p_a_str)

        if replaced_items_str != '' and replaced_items_t_str != '':
            r_i_str = (replaced_items_str.rstrip(','), replaced_items_t_str.rstrip(','))
            self.insert_data_to_db('Replaced Items', *r_i_str)

        if verified_c_age_str != '' and verified_c_age_t_str != '':
            v_c_str = (verified_c_age_str.rstrip(','), verified_c_age_t_str.rstrip(','))
            self.insert_data_to_db('Verified Customer Age', *v_c_str)

        if cancel_reship_str != '' and cancel_reship_t_str != '':
            c_r_str = (cancel_reship_str.rstrip(','), cancel_reship_t_str.rstrip(','))
            self.insert_data_to_db('Cancel Reship', *c_r_str)

        if viewed_product_str != '' and viewed_product_t_str != '':
            v_p_str = (viewed_product_str.rstrip(','), viewed_product_t_str.rstrip(','))
            self.insert_data_to_db('Viewed Product', *v_p_str)

        if viewed_pack_str != '' and viewed_pack_str != '':
            v_pack_str = (viewed_pack_str.rstrip(','), viewed_pack_t_str.rstrip(','))
            self.insert_data_to_db('Viewed Pack', *v_pack_str)

        if page_str != '':
            p_str = (page_str.rstrip(','))
            self.insert_data_to_db('page', p_str)

        if m_identifies_str != '':
            m_i_str = (m_identifies_str.rstrip(','))
            self.insert_data_to_db('metadata_identifies', m_i_str)

        if select_offer_str != '' and select_offer_t_str != '':
            s_offer_str = (select_offer_str.rstrip(','), select_offer_t_str.rstrip(','))
            self.insert_data_to_db('Selected Offer', *s_offer_str)

        if loaded_overlay_str != '' and loaded_overlay_t_str != '':
            lo_o_str = (loaded_overlay_str.rstrip(','), loaded_overlay_t_str.rstrip(','))
            self.insert_data_to_db('Loaded Overlay', *lo_o_str)

        if clicked_subnav_str != '' and clicked_subnav_t_str != '':
            cli_sub_str = (clicked_subnav_str.rstrip(','), clicked_subnav_t_str.rstrip(','))
            self.insert_data_to_db('Clicked Subnav', *cli_sub_str)

        if watched_video_str != '' and watched_video_t_str != '':
            wat_vid_str = (watched_video_str.rstrip(','), watched_video_t_str.rstrip(','))
            self.insert_data_to_db('Watched Video', *wat_vid_str)

        if selected_gift_type_str != '' and selected_gift_type_t_str != '':
            s_gift_type_str = (selected_gift_type_str.rstrip(','), selected_gift_type_t_str.rstrip(','))
            self.insert_data_to_db('Selected Gift Type', *s_gift_type_str)

        if started_video_str != '' and started_video_t_str != '':
            str_vid_str = (started_video_str.rstrip(','), started_video_t_str.rstrip(','))
            self.insert_data_to_db('Started Video', *str_vid_str)

        if paused_video_str != '' and paused_video_t_str != '':
            pau_vid_str = (paused_video_str.rstrip(','), paused_video_t_str.rstrip(','))
            self.insert_data_to_db('Paused Video', *pau_vid_str)
        
	self.mark_processed()
				
    def insert_data_to_db(self, farg, *args):
        schema_t_str = self.schema_name + '.tracks (id,received_at, anonymous_id, context_library_name, context_library_version, ' \
                       'event, event_text, original_timestamp, sent_at, timestamp, user_id, src_file_name)'

        schema_t_m_str = self.schema_name + '.tracks (id, received_at, anonymous_id, context_ip, context_library_name, ' \
                         'context_library_version, context_page_path, context_page_referrer, context_page_title, context_page_url, ' \
                         'context_user_agent, event, event_text, original_timestamp, sent_at, timestamp, ' \
                         'user_id, src_file_name)'

        if farg == 'Completed Order':
            schema_e_str = self.schema_name + '.completed_order (id,received_at, context_library_name, context_library_version, ' \
                           'customer_id, discount,event, event_text, ip_address, order_id, original_timestamp, ' \
                           'passed_compliance, products, sent_at, shipping, store_id, subtotal, tax, timestamp, total, ' \
                           'user_id, vouchers, campaign_id, partner_id, promotion_id, src_file_name, sub_id)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_str

                self.db_commit(schema_str, arg)

        elif farg == 'page':
            schema_p_str = self.schema_name + '.pages (id,received_at, anonymous_id, context_ip, context_library_name, ' \
                         'context_library_version, context_page_path, context_page_referrer, context_page_search, ' \
                         'context_page_title, context_page_url, context_user_agent, original_timestamp, path, ' \
                         'referrer, search, sent_at, timestamp, title, url, user_id, context_campaign_name, ' \
                         'context_campaign_source, context_campaign_medium, src_file_name)'

            for arg in args:
                schema_str = schema_p_str
                self.db_commit(schema_str, arg)

        elif farg == 'Selected Offer': # metadata
            schema_e_str = self.schema_name + '.selected_offer (id,received_at, anonymous_id, context_ip, context_library_name, ' \
                           'context_library_version, context_page_path, context_page_title, context_page_url, ' \
                           'context_user_agent, event, event_text, _id, original_timestamp, price, sent_at, ' \
                           'store_id, timestamp, action, context_campaign_medium, context_campaign_name, ' \
                           'context_campaign_source, context_page_referrer, context_page_search, customer_id, ' \
                           'shipping, user_id, src_file_name)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_m_str

                self.db_commit(schema_str, arg)

        elif farg == 'Selected Gift Type':  # metadata
            schema_e_str = self.schema_name + '.selected_gift_type (id, received_at, action, anonymous_id, context_ip, context_library_name, ' \
                                         'context_library_version, context_page_path, context_page_referrer, context_page_title, ' \
                                         'context_page_url, context_user_agent, event, event_text, gift_label,_id, original_timestamp, ' \
                                         'sent_at, store_id, timestamp, context_page_search, customer_id, ' \
                                         'user_id, context_campaign_medium, context_campaign_name, context_campaign_source, ' \
                                         'src_file_name)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_m_str

                self.db_commit(schema_str, arg)


        elif farg in ('Watched Video', 'Started Video', 'Paused Video'):  # metadata
            if farg == 'Watched Video':
                table_str = self.schema_name + '.watched_video'
            elif farg == 'Started Video':
                table_str = self.schema_name + '.started_video'
            elif farg == 'Paused Video':
                table_str = self.schema_name + '.paused_video'

            schema_e_str = table_str + ' (id, received_at, action, anonymous_id, context_ip, context_library_name, ' \
                                         'context_library_version, context_page_path, context_page_title, ' \
                                         'context_page_url, context_user_agent, duration, event, event_text, _id, original_timestamp, ' \
                                         '_percent, seconds, sent_at, ' \
                                         'store_id, timestamp, context_page_referrer, context_campaign_medium, ' \
                                         'context_campaign_name, context_campaign_source, context_page_search, ' \
                                         'customer_id, user_id, context_campaign_term, ' \
                                         'src_file_name)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_m_str

                self.db_commit(schema_str, arg)

        elif farg == 'Clicked Subnav':  # metadata
            schema_e_str = self.schema_name + '.clicked_subnav (id, received_at, action, anonymous_id, context_ip, context_library_name, ' \
                                         'context_library_version, context_page_path, context_page_referrer, context_page_title, ' \
                                         'context_page_url, context_user_agent, event, event_text, label, original_timestamp, sent_at, ' \
                                         'store_id, timestamp, context_page_search, user_id, customer_id, context_campaign_medium, ' \
                                         'context_campaign_name, context_campaign_source, context_campaign_term, ' \
                                         'src_file_name)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_m_str

                self.db_commit(schema_str, arg)

        elif farg == 'Loaded Overlay':  # metadata
            schema_e_str = self.schema_name + '.loaded_overlay (id, received_at, action, anonymous_id, context_ip, context_library_name, ' \
                                         'context_library_version, context_page_path, context_page_referrer, context_page_title, ' \
                                         'context_page_url, context_user_agent, event, event_text, inventory_unit_id, original_timestamp, sent_at, ' \
                                         'store_id, timestamp, context_page_search, customer_id, ' \
                                         'user_id, src_file_name)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_m_str

                self.db_commit(schema_str, arg)

        elif farg == 'Viewed Product': # metadata
            schema_e_str = self.schema_name + '.viewed_product (id,received_at, anonymous_id, context_ip, context_library_name, ' \
                           'context_library_version, context_page_path, context_page_referrer, context_page_title, context_page_url, ' \
                           'context_user_agent, event, event_text, _id, name, original_timestamp, price, ' \
                           'sent_at, timestamp, user_id, src_file_name)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_m_str

                self.db_commit(schema_str, arg)

        elif farg == 'Viewed Pack': # metadata
            schema_e_str = self.schema_name + '.viewed_pack (id,received_at, anonymous_id, context_ip, context_library_name, ' \
                           'context_library_version, context_page_path, context_page_referrer, context_page_title, context_page_url, ' \
                           'context_user_agent, event, event_text, _id, name, original_timestamp, ' \
                           'sent_at, store_id,timestamp, user_id, src_file_name)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_m_str

                self.db_commit(schema_str, arg)


        elif farg == 'Verified Customer Age':
            schema_e_str = self.schema_name + '.verified_customer_age (id,received_at, customer_id, event, context_library_version, ' \
                           'event_text, user_id, original_timestamp, agent_id, order_id, ' \
                           'order_status_id, sent_at, verification_method, context_library_name, timestamp, ' \
                           'verification_notes, src_file_name)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_str

                self.db_commit(schema_str, arg)


        elif farg == 'Replaced Items':
            schema_e_str = self.schema_name + '.replaced_items (id,received_at, agent_id, customer_id, event, ' \
                           'items_replaced, order_id, original_timestamp, override_default_action_id, replacement_notes, ' \
                           'sent_at, timestamp, user_id, context_library_name, context_library_version, event_text, ' \
                           'order_status_id, order_total, src_file_name)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_str

                self.db_commit(schema_str, arg)

        elif farg == 'Cancel Reship':
            schema_e_str = self.schema_name + '.cancel_reship (id,received_at, agent_id, cancel_reship_notes, context_library_name, ' \
                           'context_library_version, customer_id, event, event_text, order_id, order_status_id, ' \
                           'original_timestamp, reship_refund_amount, sent_at, timestamp, user_id, src_file_name)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_str

                self.db_commit(schema_str, arg)

        elif farg == 'Changed Shipping Address':
            schema_e_str = self.schema_name + '.changed_shipping_address (id,received_at, event_text, user_id, override_default_action_id, ' \
                           'order_status_id, original_address_id, agent_id, change_notes, context_library_name, ' \
                           'event, order_id, original_timestamp, sent_at, timestamp, customer_id, tax_difference, ' \
                           'ignore_tax_charges, new_address_id, context_library_version, order_total, src_file_name)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_str

                self.db_commit(schema_str, arg)

        elif farg == 'Reshipped Order':
            schema_e_str = self.schema_name + '.reshipped_order (id,received_at, sent_at, event_text, shipping_address_id, ' \
                           'reship_charge_payment_method_id, agent_id, context_library_name, context_library_version, ' \
                           'customer_id, event, items_reshipped, order_id, order_status_id, original_order_id, original_timestamp, ' \
                           'reship_reason_id, timestamp, user_id, reship_charge, reship_notes, src_file_name)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_str

                self.db_commit(schema_str, arg)

        elif farg == 'Issued Refund':
            schema_e_str = self.schema_name + '.issued_refund (id,received_at, agent_id, context_library_name, context_library_version, ' \
                         'customer_id,event, event_text, is_full_refund, is_recallable, order_id, order_status_id, ' \
                         'order_total, original_timestamp, penalty_fees, refund_amount_items, refund_amount_shipping, ' \
                         'refund_amount_tax, refund_amount_total, refund_reason_id, sent_at, ' \
                         'timestamp, user_id, refund_notes, src_file_name)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_str

                self.db_commit(schema_str, arg)

        elif farg == 'Failed payment attempt':
            schema_e_str = self.schema_name + '.failed_payment_attempt (id,received_at, event, user_id, context_library_name, ' \
                           'context_library_version, event_text, original_timestamp, timestamp, amount, ' \
                           'customer_id, sent_at, store_id, src_file_name)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_str

                self.db_commit(schema_str, arg)

        elif farg == 'Set Shipping Address':
            schema_e_str = self.schema_name + '.set_shipping_address (id,received_at, context_library_name, context_library_version, ' \
                            'customer_id, event, event_text, original_timestamp, ' \
                            'sent_at, state, store_id, timestamp, user_id, src_file_name)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_str

                self.db_commit(schema_str, arg)

        elif farg == 'Registered':
            schema_e_str = self.schema_name + '.registered (id,received_at, context_library_name, context_library_version, ' \
                            'customer_id, dob, event, event_text, original_timestamp, ' \
                            'sent_at, store_id, timestamp, user_id, src_file_name)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_str

                self.db_commit(schema_str, arg)

        elif farg == 'Logged Out':
            schema_e_str = self.schema_name + '.logged_out (id,received_at, context_library_name, context_library_version, ' \
                        'customer_id, event, event_text, original_timestamp, ' \
                        'sent_at, timestamp, user_id, src_file_name)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_str

                self.db_commit(schema_str, arg)

        elif farg in ('Added Address', 'Added Payment Method', 'Set Payment Method', 'Logged In'):
            if farg == 'Added Address':
                table_str = self.schema_name + '.added_address'
            elif farg == 'Added Payment Method':
                table_str = self.schema_name + '.added_payment_method'
            elif farg == 'Set Payment Method':
                table_str = self.schema_name + '.set_payment_method'
            elif farg == 'Logged In':
                table_str = self.schema_name + '.logged_in'

            schema_e_str = table_str + ' (id,received_at, context_library_name, context_library_version, ' \
                        'customer_id, event, event_text, original_timestamp, ' \
                        'sent_at, store_id, timestamp, user_id, src_file_name)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_str

                self.db_commit(schema_str, arg)

        elif farg == 'metadata_identifies':
            schema_o_str = self.schema_name + '.identifies (id,received_at, anonymous_id, context_library_name, ' \
                            'context_library_version, customer_id, email, first_name, last_name, original_timestamp, ' \
                            'sent_at, store_id, timestamp, uid, user_id, context_campaign_medium, ' \
                            'context_campaign_name, context_campaign_source, context_ip, context_page_path, ' \
                            'context_page_referrer, context_page_search, context_page_title, context_page_url, ' \
                            'context_user_agent, _uuid, context_campaign_content, src_file_name)'

            for arg in args:
                schema_str = schema_o_str
                self.db_commit(schema_str, arg)

        elif farg == 'Identifies':
            schema_o_str = self.schema_name + '.identifies (id,received_at, anonymous_id, context_library_name, ' \
                         'context_library_version, customer_id, email, first_name, last_name, original_timestamp, ' \
                         'sent_at, store_id, timestamp, uid, user_id, src_file_name)'
            for arg in args:
                schema_str = schema_o_str
                self.db_commit(schema_str, arg)

        elif farg == 'Removed Product':
            schema_e_str = self.schema_name + '.removed_product (id,received_at, anonymous_id, context_library_name, context_library_version, ' \
                           'event, event_text, _id, name, original_timestamp, sent_at, sku, ' \
                           'store_id, timestamp, type, user_id, src_file_name)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_str

                self.db_commit(schema_str, arg)

        elif farg == 'Added Product':
            schema_e_str = self.schema_name + '.added_product (id,received_at, anonymous_id, context_library_name, context_library_version, ' \
                         'event, event_text, _id, name, original_timestamp, ' \
                         'price, quantity, sent_at, sku, store_id, timestamp, type, ' \
                         'user_id, src_file_name)'
            i = 0
            for arg in args:
                i += 1
                if i == 1:
                    schema_str = schema_e_str
                else:
                    schema_str = schema_t_str

                self.db_commit(schema_str, arg)

    def db_commit(self, schema_str, sql_str):
        sql = "insert into " + schema_str + " values " + sql_str
        print sql
        producer.send('ho_insert_data_partition', bytes(sql))
        producer.flush()
				
    def mark_processed(self):
	sql = "insert into " + self.schema_name + ".loading_log (file_name, records) values " \
              "('" + self.object_name + "'," + str(self.records) + ")"
        print sql
        producer.send('ho_insert_data_partition', bytes(sql))
        producer.flush()

    def is_null(self, data_type, file_obj):
        if file_obj is None and data_type == 'int':
            int_file_obj = 'null'
        elif file_obj is None and data_type == 'str':
            int_file_obj = ''
        else:
            int_file_obj = str(file_obj)

        return int_file_obj


def handler(message):
    records = message.collect()
    for record in records:
        temp_str = ''
        temp_str = str(record[1]).split('***')
	hdfs_obj_name  = temp_str[1]
        store_name  = temp_str[0]
	print 'hdfs_obj_name - ' + hdfs_obj_name 
        print 'store_name - ' + store_name

        if 'E3keQDgoSf' in store_name:
          schema = 'ho_production'
        elif 'AfiTgWFG8T' in store_name:
          schema = 'atocw_production'
        elif 'mlsvV8kIgh' in store_name:
          schema = 'chw_production'
        elif 'Q5wV0yEcCZ' in store_name:
          schema = 'adw_production'
        elif 'c2wbnJfcqG' in store_name:
          schema = 'bw_production'

        EventFile(hdfs_obj_name, schema)

def main():
    sc = SparkContext(appName="PythonStreamingDirectKafka")
    ssc = StreamingContext(sc, 10)
    
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    kvs.foreachRDD(handler)

    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
  producer = KafkaProducer(bootstrap_servers='192.168.0.206:9092')
  main()