from datetime import datetime
from logging import Logger

from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository

import uuid

class DdsMessageProcessor:
    def __init__(self,
                 kafka_consumer: KafkaConsumer,
                 kafka_producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 logger: Logger) -> None:

        self._kafka_consumer = kafka_consumer
        self._kafka_producer = kafka_producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        processed_messages = 0;
        timeout: float = 3.0
        while processed_messages < self._batch_size:
                
            dct_msg = self._kafka_consumer.consume(timeout=timeout)


            if 'object_type' not in dct_msg:
                self._logger.info(f"no object type in: {dct_msg}")
                continue

            if dct_msg['object_type'] != 'order':
                continue


            load_dt = datetime.utcnow()
            load_src = 'orders_backend'


            order_id = dct_msg['payload']['id']
            h_order_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(order_id))
            order_dt = dct_msg['payload']['date']
            order_cost = dct_msg['payload']['cost']
            order_payment = dct_msg['payload']['payment']
            order_status = dct_msg['payload']['status']
            self._dds_repository.order_upsert(
                h_order_pk, 
                order_id, 
                order_dt,
                order_cost, 
                order_payment, 
                order_status,
                load_dt, 
                load_src
            )

            user_id = dct_msg['payload']['user']['id']
            h_user_pk = uuid.uuid3(uuid.NAMESPACE_X500, user_id)
            username = dct_msg['payload']['user']['name']
            userlogin = username
            if 'login' in dct_msg['payload']['user']:
                userlogin = dct_msg['payload']['user']['login']
            self._dds_repository.user_upsert(
                h_user_pk, 
                user_id, 
                username, 
                userlogin,
                load_dt, 
                load_src
            )

            restaurant_id = dct_msg['payload']['restaurant']['id']
            h_restaurant_pk = uuid.uuid3(uuid.NAMESPACE_X500, restaurant_id)
            restaurant_name = dct_msg['payload']['restaurant']['name']
            self._dds_repository.restaurant_upsert(
                h_restaurant_pk, 
                restaurant_id, 
                restaurant_name,
                load_dt, 
                load_src
            )


            dct_products = {}
            dct_categories = {}
            for product in dct_msg['payload']['products']:
                category = product['category']
                dct_categories[category] = category
                product_id = product['_id']
                product_name = product['name']
                dct_products[product_id] = product_name
            

            for next_category in dct_categories:
                h_category_pk = uuid.uuid3(uuid.NAMESPACE_X500, next_category)
                self._dds_repository.category_upsert(
                    h_category_pk, 
                    next_category, 
                    load_dt, 
                    load_src)


            hk_order_user_pk = uuid.uuid3(
                uuid.NAMESPACE_X500,
                str(h_order_pk) + '/' + str(h_user_pk)
            )
            self._dds_repository.l_order_user_upsert(
                hk_order_user_pk,
                h_order_pk, h_user_pk,
                load_dt, load_src
            )


            for product in dct_msg['payload']['products']:

                next_product_id = product['_id']
                next_product_name = product['name']
                h_product_pk = uuid.uuid3(uuid.NAMESPACE_X500, next_product_id)
                self._dds_repository.product_upsert(
                    h_product_pk, 
                    next_product_id, 
                    next_product_name,
                    load_dt, 
                    load_src
                )


                next_product_category = product['category']
                next_h_category_pk = uuid.uuid3(uuid.NAMESPACE_X500, next_product_category)
                hk_product_category_pk = uuid.uuid3(
                    uuid.NAMESPACE_X500,
                    str(h_product_pk) + '/' + str(next_h_category_pk)
                )
                
                self._dds_repository.l_product_category_upsert(
                    hk_product_category_pk,
                    h_product_pk, 
                    next_h_category_pk,
                    load_dt, 
                    load_src
                )
                
                hk_product_restaurant_pk = uuid.uuid3(
                    uuid.NAMESPACE_X500,
                    str(h_product_pk) + '/' + str(h_restaurant_pk)
                )
                self._dds_repository.l_product_restaurant_upsert(
                    hk_product_restaurant_pk,
                    h_product_pk, 
                    h_restaurant_pk,
                    load_dt, 
                    load_src
                )

                hk_order_product_pk = uuid.uuid3(
                    uuid.NAMESPACE_X500,
                    str(h_order_pk) + '/' + str(h_product_pk)
                )
                self._dds_repository.l_order_product_upsert(
                    hk_order_product_pk,
                    h_order_pk, 
                    h_product_pk,
                    load_dt, 
                    load_src
                )

            msg = 'message sent: {id}'.format(id=dct_msg['payload']['id'])
            self._kafka_producer.produce(msg)
            
            processed_messages += 1

            self._logger.info(f"{datetime.utcnow()}: FINISH")