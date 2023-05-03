import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg.pg_connect import PgConnect
from pydantic import BaseModel

    
class H_category(BaseModel):
    h_category_pk: uuid.UUID
    category_name: str
    load_dt: datetime
    load_src: str


class H_Order(BaseModel):
    h_order_pk: uuid.UUID
    order_id: str
    load_dt: datetime
    load_src: str


class H_Product(BaseModel):
    h_product_pk: uuid.UUID
    product_id: str
    load_dt: datetime
    load_src: str 

    
class H_Restaurant(BaseModel):
    h_restaurant_pk: uuid.UUID
    restaurant_id: str
    load_dt: datetime
    load_src: str
    
    
class H_User(BaseModel):
    h_user_pk: uuid.UUID
    user_id: str
    load_dt: datetime
    load_src: str


class L_order_product(BaseModel):
    hk_order_product_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_product_pk: uuid.UUID
    load_dt: datetime
    load_src: str
    

class L_order_user(BaseModel):
    hk_order_user_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_user_pk: uuid.UUID
    load_dt: datetime
    load_src: str
    
    
class L_product_category(BaseModel):
    hk_product_category_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_category_pk: uuid.UUID
    load_dt: datetime
    load_src: str
    

class L_product_restaurant(BaseModel):
    hk_product_restaurant_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_restaurant_pk: uuid.UUID
    load_dt: datetime
    load_src: str
    
    
class S_order_cost(BaseModel):
    h_order_pk: uuid.UUID
    cost: float
    payment: float
    load_dt: datetime
    load_src: str
    hk_order_cost_pk: uuid.UUID
    

class S_order_status(BaseModel):
    h_order_pk: uuid.UUID
    status: str
    load_dt: datetime
    load_src: str
    hk_order_status_pk: uuid.UUID
    
    
class S_product_names(BaseModel):
    h_product_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str
    hk_product_names_pk: uuid.UUID
    
    
class S_restaurant_names(BaseModel):
    h_restaurant_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str
    hk_restaurant_names_pk: uuid.UUID
    
    
class S_user_names(BaseModel):
    h_user_pk: uuid.UUID
    username: str
    userlogin: str
    load_dt: datetime
    load_src: str
    hk_user_names_pk: uuid.UUID
    
    
class OrderDdsBuilder:
    def __init__(self, dict: Dict) -> None:
        self._dict = dict
        self.source_system = ""
        self.order_ns_uuid = uuid.UUID('')

    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.order_ns_uuid, name=str(obj))

    def h_user(self) -> H_User:
        user_id = self._dict['user']['id']
        return H_User(
            h_user_pk=self._uuid(user_id),
            user_id=user_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def h_product(self) -> List[H_Product]: 
        products = []

        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            products.append(
                H_Product(
                    h_product_pk=self._uuid(prod_id),
                    product_id=prod_id,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )

        return products 
    
    def h_category(self) -> List[H_category]:
        categories = []

        for cat_dict in self._dict['products']:
            cat_name = cat_dict['category']
            categories.append(
                H_category(
                    h_category_pk=self._uuid(cat_name),
                    cat_name=cat_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )

        return categories
    
    def h_order(self) -> H_Order:
        order_id = self._dict['payload']['id']
        return H_Order(
            h_order_pk=self._uuid(order_id),
            order_id=order_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def h_restaurante(self) -> H_Restaurant:
        restaurant_id = self._dict['payload']['restaurant']['id']
        return H_Restaurant(
            h_restaurant_pk=self._uuid(restaurant_id),
            restaurant_id=restaurant_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
        
    def l_order_product(self) -> List[L_order_product]:
        order_products = []

        order_id = self._dict['payload']['id']
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            order_products.append(
                L_order_product(
                    hk_order_product_pk=self._uuid(f"{order_id}_{prod_id}"),
                    h_order_pk=self._uuid(order_id),
                    h_product_pk=self._uuid(prod_id),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )

        return order_products
    
    def l_order_user(self) -> L_order_user:
        order_id = self._dict['payload']['id']
        user_id = self._dict['user']['id']
        return L_order_user(
            hk_order_user_pk=self._uuid(f"{order_id}_{user_id}"),
            h_order_pk=self._uuid(order_id),
            h_user_pk=self._uuid(user_id),
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def l_product_category(self) -> List[L_product_category]:
        product_categories = []

        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            cat_name = prod_dict['category']
            product_categories.append(
                L_product_category(
                    hk_product_category_pk=self._uuid(f"{prod_id}_{cat_name}"),
                    h_product_pk=self._uuid(prod_id),
                    h_category_pk=self._uuid(cat_name),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
            
        return product_categories
    
    def l_product_restaurant(self) -> List[L_product_restaurant]:
        l_product_restaurants = []
        order_id = self._dict['payload']['id']
        
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            restaurant_id = self._dict['payload']['restaurant']['id']
            l_product_restaurants.append(
                L_product_restaurant(
                    l_order_id=order_id,
                    l_product_id=prod_id,
                    l_restaurant_id=restaurant_id,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )

        return l_product_restaurants
    
    
    def s_order_cost(self) -> S_order_cost:
        order_id = self._dict['payload']['id']
        h_order_pk=self._uuid(order_id)
        cost=self._dict['payload']['cost']
        return S_order_cost(
            h_order_pk=h_order_pk,
            cost=cost,
            payment=self._dict['payload']['payment'],
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_cost_pk=self._uuid(f"{h_order_pk}_{cost}")
        )
    
    
    def s_order_status(self) -> S_order_status:
        order_id = self._dict['payload']['id']
        h_order_pk=self._uuid(order_id)
        status = self._dict['payload']['status']
        return S_order_status(
            h_order_pk=h_order_pk,
            status=status,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_status_pk=self._uuid(f"({h_order_pk}_{status})")
        )
    
    def s_product_names(self) -> List[S_product_names]:
        products = []

        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            name = prod_dict['name']
            h_product_pk=self._uuid(prod_id)
            products.append(
                S_product_names(
                    h_product_pk=h_product_pk,
                    name=name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system,
                    hk_product_names_pk=self._uuid(f"{h_product_pk}_{name})")
                    )
            )
        return products 
    
    def s_restaurant_names(self) -> S_restaurant_names:
        restaurant_id = self._dict['payload']['restaurant']['id']
        h_restaurant_pk = self._uuid(restaurant_id)
        name = self._dict['payload']['restaurant']['name']
        return S_order_status(
            h_restaurant_pk=h_restaurant_pk,
            name=name,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_restaurant_names_pk=self._uuid(f"({h_restaurant_pk}_{name})")
        )
        
    def s_user_names(self) -> S_user_names:
        user_id = self._dict['user']['id']
        h_user_pk = self._uuid(user_id)
        username = self._dict['user']['name']
        userlogin = self._dict['user']['login']
        return S_order_status(
            h_user_pk=h_user_pk,
            username=username,
            userlogin=userlogin,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_restaurant_names_pk=self._uuid(f"({h_user_pk}_{username}_{userlogin})")
        )
    
    
class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_user_insert(self, user: H_User) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_user(
                            h_user_pk,
                            user_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_user_pk)s,
                            %(user_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_user_pk) DO NOTHING;
                    """,
                    {
                        'h_user_pk': user.h_user_pk,
                        'user_id': user.user_id,
                        'load_dt': user.load_dt,
                        'load_src': user.load_src
                    }
                )

    def h_product_insert(self, obj: H_Product) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_product(
                            h_product_pk,
                            product_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_product_pk)s,
                            %(product_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_product_pk) DO NOTHING;
                    """,
                    {
                        'h_product_pk': obj.h_product_pk,
                        'product_id': obj.product_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                ) 
    
    def h_category_insert(self, obj: H_category) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_category(
                            h_category_pk,
                            category_name,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_category_pk)s,
                            %(category_name)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_category_pk) DO NOTHING;
                    """,
                    {
                        "h_category_pk": obj.h_category_pk,
                        "category_name": obj.category_name,
                        "load_dt": obj.load_dt,
                        "load_src": obj.load_src
                    }
                )
                
    def h_order_insert(self, obj: H_Order) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_order(
                            h_order_pk,
                            order_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(order_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_order_pk) DO NOTHING;
                    """,
                    {
                        "h_order_pk": obj.h_order_pk,
                        "order_id": obj.order_id,
                        "load_dt": obj.load_dt,
                        "load_src": obj.load_src
                    }
                )
                
    def h_restaurant_insert(self, obj: H_Restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant(
                            h_restaurant_pk,
                            restaurant_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(restaurant_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_restaurant_pk) DO NOTHING;
                    """,
                    {
                        "h_restaurant_pk": obj.h_restaurant_pk,
                        "restaurant_id": obj.restaurant_id,
                        "load_dt": obj.load_dt,
                        "load_src": obj.load_src
                    }
                )
                
    def l_order_product_insert(self, obj: L_order_product) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_product(
                            hk_order_product_pk,
                            h_order_pk,
                            h_product_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_order_product_pk)s,
                            %(h_order_pk)s,
                            %(h_product_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_product_pk) DO NOTHING;
                    """,
                    {
                        "hk_order_product_pk": obj.hk_order_product_pk,
                        "h_order_pk": obj.h_order_pk,
                        "h_product_pk": obj.h_product_pk,
                        "load_dt": obj.load_dt,
                        "load_src": obj.load_src
                    }
                )
                
    def l_order_user_insert(self, obj: L_order_user) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_user(
                            hk_order_user_pk,
                            h_order_pk,
                            h_user_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_order_user_pk)s,
                            %(h_order_pk)s,
                            %(h_user_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_user_pk) DO NOTHING;
                    """,
                    {
                        "hk_order_user_pk": obj.hk_order_user_pk,
                        "h_order_pk": obj.h_order_pk,
                        "h_user_pk": obj.h_user_pk,
                        "load_dt": obj.load_dt,
                        "load_src": obj.load_src
                    }
                )
                
    def l_product_category_insert(self, obj: L_product_category) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_category(
                            hk_product_category_pk,
                            h_product_pk,
                            h_category_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_product_category_pk)s,
                            %(h_product_pk)s,
                            %(h_category_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_product_category_pk) DO NOTHING;
                    """,
                    {
                        "hk_product_category_pk": obj.hk_product_category_pk,
                        "h_product_pk": obj.h_product_pk,
                        "h_category_pk": obj.h_category_pk,
                        "load_dt": obj.load_dt,
                        "load_src": obj.load_src
                    }
                )
                
    def l_product_restaurant_insert(self, obj: L_product_restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_restaurant(
                            hk_product_restaurant_pk,
                            h_product_pk,
                            h_restaurant_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_product_restaurant_pk)s,
                            %(h_product_pk)s,
                            %(h_restaurant_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;
                    """,
                    {
                        "hk_product_restaurant_pk": obj.hk_product_restaurant_pk,
                        "h_product_pk": obj.h_product_pk,
                        "h_restaurant_pk": obj.h_restaurant_pk,
                        "load_dt": obj.load_dt,
                        "load_src": obj.load_src
                    }
                )
           
    def s_order_cost_insert(self, obj: S_order_cost) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_cost(
                            h_order_pk,
                            cost,
                            payment,
                            load_dt,
                            load_src,
                            hk_order_cost_pk
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(cost)s,
                            %(payment)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_order_cost_pk)s
                        )
                        ON CONFLICT (hk_order_cost_pk) DO NOTHING;
                    """,
                    {
                        "h_order_pk": obj.h_order_pk,
                        "cost": obj.cost,
                        "payment": obj.payment,
                        "load_dt": obj.load_dt,
                        "load_src": obj.load_src,
                        "hk_order_cost_pk": obj.hk_order_cost_pk
                    }
                )
                
    def s_order_status_insert(self, obj: S_order_status) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_status(
                            h_order_pk,
                            status,
                            load_dt,
                            load_src,
                            hk_order_status_pk
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(status)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_order_status_pk)s
                        )
                        ON CONFLICT (hk_order_status_pk) DO NOTHING;
                    """,
                    {
                        "h_order_pk": obj.h_order_pk,
                        "status": obj.status,
                        "load_dt": obj.load_dt,
                        "load_src": obj.load_src,
                        "hk_order_status_pk": obj.hk_order_status_pk
                    }
                )
                
    def s_product_names_insert(self, obj: S_product_names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_product_names(
                            h_product_pk,
                            name,
                            load_dt,
                            load_src,
                            hk_product_names_pk
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(status)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_product_names_pk)s
                        )
                        ON CONFLICT (hk_product_names_pk) DO NOTHING;
                    """,
                    {
                        "h_product_pk": obj.h_product_pk,
                        "name": obj.name,
                        "load_dt": obj.load_dt,
                        "load_src": obj.load_src,
                        "hk_product_names_pk": obj.hk_product_names_pk
                    }
                )
    
    def s_restaurant_names_insert(self, obj: S_restaurant_names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_restaurant_names(
                            h_restaurant_pk,
                            name,
                            load_dt,
                            load_src,
                            hk_restaurant_names_pk
                        )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(status)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_restaurant_names_pk)s
                        )
                        ON CONFLICT (hk_restaurant_names_pk) DO NOTHING;
                    """,
                    {
                        "h_restaurant_pk": obj.h_restaurant_pk,
                        "name": obj.name,
                        "load_dt": obj.load_dt,
                        "load_src": obj.load_src,
                        "hk_restaurant_names_pk": obj.hk_restaurant_names_pk
                    }
                )
                
    def s_user_names_insert(self, obj: S_user_names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_user_names(
                            h_user_pk,
                            username,
                            userlogin,
                            load_dt,
                            load_src,
                            hk_user_names_pk
                        )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(username)s,
                            %(userlogin)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_user_names_pk)s
                        )
                        ON CONFLICT (hk_user_names_pk) DO NOTHING;
                    """,
                    {
                        "h_user_pk": obj.h_user_pk,
                        "username": obj.username,
                        "userlogin": obj.userlogin,
                        "load_dt": obj.load_dt,
                        "load_src": obj.load_src,
                        "hk_user_names_pk": obj.hk_user_names_pk
                    }
                )