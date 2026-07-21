from datetime import datetime
from uuid import UUID

from lib.pg import PgConnect


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def insert_hub_order(
        self,
        order_id: str,
        order_dt: datetime,
        load_src: str,
    ) -> UUID:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH inserted AS (
                        INSERT INTO dds.h_order (
                            order_id,
                            order_dt,
                            load_dt,
                            load_src
                        )
                        VALUES (
                            %(order_id)s,
                            %(order_dt)s,
                            NOW(),
                            %(load_src)s
                        )
                        ON CONFLICT (order_id) DO NOTHING
                        RETURNING h_order_pk
                    )
                    SELECT h_order_pk FROM inserted
                    UNION ALL
                    SELECT h_order_pk
                    FROM dds.h_order
                    WHERE order_id = %(order_id)s
                    LIMIT 1;
                    """,
                    {
                        "order_id": order_id,
                        "order_dt": order_dt,
                        "load_src": load_src,
                    },
                )
                return cur.fetchone()[0]

    def insert_hub_user(
        self,
        user_id: str,
        load_src: str,
    ) -> UUID:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH inserted AS (
                        INSERT INTO dds.h_user (
                            user_id,
                            load_dt,
                            load_src
                        )
                        VALUES (
                            %(user_id)s,
                            NOW(),
                            %(load_src)s
                        )
                        ON CONFLICT (user_id) DO NOTHING
                        RETURNING h_user_pk
                    )
                    SELECT h_user_pk FROM inserted
                    UNION ALL
                    SELECT h_user_pk
                    FROM dds.h_user
                    WHERE user_id = %(user_id)s::varchar
                    LIMIT 1;
                    """,
                    {
                        "user_id": user_id,
                        "load_src": load_src,
                    },
                )
                return cur.fetchone()[0]

    def insert_hub_restaurant(
        self,
        restaurant_id: str,
        load_src: str,
    ) -> UUID:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH inserted AS (
                        INSERT INTO dds.h_restaurant (
                            restaurant_id,
                            load_dt,
                            load_src
                        )
                        VALUES (
                            %(restaurant_id)s,
                            NOW(),
                            %(load_src)s
                        )
                        ON CONFLICT (restaurant_id) DO NOTHING
                        RETURNING h_restaurant_pk
                    )
                    SELECT h_restaurant_pk FROM inserted
                    UNION ALL
                    SELECT h_restaurant_pk
                    FROM dds.h_restaurant
                    WHERE restaurant_id = %(restaurant_id)s::varchar
                    LIMIT 1;
                    """,
                    {
                        "restaurant_id": restaurant_id,
                        "load_src": load_src,
                    },
                )
                return cur.fetchone()[0]

    def insert_hub_product(
        self,
        product_id: str,
        load_src: str,
    ) -> UUID:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH inserted AS (
                        INSERT INTO dds.h_product (
                            product_id,
                            load_dt,
                            load_src
                        )
                        VALUES (
                            %(product_id)s,
                            NOW(),
                            %(load_src)s
                        )
                        ON CONFLICT (product_id) DO NOTHING
                        RETURNING h_product_pk
                    )
                    SELECT h_product_pk FROM inserted
                    UNION ALL
                    SELECT h_product_pk
                    FROM dds.h_product
                    WHERE product_id = %(product_id)s::varchar
                    LIMIT 1;
                    """,
                    {
                        "product_id": product_id,
                        "load_src": load_src,
                    },
                )
                return cur.fetchone()[0]

    def insert_hub_category(
        self,
        category_name: str,
        load_src: str,
    ) -> UUID:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH inserted AS (
                        INSERT INTO dds.h_category (
                            category_name,
                            load_dt,
                            load_src
                        )
                        VALUES (
                            %(category_name)s,
                            NOW(),
                            %(load_src)s
                        )
                        ON CONFLICT (category_name) DO NOTHING
                        RETURNING h_category_pk
                    )
                    SELECT h_category_pk FROM inserted
                    UNION ALL
                    SELECT h_category_pk
                    FROM dds.h_category
                    WHERE category_name = %(category_name)s::varchar
                    LIMIT 1;
                    """,
                    {
                        "category_name": category_name,
                        "load_src": load_src,
                    },
                )
                return cur.fetchone()[0]

    def insert_link_order_user(
        self,
        h_order_pk: UUID,
        h_user_pk: UUID,
        load_src: str,
    ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_order_user (
                        h_order_pk,
                        h_user_pk,
                        load_dt,
                        load_src
                    )
                    VALUES (
                        %(h_order_pk)s,
                        %(h_user_pk)s,
                        NOW(),
                        %(load_src)s
                    )
                    ON CONFLICT DO NOTHING;
                    """,
                    {
                        "h_order_pk": h_order_pk,
                        "h_user_pk": h_user_pk,
                        "load_src": load_src,
                    },
                )

    def insert_link_order_product(
        self,
        h_order_pk: UUID,
        h_product_pk: UUID,
        load_src: str,
    ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_order_product (
                        h_order_pk,
                        h_product_pk,
                        load_dt,
                        load_src
                    )
                    VALUES (
                        %(h_order_pk)s,
                        %(h_product_pk)s,
                        NOW(),
                        %(load_src)s
                    )
                    ON CONFLICT DO NOTHING;
                    """,
                    {
                        "h_order_pk": h_order_pk,
                        "h_product_pk": h_product_pk,
                        "load_src": load_src,
                    },
                )

    def insert_link_product_category(
        self,
        h_product_pk: UUID,
        h_category_pk: UUID,
        load_src: str,
    ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_product_category (
                        h_product_pk,
                        h_category_pk,
                        load_dt,
                        load_src
                    )
                    VALUES (
                        %(h_product_pk)s,
                        %(h_category_pk)s,
                        NOW(),
                        %(load_src)s
                    )
                    ON CONFLICT DO NOTHING;
                    """,
                    {
                        "h_product_pk": h_product_pk,
                        "h_category_pk": h_category_pk,
                        "load_src": load_src,
                    },
                )

    def insert_link_product_restaurant(
        self,
        h_product_pk: UUID,
        h_restaurant_pk: UUID,
        load_src: str,
    ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_product_restaurant (
                        h_product_pk,
                        h_restaurant_pk,
                        load_dt,
                        load_src
                    )
                    VALUES (
                        %(h_product_pk)s,
                        %(h_restaurant_pk)s,
                        NOW(),
                        %(load_src)s
                    )
                    ON CONFLICT DO NOTHING;
                    """,
                    {
                        "h_product_pk": h_product_pk,
                        "h_restaurant_pk": h_restaurant_pk,
                        "load_src": load_src,
                    },
                )

    def insert_sat_order_cost(
        self,
        h_order_pk: UUID,
        cost: float,
        payment: float,
        load_src: str,
    ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_order_cost (
                        h_order_pk,
                        cost,
                        payment,
                        load_dt,
                        load_src
                    )
                    SELECT
                        %(h_order_pk)s,
                        %(cost)s,
                        %(payment)s,
                        NOW(),
                        %(load_src)s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM dds.s_order_cost
                        WHERE h_order_pk = %(h_order_pk)s
                        ORDER BY load_dt DESC
                        LIMIT 1
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM dds.s_order_cost
                        WHERE h_order_pk = %(h_order_pk)s
                        ORDER BY load_dt DESC
                        LIMIT 1
                    )
                    AND (
                        SELECT cost IS DISTINCT FROM %(cost)s
                               OR payment IS DISTINCT FROM %(payment)s
                        FROM dds.s_order_cost
                        WHERE h_order_pk = %(h_order_pk)s
                        ORDER BY load_dt DESC
                        LIMIT 1
                    );
                    """,
                    {
                        "h_order_pk": h_order_pk,
                        "cost": cost,
                        "payment": payment,
                        "load_src": load_src,
                    },
                )

    def insert_sat_order_status(
        self,
        h_order_pk: UUID,
        status: str,
        load_src: str,
    ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_order_status (
                        h_order_pk,
                        status,
                        load_dt,
                        load_src
                    )
                    SELECT
                        %(h_order_pk)s,
                        %(status)s,
                        NOW(),
                        %(load_src)s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM dds.s_order_status
                        WHERE h_order_pk = %(h_order_pk)s
                    )
                    OR (
                        SELECT status IS DISTINCT FROM %(status)s::varchar
                        FROM dds.s_order_status
                        WHERE h_order_pk = %(h_order_pk)s
                        ORDER BY load_dt DESC
                        LIMIT 1
                    );
                    """,
                    {
                        "h_order_pk": h_order_pk,
                        "status": status,
                        "load_src": load_src,
                    },
                )

    def insert_sat_user_names(
        self,
        h_user_pk: UUID,
        username: str,
        userlogin: str,
        load_src: str,
    ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_user_names (
                        h_user_pk,
                        username,
                        userlogin,
                        load_dt,
                        load_src
                    )
                    SELECT
                        %(h_user_pk)s,
                        %(username)s,
                        %(userlogin)s,
                        NOW(),
                        %(load_src)s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM dds.s_user_names
                        WHERE h_user_pk = %(h_user_pk)s
                    )
                    OR (
                        SELECT username IS DISTINCT FROM %(username)s::varchar
                               OR userlogin IS DISTINCT FROM %(userlogin)s::varchar
                        FROM dds.s_user_names
                        WHERE h_user_pk = %(h_user_pk)s
                        ORDER BY load_dt DESC
                        LIMIT 1
                    );
                    """,
                    {
                        "h_user_pk": h_user_pk,
                        "username": username,
                        "userlogin": userlogin,
                        "load_src": load_src,
                    },
                )

    def insert_sat_restaurant_names(
        self,
        h_restaurant_pk: UUID,
        name: str,
        load_src: str,
    ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_restaurant_names (
                        h_restaurant_pk,
                        name,
                        load_dt,
                        load_src
                    )
                    SELECT
                        %(h_restaurant_pk)s,
                        %(name)s,
                        NOW(),
                        %(load_src)s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM dds.s_restaurant_names
                        WHERE h_restaurant_pk = %(h_restaurant_pk)s
                    )
                    OR (
                        SELECT name IS DISTINCT FROM %(name)s::varchar
                        FROM dds.s_restaurant_names
                        WHERE h_restaurant_pk = %(h_restaurant_pk)s
                        ORDER BY load_dt DESC
                        LIMIT 1
                    );
                    """,
                    {
                        "h_restaurant_pk": h_restaurant_pk,
                        "name": name,
                        "load_src": load_src,
                    },
                )

    def insert_sat_product_names(
        self,
        h_product_pk: UUID,
        name: str,
        load_src: str,
    ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_product_names (
                        h_product_pk,
                        name,
                        load_dt,
                        load_src
                    )
                    SELECT
                        %(h_product_pk)s,
                        %(name)s,
                        NOW(),
                        %(load_src)s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM dds.s_product_names
                        WHERE h_product_pk = %(h_product_pk)s
                    )
                    OR (
                        SELECT name IS DISTINCT FROM %(name)s::varchar
                        FROM dds.s_product_names
                        WHERE h_product_pk = %(h_product_pk)s
                        ORDER BY load_dt DESC
                        LIMIT 1
                    );
                    """,
                    {
                        "h_product_pk": h_product_pk,
                        "name": name,
                        "load_src": load_src,
                    },
                )
