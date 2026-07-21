from uuid import UUID

from lib.pg import PgConnect


class CdmRepository:
    def __init__(self, pg_connect: PgConnect):
        self._pg = pg_connect

    def upsert_user_product_counter(
        self,
        user_id: str,
        product_id: str,
        product_name: str
    ) -> None:
        with self._pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cdm.user_product_counters
                    (
                        user_id,
                        product_id,
                        product_name,
                        order_cnt
                    )
                    VALUES (
                        %(user_id)s,
                        %(product_id)s,
                        %(product_name)s,
                        1
                    )
                    ON CONFLICT (user_id, product_id)
                    DO UPDATE SET
                        order_cnt = cdm.user_product_counters.order_cnt + 1,
                        product_name = EXCLUDED.product_name;
                    """,
                    {
                        "user_id": user_id,
                        "product_id": product_id,
                        "product_name": product_name,
                    }
                )

    def get_category_id(
        self,
        category_name: str
    ) -> UUID | None:
        with self._pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT h_category_pk
                    FROM dds.h_category
                    WHERE category_name = %(category_name)s
                    LIMIT 1;
                    """,
                    {
                        "category_name": category_name
                    }
                )

                result = cur.fetchone()

                if result:
                    return result[0]

                return None

    def upsert_user_category_counter(
        self,
        user_id: str,
        category_id: str,
        category_name: str
    ) -> None:
        with self._pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cdm.user_category_counters
                    (
                        user_id,
                        category_id,
                        category_name,
                        order_cnt
                    )
                    VALUES (
                        %(user_id)s,
                        %(category_id)s,
                        %(category_name)s,
                        1
                    )
                    ON CONFLICT (user_id, category_id)
                    DO UPDATE SET
                        order_cnt = cdm.user_category_counters.order_cnt + 1,
                        category_name = EXCLUDED.category_name;
                    """,
                    {
                        "user_id": user_id,
                        "category_id": category_id,
                        "category_name": category_name,
                    }
                )
