from pydantic import BaseModel, PrivateAttr
import polars as pl

from taq_etl.dal.dao.crsp_dao import CrspDao, CrspColumn as c
from taq_etl.dal.models.database import Database


class CrspService(BaseModel):
    database: Database
    _dao: CrspDao = PrivateAttr()

    def model_post_init(self, __context) -> None:
        self._dao = CrspDao(database=self.database)

    def build_crsp_map_by_year(self, year: int) -> None:
        crsp_raw = self._dao.load_crsp_by_year(year)

        crsp = (
            crsp_raw.with_columns(
                pl.col(c.TICKER).count().over([c.DATE, c.TICKER]).gt(1).alias('is_multiclass'),
                pl.col(c.SHARE_CLASS).fill_null(""),
                pl.col(c.TICKER).alias('base_ticker'),
                pl.col(c.TICKER).add(pl.col(c.SHARE_CLASS)).alias('concat_ticker'),
            )
        )

        # Priority 1: Multi-class exact matches
        p1 = crsp.filter(pl.col("is_multiclass")).select(
            pl.col("date"),
            pl.col("concat_ticker").alias("join_ticker"),
            pl.col("permno"),
            pl.lit(1).alias("priority")
        )

        # Priority 2: Single-class base matches (strip the A/B)
        p2 = crsp.filter(~pl.col("is_multiclass")).select(
            pl.col("date"),
            pl.col("base_ticker").alias("join_ticker"),
            pl.col("permno"),
            pl.lit(2).alias("priority")
        )

        # Priority 3: Fallback (keep the A/B just in case TAQ forces it)
        p3 = crsp.filter(~pl.col("is_multiclass")).select(
            pl.col("date"),
            pl.col("concat_ticker").alias("join_ticker"),
            pl.col("permno"),
            pl.lit(3).alias("priority")
        )

        crsp_mapping = (
            pl.concat([p1, p2, p3])
            .sort(["date", "join_ticker", "priority"])
            .unique(subset=["date", "join_ticker"], keep="first")
        )

        self._dao.save_parquet_interim(
            df=crsp_mapping,
            path="crsp_mapping",
            name=f"crsp_mapping_{year}"
        )