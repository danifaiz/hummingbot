#!/usr/bin/env python

from sqlalchemy import (
    Column,
    ForeignKey,
    Text,
    Integer,
    Index,
    BigInteger,
    Float,
    JSON
)
from sqlalchemy.orm import relationship

from . import HummingbotBase


class TradeFill(HummingbotBase):
    __tablename__ = "TradeFill"
    __table_args__ = (Index("tf_config_timestamp_index",
                            "config_file_path", "timestamp"),
                      Index("tf_market_symbol_timestamp_index",
                            "market", "symbol", "timestamp"),
                      Index("tf_market_base_asset_timestamp_index",
                            "market", "base_asset", "timestamp"),
                      Index("tf_market_quote_asset_timestamp_index",
                            "market", "quote_asset", "timestamp")
                      )

    id = Column(Integer, primary_key=True, nullable=False)
    config_file_path = Column(Text, nullable=False)
    strategy = Column(Text, nullable=False)
    market = Column(Text, nullable=False)
    symbol = Column(Text, nullable=False)
    base_asset = Column(Text, nullable=False)
    quote_asset = Column(Text, nullable=False)
    timestamp = Column(BigInteger, nullable=False)
    order_id = Column(Text, ForeignKey("Order.id"), nullable=False)
    trade_type = Column(Text, nullable=False)
    order_type = Column(Text, nullable=False)
    price = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)
    trade_fee = Column(JSON, nullable=False)
    order = relationship("Order", back_populates="trade_fills")

    def __repr__(self) -> str:
        return f"TradeFill(id={self.id}, config_file_path='{self.config_file_path}', strategy='{self.strategy}', " \
            f"market='{self.market}', symbol='{self.symbol}', base_asset='{self.base_asset}', " \
            f"quote_asset='{self.quote_asset}', timestamp={self.timestamp}, order_id='{self.order_id}', " \
            f"trade_type='{self.trade_type}', order_type='{self.order_type}', price={self.price}, " \
            f"amount={self.amount}, trade_fee={self.trade_fee})"
