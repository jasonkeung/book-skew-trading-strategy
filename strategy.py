import math
from pprint import pprint
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional, List

import pandas as pd
import databento as db

from config import Config

@dataclass
class Strategy:
    # Static configuration
    config: Config

    # Current position, in contract units
    position: int = 0
    # Number of long contract sides traded
    buy_qty: int = 0
    # Number of short contract sides traded
    sell_qty: int = 0

    # Total realized buy price
    real_total_buy_px: Decimal = Decimal("0")
    # Total realized sell price
    real_total_sell_px: Decimal = Decimal("0")

    # Total buy price to liquidate current position
    theo_total_buy_px: Decimal = Decimal("0")
    # Total sell price to liquidate current position
    theo_total_sell_px: Decimal = Decimal("0")

    # Total fees paid
    fees: Decimal = Decimal("0")

    # List to track results
    results: List[object] = field(default_factory=list)

    def run_live(self) -> None:
        client = db.Live(self.config.api_key)
        client.subscribe(
            dataset=self.config.dataset,
            schema="mbp-1",
            stype_in=self.config.stype_in,
            symbols=[self.config.symbol],
        )
        for record in client:
            if isinstance(record, db.MBP1Msg):

                # ask_size = record.levels[0].ask_sz
                # bid_size = record.levels[0].bid_sz
                # ask_price = record.levels[0].ask_px / Decimal("1e9")
                # bid_price = record.levels[0].bid_px / Decimal("1e9")
                self.update(record)

    def run_historical(self) -> None:
        client = db.Historical(self.config.api_key)

        # df = client.timeseries.get_range(
        #     dataset="GLBX.MDP3",
        #     schema="trades",
        #     symbols=["ESU3-ESZ3"],
        #     start="2023-08-25",
        # ).to_df()
        df = client.timeseries.get_range(
            dataset=self.config.dataset,
            schema="MBP-1",
            symbols=["ESU3-ESZ3"],
            start="2023-08-25",
            end="2023-10-10",
        ).to_df()

        for row in df.iterrows():
            _, record = row
            ask_size = record['ask_sz_00']
            bid_size = record['bid_sz_00']
            ask_price = Decimal(record["ask_px_00"])
            bid_price = Decimal(record["bid_px_00"])
            ts_recv = ''
            self.update(ask_size, bid_size, ask_price, bid_price, ts_recv)

        print("realized profit: ", self.real_total_sell_px - self.real_total_buy_px)
        print("fees: ", self.fees)
        # print(self.results)


    def update(self, ask_size, bid_size, ask_price, bid_price, ts_recv) -> None:
        if bid_size == 0 or ask_size == 0: return

        # Calculate skew feature
        skew = math.log10(bid_size) - math.log10(ask_size)

        # Buy/sell based when skew signal is large
        if (
            skew > self.config.skew_threshold
            and self.position < self.config.position_max
        ):
            self.position += 1
            self.buy_qty += 1
            self.real_total_buy_px += ask_price
            self.fees += self.config.fees_per_side
        elif (
            skew < -self.config.skew_threshold
            and self.position > -self.config.position_max
        ):
            self.position -= 1
            self.sell_qty += 1
            self.real_total_sell_px += bid_price
            self.fees += self.config.fees_per_side

        # Update prices
        # Fill prices are based on BBO with assumed zero latency
        # In practice, fill prices will likely be worse
        if self.position == 0:
            self.theo_total_buy_px = Decimal("0")
            self.theo_total_sell_px = Decimal("0")
        elif self.position > 0:
            self.theo_total_sell_px = bid_price * abs(self.position)
        elif self.position < 0:
            self.theo_total_buy_px = ask_price * abs(self.position)

        # Compute PnL
        theo_pnl = (
            self.config.point_value
            * (
                self.real_total_sell_px
                + self.theo_total_sell_px
                - self.real_total_buy_px
                - self.theo_total_buy_px
            )
            - self.fees
        )

        # Print & store results
        result = {
            "ts_strategy": ts_recv,
            "bid": bid_price,
            "ask": ask_price,
            "skew": skew,
            "position": self.position,
            "trade_ct": self.buy_qty + self.sell_qty,
            "fees": self.fees,
            "pnl": theo_pnl,
        }
        # pprint(result)
        self.results.append(result)


if __name__ == "__main__":
    config = Config()
    strategy = Strategy(config=config)
    try:
        strategy.run_historical()
    except KeyboardInterrupt:
        pass
    # df = pd.DataFrame.from_records(strategy.results, index="ts_strategy")
    # df.to_csv("strategy_log.csv")