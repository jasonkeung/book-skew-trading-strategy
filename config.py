import math
from pprint import pprint
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional, List

import pandas as pd
import databento as db


@dataclass(frozen=True)
class Config:
    # Databento API Key
    api_key: Optional[str] = None
    with open(".secrets", "r") as f:
        api_key = f.read().split("=")[1]

    # Alpha threshold to buy/sell, k
    skew_threshold: float = .1

    # Databento dataset
    dataset: str = "GLBX.MDP3"

    # Instrument information
    symbol: str = "ES.c.0"
    stype_in: str = "continuous"
    point_value: Decimal = Decimal("50")  # $50 per index point

    # Fees
    venue_fees_per_side: Decimal = Decimal("0.39")
    clearing_fees_per_side: Decimal = Decimal("0.05")

    @property
    def fees_per_side(self) -> Decimal:
        return self.venue_fees_per_side + self.clearing_fees_per_side

    # Position limit
    position_max: int = 10