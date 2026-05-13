from taq_backtester.model.config import BTConfig
from taq_backtester.model.schema import WeightsSchema, WeightsDf

class Backtester():
    def __init__(self, config: BTConfig):
        self.config = config
        self.portfolio: WeightsDf = WeightsDf.empty(schema=WeightsSchema)
        self.aum = self.config.initial_aum

    def rebalance_on_date(self, date, optimal_weights: WeightsDf):
        pass