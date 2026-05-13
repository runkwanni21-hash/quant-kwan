from statsmodels.tsa.statespace.sarimax import SARIMAX
from .base_model import QuantitativeModel

class SarimaxModel(QuantitativeModel):
    def __init__(self, order=(1,1,1), seasonal_order=(0,0,0,0)):
        super().__init__("SARIMAX")
        self.order = order
        self.seasonal_order = seasonal_order

    def fit(self, data):
        self.model_result = SARIMAX(data, order=self.order,
                                    seasonal_order=self.seasonal_order).fit(disp=False)
        self.is_fitted = True

    def predict(self, steps=10):
        forecast = self.model_result.get_forecast(steps=steps)
        return forecast.summary_frame()

    def get_signal(self):
        # 예측값의 마지막이 현재가보다 높으면 1(환노출), 낮으면 -1(환헤지)
        forecast_mean = self.predict(steps=5)['mean'].iloc[-1]
        current_val = self.model_result.data.orig_endog[-1]
        return 1 if forecast_mean > current_val else -1
