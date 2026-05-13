from statsmodels.tsa.vector_ar.vecm import VECM
from .base_model import QuantitativeModel

class CointegrationModel(QuantitativeModel):
    def __init__(self, k_ar_diff=1):
        super().__init__("VECM_Cointegration")
        self.k_ar_diff = k_ar_diff

    def fit(self, data):
        # data는 [환율, 금리차] 다변량 데이터프레임
        self.model_result = VECM(data, k_ar_diff=self.k_ar_diff,
                                 coint_rank=1, deterministic='ci').fit()
        self.is_fitted = True

    def predict(self, steps=10):
        return self.model_result.predict(steps=steps)

    def get_signal(self):
        # 오차수정항(ECT)이 양수면 고평가 상태이므로 환헤지(-1)
        # 자료 [08]의 공적분 원리 적용
        ect = self.model_result.alpha @ self.model_result.beta.T
        return -1 if ect[0] > 0 else 1
