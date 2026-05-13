import pandas as pd
import numpy as np
from abc import ABC, abstractmethod

class QuantitativeModel(ABC):
    """모든 퀀트 모델의 기본이 되는 추상 클래스"""

    def __init__(self, name):
        self.name = name
        self.model_result = None
        self.is_fitted = False

    @abstractmethod
    def fit(self, data: pd.DataFrame):
        """데이터를 받아 모델을 학습시키는 메서드"""
        pass

    @abstractmethod
    def predict(self, steps: int):
        """미래 수치를 예측하는 메서드"""
        pass

    @abstractmethod
    def get_signal(self):
        """모델 결과를 기반으로 매수/매도/스위칭 신호를 반환하는 메서드"""
        pass
