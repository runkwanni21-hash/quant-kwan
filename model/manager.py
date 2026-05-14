import numpy as np
import pandas as pd
# AS-IS: list, 파라미터 타입 없음
# TO-BE: 모델 타입 명시 및 List 타입 힌트 추가
from typing import List
from model.base_model import QuantitativeModel

class StrategyManager:
    # AS-IS: def __init__(self, models: list):
    # TO-BE: QuantitativeModel 인터페이스의 리스트만 허용하도록 타입 지정
    def __init__(self, models: List[QuantitativeModel]) -> None:
        self.models = models

    # AS-IS: def get_final_decision(self, data):
    # TO-BE: 인자 및 반환형 명시
    def get_final_decision(self, data: pd.DataFrame) -> str:
        signals: List[float] = [] # 시그널을 담을 리스트 타입 명시
        
        for model in self.models:
            model.fit(data)
            signals.append(model.get_signal())

        # 신호 결합 (알파 결합)
        avg_signal: float = np.mean(signals)

        if avg_signal > 0.5:
            return "🚀 환노출 (달러 롱) 추천"
        elif avg_signal < -0.5:
            return "🛡️ 환헤지 (달러 숏) 추천"
        else:
            return "⌛ 중립 (기존 포지션 유지)"