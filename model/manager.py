import numpy as np

class StrategyManager:
    def __init__(self, models: list):
        self.models = models

    def get_final_decision(self, data):
        signals = []
        for model in self.models:
            model.fit(data)
            signals.append(model.get_signal())

        # 신호 결합 (예: 모든 모델이 일치할 때만 스위칭하는 '알파 결합' 원리)
        avg_signal = np.mean(signals)

        if avg_signal > 0.5:
            return "🚀 환노출 (달러 롱) 추천"
        elif avg_signal < -0.5:
            return "🛡️ 환헤지 (달러 숏) 추천"
        else:
            return "⌛ 중립 (기존 포지션 유지)"
