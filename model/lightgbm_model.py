import pandas as pd
import numpy as np
import lightgbm as lgb
from typing import List, Callable, Optional
from .base_model import QuantitativeModel

class LightGBMRegimeModel(QuantitativeModel):
    def __init__(
        self,
        name: str = "LGBM_Regime",
        train_window: int = 500,
        target_col: str = "target",
        feature_func: Optional[Callable] = None
    ):
        """
        일반화된 LGBM 국면 분석 모델
        :param target_col: 학습 시 목표가 되는 컬럼명 (예: 'target_equity', 'target_vol')
        :param feature_func: 데이터를 받아 특성을 추가해주는 외부 함수
        """
        super().__init__(name)
        self.train_window = train_window
        self.target_col = target_col
        self.feature_func = feature_func

        self.model = lgb.LGBMClassifier(
            n_estimators=200,
            learning_rate=0.05,
            num_leaves=15,
            min_child_samples=20,
            reg_alpha=0.1,
            reg_lambda=0.1,
            random_state=42,
            verbose=-1
        )
        self.feature_cols = []
        self.adaptive_threshold = 0.5
        self.latest_proba = 0.5

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """외부에서 주입된 함수를 통해 피처를 생성하거나 기본 피처 생성"""
        if self.feature_func:
            return self.feature_func(df)
        return df # 외부에서 이미 가공된 데이터가 들어오는 경우

    def fit(self, data: pd.DataFrame):
        df_feat = self._prepare_data(data)

        # 특징 컬럼 자동 추출 (데이터 Plane 컬럼 제외)
        exclude = ["Open", "High", "Low", "Close", "Volume", "next_return",
                   "target", "target_equity", "target_vol", "target_bond"]
        self.feature_cols = [c for c in df_feat.columns if c not in exclude]

        # 학습 데이터 준비
        train_data = df_feat.dropna(subset=[self.target_col] + self.feature_cols).iloc[-self.train_window:]

        if len(train_data) < 100:
            return

        X_train = train_data[self.feature_cols].values
        y_train = train_data[self.target_col].values

        self.model.fit(X_train, y_train)

        # Adaptive Threshold 계산 (해당 자산군의 특성에 맞게 조정됨)
        train_proba = self.model.predict_proba(X_train)[:, 1]
        self.adaptive_threshold = float(np.quantile(train_proba, 1 - y_train.mean()) if len(train_proba) > 1 else 0.5)
        self.is_fitted = True

    def predict(self, data: pd.DataFrame) -> float:
        """최신 1일치(가장 마지막 행)의 상승 확률 예측"""
        if not self.is_fitted:
            return 0.5

        df_feat = self._prepare_data(data)
        latest_X = df_feat[self.feature_cols].iloc[-1:].values

        # 확률 추출 (클래스 1의 확률)
        self.latest_proba = float(self.model.predict_proba(latest_X)[0, 1])
        return self.latest_proba

    def get_signal(self):
        """manager.py 또는 macro_features.py에서 사용할 신호 반환"""
        if not self.is_fitted:
            return 0 # 중립

        # 임계값보다 확률이 높으면 1 (매수/환노출), 낮으면 -1 (매도/환헤지)
        if self.latest_proba > self.adaptive_threshold:
            return 1
        elif self.latest_proba < self.adaptive_threshold:
            return -1
        else:
            return 0
