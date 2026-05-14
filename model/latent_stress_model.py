import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from sklearn.preprocessing import RobustScaler
from scipy.stats import norm, percentileofscore # 🌟 norm 추가
from .base_model import QuantitativeModel
from typing import Dict, Any, Optional

class PCALatentStressModel(QuantitativeModel):
    def __init__(self, name="PCA_Fragility_AI", window=252, feature_builder=None):
        super().__init__(name)
        self.window = window
        self.feature_builder = feature_builder
        self.pca = PCA(n_components=1)
        self.scaler = RobustScaler()
        self.feature_cols = [
            "breadth_mom",       # S&P500 내부 체력
            "vix_term",          # 변동성 기간 구조
            "credit_spread",     # 우량/비우량 신용 격차
            "liquidity_stress",  # 유동성 경색 (복구)
            "semi_breadth",      # 반도체 리더십 (복구)
            "consumer_cyclical"  # 소비 활력 (복구)
        ]

    # AS-IS: def fit(self, data: pd.DataFrame): self.is_fitted = True
    # TO-BE: fit은 초기 모델 세팅용으로 update를 호출하도록 변경
    def fit(self, data: pd.DataFrame) -> None:
        self.update(data)

    # 🌟 [신규 추가] 매일 실행되는 증분 학습 로직 (Rolling PCA)
    def update(self, data: pd.DataFrame, candidate_mode: bool = False, candidate_name: Optional[str] = None) -> None:
        """최신 데이터를 받아 최근 window(252일) 기준으로 PCA를 Rolling Retrain 합니다."""
        df_feat = self.feature_builder.process(data, is_training=False)
        stress_df = df_feat[self.feature_cols].dropna()

        if len(stress_df) < 50:
            self.is_fitted = False
            return

        # 최신 윈도우 추출
        train_stress = stress_df.iloc[-self.window:]

        if len(train_stress) < 10:
            self.is_fitted = False
            return
            
        # 1. Scaler & PCA 학습 (Training)
        self.scaler.fit(train_stress)
        scaled_train = self.scaler.transform(train_stress)
        self.pca.fit(scaled_train)
        
        # 2. 전체 기간 PC1 변환
        all_pc1 = self.pca.transform(scaled_train)[:, 0]
        
        # 3. 튼튼한 이중 닻 (Double Anchor) 방향 교정 (캐싱)
        vix_idx = self.feature_cols.index("vix_term")
        credit_idx = self.feature_cols.index("credit_spread")
        
        corr_vix = np.corrcoef(scaled_train[:, vix_idx], all_pc1)[0, 1]
        corr_credit = np.corrcoef(scaled_train[:, credit_idx], all_pc1)[0, 1]
        
        # 음의 상관관계일 경우 PC1 부호 반전을 위한 multiplier 저장
        self.sign_multiplier = -1.0 if (corr_vix + corr_credit) < 0 else 1.0
        all_pc1 = all_pc1 * self.sign_multiplier
        
        # 4. 추론 시 Z-score 및 Velocity 계산을 위한 과거 기록 저장
        self.pc1_history = pd.Series(all_pc1, index=train_stress.index)
        self.is_fitted = True


    # AS-IS: predict 안에서 매번 fit을 호출하는 비효율 발생
    # TO-BE: predict는 오직 당일(최신) 데이터에 대한 Transform(추론)만 수행하여 O(1) 속도 보장
    def predict(self, data: pd.DataFrame) -> Dict[str, Any]:
        default_resp = {"fragility_z_score": 0.0, "transition_risk": 0.5, "shock_state": False}
        if not self.is_fitted or self.pc1_history is None: 
            return default_resp

        # 추론은 최근 데이터(충격 감지용 여유분 포함 5일)만 피처 프로세싱
        df_feat = self.feature_builder.process(data.iloc[-5:], is_training=False)
        latest_feat = df_feat[self.feature_cols].dropna().iloc[-1:] # 오늘자 데이터

        if latest_feat.empty:
            return default_resp

        # 1. 오늘 데이터 Transform (저장된 scaler, pca, sign_multiplier 사용)
        latest_scaled = self.scaler.transform(latest_feat)
        today_pc1 = self.pca.transform(latest_scaled)[0, 0] * self.sign_multiplier

        # ==========================================
        # 1. Fragility Level (수위 - 위치 에너지)
        # ==========================================
        # update()에서 저장해둔 역사적 데이터의 평균/표준편차 활용
        past_pc1 = self.pc1_history.values
        pc1_mean = np.mean(past_pc1)
        pc1_std = np.std(past_pc1) + 1e-6
        fragility_z = (today_pc1 - pc1_mean) / pc1_std

        # ==========================================
        # 2. Transition Risk (Level + Velocity 블렌딩)
        # ==========================================
        # 임시로 과거 기록에 오늘 PC1을 이어붙여 속도(diff) 계산
        temp_history = pd.concat([self.pc1_history, pd.Series([today_pc1])])
        velocity_series = temp_history.diff(5).dropna()
        
        if len(velocity_series) < 10:
            transition_risk = 0.5
        else:
            current_velocity = temp_history.iloc[-1] - temp_history.iloc[-6]
            velocity_risk = percentileofscore(velocity_series.values, current_velocity) / 100.0
            level_risk = norm.cdf(fragility_z)
            transition_risk = (0.6 * level_risk) + (0.4 * velocity_risk)
            
        # 충격 이벤트 로직 (기존 유지)
        try:
            gap_series = df_feat["gap_shock"].dropna()
            gap = gap_series.iloc[-1] if not gap_series.empty else 0.0
            
            vix_series = df_feat["vix_jump"].dropna()
            vix_jump = vix_series.iloc[-1] if not vix_series.empty else 0.0
            
            shock_state = bool((gap < -0.015) or (vix_jump > 0.15))
        except Exception:
            shock_state = False
        
        return {
            "fragility_z_score": round(float(fragility_z), 3),
            "transition_risk": round(float(transition_risk), 3),
            "shock_state": shock_state
        }

    def get_signal(self):
        return 1