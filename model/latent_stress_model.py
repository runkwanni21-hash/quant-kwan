import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from sklearn.preprocessing import RobustScaler
from scipy.stats import norm, percentileofscore # 🌟 norm 추가
from .base_model import QuantitativeModel

class PCALatentStressModel(QuantitativeModel):
    def __init__(self, name="PCA_Fragility_AI", window=252, feature_builder=None):
        super().__init__(name)
        self.window = window
        self.feature_builder = feature_builder
        self.pca = PCA(n_components=1)
        self.scaler = RobustScaler()
        # 🌟 다시 가장 직교성이 높은 3대 핵심 지표로 복귀
        self.feature_cols = ["breadth_mom", "vix_term", "credit_spread"]

    def fit(self, data: pd.DataFrame):
        self.is_fitted = True

    def predict(self, data: pd.DataFrame) -> dict:
        default_resp = {"fragility_z_score": 0.0, "transition_risk": 0.5, "shock_state": False}
        if not self.is_fitted: return default_resp

        df_feat = self.feature_builder.process(data, is_training=False)
        stress_df = df_feat[self.feature_cols].dropna()

        if len(stress_df) < 50:
            return default_resp

        all_stress = stress_df.iloc[-self.window:]
        train_stress = all_stress.iloc[:-1]
        
        if len(train_stress) < 10:
            return default_resp
            
        self.scaler.fit(train_stress)
        all_scaled = self.scaler.transform(all_stress)
        self.pca.fit(all_scaled[:-1])
        
        all_pc1 = self.pca.transform(all_scaled)[:, 0]
        
        vix_idx = self.feature_cols.index("vix_term")
        credit_idx = self.feature_cols.index("credit_spread")
        
        # 튼튼한 이중 닻 (Double Anchor) 방향 교정
        corr_vix = np.corrcoef(all_scaled[:-1, vix_idx], all_pc1[:-1])[0, 1]
        corr_credit = np.corrcoef(all_scaled[:-1, credit_idx], all_pc1[:-1])[0, 1]
        if (corr_vix + corr_credit) < 0:
            all_pc1 = -all_pc1

        # ==========================================
        # 1. Fragility Level (수위 - 위치 에너지)
        # ==========================================
        pc1_mean = np.mean(all_pc1[:-1])
        pc1_std = np.std(all_pc1[:-1]) + 1e-6
        fragility_z = (all_pc1[-1] - pc1_mean) / pc1_std

        # ==========================================
        # 2. Transition Risk (Level + Velocity 블렌딩)
        # ==========================================
        pc1_series = pd.Series(all_pc1)
        velocity_series = pc1_series.diff(5).dropna()
        
        if len(velocity_series) < 10:
            transition_risk = 0.5
        else:
            current_velocity = pc1_series.iloc[-1] - pc1_series.iloc[-6]
            
            # A. 가속도 위험 (0 ~ 1)
            velocity_risk = percentileofscore(velocity_series, current_velocity) / 100.0
            
            # B. 절대 수위 위험 (0 ~ 1, Z-score의 누적분포함수 변환)
            level_risk = norm.cdf(fragility_z)
            
            # 🌟 C. 최종 붕괴 확률 (60% Level + 40% Velocity)
            transition_risk = (0.6 * level_risk) + (0.4 * velocity_risk)
            
        # 충격 이벤트
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