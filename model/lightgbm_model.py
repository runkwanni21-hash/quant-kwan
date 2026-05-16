import pandas as pd
import numpy as np
import lightgbm as lgb
import copy
import os
from sklearn.multioutput import MultiOutputRegressor
from typing import List, Dict, Any, Optional
from .base_model import QuantitativeModel

class LightGBMMIMOMacroModel(QuantitativeModel):
    def __init__(self, name: str = "LGBM_Macro_Brain", train_window: int = 2000, 
                 target_cols: Optional[List[str]] = None, feature_builder=None) -> None:
        super().__init__(name)
        self.train_window = train_window
        self.target_cols = target_cols if target_cols else [
            "macro_growth", "macro_inflation", "macro_liquidity", "macro_stress"
        ]
        self.feature_builder = feature_builder
        
        base_model = lgb.LGBMRegressor(
            n_estimators=150, learning_rate=0.03, max_depth=5, 
            min_child_samples=40, random_state=42, verbose=-1, objective='huber'
        )
        self.model = MultiOutputRegressor(base_model)
        
        self.feature_cols: List[str] = []
        self.latest_states: Dict[str, float] = {} 
        self.latest_confidence: Dict[str, float] = {} 
        self.latest_features: Dict[str, float] = {}
        
        self.ema_states: Dict[str, float] = {}
        self.ema_alpha: float = 0.15

    def fit(self, data: pd.DataFrame) -> None:
        df_feat = self.feature_builder.process(data, is_training=True)
        exclude = ["Open", "High", "Low", "Close", "Volume"] + self.target_cols
        self.feature_cols = [c for c in df_feat.columns if c not in exclude and not c.startswith("Close_")]
        
        train_data = df_feat.dropna(subset=self.target_cols + self.feature_cols).iloc[-self.train_window:]
        self.model.fit(train_data[self.feature_cols], train_data[self.target_cols].values)
        self.is_fitted = True

    def update(self, data: pd.DataFrame, candidate_mode: bool = False, candidate_name: Optional[str] = None) -> None:
        if not self.is_fitted:
            self.fit(data)
            return

        df_feat = self.feature_builder.process(data, is_training=True)
        train_data = df_feat.dropna(subset=self.target_cols + self.feature_cols).iloc[-self.train_window:]
        
        if len(train_data) < 50: return
        X_train = train_data[self.feature_cols]
        y_train = train_data[self.target_cols].values

        if candidate_mode:
            candidate = copy.deepcopy(self)
            candidate.name = candidate_name if candidate_name else f"{self.name}_Candidate"
            candidate.model.fit(X_train, y_train)
            candidate.is_fitted = True
            save_dir = "models/v1/candidates"
            if not os.path.exists(save_dir): os.makedirs(save_dir)
            candidate.save(save_dir)
            return

        self.model.fit(X_train, y_train)
        self.is_fitted = True

    def predict(self, data: pd.DataFrame) -> Dict[str, Any]:
        if not self.is_fitted: return {}
            
        df_feat = self.feature_builder.process(data, is_training=False)
        latest_X = df_feat[self.feature_cols].iloc[-1:]
        self.latest_features = latest_X.to_dict('records')[0]
        
        preds = self.model.predict(latest_X)[0]
        preds = np.clip(preds, -1.0, 1.0)
        
        self.latest_states = {}
        self.latest_confidence = {}
        
        for idx, target_name in enumerate(self.target_cols):
            pred_val = float(preds[idx])
            self.latest_states[target_name] = pred_val
            self.latest_confidence[target_name] = abs(pred_val)
            
            if target_name not in self.ema_states:
                self.ema_states[target_name] = pred_val
            else:
                self.ema_states[target_name] = (self.ema_states[target_name] * (1 - self.ema_alpha)) + (pred_val * self.ema_alpha)
            
        return {"states": self.ema_states, "confidences": self.latest_confidence}

    def get_raw_macro_vector(self) -> Dict[str, float]:
        if not self.ema_states: return {}
        
        g = self.ema_states.get("macro_growth", 0.0)
        i = self.ema_states.get("macro_inflation", 0.0)
        l = self.latest_states.get("macro_liquidity", 0.0) 
        s = self.ema_states.get("macro_stress", 0.0)

        base = {"equity": 0.40, "income": 0.15, "bond": 0.35, "commodity": 0.05, "cash": 0.05}
        
        inc_tilt = (g * 0.10) + (i * 0.10) + (l * 0.05) - (max(0, s) * 0.10)
        eq_tilt = (g * 0.30) + (l * 0.20) - (max(0, s) * 0.35) - (i * 0.10)
        bd_tilt = -(g * 0.10) - (i * 0.20) + (max(0, s) * 0.20)
        cmd_tilt = (i * 0.20) + (max(0, s) * 0.05) - (g * 0.05)
        
        cash_tilt = max(0.0, np.tanh(s * 2.0)) * 0.35 - (l * 0.05)
        
        # 🌟 1. Cash를 독립 변수(Policy Variable)로 먼저 확정
        cash_target = max(0.05, min(1.0, base["cash"] + cash_tilt))
        
        # 🌟 2. 남은 위험자산 풀 (Risk Budget) 계산
        risky_budget = 1.0 - cash_target

        risky_vector = {
            "equity": max(0.0, base["equity"] + eq_tilt),
            "income": max(0.0, base["income"] + inc_tilt),
            "bond": max(0.0, base["bond"] + bd_tilt),
            "commodity": max(0.0, base["commodity"] + cmd_tilt)
        }
        
        # 🌟 3. 위험자산 내부 정규화 (risky_budget 안에서만 경쟁)
        risky_sum = sum(risky_vector.values())
        if risky_sum > 0:
            for k in risky_vector:
                risky_vector[k] = (risky_vector[k] / risky_sum) * risky_budget
        else:
            # 모든 위험자산 0 처리 (극단적 현금화 시)
            for k in risky_vector:
                risky_vector[k] = 0.0
                
        # 🌟 4. 최종 벡터 조립
        vector = risky_vector.copy()
        vector["cash_target"] = cash_target
        
        vector["kr_equity_ratio"] = max(0.0, min(1.0, 0.20 + (l * 0.15)))
        vector["us_equity_ratio"] = 1.0 - vector["kr_equity_ratio"]
        vector["fx_hedge_ratio"] = max(0.0, min(1.0, 0.50 - (s * 0.30)))
        
        return vector

    def print_diagnostics(self) -> None:
        if not self.ema_states: return
        print("\n🧠 [Latent Macro State Diagnostics (EMA Smoothed)]")
        for target, val in self.ema_states.items():
            conf = self.latest_confidence.get(target, 0.0)
            bar_len = 12
            filled = int((val + 1.0) / 2.0 * bar_len) 
            filled = max(0, min(bar_len, filled))
            bar = "[" + "=" * filled + " " * (bar_len - filled) + "]"
            print(f"  └ {target[6:].upper():<10} | EMA Score: {val:+.3f} {bar} | Conf: {conf:.2f}")

    def analyze_factor_correlation(self, df: pd.DataFrame) -> None:
        labels = self.feature_builder.build_labels(df).dropna(subset=self.target_cols)
        corr_matrix = labels[self.target_cols].corr()
        print("\n" + "="*50)
        print("🔍 [Macro Factor Correlation Diagnostics]")
        print("="*50)
        print(corr_matrix.round(3))

    def get_signal(self) -> float: return 1.0