import pandas as pd
import numpy as np
import lightgbm as lgb
from .base_model import QuantitativeModel

# model/lightgbm_model.py 전체 교체본

import pandas as pd
import numpy as np
import lightgbm as lgb
from .base_model import QuantitativeModel

class LightGBMMultiRegimeModel(QuantitativeModel): 
    # 🌟 단, 기본 타겟 컬럼은 분류용(target_regime)에서 회귀용(target_return)으로 몰래 바꿉니다.
    def __init__(self, name="LGBM_Macro_AI", train_window=2000, target_col="target_return", feature_builder=None):
        super().__init__(name)
        self.train_window = train_window
        self.target_col = target_col
        self.feature_builder = feature_builder
        
        # 🌟 이름은 MultiRegime이지만, 엔진은 LGBMRegressor (연속형) 장착!
        self.model = lgb.LGBMRegressor(
            n_estimators=100, 
            learning_rate=0.05, 
            max_depth=4, 
            min_child_samples=10, 
            objective='huber', # 아웃라이어 방어용 (퀀트 필수)
            random_state=42, 
            verbose=-1,
            importance_type='gain'
        )
        self.feature_cols = []

    def fit(self, data: pd.DataFrame):
        df_feat = self.feature_builder.process(data, is_training=True)
        exclude = ["Open", "High", "Low", "Close", "Volume", "target_return"]
        self.feature_cols = [c for c in df_feat.columns if c not in exclude and not c.startswith("Close_")]
        
        # 라벨이 존재하는 구간만 추출 (최근 60일은 미래 수익률이 없으므로 자동 제외)
        train_data = df_feat.dropna(subset=[self.target_col]).iloc[-self.train_window:]
        
        if len(train_data) < 50:
            print(f"⚠️ [{self.name}] 학습 실패: 유효 데이터 부족")
            self.is_fitted = False
            return

        self.model.fit(
            train_data[self.feature_cols], 
            train_data[self.target_col].values
        )
        self.is_fitted = True

    def predict(self, data: pd.DataFrame) -> dict:
        default_resp = {"expected_return": 0.0}
        
        if not self.is_fitted: return default_resp

        try:
            df_feat = self.feature_builder.process(data, is_training=False)
            latest_X = df_feat[self.feature_cols].iloc[-1:]
            
            # 예측: 60일 미래 기대 수익률 추정치
            pred_return = self.model.predict(latest_X)[0]
            
            return {"expected_return": float(pred_return)}
        except Exception as e:
            print(f"🚨 [{self.name}] 예측 중 치명적 에러 발생: {e}")
            return default_resp

    def predict_batch(self, data: pd.DataFrame, smoothing_span=3) -> pd.DataFrame:
        if not self.is_fitted: return pd.DataFrame()
        
        df_feat = self.feature_builder.process(data, is_training=False)
        X_test = df_feat[self.feature_cols]
        
        probs = self.model.predict_proba(X_test)
        prob_df = pd.DataFrame(probs, columns=["Bear_Prob", "Neutral_Prob", "Bull_Prob"], index=X_test.index)
        
        prob_df = prob_df.ewm(span=smoothing_span, adjust=False).mean()
        
        def calc_margin(row):
            sorted_probs = sorted(row.values, reverse=True)
            return sorted_probs[0] - sorted_probs[1]
            
        prob_df["Confidence_Margin"] = prob_df[["Bear_Prob", "Neutral_Prob", "Bull_Prob"]].apply(calc_margin, axis=1)
        
        latest_row = prob_df.iloc[-1]
        self.latest_probs = {"Bear_Prob": latest_row["Bear_Prob"], "Neutral_Prob": latest_row["Neutral_Prob"], "Bull_Prob": latest_row["Bull_Prob"]}
        self.latest_confidence = latest_row["Confidence_Margin"]
        return prob_df

    def get_signal(self, min_confidence=0.10):
        if not self.latest_probs: return 1
        max_state = max(self.latest_probs, key=self.latest_probs.get)
        if self.latest_confidence < min_confidence: return 1 
        if max_state == "Bull_Prob": return 2
        elif max_state == "Bear_Prob": return 0
        else: return 1