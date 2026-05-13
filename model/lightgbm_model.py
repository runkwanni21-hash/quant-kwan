import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.calibration import CalibratedClassifierCV
from sklearn.model_selection import TimeSeriesSplit
from .base_model import QuantitativeModel

class LightGBMMultiRegimeModel(QuantitativeModel):
    def __init__(self, name="LGBM_Macro_AI", train_window=2000, target_col="target_regime", feature_builder=None, calib_method="sigmoid"):
        super().__init__(name)
        self.train_window = train_window
        self.target_col = target_col
        self.feature_builder = feature_builder
        
        base_model = lgb.LGBMClassifier(
            n_estimators=150, learning_rate=0.05, max_depth=5, 
            min_child_samples=40, random_state=42, verbose=-1,
            class_weight='balanced'
        )
        
        tscv = TimeSeriesSplit(n_splits=5)
        self.model = CalibratedClassifierCV(estimator=base_model, method=calib_method, cv=tscv)
        
        self.feature_cols = []
        self.latest_probs = {}
        self.latest_confidence = 0.0

    def fit(self, data: pd.DataFrame):
        df_feat = self.feature_builder.process(data, is_training=True)
        exclude = ["Open", "High", "Low", "Close", "Volume", "target_regime"]
        self.feature_cols = [c for c in df_feat.columns if c not in exclude and not c.startswith("Close_")]
        
        train_data = df_feat.dropna(subset=[self.target_col] + self.feature_cols).iloc[-self.train_window:]
        X_train = train_data[self.feature_cols]
        y_train = train_data[self.target_col].values
        
        self.model.fit(X_train, y_train)
        self.is_fitted = True

    def predict(self, data: pd.DataFrame) -> dict:
        if not self.is_fitted: 
            return {"Bear_Prob": 0.33, "Neutral_Prob": 0.34, "Bull_Prob": 0.33}
            
        df_feat = self.feature_builder.process(data, is_training=False)
        latest_X = df_feat[self.feature_cols].iloc[-1:]
        
        probs = self.model.predict_proba(latest_X)[0]
        self.latest_probs = {
            "Bear_Prob": probs[0], 
            "Neutral_Prob": probs[1], 
            "Bull_Prob": probs[2]
        }
        
        sorted_probs = sorted(probs, reverse=True)
        self.latest_confidence = sorted_probs[0] - sorted_probs[1]
        
        return self.latest_probs

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