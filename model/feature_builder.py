# model/feature_builder.py

import pandas as pd
import numpy as np
from .base_model import BaseFeatureBuilder

# model/feature_builder.py
class AdvancedMacroRegimeBuilder(BaseFeatureBuilder):
    def __init__(self, target_window=60):
        self.target_window = target_window

    def build_features(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        
        # 주가 파생 피처
        out["spy_return_60d"] = out["Close_SPY"].pct_change(60, fill_method=None)
        rolling_max = out["Close_SPY"].rolling(252, min_periods=1).max()
        out["spy_drawdown"] = out["Close_SPY"] / rolling_max - 1
        
        # 리더십 피처 (QQQ, EWY) Z-score
        if "Close_QQQ" in out.columns and "Close_SPY" in out.columns:
            ratio = out["Close_QQQ"] / out["Close_SPY"]
            out["qqq_spy_mom_20d"] = ratio.pct_change(20, fill_method=None)
            out["qqq_spy_z_252d"] = (ratio - ratio.rolling(252).mean()) / (ratio.rolling(252).std() + 1e-6)

        if "Close_EWY" in out.columns and "Close_SPY" in out.columns:
            ratio = out["Close_EWY"] / out["Close_SPY"]
            out["ewy_spy_mom_20d"] = ratio.pct_change(20, fill_method=None)
            out["ewy_spy_z_252d"] = (ratio - ratio.rolling(252).mean()) / (ratio.rolling(252).std() + 1e-6)

        out.replace([np.inf, -np.inf], np.nan, inplace=True)
        return out

    def build_labels(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        # 🌟 Binary Label 제거 -> 순수 기대수익률(Continuous Target)로 변경
        past_returns = df["Close_SPY"].pct_change(self.target_window, fill_method=None)
        out["target_return"] = past_returns.shift(-self.target_window)
        return out

class LatentStressFeatureBuilder(BaseFeatureBuilder):
    def build_features(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        
        # 🌟 기초 구조적 취약성 (Fragility Core)
        # A. 시장 폭 (RSP vs SPY)
        if "Close_RSP" in out.columns and "Close_SPY" in out.columns:
            out["breadth_mom"] = (out["Close_RSP"] / out["Close_SPY"]).pct_change(20, fill_method=None)
        # B. 변동성 기간 구조 (VIX vs VIX3M)
        if "Close_^VIX" in out.columns and "Close_^VIX3M" in out.columns:
            out["vix_term"] = out["Close_^VIX"] / out["Close_^VIX3M"]
        # C. 신용 스프레드 (LQD vs HYG)
        if "Close_LQD" in out.columns and "Close_HYG" in out.columns:
            out["credit_spread"] = (out["Close_LQD"] / out["Close_HYG"]).pct_change(20, fill_method=None)
            
        # 당일 긴급 충격
        if "Open_SPY" in out.columns and "Close_SPY" in out.columns:
            out["gap_shock"] = (out["Open_SPY"] / out["Close_SPY"].shift(1)) - 1
        if "Close_^VIX" in out.columns:
            out["vix_jump"] = out["Close_^VIX"].pct_change(fill_method=None)

        out.replace([np.inf, -np.inf], np.nan, inplace=True)
        return out

    def build_labels(self, df: pd.DataFrame) -> pd.DataFrame:
        return df