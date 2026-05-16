import pandas as pd
import numpy as np
from typing import List, Optional
from .base_model import BaseFeatureBuilder

class AdvancedMacroRegimeBuilder(BaseFeatureBuilder):
    """
    자산의 직접적인 비중이 아닌, 4대 거시 경제 팩터의 강도(-1.0 ~ 1.0)를 정답지로 생성합니다.
    """
    
    def __init__(self, target_window: int = 20) -> None:
        self.target_window: int = target_window
        self.max_window: int = target_window

    def build_features(self, df: pd.DataFrame) -> pd.DataFrame:
        out: pd.DataFrame = df.copy()
        
        if "Close_SPY" in out.columns:
            out["spy_return_60d"] = out["Close_SPY"].pct_change(60, fill_method=None)
            rolling_max = out["Close_SPY"].rolling(252, min_periods=1).max()
            out["spy_drawdown"] = out["Close_SPY"] / rolling_max - 1
            out["momentum_acceleration"] = out["Close_SPY"].pct_change(20, fill_method=None) - (out["spy_return_60d"] / 3) 
            
        if "Close_^VIX" in out.columns and "Close_^VIX3M" in out.columns:
            out["vix_level"] = out["Close_^VIX"]
            out["vix_term_structure"] = out["Close_^VIX3M"] - out["Close_^VIX"]
            
        if "Close_HYG" in out.columns and "Close_LQD" in out.columns:
            out["credit_spread_ratio"] = out["Close_HYG"] / out["Close_LQD"]
            
        if "Close_DX-Y.NYB" in out.columns:
            out["dxy_momentum_20d"] = out["Close_DX-Y.NYB"].pct_change(20, fill_method=None)

        if "Close_^IRX" in out.columns and "Close_^TNX" in out.columns:
            out["yield_curve_spread"] = out["Close_^TNX"] - out["Close_^IRX"]

        out.replace([np.inf, -np.inf], np.nan, inplace=True)
        return out

    def build_labels(self, df: pd.DataFrame) -> pd.DataFrame:
        out: pd.DataFrame = df.copy()
        w: int = self.target_window
        
        future_spy = df["Close_SPY"].pct_change(w, fill_method=None).shift(-w)
        future_lqd = df["Close_LQD"].pct_change(w, fill_method=None).shift(-w) if "Close_LQD" in df.columns else future_spy * 0
        future_gld = df["Close_GLD"].pct_change(w, fill_method=None).shift(-w) if "Close_GLD" in df.columns else future_spy * 0
        future_krw = df["Close_KRW=X"].pct_change(w, fill_method=None).shift(-w) if "Close_KRW=X" in df.columns else future_spy * 0
        future_vix = df["Close_^VIX"].pct_change(w, fill_method=None).shift(-w) if "Close_^VIX" in df.columns else future_spy * 0
        
        def get_continuous_target(spread: pd.Series, rolling_window: int = 252, z_scaler: float = 2.0) -> pd.Series:
            z_score = (spread - spread.rolling(rolling_window).mean()) / spread.rolling(rolling_window).std()
            smoothed_z = z_score.rolling(3).mean() 
            target = np.tanh(smoothed_z / z_scaler) 
            target[spread.isna()] = np.nan
            return target

        # 🌟 4대 매크로 팩터 타겟 생성
        out["macro_growth"] = get_continuous_target(future_spy - future_lqd)
        out["macro_inflation"] = get_continuous_target(future_gld - future_lqd)
        out["macro_liquidity"] = get_continuous_target(-future_krw)
        out["macro_stress"] = get_continuous_target(future_vix, z_scaler=1.5)

        return out