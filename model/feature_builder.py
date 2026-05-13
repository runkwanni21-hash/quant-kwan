import pandas as pd
import numpy as np
from .base_model import BaseFeatureBuilder

class AdvancedMacroRegimeBuilder(BaseFeatureBuilder):
    def __init__(self, target_window=60):
        # 느린 방향성을 위해 타겟을 다시 60일(약 3개월)로 설정
        self.target_window = target_window

    # model/feature_builder.py의 AdvancedMacroRegimeBuilder 수정

    def build_features(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        
        # 1. 주가 장기 파생 피처 (SPY)
        out["spy_return_60d"] = out["Close_SPY"].pct_change(60, fill_method=None)
        rolling_max = out["Close_SPY"].rolling(252, min_periods=1).max()
        out["spy_drawdown"] = out["Close_SPY"] / rolling_max - 1
        
        # 2. 모멘텀 가속도
        spy_mom_20d = out["Close_SPY"].pct_change(20, fill_method=None)
        out["momentum_acceleration"] = spy_mom_20d - (out["spy_return_60d"] / 3) 

        # 🌟 3. 글로벌 리더십 직교 피처 (비정상성 제거: Momentum & Z-Score)
        if "Close_QQQ" in out.columns and "Close_SPY" in out.columns:
            # 원본 비율은 중간 변수로만 사용 (저장 안 함)
            ratio_qqq_spy = out["Close_QQQ"] / out["Close_SPY"]
            out["qqq_spy_mom_20d"] = ratio_qqq_spy.pct_change(20, fill_method=None)
            out["qqq_spy_z_252d"] = (ratio_qqq_spy - ratio_qqq_spy.rolling(252).mean()) / (ratio_qqq_spy.rolling(252).std() + 1e-6)

        if "Close_EWY" in out.columns and "Close_SPY" in out.columns:
            ratio_ewy_spy = out["Close_EWY"] / out["Close_SPY"]
            out["ewy_spy_mom_20d"] = ratio_ewy_spy.pct_change(20, fill_method=None)
            out["ewy_spy_z_252d"] = (ratio_ewy_spy - ratio_ewy_spy.rolling(252).mean()) / (ratio_ewy_spy.rolling(252).std() + 1e-6)

        if "Close_EWY" in out.columns and "Close_QQQ" in out.columns:
            ratio_ewy_qqq = out["Close_EWY"] / out["Close_QQQ"]
            out["ewy_qqq_mom_20d"] = ratio_ewy_qqq.pct_change(20, fill_method=None)
            out["ewy_qqq_z_252d"] = (ratio_ewy_qqq - ratio_ewy_qqq.rolling(252).mean()) / (ratio_ewy_qqq.rolling(252).std() + 1e-6)

        # 4. 거시경제 / 금리 환경
        if "Close_^IRX" in out.columns and "Close_^TNX" in out.columns:
            out["yield_curve_spread"] = out["Close_^TNX"] - out["Close_^IRX"]
            
        # 5. 대체자산 트렌드
        if "Close_GLD" in out.columns:
            out["gold_momentum_60d"] = out["Close_GLD"].pct_change(60, fill_method=None)
        if "Close_USO" in out.columns:
            out["oil_momentum_60d"] = out["Close_USO"].pct_change(60, fill_method=None)

        out.replace([np.inf, -np.inf], np.nan, inplace=True)
        return out

    def build_labels(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        past_returns = df["Close_SPY"].pct_change(self.target_window, fill_method=None)
        
        # Leakage 방지를 위해 전일자 분포 기준 산출
        historical_dist = past_returns.shift(1)
        
        bull_thresh = historical_dist.rolling(756, min_periods=252).quantile(0.65)
        bear_thresh = historical_dist.rolling(756, min_periods=252).quantile(0.35)
        
        future_return = past_returns.shift(-self.target_window)
        
        out["target_regime"] = np.where(
            future_return >= bull_thresh, 2,  
            np.where(future_return <= bear_thresh, 0, 1)  
        )
        return out
    
# model/feature_builder.py 하단에 추가

class LatentStressFeatureBuilder(BaseFeatureBuilder):
    def build_features(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        
        # 🌟 ffill과 fillna(0) 완전 제거! 순수 변동성(Latent Signal) 보존
        if "Close_RSP" in out.columns and "Close_SPY" in out.columns:
            out["breadth_mom"] = (out["Close_RSP"] / out["Close_SPY"]).pct_change(20, fill_method=None)
            
        if "Close_^VIX" in out.columns and "Close_^VIX3M" in out.columns:
            out["vix_term"] = out["Close_^VIX"] / out["Close_^VIX3M"]
            
        if "Close_LQD" in out.columns and "Close_SPY" in out.columns:
            spy_ret = out["Close_SPY"].pct_change(fill_method=None)
            lqd_ret = out["Close_LQD"].pct_change(fill_method=None)
            out["stock_bond_corr"] = spy_ret.rolling(20).corr(lqd_ret)
            
        if "Close_LQD" in out.columns and "Close_HYG" in out.columns:
            out["credit_spread"] = (out["Close_LQD"] / out["Close_HYG"]).pct_change(20, fill_method=None)

        # 당일 충격 이벤트
        if "Open_SPY" in out.columns and "Close_SPY" in out.columns:
            out["gap_shock"] = (out["Open_SPY"] / out["Close_SPY"].shift(1)) - 1
            
        if "Close_^VIX" in out.columns:
            out["vix_jump"] = out["Close_^VIX"].pct_change(fill_method=None)

        # 무한대만 조용히 NaN으로 처리 (이후 Model 단에서 dropna 수행)
        out.replace([np.inf, -np.inf], np.nan, inplace=True)
        return out

    def build_labels(self, df: pd.DataFrame) -> pd.DataFrame:
        return df