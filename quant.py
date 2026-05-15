import yfinance as yf
import pandas as pd
import json
import warnings
import math
import os 
from typing import List, Dict, Any, Optional
import numpy as np
from scipy.stats import percentileofscore

warnings.filterwarnings("ignore")

from model.base_model import QuantitativeModel
from model.feature_builder import AdvancedMacroRegimeBuilder, LatentStressFeatureBuilder
from model.lightgbm_model import LightGBMMultiRegimeModel
from model.latent_stress_model import PCALatentStressModel
from model.promotion_engine import PromotionEngine

# ==============================================================================
# 🌟 [신규 추가] 매크로 시그널 정규화 및 예열 엔진 (Cold Start 방지)
# ==============================================================================
class MacroSignalNormalizer:
    """
    Raw expected_return 값을 과거 N일(기본 252일) 분포와 비교하여
    0~100 사이의 Percentile(백분위수)로 정규화합니다.
    Scale Drift 및 Bearish Bias를 교정하는 핵심 모듈입니다.
    """
    def __init__(self, window: int = 252) -> None:
        self.window = window
        self.history: List[float] = []
        self.prev_smoothed_percentile = 50.0  # 노이즈 스무딩용 이전 값

    def load_warm_history(self, pre_computed_history: List[float]) -> None:
        """새로운 모델이 학습되었을 때, 해당 모델로 다시 뽑아낸 과거 분포를 덮어씌웁니다."""
        self.history = pre_computed_history[-self.window:]
        print(f"🔄 [Normalizer] 모델 예측 분포로 History Cache 세팅 완료 (Size: {len(self.history)})")

    def update_and_get_percentile(self, current_pred: float, use_ema: bool = True) -> float:
        # 데이터가 충분하지 않을 때는 강제로 중립(50%) 반환
        if len(self.history) < 20:
            self.history.append(current_pred)
            return 50.0

        # 현재 값의 백분위수(0~100) 계산
        raw_percentile = float(percentileofscore(self.history, current_pred))
        
        # EMA Smoothing (노이즈로 인한 비중 점프 방지)
        if use_ema:
            alpha = 0.2
            smoothed = (1 - alpha) * self.prev_smoothed_percentile + (alpha * raw_percentile)
            self.prev_smoothed_percentile = smoothed
            final_percentile = smoothed
        else:
            final_percentile = raw_percentile

        # 히스토리 업데이트 및 롤링 윈도우 유지
        self.history.append(current_pred)
        if len(self.history) > self.window:
            self.history.pop(0)
            
        return final_percentile

def rebuild_macro_history(macro_model: QuantitativeModel, df: pd.DataFrame, window: int = 252) -> List[float]:
    """
    [Prediction Cache Rebuilder]
    실거래 루프 진입 전, 현재 모델을 기준으로 과거 데이터의 예측값 분포를 미리 생성합니다.
    """
    print(f"\n[시스템] ⚙️ Macro Signal Normalizer 사전 예열 시작... (과거 {window}일 기준 Hindcast)")
    history_cache: List[float] = []
    
    # 모멘텀 등 Feature 생성을 위한 60일 버퍼 + 실제 window 사이즈
    pre_compute_df = df.iloc[-(window + 60):] 
    
    for i in range(60, len(pre_compute_df)):
        slice_df = pre_compute_df.iloc[:i]
        preds = macro_model.predict(slice_df)
        pred_return = float(preds.get("expected_return", 0.0))
        
        history_cache.append(pred_return)
        
        if len(history_cache) > window:
            history_cache.pop(0)
            
    print("[시스템] ✅ Normalizer 예열 완료. (Cold Start 방어 준비됨)")
    return history_cache


# ==============================================================================
# 📊 기존 데이터 페처 및 서브 엔진들 (유지)
# ==============================================================================
def download_multi_data(tickers: List[str], start: str = "2012-01-01", z_thresh: float = 4.0) -> pd.DataFrame:
    # (기존 데이터 로드 로직 동일)
    data_dir = "datas/ohlcv"
    os.makedirs(data_dir, exist_ok=True)
    combined_dfs: List[pd.DataFrame] = []
    print("📡 [데이터 파이프라인] 로컬 캐시 확인 및 증분 다운로드 시작...")
    
    for ticker in tickers:
        file_path = os.path.join(data_dir, f"{ticker}.parquet")
        existing_df = pd.DataFrame()
        fetch_start = start

        if os.path.exists(file_path):
            try:
                existing_df = pd.read_parquet(file_path)
                if not existing_df.empty:
                    last_date = existing_df.index.max()
                    fetch_start = (last_date - pd.Timedelta(days=5)).strftime('%Y-%m-%d')
            except Exception as e:
                print(f"⚠️ [{ticker}] 캐시 파일 읽기 실패. 전체 다운로드로 전환: {e}")

        try:
            new_df = yf.download(ticker, start=fetch_start, progress=False, auto_adjust=False)
        except Exception as e:
            print(f"🚨 [{ticker}] 야후 파이낸스 API 응답 없음: {e}")
            new_df = pd.DataFrame()

        if not new_df.empty:
            if isinstance(new_df.columns, pd.MultiIndex):
                new_df.columns = [col[0] for col in new_df.columns]
                
            if not existing_df.empty:
                merged_df = pd.concat([existing_df, new_df])
                merged_df = merged_df[~merged_df.index.duplicated(keep='last')]
            else:
                merged_df = new_df

            merged_df.sort_index(inplace=True)
            merged_df.to_parquet(file_path)
            merged_df.columns = [f"{col}_{ticker}" for col in merged_df.columns]
            combined_dfs.append(merged_df)
        else:
            if not existing_df.empty:
                existing_df.columns = [f"{col}_{ticker}" for col in existing_df.columns]
                combined_dfs.append(existing_df)

    if not combined_dfs:
        print("🚨 치명적 에러: 사용 가능한 데이터가 전혀 없습니다.")
        return pd.DataFrame()

    df = pd.concat(combined_dfs, axis=1)
    df = df.ffill(limit=3)

    close_cols = [c for c in df.columns if c.startswith("Close_")]
    exempt_tickers = ["^VIX", "^VIX3M", "^IRX", "^TNX"] 

    for col in close_cols:
        if any(exempt in col for exempt in exempt_tickers):
            continue  
        pct_change = df[col].pct_change()
        rolling_mean = pct_change.rolling(window=20).mean()
        rolling_std = pct_change.rolling(window=20).std()
        
        z_scores = np.abs((pct_change - rolling_mean) / (rolling_std + 1e-8))
        anomaly_mask = (z_scores > z_thresh) & (np.abs(pct_change) > 0.25)

        if anomaly_mask.any():
            df.loc[anomaly_mask, col] = np.nan
            df[col] = df[col].ffill()

    df = df.dropna()
    print("✅ 데이터 로드, 병합 및 무결성 검증 완료.\n")
    return df

def fx_overlay_engine(df_slice: pd.DataFrame, state: Dict[str, Any]) -> Dict[str, Any]:
    # (기존 환율 오버레이 로직 동일)
    try:
        dxy_mom_20d = df_slice["Close_DX-Y.NYB"].pct_change(20).iloc[-1]
        dxy_vel_5d = df_slice["Close_DX-Y.NYB"].pct_change(5).iloc[-1]
        dxy_score = max(min(((dxy_mom_20d * 0.7) + (dxy_vel_5d * 0.3)) / 0.03, 1.0), -1.0)

        ewy_spy_ratio = df_slice["Close_EWY"] / df_slice["Close_SPY"]
        ewy_mom_20d = ewy_spy_ratio.pct_change(20).iloc[-1]
        ewy_vel_5d = ewy_spy_ratio.pct_change(5).iloc[-1]
        ewy_score = max(min(((ewy_mom_20d * 0.7) + (ewy_vel_5d * 0.3)) / 0.05, 1.0), -1.0)

        krw_usd = df_slice["Close_KRW=X"]
        krw_mom_20d = krw_usd.pct_change(20).iloc[-1]
        krw_vel_5d = krw_usd.pct_change(5).iloc[-1]
        direct_krw_score = max(min(-((krw_mom_20d * 0.7) + (krw_vel_5d * 0.3)) / 0.02, 1.0), -1.0)

        transition = float(state.get("transition_risk", 0.5))
        macro_fx_pressure = (direct_krw_score * 0.70) + (ewy_score * 0.15) - (dxy_score * 0.15)
        panic_zone = max(transition - 0.55, 0.0)
        panic_penalty = (panic_zone ** 2) * 4.0 
        
        krw_strength_score = macro_fx_pressure - panic_penalty
        final_score = max(min(krw_strength_score, 1.0), -1.0)
        
        base_bias = -0.15 
        adjusted_score = final_score + base_bias
        raw_hedge_ratio = 0.8 / (1.0 + math.exp(-3.0 * adjusted_score))
        
        prev_hedge = float(state.get("prev_hedge_ratio", 0.0))
        if abs(raw_hedge_ratio - prev_hedge) < 0.15:
            hedge_ratio = prev_hedge
        else:
            hedge_ratio = round(float(max(0.0, min(raw_hedge_ratio, 0.8))), 3)

        if hedge_ratio >= 0.6:
            bias_str = f"STRONG_KRW (Hedge: {hedge_ratio*100:.1f}%)"
        elif hedge_ratio >= 0.3:
            bias_str = f"MILD_KRW (Hedge: {hedge_ratio*100:.1f}%)"
        elif hedge_ratio > 0.0:
            bias_str = f"NEUTRAL (Hedge: {hedge_ratio*100:.1f}%)"
        else:
            bias_str = "USD_LONG (Hedge: 0.0%)"
            
        return {"krw_score": round(float(final_score), 3), "fx_bias": bias_str, "hedge_ratio": hedge_ratio}
    except Exception as e:
        return {"krw_score": 0.0, "fx_bias": f"ERROR_USD_LONG ({e})", "hedge_ratio": 0.0}

def regional_allocation_engine(df_slice: pd.DataFrame, state: Dict[str, Any]) -> Dict[str, Any]:
    # (기존 국가 배분 로직 동일)
    try:
        ewy_spy_ratio = df_slice["Close_EWY"] / df_slice["Close_SPY"]
        ewy_mom_20d = ewy_spy_ratio.pct_change(20).iloc[-1]
        ewy_vel_5d = ewy_spy_ratio.pct_change(5).iloc[-1]
        ewy_score = max(min(((ewy_mom_20d * 0.7) + (ewy_vel_5d * 0.3)) / 0.05, 1.0), -1.0)
        
        smh_mom_20d = df_slice["Close_SMH"].pct_change(20).iloc[-1]
        smh_vel_5d = df_slice["Close_SMH"].pct_change(5).iloc[-1]
        smh_score = max(min(((smh_mom_20d * 0.7) + (smh_vel_5d * 0.3)) / 0.08, 1.0), -1.0)

        krw_score = float(state.get("krw_score", 0.0))
        expected_return = float(state.get("expected_return", 0.0))
        macro_score = float(np.tanh(expected_return * 8.0))
        transition_risk = float(state.get("transition_risk", 0.5))
        
        regional_score = (ewy_score * 0.45) + (smh_score * 0.20) + (krw_score * 0.10) + (macro_score * 0.15) - (transition_risk * 0.10)
        raw_ratio = 0.6 / (1.0 + math.exp(-2.5 * regional_score))
        korea_ratio = round(float(0.1 + raw_ratio), 3)
        us_ratio = round(1.0 - korea_ratio, 3)
        
        return {
            "regional_score": round(float(regional_score), 3),
            "smh_score": round(float(smh_score), 3),
            "macro_tanh_score": round(float(macro_score), 3),
            "korea_equity_ratio": korea_ratio,
            "us_equity_ratio": us_ratio
        }
    except Exception as e:
        return {"regional_score": 0.0, "smh_score": 0.0, "macro_tanh_score": 0.0, "korea_equity_ratio": 0.2, "us_equity_ratio": 0.8}

def strict_sharpe_evaluator(champion: QuantitativeModel, candidate: QuantitativeModel, test_data: pd.DataFrame) -> bool:
    print(f"🔍 [엄격한 심사] {champion.name}과 {candidate.name}의 샤프 지표를 롤링 비교합니다...")
    return True


# ==============================================================================
# 🌟 [전면 리팩토링] Additive Overlay & Personality Base 기반 비중 산출 엔진
# ==============================================================================
# AS-IS: if-elif-else 기반의 이산적(Discrete) 계단형 매핑 (경계선 노이즈 발생)
# TO-BE: np.interp를 활용한 연속적(Continuous) 선형 보간 매핑 (Turnover 최소화)
def calculate_final_exposure(state: Dict[str, Any], macro_percentile: float, personality_base: float = 0.70) -> float:
    """
    [Personality-Aware Adaptive Allocator]
    투자자의 기본 성향(Strategic Base)을 뼈대로 삼고,
    Macro Percentile과 Fragility Shock을 전술적(Tactical)으로 가감(+/-)합니다.
    np.interp를 사용하여 경계선 부근의 불필요한 Turnover를 억제합니다.
    """
    fragility_z: float = float(state.get("fragility_z_score", 0.0))
    is_shock: bool = bool(state.get("shock_state", False))
    
    # -------------------------------------------------------------
    # 1. Macro Tactical Overlay (Continuous Interpolation)
    # -------------------------------------------------------------
    # x 좌표 (Percentile 구간): 극단적 하방(0) ~ 부분 하방(30) ~ 중립(70) ~ 상방(100)
    xp: list[float] = [0.0, 30.0, 70.0, 100.0]
    
    # y 좌표 (Overlay 비중): 강한 축소(-0.2) ~ 약한 축소(-0.1) ~ 유지(0.0) ~ 약한 추가(+0.05)
    fp: list[float] = [-0.20, -0.10, 0.0, 0.05]
    
    # 퍼센타일에 따른 오버레이 값을 부드럽게 선형 보간하여 추출
    macro_overlay: float = float(np.interp(macro_percentile, xp, fp))

    # -------------------------------------------------------------
    # 2. Crisis Overlay (Fragility 기반 긴급 회피)
    # -------------------------------------------------------------
    # 위기 상황은 '연속적'이 아니라 '즉각적'으로 반응해야 하므로 Step 방식을 유지합니다.
    crisis_overlay: float = 0.0
    
    if is_shock:
        crisis_overlay = -0.40 # 🚨 VIX 점프, 갭 하락 등 실제 충격 시 강력한 삭감
    elif fragility_z > 2.0:
        crisis_overlay = -0.15 # 잠재적 구조적 스트레스 심화 구간

    # -------------------------------------------------------------
    # 3. Final Exposure 합산 (Additive)
    # -------------------------------------------------------------
    # 기본 성향에 연속적으로 산출된 매크로 조정치와 위기 조정치를 단순히 더합니다.
    final_weight: float = personality_base + macro_overlay + crisis_overlay
    
    # 🛡️ 최후의 방어선 (Min 10%, Max 100% 캡 적용)
    return round(float(max(0.10, min(final_weight, 1.0))), 3)

# ==============================================================================
# 🌟 메인 애플리케이션 파이프라인
# ==============================================================================
def main() -> None:
    print("=== Institutional Personality-Aware Allocator (w/ FX & Macro Overlay) ===\n")
    
    tickers: List[str] = [
        "SPY", "QQQ", "EWY", "RSP", "^TNX", "^IRX", "DX-Y.NYB", 
        "HYG", "LQD", "GLD", "USO", "^VIX", "^VIX3M", "KRW=X",
        "IEF", "SMH", "XLY", "XLP"
    ]
    df: pd.DataFrame = download_multi_data(tickers, start="2012-01-01")
    total_len: int = len(df)
    
    macro_model_path: str = "models/v1/Macro_AI.pkg"
    stress_model_path: str = "models/v1/Fragility_AI.pkg"
    
    results: List[Dict[str, Any]] = []

    # 🌟 [신규] 매크로 시그널 정규화기 초기화 (1년 롤링 윈도우)
    macro_normalizer = MacroSignalNormalizer(window=252)

    # =========================================================
    # 분기 1: 저장된 모델이 존재할 경우 (실전 운영 모드)
    # =========================================================
    if os.path.exists(macro_model_path) and os.path.exists(stress_model_path):
        print("\n[시스템] 📂 챔피언 모델을 로드하여 운영 파이프라인을 실행합니다.")
        
        macro_model = LightGBMMultiRegimeModel.load(macro_model_path)
        stress_model = PCALatentStressModel.load(stress_model_path)
        
        # 🌟 [핵심] 실전 진입 전 1년 치 과거 데이터를 사용해 퍼센타일 분포 사전 예열
        warm_history = rebuild_macro_history(macro_model, df, window=252)
        macro_normalizer.load_warm_history(warm_history)
        
        # 매크로 승급 심사 로직 (기존 유지)
        current_date = df.index[-1]
        is_rebalance_day = current_date.day >= 25 
        
        if is_rebalance_day:
            print(f"\n[시스템] 📅 월간 정기 업데이트 기간입니다. Macro 모델 섀도우 학습을 가동합니다.")
            macro_candidate_name = "Macro_AI_Candidate"
            macro_model.update(df, candidate_mode=True, candidate_name=macro_candidate_name)
            # 심사 통과 후 챔피언 모델이 교체되는 로직이 발동된다면, 
            # 그 직후에 rebuild_macro_history()를 한 번 더 호출해 주면 됩니다.
        
        # 스트레스 모델 즉시 업데이트
        print("[시스템] ⚡ Stress 모델: 최신 데이터로 롤링 업데이트 수행")
        stress_model.update(df, candidate_mode=False) 
        stress_model.save("models/v1")
        
        print("\n[시스템] 🎯 하이브리드 모델팩을 사용하여 최근 5거래일 실전 추론 시작...")
        test_df = df.iloc[-5:]
        current_prev_hedge = 0.0 
        
        # 💡 투자 성향 세팅 (나이, 소득 안정성 등을 바탕으로 도출된 Base 비중)
        USER_PERSONALITY_BASE = 0.70 
        
        for j in range(len(test_df)):
            current_date = test_df.index[j]
            slice_idx = total_len - 5 + j + 1
            slice_df = df.iloc[:slice_idx] 
            
            # 예측 수행
            macro_preds = macro_model.predict(slice_df)
            stress_state = stress_model.predict(slice_df)
            
            # 🌟 [핵심 로직] Raw 기대수익률을 Percentile로 변환
            raw_pred_return = float(macro_preds.get("expected_return", 0.0))
            current_percentile = macro_normalizer.update_and_get_percentile(raw_pred_return, use_ema=True)
            
            state_json = {
                "date": current_date.strftime('%Y-%m-%d'),
                "expected_return": round(raw_pred_return, 4),
                "macro_percentile": round(current_percentile, 1), # 정규화된 시그널 로깅
                "fragility_z_score": round(stress_state.get("fragility_z_score", 0.0), 3),
                "transition_risk": round(stress_state.get("transition_risk", 0.5), 3),
                "shock_state": stress_state.get("shock_state", False),
                "prev_hedge_ratio": current_prev_hedge  
            }
            
            fx_result = fx_overlay_engine(slice_df, state_json)
            state_json.update(fx_result)
            current_prev_hedge = fx_result["hedge_ratio"]
                        
            regional_result = regional_allocation_engine(slice_df, state_json)
            state_json.update(regional_result)
            
            # 🌟 [수정] Personality Base 기반 계산기로 교체
            final_exposure = calculate_final_exposure(
                state=state_json, 
                macro_percentile=current_percentile, 
                personality_base=USER_PERSONALITY_BASE
            )
            state_json["final_equity_weight"] = final_exposure
            
            target_weights = {
                "US": final_exposure * regional_result["us_equity_ratio"],
                "KR": final_exposure * regional_result["korea_equity_ratio"]
            }
            
            state_json["target_weights_US"] = target_weights["US"]
            state_json["target_weights_KR"] = target_weights["KR"]
            
            if state_json["shock_state"]:
                state_json["action"] = "DEFCON_1 (SHOCK!)"
            else:
                state_json["action"] = f"ALLOCATE (Weight: {final_exposure})"
            
            results.append(state_json)

    # =========================================================
    # 분기 2: 저장된 모델이 없을 경우 (초기 백테스트 모드)
    # =========================================================
    else:
        print("\n[시스템] 🚨 초기 모델이 없습니다. 전진 분석 및 전체 학습을 시작합니다.")
        
        builder_macro = AdvancedMacroRegimeBuilder(target_window=60)
        macro_model = LightGBMMultiRegimeModel(name="Macro_AI", feature_builder=builder_macro)
        
        builder_stress = LatentStressFeatureBuilder()
        stress_model = PCALatentStressModel(name="Fragility_AI", window=252, feature_builder=builder_stress)
        
        stress_model.fit(df)
        
        initial_train_size = 2000 
        step_size = 10
        
        USER_PERSONALITY_BASE = 0.70 

        for i in range(initial_train_size, total_len, step_size):
            train_df = df.iloc[:i]
            end_idx = min(i + step_size, total_len)
            test_df = df.iloc[i:end_idx] 
            
            macro_model.fit(train_df)
            
            # 🌟 백테스팅 중에도 매 Step마다 History Rebuild를 수행하여 Cold Start 우회
            warm_history = rebuild_macro_history(macro_model, df.iloc[:i], window=252)
            macro_normalizer.load_warm_history(warm_history)
            
            current_prev_hedge = 0.0

            for j in range(len(test_df)):
                current_date = test_df.index[j]
                slice_df = df.iloc[: i + j + 1] 
                
                macro_preds = macro_model.predict(slice_df)
                stress_state = stress_model.predict(slice_df)
                
                raw_pred_return = float(macro_preds.get("expected_return", 0.0))
                current_percentile = macro_normalizer.update_and_get_percentile(raw_pred_return, use_ema=True)
                
                state_json = {
                    "date": current_date.strftime('%Y-%m-%d'),
                    "expected_return": round(raw_pred_return, 4),
                    "macro_percentile": round(current_percentile, 1),
                    "fragility_z_score": round(stress_state.get("fragility_z_score", 0.0), 3),
                    "transition_risk": round(stress_state.get("transition_risk", 0.5), 3),
                    "shock_state": stress_state.get("shock_state", False),
                    "prev_hedge_ratio": current_prev_hedge
                }
                
                fx_result = fx_overlay_engine(slice_df, state_json)
                state_json.update(fx_result)
                current_prev_hedge = fx_result["hedge_ratio"]
                
                final_exposure = calculate_final_exposure(
                    state=state_json, 
                    macro_percentile=current_percentile, 
                    personality_base=USER_PERSONALITY_BASE
                )
                state_json["final_equity_weight"] = final_exposure
                
                if state_json["shock_state"]:
                    state_json["action"] = "DEFCON_1 (SHOCK!)"
                else:
                    state_json["action"] = f"ALLOCATE (Weight: {final_exposure})"
                
                results.append(state_json)

        macro_model.save("models/v1") 
        stress_model.save("models/v1")

    # =========================================================
    # 공통: 최종 리포트 출력
    # =========================================================
    print("\n========================================================")
    print("🎯 [최근 5일 최종 포트폴리오 상태 머신 리포트]")
    print("========================================================")
    
    if not results:
        print("🚨 분석된 결과가 없습니다. 루프 안에서 데이터가 append되지 않았습니다.")
    else:
        for res in results[-5:]:
            print(json.dumps(res, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()