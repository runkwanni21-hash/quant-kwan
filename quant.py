import yfinance as yf
import pandas as pd
import json
import warnings
import math
import os 
from typing import List, Dict, Any # AS-IS: 누락됨 -> TO-BE: 추가
import numpy as np
warnings.filterwarnings("ignore")

# (기존 import 문 유지...)
from model.base_model import QuantitativeModel
from model.feature_builder import AdvancedMacroRegimeBuilder, LatentStressFeatureBuilder
from model.lightgbm_model import LightGBMMultiRegimeModel
from model.latent_stress_model import PCALatentStressModel
from model.promotion_engine import PromotionEngine

# AS-IS: def download_multi_data(tickers, start="2012-01-01"):
# TO-BE: tickers 리스트 타입 명시, 반환 타입(pd.DataFrame) 명시
def download_multi_data(tickers: List[str], start: str = "2012-01-01", z_thresh: float = 4.0) -> pd.DataFrame:
    """
    [Production용 데이터 페처]
    1. 로컬 파켓(Parquet) 캐싱을 통해 중복 다운로드를 방지하고 증분(Append) 업데이트를 수행합니다.
    2. 결측치 방어(Forward Fill) 및 비정상 스파이크(Z-Score) 교정 기능을 포함합니다.
    """
    # 데이터 저장 디렉토리 생성
    data_dir = "datas/ohlcv"
    os.makedirs(data_dir, exist_ok=True)
    
    combined_dfs: List[pd.DataFrame] = []

    print("📡 [데이터 파이프라인] 로컬 캐시 확인 및 증분 다운로드 시작...")
    
    for ticker in tickers:
        file_path = os.path.join(data_dir, f"{ticker}.parquet")
        existing_df = pd.DataFrame()
        fetch_start = start

        # -------------------------------------------------------------
        # 1. 기존 파켓 파일이 존재할 경우: 마지막 날짜 확인
        # -------------------------------------------------------------
        if os.path.exists(file_path):
            try:
                existing_df = pd.read_parquet(file_path)
                if not existing_df.empty:
                    # 안정성을 위해 마지막 날짜 기준 5일 전부터 겹치게 다운로드 (최근 수정치 반영)
                    last_date = existing_df.index.max()
                    fetch_start = (last_date - pd.Timedelta(days=5)).strftime('%Y-%m-%d')
            except Exception as e:
                print(f"⚠️ [{ticker}] 캐시 파일 읽기 실패. 전체 다운로드로 전환: {e}")

        # -------------------------------------------------------------
        # 2. 새로운 데이터 다운로드
        # -------------------------------------------------------------
        try:
            # 단일 종목 다운로드 (MultiIndex가 아님)
            new_df = yf.download(ticker, start=fetch_start, progress=False, auto_adjust=False)
        except Exception as e:
            print(f"🚨 [{ticker}] 야후 파이낸스 API 응답 없음: {e}")
            new_df = pd.DataFrame()

        # -------------------------------------------------------------
        # 3. 기존 데이터와 신규 데이터 병합 (Append & Deduplicate)
        # -------------------------------------------------------------
        if not new_df.empty:
            # yfinance 내부 변경 대비 MultiIndex 평탄화 방어 로직
            if isinstance(new_df.columns, pd.MultiIndex):
                new_df.columns = [col[0] for col in new_df.columns]
                
            if not existing_df.empty:
                # 위아래로 이어 붙인 뒤, 인덱스(날짜)가 겹치면 나중에 다운받은 최신 데이터(last)를 남김
                merged_df = pd.concat([existing_df, new_df])
                merged_df = merged_df[~merged_df.index.duplicated(keep='last')]
            else:
                merged_df = new_df

            merged_df.sort_index(inplace=True)

            # 💾 파켓 파일로 덮어쓰기 (업데이트 완료)
            merged_df.to_parquet(file_path)

            # 열 이름을 시스템 규격에 맞게 변경 (Close -> Close_SPY)
            merged_df.columns = [f"{col}_{ticker}" for col in merged_df.columns]
            combined_dfs.append(merged_df)
            
        else:
            # API 다운로드 실패 시 로컬 캐시만이라도 살려서 활용
            if not existing_df.empty:
                existing_df.columns = [f"{col}_{ticker}" for col in existing_df.columns]
                combined_dfs.append(existing_df)

    if not combined_dfs:
        print("🚨 치명적 에러: 사용 가능한 데이터가 전혀 없습니다.")
        return pd.DataFrame()

    # -------------------------------------------------------------
    # 4. 모든 티커를 가로(axis=1)로 조인하여 마스터 데이터프레임 생성
    # -------------------------------------------------------------
    df = pd.concat(combined_dfs, axis=1)

    # -------------------------------------------------------------
    # 🛡️ 5. 결측치 및 지연 데이터 방어 (Forward Fill)
    # -------------------------------------------------------------
    # 최대 3일까지 빈칸(휴장일 등)을 앞의 데이터로 채움
    df = df.ffill(limit=3)

    # -------------------------------------------------------------
    # 🚨 6. 비정상 가격 스파이크 검증 (Sanity Check)
    # -------------------------------------------------------------
    close_cols = [c for c in df.columns if c.startswith("Close_")]
    # 🌟 [추가] 지수 특성상 25% 이상 폭등이 가능한(정상적인) 종목들은 필터링 면제
    exempt_tickers = ["^VIX", "^VIX3M", "^IRX", "^TNX"] 

    for col in close_cols:
        # 면제 대상 티커인지 확인
        if any(exempt in col for exempt in exempt_tickers):
            continue  # VIX나 금리가 튀는 것은 오류가 아니라 '진짜 위기'이므로 건들지 않음!
        pct_change = df[col].pct_change()
        rolling_mean = pct_change.rolling(window=20).mean()
        rolling_std = pct_change.rolling(window=20).std()
        
        z_scores = np.abs((pct_change - rolling_mean) / (rolling_std + 1e-8))
        anomaly_mask = (z_scores > z_thresh) & (np.abs(pct_change) > 0.25)

        if anomaly_mask.any():
            anomaly_dates = df.index[anomaly_mask]
            for date in anomaly_dates:
                print(f"⚠️ [데이터 경고] {col} 비정상 스파이크 감지/교정 (날짜: {date.strftime('%Y-%m-%d')})")
            
            # 오류 구간을 지우고 직전 가격으로 채움
            df.loc[anomaly_mask, col] = np.nan
            df[col] = df[col].ffill()

    # 초반의 NaN 데이터들 일괄 제거
    df = df.dropna()
    print("✅ 데이터 로드, 병합 및 무결성 검증 완료.\n")
    
    return df

def fx_overlay_engine(df_slice: pd.DataFrame, state: Dict[str, Any]) -> Dict[str, Any]:
    try:
        # =====================================================================
        # 1. 팩터 스코어링 (다중공선성 방지를 위한 가중치 재조정: 70/15/15)
        # =====================================================================
        dxy_mom_20d = df_slice["Close_DX-Y.NYB"].pct_change(20).iloc[-1]
        dxy_vel_5d = df_slice["Close_DX-Y.NYB"].pct_change(5).iloc[-1]
        dxy_score = max(min(((dxy_mom_20d * 0.7) + (dxy_vel_5d * 0.3)) / 0.03, 1.0), -1.0)

        ewy_spy_ratio = df_slice["Close_EWY"] / df_slice["Close_SPY"]
        ewy_mom_20d = ewy_spy_ratio.pct_change(20).iloc[-1]
        ewy_vel_5d = ewy_spy_ratio.pct_change(5).iloc[-1]
        ewy_score = max(min(((ewy_mom_20d * 0.7) + (ewy_vel_5d * 0.3)) / 0.05, 1.0), -1.0)

        # Ground Truth: 실제 환율
        krw_usd = df_slice["Close_KRW=X"]
        krw_mom_20d = krw_usd.pct_change(20).iloc[-1]
        krw_vel_5d = krw_usd.pct_change(5).iloc[-1]
        direct_krw_score = max(min(-((krw_mom_20d * 0.7) + (krw_vel_5d * 0.3)) / 0.02, 1.0), -1.0)

        # =====================================================================
        # 2. 매크로 압력 계산 및 패닉 페널티 반영
        # =====================================================================
        transition = float(state.get("transition_risk", 0.5))
        macro_fx_pressure = (direct_krw_score * 0.70) + (ewy_score * 0.15) - (dxy_score * 0.15)
        panic_zone = max(transition - 0.55, 0.0)
        panic_penalty = (panic_zone ** 2) * 4.0 
        
        krw_strength_score = macro_fx_pressure - panic_penalty
        final_score = max(min(krw_strength_score, 1.0), -1.0)
        
        # =====================================================================
        # 3. 🌟 시그모이드 비선형 매핑 (Sigmoid Mapping)
        # =====================================================================
        base_bias = -0.15 
        adjusted_score = final_score + base_bias
        
        # Sigmoid: 중립 구간 둔감, 극단 구간 민감 / 최대 헤지 80%(0.8)
        raw_hedge_ratio = 0.8 / (1.0 + math.exp(-3.0 * adjusted_score))
        
        # =====================================================================
        # 4. 🛡️ 히스테리시스 (Hysteresis) 완충 영역 적용
        # =====================================================================
        prev_hedge = float(state.get("prev_hedge_ratio", 0.0))
        
        # 이전 헤지 비율과 15% 미만으로 차이나면 무시 (Turnover, 세금, 슬리피지 방어)
        if abs(raw_hedge_ratio - prev_hedge) < 0.15:
            hedge_ratio = prev_hedge
        else:
            hedge_ratio = round(float(max(0.0, min(raw_hedge_ratio, 0.8))), 3)

        # =====================================================================
        # 5. 상태 로깅
        # =====================================================================
        if hedge_ratio >= 0.6:
            bias_str = f"STRONG_KRW (Hedge: {hedge_ratio*100:.1f}% - 강한 원화 방어)"
        elif hedge_ratio >= 0.3:
            bias_str = f"MILD_KRW (Hedge: {hedge_ratio*100:.1f}% - 부분 헤지)"
        elif hedge_ratio > 0.0:
            bias_str = f"NEUTRAL (Hedge: {hedge_ratio*100:.1f}% - 환노출 중심/약한 헤지)"
        else:
            bias_str = "USD_LONG (Hedge: 0.0% - 100% 환노출/달러 안전자산 작동)"
            
        return {
            "krw_score": round(float(final_score), 3),
            "fx_bias": bias_str,
            "hedge_ratio": hedge_ratio
        }
        
    except Exception as e:
        return {
            "krw_score": 0.0, 
            "fx_bias": f"ERROR_USD_LONG (Data Error: {e})", 
            "hedge_ratio": 0.0
        }

# AS-IS: def calculate_final_exposure(state: dict) -> float:
# TO-BE: Dict 구조 구체화, 내부 변수 float 타입 명시
def calculate_final_exposure(state: Dict[str, Any], k: float = 7.0) -> float:
    """
    Macro AI의 기대 수익률과 Stress 모델의 페널티를 결합하여 최종 주식 비중을 산출합니다.
    k 값(기본 7.0)을 조절하여 기대 수익률 변동에 대한 비중 조절의 민감도를 부드럽게 제어합니다.
    """
    pred_return: float = float(state.get("expected_return", 0.0))
    fragility_z: float = float(state.get("fragility_z_score", 0.0))
    transition_risk: float = float(state.get("transition_risk", 0.5))
    
    # 🌟 [수정] k 값을 10.0 -> 7.0으로 완화하여 과민 반응(Whipsaw) 및 불필요한 Turnover 방지
    base_weight: float = 1.0 / (1.0 + math.exp(-k * pred_return))
    
    # 스트레스 수위가 높을수록 비중 축소 (최대 70% 페널티)
    stress_penalty: float = max(0.0, min(fragility_z / 3.0, 0.7))
    
    # 붕괴 가속도가 붙을 때 추가 페널티
    transition_penalty: float = transition_risk * 0.5
    
    # 베이스 비중에 페널티들을 곱하여 최종 할당 비중 계산
    final_weight: float = base_weight * (1.0 - stress_penalty) * (1.0 - transition_penalty)
    
    return round(float(max(0.0, min(final_weight, 1.0))), 3)

def strict_sharpe_evaluator(champion: QuantitativeModel, candidate: QuantitativeModel, test_data: pd.DataFrame) -> bool:
    print(f"🔍 [엄격한 심사] {champion.name}과 {candidate.name}의 샤프 지표를 롤링 비교합니다...")
    # 실제로는 predict() 결과를 바탕으로 50일간의 백테스트 수익률/변동성을 비교하는 로직 구현
    # ...
    # 지금은 테스트를 위해 무조건 도전자 승리로 세팅
    return True

# ==============================================================================
# 🌟 메인 애플리케이션 파이프라인
# ==============================================================================
def main() -> None:
    print("=== Institutional Multi-Layer Allocator (w/ FX Overlay) ===\n")
    
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

    # =========================================================
    # 분기 1: 저장된 모델이 존재할 경우 (하이브리드 업데이트 모드)
    # =========================================================
    if os.path.exists(macro_model_path) and os.path.exists(stress_model_path):
        print("\n[시스템] 📂 챔피언 모델을 로드하여 운영 파이프라인을 실행합니다.")
        
        # 1. 모델 복원
        macro_model = LightGBMMultiRegimeModel.load(macro_model_path)
        stress_model = PCALatentStressModel.load(stress_model_path)
        
        # 2. 모델 업데이트 전략 차별화
        # ---------------------------------------------------------
        # A. Macro 모델: 승급 심사를 위해 Candidate(도전자) 생성
        # ---------------------------------------------------------
        print("\n[시스템] 🛡️ Macro 모델: 섀도우 학습 모드 (도전자 생성)")
        macro_candidate_name = "Macro_AI_Candidate"
        macro_model.update(df, candidate_mode=True, candidate_name=macro_candidate_name)
        
        # ---------------------------------------------------------
        # B. Stress 모델: 승급 심사 없이 즉시 업데이트 (Direct Update)
        # ---------------------------------------------------------
        # AS-IS: candidate_mode=True
        # TO-BE: candidate_mode=False로 설정하여 현재 객체를 즉시 갱신하고 저장
        print("[시스템] ⚡ Stress 모델: 즉시 업데이트 모드 (심사 생략)")
        stress_model.update(df, candidate_mode=False) 
        stress_model.save("models/v1") # 업데이트된 상태를 파일에 바로 반영
        
        # 3. 최신 5일 실전 추론
        # Macro는 검증된 구모델(Champ)을, Stress는 방금 업데이트된 최신 모델을 사용합니다.
        print("\n[시스템] 🎯 하이브리드 모델팩을 사용하여 최근 5거래일 추론 시작...")
        test_df = df.iloc[-5:]
        
        # =================================================
        # 🌟 루프 진입 전, 이전(초기) 헤지 비율을 0.0으로 세팅 (혹은 DB에서 불러옴)
        # =================================================
        current_prev_hedge = 0.0 
        
        for j in range(len(test_df)):
            current_date = test_df.index[j]
            slice_idx = total_len - 5 + j + 1
            slice_df = df.iloc[:slice_idx] 
            
            macro_preds = macro_model.predict(slice_df)
            stress_state = stress_model.predict(slice_df)
            
            state_json = {
                "date": current_date.strftime('%Y-%m-%d'),
                "expected_return": round(macro_preds.get("expected_return", 0.0), 4),
                "fragility_z_score": round(stress_state.get("fragility_z_score", 0.0), 3),
                "transition_risk": round(stress_state.get("transition_risk", 0.5), 3),
                "shock_state": stress_state.get("shock_state", False),
                "prev_hedge_ratio": current_prev_hedge  # 🌟 직전 헤지 비율 주입!
            }
            
            # 환율 엔진 가동
            fx_result = fx_overlay_engine(slice_df, state_json)
            state_json.update(fx_result)
            
            # 🌟 내일(다음 루프)을 위해 계산된 헤지 비율을 저장
            current_prev_hedge = fx_result["hedge_ratio"]
            
            final_exposure = calculate_final_exposure(state_json)
            state_json["final_equity_weight"] = final_exposure
            
            if state_json["shock_state"]:
                state_json["action"] = "DEFCON_1 (SHOCK!)"
            else:
                state_json["action"] = f"ALLOCATE (Weight: {final_exposure})"
            
            results.append(state_json)
            
        # 4. Macro 모델만 승급 심사 수행
        print("\n========================================================")
        print("🏛️ [시스템] 운영 종료. Macro 모델 승급 심사를 시작합니다.")
        print("========================================================")
        
        engine = PromotionEngine()
        
        # Macro 모델만 심사 루틴 실행
        #engine.execute(
        #    champion_path=macro_model_path,
        #    candidate_path=f"models/v1/candidates/{macro_candidate_name}.pkg",
        #    test_data=df,
        #    custom_evaluator=strict_sharpe_evaluator
        #)
        engine.execute(
            champion_path=macro_model_path,
            candidate_path=f"models/v1/candidates/{macro_candidate_name}.pkg",
            test_data=df
        )
        
        # Stress 모델은 이미 업데이트 및 저장이 완료되었으므로 심사 엔진 호출을 생략합니다.

    # =========================================================
    # 분기 2: 저장된 모델이 없을 경우 (초기 학습 모드)
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

        for i in range(initial_train_size, total_len, step_size):
            train_df = df.iloc[:i]
            end_idx = min(i + step_size, total_len)
            test_df = df.iloc[i:end_idx] 
            
            macro_model.fit(train_df)
            
            for j in range(len(test_df)):
                current_date = test_df.index[j]
                slice_df = df.iloc[: i + j + 1] 
                
                macro_preds = macro_model.predict(slice_df)
                stress_state = stress_model.predict(slice_df)
                
                state_json = {
                    "date": current_date.strftime('%Y-%m-%d'),
                    "expected_return": round(macro_preds.get("expected_return", 0.0), 4),
                    "fragility_z_score": round(stress_state.get("fragility_z_score", 0.0), 3),
                    "transition_risk": round(stress_state.get("transition_risk", 0.5), 3),
                    "shock_state": stress_state.get("shock_state", False)
                }
                
                fx_result = fx_overlay_engine(slice_df, state_json)
                state_json.update(fx_result)
                
                final_exposure = calculate_final_exposure(state_json)
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