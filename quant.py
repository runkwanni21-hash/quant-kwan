import yfinance as yf
import pandas as pd
import json
import warnings
import math
import os 
from typing import List, Dict, Any # AS-IS: 누락됨 -> TO-BE: 추가
warnings.filterwarnings("ignore")

# (기존 import 문 유지...)
from model.base_model import QuantitativeModel
from model.feature_builder import AdvancedMacroRegimeBuilder, LatentStressFeatureBuilder
from model.lightgbm_model import LightGBMMultiRegimeModel
from model.latent_stress_model import PCALatentStressModel
from model.promotion_engine import PromotionEngine

# AS-IS: def download_multi_data(tickers, start="2012-01-01"):
# TO-BE: tickers 리스트 타입 명시, 반환 타입(pd.DataFrame) 명시
def download_multi_data(tickers: List[str], start: str = "2012-01-01") -> pd.DataFrame:
    print(f"데이터 다운로드 중: {tickers}...")
    df = yf.download(tickers, start=start, progress=False, auto_adjust=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [f"{col[0]}_{col[1]}" for col in df.columns]
    return df

# AS-IS: def fx_overlay_engine(df_slice: pd.DataFrame, state: dict) -> dict:
# TO-BE: Dict의 내부 요소 타입(Any) 명시, 내부 변수 스칼라 타입 힌트 추가
def fx_overlay_engine(df_slice: pd.DataFrame, state: Dict[str, Any]) -> Dict[str, Any]:
    try:
        # AS-IS: 변수 타입 암시적 -> TO-BE: float 명시적 선언
        dxy_mom_20d: float = df_slice["Close_DX-Y.NYB"].pct_change(20).iloc[-1]
        dxy_vel_5d: float = df_slice["Close_DX-Y.NYB"].pct_change(5).iloc[-1]
        dxy_mixed: float = (dxy_mom_20d * 0.7) + (dxy_vel_5d * 0.3)
        dxy_score: float = max(min(dxy_mixed / 0.03, 1.0), -1.0) 

        ewy_spy_ratio: pd.Series = df_slice["Close_EWY"] / df_slice["Close_SPY"]
        ewy_mom_20d: float = ewy_spy_ratio.pct_change(20).iloc[-1]
        ewy_vel_5d: float = ewy_spy_ratio.pct_change(5).iloc[-1]
        ewy_mixed: float = (ewy_mom_20d * 0.7) + (ewy_vel_5d * 0.3)
        ewy_score: float = max(min(ewy_mixed / 0.05, 1.0), -1.0) 

        transition: float = float(state.get("transition_risk", 0.5))
        macro_fx_pressure: float = (ewy_score * 0.6) - (dxy_score * 0.4)
        panic_zone: float = max(transition - 0.55, 0.0)
        panic_penalty: float = (panic_zone ** 2) * 4.0 
        
        krw_strength_score: float = macro_fx_pressure - panic_penalty
        final_score: float = max(min(krw_strength_score, 1.0), -1.0)
        
        bias: str = ""
        hedge_ratio: float = 0.0
        
        if final_score > 0.7:
            bias = "STRONG_KRW (100% 환헤지 - 원화 강세 랠리)"
            hedge_ratio = 1.0
        elif final_score > 0.3:
            bias = "MILD_KRW (50% 부분 환헤지 - 헤지 비용 고려)"
            hedge_ratio = 0.5
        elif final_score > -0.3:
            bias = "NEUTRAL (환노출 유지 - 환율 박스권/비용 방어)"
            hedge_ratio = 0.0
        elif final_score > -0.7:
            bias = "MILD_USD (100% 환노출 - 안전자산 쉴드 작동)"
            hedge_ratio = 0.0
        else:
            bias = "PANIC_USD (환노출 + 달러 비중 확대 권장)"
            hedge_ratio = 0.0 
            
        return {
            "krw_score": round(float(final_score), 3),
            "fx_bias": bias,
            "hedge_ratio": hedge_ratio
        }
    except Exception:
        return {"krw_score": 0.0, "fx_bias": "NEUTRAL (Data Error)", "hedge_ratio": 0.0}

# AS-IS: def calculate_final_exposure(state: dict) -> float:
# TO-BE: Dict 구조 구체화, 내부 변수 float 타입 명시
def calculate_final_exposure(state: Dict[str, Any]) -> float:
    pred_return: float = float(state.get("expected_return", 0.0))
    fragility_z: float = float(state.get("fragility_z_score", 0.0))
    transition_risk: float = float(state.get("transition_risk", 0.5))
    
    k: float = 10.0 
    base_weight: float = 1.0 / (1.0 + math.exp(-k * pred_return))
    
    stress_penalty: float = max(0.0, min(fragility_z / 3.0, 0.7))
    transition_penalty: float = transition_risk * 0.5
    
    final_weight: float = base_weight * (1.0 - stress_penalty) * (1.0 - transition_penalty)
    return round(max(0.0, min(final_weight, 1.0)), 3)

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