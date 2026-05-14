import yfinance as yf
import pandas as pd
import json
import warnings
import math
import os 
from typing import List, Dict, Any # AS-IS: 누락됨 -> TO-BE: 추가
warnings.filterwarnings("ignore")

# (기존 import 문 유지...)
from model.feature_builder import AdvancedMacroRegimeBuilder, LatentStressFeatureBuilder
from model.lightgbm_model import LightGBMMultiRegimeModel
from model.latent_stress_model import PCALatentStressModel

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

def main() -> None:
    print("=== Institutional Multi-Layer Allocator (w/ FX Overlay) ===\n")
    
    # AS-IS: tickers = [...]
    # TO-BE: 명시적인 문자열 리스트로 타입 지정
    tickers: List[str] = [
        "SPY", "QQQ", "EWY", "RSP", "^TNX", "^IRX", "DX-Y.NYB", 
        "HYG", "LQD", "GLD", "USO", "^VIX", "^VIX3M", "KRW=X",
        "IEF", "SMH", "XLY", "XLP"
    ]
    df: pd.DataFrame = download_multi_data(tickers, start="2012-01-01")
    total_len: int = len(df)
    
    macro_model_path: str = "models/v1/Macro_AI.pkg"
    stress_model_path: str = "models/v1/Fragility_AI.pkg"
    
    # AS-IS: results = []
    # TO-BE: 결과를 담는 리스트의 내부 딕셔너리 구조 명시
    results: List[Dict[str, Any]] = []

    # =========================================================
    # 분기 1: 저장된 모델이 존재할 경우 (🚀 초고속 실전 추론 모드)
    # =========================================================
    if os.path.exists(macro_model_path) and os.path.exists(stress_model_path):
        print("\n[시스템] 📂 저장된 모델을 발견했습니다. 학습을 건너뛰고 최신 5거래일 추론을 시작합니다...")
        
        # 모델 복원 (is_fitted 상태 포함)
        macro_model = LightGBMMultiRegimeModel.load(macro_model_path)
        stress_model = PCALatentStressModel.load(stress_model_path)
        
        # 최근 5일 데이터만 처리
        test_df = df.iloc[-5:]
        
        for j in range(len(test_df)):
            current_date = test_df.index[j]
            # 전체 데이터 중 현재 날짜까지만 잘라서 예측 (미래 데이터 참조 방지)
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

    # =========================================================
    # 분기 2: 저장된 모델이 없을 경우 (🏋️‍♂️ 전진 분석 및 딥러닝 모드)
    # =========================================================
    else:
        print("\n[시스템] 🚨 저장된 모델이 없습니다. 직교 상태 전진 분석(Walk-Forward) 및 모델 학습을 시작합니다...")
        
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

        # 🌟 루프 종료 후 모델을 하드디스크에 추출(Save)
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