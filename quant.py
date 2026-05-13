import yfinance as yf
import pandas as pd
import json
import warnings
warnings.filterwarnings("ignore")

from model.feature_builder import AdvancedMacroRegimeBuilder, LatentStressFeatureBuilder
from model.lightgbm_model import LightGBMMultiRegimeModel
from model.latent_stress_model import PCALatentStressModel

def download_multi_data(tickers, start="2012-01-01"):
    print(f"데이터 다운로드 중: {tickers}...")
    df = yf.download(tickers, start=start, progress=False, auto_adjust=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [f"{col[0]}_{col[1]}" for col in df.columns]
    return df

def execution_layer(state: dict) -> str:
    """사용자 제안형 직교 교차 방어 로직 (Level + Velocity 통합 버전)"""
    
    mac = state["macro_regime"]
    frag = state["fragility_z_score"]
    trans = state["transition_risk"]
    shock = state["shock_state"]

    # 1. 최상위 오버라이드: 당일 폭락 이벤트
    if shock:
        return "DEFCON_1 (긴급 탈출, Exposure 0.0)"
        
    # 2. Level과 Velocity가 결합된 종합 붕괴 확률이 85% 이상일 때
    elif trans > 0.85:
        return f"DEFCON_2 (체제 붕괴 위험! Transition {trans:.1%}, Exposure 0.3)"
        
    # 3. 속도(Velocity)는 낮아도, 수위(Level) 자체가 1.5 Z-score를 넘는 위험 지대일 때
    elif frag > 1.5:
        return f"DEFCON_3 (구조적 취약성 경고, Exposure 0.5)"
        
    # 4. 구조적으로 안전한 구간에서의 매크로 틸트
    elif mac == "Bear" and state["macro_confidence"] > 0.1:
        return "RISK_OFF (거시적 하락 추세, Exposure 0.4)"
        
    elif mac == "Bull":
        return "RISK_ON (평온한 상승장, Exposure 1.0)"
        
    else:
        return "NEUTRAL (횡보장, Exposure 0.7)"

def main():
    print("=== Institutional State Extraction Engine (Phase Space Risk) ===\n")
    
    # 🌟 QQQ, EWY 추가!
    tickers = ["SPY", "QQQ", "EWY", "RSP", "^TNX", "^IRX", "DX-Y.NYB", "HYG", "LQD", "GLD", "USO", "^VIX", "^VIX3M"]
    df = download_multi_data(tickers, start="2012-01-01")
    
    builder_macro = AdvancedMacroRegimeBuilder(target_window=60)
    macro_model = LightGBMMultiRegimeModel(name="Macro_AI", feature_builder=builder_macro, calib_method="sigmoid")
    
    builder_stress = LatentStressFeatureBuilder()
    stress_model = PCALatentStressModel(name="Fragility_AI", window=252, feature_builder=builder_stress)
    stress_model.fit(df)

    initial_train_size = 2500 
    step_size = 20            
    total_len = len(df)
    
    print("\n[시스템] 직교 상태 전진 분석(Walk-Forward) 중...")
    results = []

    for i in range(total_len - (step_size * 3), total_len, step_size):
        train_df = df.iloc[:i]
        end_idx = min(i + step_size, total_len)
        test_df = df.iloc[i:end_idx] 
        
        macro_model.fit(train_df)
        
        for j in range(len(test_df)):
            current_date = test_df.index[j]
            slice_df = df.iloc[: i + j + 1] 
            
            macro_probs = macro_model.predict(slice_df)
            macro_regime = max(macro_probs, key=macro_probs.get).split('_')[0]
            sorted_probs = sorted(macro_probs.values(), reverse=True)
            macro_confidence = round(sorted_probs[0] - sorted_probs[1], 3)
            
            stress_state = stress_model.predict(slice_df)
            
            state_json = {
                "date": current_date.strftime('%Y-%m-%d'),
                "macro_regime": macro_regime,
                "macro_confidence": macro_confidence,
                "fragility_z_score": stress_state["fragility_z_score"],
                "transition_risk": stress_state["transition_risk"],
                "shock_state": stress_state["shock_state"]
            }
            state_json["action"] = execution_layer(state_json)
            results.append(state_json)

    print("\n========================================================")
    print("🎯 [최근 5일 최종 포트폴리오 상태 머신 (Phase Space 혼합 엔진)]")
    print("========================================================")
    for res in results[-5:]:
        print(json.dumps(res, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()