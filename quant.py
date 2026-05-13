import yfinance as yf
import pandas as pd
import json
import warnings
import math
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

def fx_overlay_engine(df_slice: pd.DataFrame, state: dict) -> dict:
    """🌟 복구됨: 비선형 패닉 페널티와 5일 유속(Flow)이 결합된 환율 압력 엔진"""
    try:
        # 1. 달러 인덱스 (DXY)
        dxy_mom_20d = df_slice["Close_DX-Y.NYB"].pct_change(20).iloc[-1]
        dxy_vel_5d = df_slice["Close_DX-Y.NYB"].pct_change(5).iloc[-1]
        dxy_mixed = (dxy_mom_20d * 0.7) + (dxy_vel_5d * 0.3)
        dxy_score = max(min(dxy_mixed / 0.03, 1.0), -1.0) 

        # 2. 한국 수출/실물 경기 (EWY vs SPY)
        ewy_spy_ratio = df_slice["Close_EWY"] / df_slice["Close_SPY"]
        ewy_mom_20d = ewy_spy_ratio.pct_change(20).iloc[-1]
        ewy_vel_5d = ewy_spy_ratio.pct_change(5).iloc[-1]
        ewy_mixed = (ewy_mom_20d * 0.7) + (ewy_vel_5d * 0.3)
        ewy_score = max(min(ewy_mixed / 0.05, 1.0), -1.0) 

        # 3. 체제 붕괴 위험
        transition = state["transition_risk"]

        # 원화 강도 합성 및 비선형 패닉 패널티
        macro_fx_pressure = (ewy_score * 0.6) - (dxy_score * 0.4)
        panic_zone = max(transition - 0.55, 0.0)
        panic_penalty = (panic_zone ** 2) * 4.0 
        
        krw_strength_score = macro_fx_pressure - panic_penalty
        final_score = max(min(krw_strength_score, 1.0), -1.0)
        
        # 실전적 Tiered 환헤지 룰
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

def calculate_final_exposure(state: dict) -> float:
    """🌟 기관급 Continuous Allocator (Sigmoid + Penalty)"""
    pred_return = state["expected_return"]
    fragility_z = state["fragility_z_score"]
    transition_risk = state["transition_risk"]
    
    # 1. Base Weight (Sigmoid Mapping)
    # 미래 기대수익률을 0 ~ 1 사이의 비중 곡선으로 부드럽게 변환
    k = 10.0 # 민감도 계수
    base_weight = 1.0 / (1.0 + math.exp(-k * pred_return))
    
    # 2. Stress Penalty (취약성 반영)
    # Z-score를 3으로 나누어 페널티 산출 (최대 0.7까지만 페널티 적용)
    stress_penalty = max(0.0, min(fragility_z / 3.0, 0.7))
    
    # 3. Transition Penalty (붕괴 가속도 반영)
    transition_penalty = transition_risk * 0.5
    
    # 4. Final Weight
    final_weight = base_weight * (1.0 - stress_penalty) * (1.0 - transition_penalty)
    
    return round(max(0.0, min(final_weight, 1.0)), 3)

def main():
    print("=== Institutional Multi-Layer Allocator (w/ FX Overlay) ===\n")
    
    tickers = [
        "SPY", "QQQ", "EWY", "RSP", "^TNX", "^IRX", "DX-Y.NYB", 
        "HYG", "LQD", "GLD", "USO", "^VIX", "^VIX3M", "KRW=X",
        "IEF", "SMH", "XLY", "XLP"
    ]
    df = download_multi_data(tickers, start="2012-01-01")
    
    builder_macro = AdvancedMacroRegimeBuilder(target_window=60)
    macro_model = LightGBMMultiRegimeModel(name="Macro_AI", feature_builder=builder_macro)
    
    builder_stress = LatentStressFeatureBuilder()
    stress_model = PCALatentStressModel(name="Fragility_AI", window=252, feature_builder=builder_stress)
    stress_model.fit(df)

    # 2500은 너무 최신입니다. 더 과거의 추세를 배우도록 낮춰보세요.
    initial_train_size = 2000 
    # 더 자주 학습하도록 스텝 사이즈 조정
    step_size = 10
    total_len = len(df)
    
    print("\n[시스템] 직교 상태 전진 분석(Walk-Forward) 중...")
    results = []

    for i in range(initial_train_size, total_len, step_size):
        train_df = df.iloc[:i]
        end_idx = min(i + step_size, total_len)
        test_df = df.iloc[i:end_idx] 
        
        macro_model.fit(train_df)
        
        for j in range(len(test_df)):
            current_date = test_df.index[j]
            slice_df = df.iloc[: i + j + 1] 
            
            # 예측 함수 실행
            macro_preds = macro_model.predict(slice_df)
            stress_state = stress_model.predict(slice_df)
            
            state_json = {
                "date": current_date.strftime('%Y-%m-%d'),
                "expected_return": round(macro_preds["expected_return"], 4), # 🌟 확률이 아닌 수익률로 변경
                "fragility_z_score": round(stress_state["fragility_z_score"], 3),
                "transition_risk": round(stress_state["transition_risk"], 3),
                "shock_state": stress_state["shock_state"]
            }
            
            # FX 결합
            fx_result = fx_overlay_engine(slice_df, state_json)
            state_json["krw_score"] = fx_result["krw_score"]
            state_json["fx_bias"] = fx_result["fx_bias"]
            state_json["hedge_ratio"] = fx_result["hedge_ratio"]
            
            # 최종 익스포저
            final_exposure = calculate_final_exposure(state_json)
            state_json["final_equity_weight"] = final_exposure
            
            # (선택) 액션 상태 추가
            if state_json["shock_state"]:
                state_json["action"] = "DEFCON_1 (SHOCK!)"
            else:
                state_json["action"] = f"ALLOCATE (Weight: {final_exposure})"
            
            # 🌟 [여기입니다!] 계산된 state_json을 results 리스트에 추가합니다.
            results.append(state_json)

    # --- 루프 종료 ---

    print("\n========================================================")
    print("🎯 [최근 5일 최종 포트폴리오 상태 머신 리포트]")
    print("========================================================")
    
    # 🌟 방어막: 만약 그래도 비어있다면 알려줌
    if not results:
        print("🚨 분석된 결과가 없습니다. 루프 안에서 데이터가 append되지 않았습니다.")
    else:
        for res in results[-5:]:
            print(json.dumps(res, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()