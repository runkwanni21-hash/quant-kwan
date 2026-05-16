import yfinance as yf
import pandas as pd
import numpy as np
import json
from typing import List, Dict, Any

from model.feature_builder import AdvancedMacroRegimeBuilder
from model.lightgbm_model import LightGBMMIMOMacroModel

def download_multi_data(tickers: List[str], start: str = "2012-01-01") -> pd.DataFrame:
    print(f"데이터 다운로드 중: {tickers}...")
    df = yf.download(tickers, start=start, progress=False, auto_adjust=False)
    
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [f"{col[0]}_{col[1]}" for col in df.columns]
        
    df = df.ffill().bfill()
    return df

def apply_institutional_exposure_control(raw_vector: Dict[str, float], full_slice_df: pd.DataFrame, target_vol: float = 0.10, window: int = 60) -> Dict[str, Any]:
    """
    [Institutional Exposure Control Engine]
    Macro Regime Engine이 Volatility Targeting Engine의 '상한선(Ceiling)'으로 작용합니다.
    낮은 변동성에 속아 매크로 위험 신호를 무시하는 치명적 결함을 방어합니다.
    """
    # 🌟 1. Macro Ceiling 설정 (Macro AI가 지시한 필수 방어 현금 비중)
    macro_cash_target = raw_vector.get("cash_target", 0.05)
    macro_ceiling = 1.0 - macro_cash_target

    if len(full_slice_df) < window:
        raw_vector["gross_exposure"] = macro_ceiling
        raw_vector["realized_vol"] = target_vol
        raw_vector["active_constraint"] = "Macro_Ceiling"
        return raw_vector

    spy_ret = full_slice_df["Close_SPY"].pct_change().fillna(0)
    schd_ret = full_slice_df["Close_SCHD"].pct_change().fillna(0) if "Close_SCHD" in full_slice_df.columns else spy_ret * 0.7
    vnq_ret = full_slice_df["Close_VNQ"].pct_change().fillna(0) if "Close_VNQ" in full_slice_df.columns else spy_ret * 0.8
    hyg_ret = full_slice_df["Close_HYG"].pct_change().fillna(0) if "Close_HYG" in full_slice_df.columns else spy_ret * 0.3
    lqd_ret = full_slice_df["Close_LQD"].pct_change().fillna(0) if "Close_LQD" in full_slice_df.columns else spy_ret * 0.0
    gld_ret = full_slice_df["Close_GLD"].pct_change().fillna(0) if "Close_GLD" in full_slice_df.columns else spy_ret * 0.0

    income_ret = (0.45 * schd_ret) + (0.35 * vnq_ret) + (0.20 * hyg_ret)

    ret = pd.DataFrame({
        "equity": spy_ret,
        "income": income_ret,
        "bond": lqd_ret,
        "commodity": gld_ret
    }).tail(window)
    
    cov_matrix = ret.cov()
    
    # 리스크 자산들의 초기 비중 추출 (합산 = macro_ceiling)
    weights = np.array([
        raw_vector.get("equity", 0.0),
        raw_vector.get("income", 0.0),
        raw_vector.get("bond", 0.0),
        raw_vector.get("commodity", 0.0)
    ])
    
    port_var = weights.T @ cov_matrix @ weights
    realized_vol = np.sqrt(port_var) * np.sqrt(252) if port_var > 0 else 0.01

    # 🌟 2. Volatility Scaling (레버리지 1.0 초과 금지)
    vol_scale = min(1.0, target_vol / realized_vol)

    # 🌟 3. Final Exposure 결정 로직 : min(Macro Ceiling, Vol Scale)
    gross_exposure = min(macro_ceiling, vol_scale)
    
    # 현재 리스크 자산 비중 합계는 macro_ceiling임. 이를 gross_exposure 스케일로 축소/유지
    scale_factor = gross_exposure / macro_ceiling if macro_ceiling > 0 else 0.0

    final_vector = raw_vector.copy()
    for k in ["equity", "income", "bond", "commodity"]:
        if k in final_vector:
            final_vector[k] *= scale_factor

    # 🌟 4. Preserve Cash Regime (스케일링 후 남은 공간은 모두 절대 현금으로 보존)
    final_vector["cash_target"] = 1.0 - gross_exposure
    final_vector["gross_exposure"] = gross_exposure
    final_vector["realized_vol"] = realized_vol
    
    # 디버깅용: 어떤 제약이 포트폴리오를 억누르고 있는지 확인
    if macro_ceiling <= vol_scale:
        final_vector["active_constraint"] = f"Macro Engine (Cap: {macro_ceiling:.1%})"
    else:
        final_vector["active_constraint"] = f"Vol Engine (Cap: {vol_scale:.1%})"

    if port_var > 0:
        mrc = cov_matrix @ weights
        rrc = (weights * mrc) / port_var
        final_vector["rc_equity"] = round(rrc.iloc[0], 3)
        final_vector["rc_income"] = round(rrc.iloc[1], 3)
        final_vector["rc_bond"] = round(rrc.iloc[2], 3)
        final_vector["rc_cmdty"] = round(rrc.iloc[3], 3)
    else:
        final_vector["rc_equity"] = final_vector["rc_income"] = final_vector["rc_bond"] = final_vector["rc_cmdty"] = 0.0

    return final_vector

def main() -> None:
    print("=== Institutional Risk-Managed Macro Allocation Engine ===\n")
    
    tickers = ["SPY", "RSP", "^TNX", "^IRX", "DX-Y.NYB", "HYG", "LQD", "GLD", "USO", "^VIX", "^VIX3M", "EWY", "KRW=X", "SCHD", "VNQ"]
    df = download_multi_data(tickers, start="2012-01-01")
    
    builder_macro = AdvancedMacroRegimeBuilder(target_window=20)
    policy_model = LightGBMMIMOMacroModel(name="Macro_Brain", feature_builder=builder_macro)
    
    policy_model.analyze_factor_correlation(df)
    
    print("🚀 [Walk-Forward 시뮬레이션 시작]")
    print("="*50)
    
    initial_train_size, step_size = 2500, 20            
    total_len = len(df)
    results = []

    for i in range(total_len - (step_size * 3), total_len, step_size):
        train_df = df.iloc[:i]
        end_idx = min(i + step_size, total_len)
        test_df = df.iloc[i:end_idx]
        
        policy_model.fit(train_df)
        
        for j in range(len(test_df)):
            current_date = test_df.index[j]
            full_slice_df = df.iloc[:i+j+1]
            
            # 1. Macro Brain (생각)
            policy_model.predict(full_slice_df)
            raw_vector = policy_model.get_raw_macro_vector()
            
            # 2. Exposure Control (통제)
            final_vector = apply_institutional_exposure_control(raw_vector, full_slice_df, target_vol=0.10)
            
            if j == len(test_df) - 1:
                print(f"\n[{current_date.strftime('%Y-%m-%d')}] 모델 매크로 팩터 추론 상태:")
                policy_model.print_diagnostics()
                
                print("\n📊 [Hierarchical Exposure Control Status]")
                print(f"  └ Active Bottleneck: {final_vector['active_constraint']}")
                print(f"  └ Realized Vol: {final_vector['realized_vol']:.1%} ➡️ Target Vol: 10.0%")
                print(f"  └ Gross Exposure: {final_vector['gross_exposure']:.1%} (Cash Buffer: {final_vector['cash_target']:.1%})")
                
                print("\n⚖️ [Portfolio Risk Budgeting]")
                print(f"  └ Equity | Weight: {final_vector['equity']:.1%} -> RC: {final_vector['rc_equity']:.1%}")
                print(f"  └ Income | Weight: {final_vector['income']:.1%} -> RC: {final_vector['rc_income']:.1%}")
                print(f"  └ Bond   | Weight: {final_vector['bond']:.1%} -> RC: {final_vector['rc_bond']:.1%}")

            results.append({
                "date": current_date.strftime('%Y-%m-%d'),
                "constraint": final_vector["active_constraint"],
                "gross_exposure": round(final_vector['gross_exposure'], 3),
                "weights": {k: round(final_vector[k], 4) for k in ["equity", "income", "bond", "commodity", "cash_target"] if k in final_vector}
            })

    print("\n========================================================")
    print("🎯 [최근 5일 최종 익스포저 통제 결과 (JSON)]")
    print("========================================================")
    for res in results[-5:]:
        print(json.dumps(res, indent=2))

if __name__ == "__main__":
    main()