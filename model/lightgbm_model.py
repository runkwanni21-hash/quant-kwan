import pandas as pd
import numpy as np
import lightgbm as lgb
from .base_model import QuantitativeModel

# model/lightgbm_model.py 전체 교체본

import pandas as pd
import numpy as np
import lightgbm as lgb
from .base_model import QuantitativeModel

import pandas as pd
import numpy as np
import lightgbm as lgb
# AS-IS: 타입 힌트 모듈 부재
# TO-BE: 안정적인 시스템 운영을 위한 typing 모듈 추가
from typing import Dict, Any, List, Optional
from .base_model import QuantitativeModel

class LightGBMMultiRegimeModel(QuantitativeModel): 
    # AS-IS: 파라미터 타입 및 반환형 명시 안 됨
    # TO-BE: 명확한 타입 힌트와 내부 상태 변수(latest_probs 등) 초기화
    def __init__(self, name: str = "LGBM_Macro_AI", train_window: int = 2000, target_col: str = "target_return", feature_builder: Any = None) -> None:
        super().__init__(name)
        self.train_window = train_window
        self.target_col = target_col
        self.feature_builder = feature_builder
        
        # 🌟 이름은 MultiRegime이지만, 엔진은 LGBMRegressor (연속형) 장착!
        self.model = lgb.LGBMRegressor(
            n_estimators=100, 
            learning_rate=0.05, 
            max_depth=4, 
            min_child_samples=10, 
            objective='huber', # 아웃라이어 방어용 (퀀트 필수)
            random_state=42, 
            verbose=-1,
            importance_type='gain'
        )
        self.feature_cols: List[str] = []
        
        # 🌟 [추가] 과거 분류형 메서드(predict_batch)의 잔재를 위한 기본값 초기화
        self.latest_probs: Dict[str, float] = {}
        self.latest_confidence: float = 0.0

    # AS-IS: def fit(self, data: pd.DataFrame):
    # TO-BE: 반환형 명시
    def fit(self, data: pd.DataFrame) -> None:
        """초기 대규모 데이터를 이용한 모델의 최초 학습 (Batch Train)"""
        df_feat = self.feature_builder.process(data, is_training=True)
        exclude = ["Open", "High", "Low", "Close", "Volume", "target_return"]
        self.feature_cols = [c for c in df_feat.columns if c not in exclude and not c.startswith("Close_")]
        
        # 라벨이 존재하는 구간만 추출 (최근 데이터 중 타겟이 결측치인 경우 제외)
        train_data = df_feat.dropna(subset=[self.target_col]).iloc[-self.train_window:]
        
        if len(train_data) < 50:
            print(f"⚠️ [{self.name}] 학습 실패: 유효 데이터 부족")
            self.is_fitted = False
            return

        self.model.fit(
            train_data[self.feature_cols], 
            train_data[self.target_col].values
        )
        self.is_fitted = True

    # AS-IS: def update(self, data: pd.DataFrame) -> None:
    # TO-BE: Candidate 섀도우 모드 추가
    def update(self, data: pd.DataFrame, candidate_mode: bool = False, candidate_name: Optional[str] = None) -> None:
        if not self.is_fitted:
            print(f"⚠️ [{self.name}] 기존 모델이 없습니다. 최초 학습(fit)으로 대체합니다.")
            self.fit(data)
            return

        df_feat = self.feature_builder.process(data, is_training=True)
        train_data = df_feat.dropna(subset=[self.target_col]).iloc[-self.train_window:]
        
        if len(train_data) < 50:
            print(f"⚠️ [{self.name}] 업데이트 실패: 유효 데이터 부족")
            return
            
        # ================================================================
        # 🌟 분기: 승급 심사용 섀도우 학습 모드 (현재 모델 오염 방지)
        # ================================================================
        if candidate_mode:
            print(f"🕵️‍♂️ [{self.name}] 승급 심사용(Candidate) 섀도우 모델 학습을 시작합니다...")
            
            # 1. 딥카피: 현재 운영(Production) 모델의 가중치와 상태를 완전히 복제
            candidate = copy.deepcopy(self)
            
            # 2. 식별용 이름 부여
            candidate.name = candidate_name if candidate_name else f"{self.name}_Candidate"
            
            # 3. 복제된 도전자 객체만 최신 데이터로 학습 (In-place 덮어쓰기)
            candidate.model.fit(
                train_data[candidate.feature_cols], 
                train_data[candidate.target_col].values
            )
            candidate.is_fitted = True
            
            # 4. 운영 모델과 섞이지 않도록 'candidates' 하위 폴더에 저장
            candidate.save("models/v1/candidates")
            print(f"✅ [{candidate.name}] 학습 완료 및 승급 대기열 저장 완료. (현재 운영 모델은 안전하게 유지됩니다)")
            return

        # ================================================================
        # 기본 분기: 기존처럼 바로 운영 모델에 적용 (In-place Update)
        # ================================================================
        self.model.fit(
            train_data[self.feature_cols], 
            train_data[self.target_col].values
        )
        print(f"🔄 [{self.name}] 최신 {len(train_data)}개 데이터로 Rolling Retrain 완료.")
        self.is_fitted = True

    # AS-IS: def predict(self, data: pd.DataFrame) -> dict:
    # TO-BE: Dict 구조 엄격화
    def predict(self, data: pd.DataFrame) -> Dict[str, float]:
        default_resp = {"expected_return": 0.0}
        
        if not self.is_fitted: return default_resp

        try:
            # 추론 시에는 최신 1일 치 예측을 위해 최소한의 데이터만 사용 (속도 최적화)
            df_feat = self.feature_builder.process(data.iloc[-50:], is_training=False)
            latest_X = df_feat[self.feature_cols].iloc[-1:]
            
            # 예측: 미래 기대 수익률 추정치
            pred_return = self.model.predict(latest_X)[0]
            
            return {"expected_return": float(pred_return)}
        except Exception as e:
            print(f"🚨 [{self.name}] 예측 중 치명적 에러 발생: {e}")
            return default_resp


    def get_signal(self, min_confidence: float = 0.10) -> float:
        # AS-IS: Return 값 정수(1, 2, 0)
        # TO-BE: BaseModel의 Float 반환형 Interface 강제 규약에 맞춤
        if not self.latest_probs: return 1.0
        
        max_state = max(self.latest_probs, key=self.latest_probs.get) # type: ignore
        
        if self.latest_confidence < min_confidence: return 1.0 
        
        if max_state == "Bull_Prob": return 2.0
        elif max_state == "Bear_Prob": return 0.0
        else: return 1.0