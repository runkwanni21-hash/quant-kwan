import pandas as pd
import joblib
import os
from abc import ABC, abstractmethod
# AS-IS: 타입 힌팅 부재
# TO-BE: 강력한 타입 힌팅을 위해 typing 모듈 추가
from typing import Dict, Any, Optional

class BaseFeatureBuilder(ABC):
    """모든 피처 빌더(Feature Builder)가 반드시 지켜야 하는 규칙 (Interface)"""
    
    @abstractmethod
    def build_features(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    @abstractmethod
    def build_labels(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    def process(self, df: pd.DataFrame, is_training: bool = False) -> pd.DataFrame:
        out = self.build_features(df)
        if is_training:
            out = self.build_labels(out)
        return out


class QuantitativeModel(ABC):
    """모든 퀀트 AI 모델이 반드시 지켜야 하는 규칙 (Interface)"""
    
    # AS-IS: def __init__(self, name: str):
    # TO-BE: 생성자의 반환형(None) 명시
    def __init__(self, name: str) -> None:
        self.name = name
        self.is_fitted = False
        
    @abstractmethod
    # AS-IS: def fit(self, data: pd.DataFrame):
    # TO-BE: 반환형(None) 명시
    def fit(self, data: pd.DataFrame) -> None:
        pass
    
    # AS-IS: def update(self, data: pd.DataFrame) -> None:
    # TO-BE: 승급 심사를 위한 candidate_mode 파라미터 추가
    @abstractmethod
    def update(self, data: pd.DataFrame, candidate_mode: bool = False, candidate_name: Optional[str] = None) -> None:
        """
        매일 새로 수집된 데이터를 받아 기존 모델을 업데이트합니다.
        candidate_mode=True일 경우 운영 모델을 덮어쓰지 않고 승급 심사용 모델로 분리 저장합니다.
        """
        pass
        
    @abstractmethod
    # AS-IS: def predict(self, data: pd.DataFrame) -> dict:
    # TO-BE: 구체화된 Dict 구조 명시
    def predict(self, data: pd.DataFrame) -> Dict[str, Any]:
        pass
        
    @abstractmethod
    # AS-IS: def get_signal(self):
    # TO-BE: Manager가 기대하는 숫자형(float) 타입으로 반환형 명시
    def get_signal(self) -> float:
        pass
    
    # AS-IS: def save(self, folder_path: str = "checkpoints"):
    # TO-BE: 반환형 명시
    def save(self, folder_path: str = "checkpoints") -> None:
        if not self.is_fitted:
            print(f"⚠️ [{self.name}] 모델이 학습되지 않아 저장할 수 없습니다.")
            return
        
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            
        file_path = os.path.join(folder_path, f"{self.name}.pkg")
        joblib.dump(self, file_path)
        print(f"💾 [{self.name}] 모델이 성공적으로 추출되었습니다: {file_path}")

    @classmethod
    # AS-IS: def load(cls, file_path: str):
    # TO-BE: 반환형을 자기 자신 클래스(QuantitativeModel)로 명시
    def load(cls, file_path: str) -> 'QuantitativeModel':
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"🚨 모델 파일을 찾을 수 없습니다: {file_path}")
            
        model = joblib.load(file_path)
        print(f"📂 [{model.name}] 모델을 성공적으로 불러왔습니다.")
        return model