import pandas as pd
import joblib
import os
from abc import ABC, abstractmethod

class BaseFeatureBuilder(ABC):
    """
    모든 피처 빌더(Feature Builder)가 반드시 지켜야 하는 규칙 (Interface)
    """
    
    @abstractmethod
    def build_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """순수 피처(X)를 생성하는 로직을 구현해야 합니다."""
        pass

    @abstractmethod
    def build_labels(self, df: pd.DataFrame) -> pd.DataFrame:
        """정답지(y, Target)를 생성하는 로직을 구현해야 합니다."""
        pass

    def process(self, df: pd.DataFrame, is_training: bool = False) -> pd.DataFrame:
        """
        [템플릿 메서드] 
        데이터를 받아 피처를 만들고, 학습 모드일 때만 정답지를 추가합니다.
        자식 클래스는 이 함수를 덮어쓰지 않고 그대로 사용합니다.
        """
        out = self.build_features(df)
        if is_training:
            out = self.build_labels(out)
        return out


class QuantitativeModel(ABC):
    """
    모든 퀀트 AI 모델(LGBM, PCA 등)이 반드시 지켜야 하는 규칙 (Interface)
    """
    def __init__(self, name: str):
        self.name = name
        self.is_fitted = False
        
    @abstractmethod
    def fit(self, data: pd.DataFrame):
        """데이터를 받아 모델을 학습시키는 로직을 구현해야 합니다."""
        pass
        
    @abstractmethod
    def predict(self, data: pd.DataFrame) -> dict:
        """데이터를 받아 확률, 점수 등의 예측 결과를 딕셔너리로 반환해야 합니다."""
        pass
        
    @abstractmethod
    def get_signal(self):
        """최종적으로 황제(Emperor)에게 전달할 상태/신호를 반환해야 합니다."""
        pass
    
    # 🌟 [추가] 모델 추출 (Save)
    def save(self, folder_path: str = "checkpoints"):
        """학습된 모델 객체 전체를 파일로 저장합니다."""
        if not self.is_fitted:
            print(f"⚠️ [{self.name}] 모델이 학습되지 않아 저장할 수 없습니다.")
            return
        
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            
        file_path = os.path.join(folder_path, f"{self.name}.pkg")
        joblib.dump(self, file_path)
        print(f"💾 [{self.name}] 모델이 성공적으로 추출되었습니다: {file_path}")

    # 🌟 [추가] 모델 로딩 (Load)
    @classmethod
    def load(cls, file_path: str):
        """저장된 모델 파일을 읽어와 인스턴스를 복원합니다."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"🚨 모델 파일을 찾을 수 없습니다: {file_path}")
            
        model = joblib.load(file_path)
        print(f"📂 [{model.name}] 모델을 성공적으로 불러왔습니다.")
        return model