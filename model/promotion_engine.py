import os
import shutil
import random
import pandas as pd
# 🌟 TO-BE: 의존성 주입(DI)을 위한 함수 타입 힌팅 추가
from typing import Callable, Optional

from .base_model import QuantitativeModel

class PromotionEngine:
    """
    도전자(Candidate) 모델이 운영(Champion) 모델보다 우수한지 심사하는 엔진.
    사용자 정의 평가 함수(Strategy)를 주입받아 확장성 있는 백테스트를 지원합니다.
    """
    def __init__(self) -> None:
        pass

    def evaluate_performance(self, champion: QuantitativeModel, candidate: QuantitativeModel, test_data: pd.DataFrame) -> bool:
        """
        [기본 평가기] 별도의 평가 함수가 주입되지 않았을 때 사용되는 폴백(Fallback) 메서드.
        """
        print(f"📊 [{champion.name} vs {candidate.name}] 기본 백테스트 심사 진행 중 (Random 모드)...")
        is_approved: bool = random.choice([True, False])
        return is_approved

    # AS-IS: def execute(self, champion_path: str, candidate_path: str, test_data: pd.DataFrame) -> bool:
    # TO-BE: custom_evaluator 파라미터를 추가하여 런타임에 외부 평가 로직 주입 허용
    def execute(
        self, 
        champion_path: str, 
        candidate_path: str, 
        test_data: pd.DataFrame,
        custom_evaluator: Optional[Callable[[QuantitativeModel, QuantitativeModel, pd.DataFrame], bool]] = None
    ) -> bool:
        """
        경로를 받아 모델을 로드하고 심사한 뒤, 파일 시스템(승격/폐기)을 관리합니다.
        custom_evaluator가 제공되면 해당 함수를 사용하고, 없으면 내장 evaluate_performance를 사용합니다.
        """
        if not os.path.exists(champion_path):
            print(f"⚠️ [승급 심사] 챔피언 모델을 찾을 수 없습니다: {champion_path}")
            return False
            
        if not os.path.exists(candidate_path):
            print(f"⚠️ [승급 심사] 도전자 모델을 찾을 수 없습니다: {candidate_path}")
            return False

        try:
            champion: QuantitativeModel = QuantitativeModel.load(champion_path)
            candidate: QuantitativeModel = QuantitativeModel.load(candidate_path)
        except Exception as e:
            print(f"🚨 [승급 심사] 모델 로드 중 치명적 에러 발생: {e}")
            return False

        print(f"\n⚖️ [승급 심사 개시] 👑 챔피언({champion.name}) VS 🗡️ 도전자({candidate.name})")
        
        # =========================================================
        # 🌟 핵심: 의존성 주입(DI) 라우팅
        # 주입된 함수가 있으면 사용, 없으면 내장 클래스 메서드 사용
        # =========================================================
        evaluator: Callable[[QuantitativeModel, QuantitativeModel, pd.DataFrame], bool] = (
            custom_evaluator if custom_evaluator is not None else self.evaluate_performance
        )
        
        # 다형성을 활용한 심사 로직 실행
        is_approved: bool = evaluator(champion, candidate, test_data)

        if is_approved:
            print("🎉 [심사 결과] 도전자 모델 승인! (성능 우위 감지)")
            print(f"✅ 챔피언 교체: {candidate_path} -> {champion_path}")
            
            shutil.copy2(candidate_path, champion_path)
            os.remove(candidate_path)
            
            print("🏆 새로운 챔피언이 성공적으로 등극했습니다.\n")
            return True
        else:
            print("❌ [심사 결과] 도전자 모델 반려! (기존 챔피언 방어 성공)")
            print(f"🗑️ 도전자 모델 폐기: {candidate_path}")
            
            os.remove(candidate_path)
            
            print("🛡️ 기존 챔피언 모델이 그대로 유지됩니다.\n")
            return False