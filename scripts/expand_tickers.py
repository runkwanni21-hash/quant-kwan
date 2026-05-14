#!/usr/bin/env python3
"""Expand ticker_aliases.yml to near-complete KR (KOSPI/KOSDAQ) + US (S&P500) coverage.

Usage:
    uv run python3 scripts/expand_tickers.py

Appends new entries to config/ticker_aliases.yml.
Skips symbols already present. Requires no external API calls for KR stocks
(Korean names are hardcoded). US names are fetched from yfinance in small batches.
"""
from __future__ import annotations

import sys
from pathlib import Path

import yaml

# ─── Korean master: symbol → (korean_name, english_name, board, sector, extra_aliases) ─
KR_MASTER: dict[str, tuple[str, str, str, str, list[str]]] = {
    # ── 지주사 ──────────────────────────────────────────────────────────────────
    "028260.KS": ("삼성물산", "Samsung C&T", "KOSPI", "지주/건설", ["삼성C&T"]),
    "003550.KS": ("LG", "LG Corporation", "KOSPI", "지주", ["LG지주", "LG그룹"]),
    "001040.KS": ("CJ", "CJ Corporation", "KOSPI", "지주", ["CJ지주", "CJ그룹"]),
    "004990.KS": ("롯데지주", "Lotte Holdings", "KOSPI", "지주", ["롯데그룹"]),
    "180640.KS": ("한진칼", "Hanjin KAL", "KOSPI", "항공지주", ["한진그룹"]),
    "000150.KS": ("두산", "Doosan Corporation", "KOSPI", "지주", ["두산지주", "두산홀딩스", "두산그룹"]),
    "402340.KS": ("SK스퀘어", "SK Square", "KOSPI", "IT지주", []),
    "000210.KS": ("DL", "DL Corporation", "KOSPI", "지주", ["DL그룹"]),
    "004800.KS": ("효성", "Hyosung Corporation", "KOSPI", "지주", ["효성그룹"]),
    "002020.KS": ("코오롱", "Kolon Corporation", "KOSPI", "지주", ["코오롱그룹"]),
    # ── KOSPI IT/전자 ────────────────────────────────────────────────────────────
    "018260.KS": ("삼성SDS", "Samsung SDS", "KOSPI", "IT서비스", ["삼성SDS"]),
    "011070.KS": ("LG이노텍", "LG Innotek", "KOSPI", "전자부품", ["LG이노텍"]),
    "034220.KS": ("LG디스플레이", "LG Display", "KOSPI", "디스플레이", ["LGD", "LG Display"]),
    "307950.KS": ("현대오토에버", "Hyundai AutoEver", "KOSPI", "IT서비스", ["현대오토에버"]),
    "053800.KS": ("안랩", "AhnLab", "KOSPI", "보안소프트웨어", ["안철수연구소", "AhnLab"]),
    "047560.KS": ("이스트소프트", "ESTsoft", "KOSPI", "소프트웨어", ["이스트소프트"]),
    # ── KOSPI 바이오/제약 ─────────────────────────────────────────────────────
    "326030.KS": ("SK바이오팜", "SK Biopharmaceuticals", "KOSPI", "바이오", ["SK바이오팜"]),
    "302440.KS": ("SK바이오사이언스", "SK Bioscience", "KOSPI", "바이오", ["SK바이오사이언스"]),
    "145020.KQ": ("휴젤", "Hugel", "KOSDAQ", "보톡스/필러", ["휴젤", "Hugel"]),
    "214370.KQ": ("케어젠", "Caregen", "KOSDAQ", "바이오/성장인자", ["케어젠"]),
    "036540.KQ": ("SFA반도체", "SFA Semiconductor", "KOSDAQ", "반도체", ["SFA반도체"]),
    # ── KOSPI 방산 ─────────────────────────────────────────────────────────────
    "272210.KS": ("한화시스템", "Hanwha Systems", "KOSPI", "방산/ICT", ["한화시스템"]),
    "047050.KS": ("포스코인터내셔널", "POSCO International", "KOSPI", "종합상사", ["포스코인터내셔널", "포스코인터"]),
    # ── KOSPI 조선/해양 ────────────────────────────────────────────────────────
    "443060.KS": ("HD현대마린솔루션", "HD Hyundai Marine Solution", "KOSPI", "조선서비스", ["HD현대마린솔루션"]),
    # ── KOSPI 소재/화학 ────────────────────────────────────────────────────────
    "120110.KS": ("코오롱인더", "Kolon Industries", "KOSPI", "화학/소재", ["코오롱인더스트리", "코오롱인더"]),
    "298050.KS": ("효성첨단소재", "Hyosung Advanced Materials", "KOSPI", "탄소섬유", ["효성첨단소재"]),
    "298020.KS": ("효성티앤씨", "Hyosung TNC", "KOSPI", "섬유/화학", ["효성티앤씨"]),
    "020150.KS": ("롯데에너지머티리얼즈", "Lotte Energy Materials", "KOSPI", "2차전지소재", ["롯데에너지머티리얼즈", "일진머티리얼즈"]),
    "103140.KS": ("풍산", "Poongsan Corporation", "KOSPI", "비철금속", ["풍산"]),
    "002380.KS": ("KCC", "KCC Corporation", "KOSPI", "건자재/소재", ["KCC"]),
    "344820.KS": ("KCC글라스", "KCC Glass", "KOSPI", "유리", ["KCC글라스"]),
    "011790.KS": ("SKC", "SKC", "KOSPI", "화학소재", ["SKC"]),
    # ── KOSPI 자동차 ───────────────────────────────────────────────────────────
    "161390.KS": ("한국타이어앤테크놀로지", "Hankook Tire", "KOSPI", "자동차부품", ["한국타이어", "한국타이어앤테크놀로지", "Hankook"]),
    "073240.KS": ("금호타이어", "Kumho Tire", "KOSPI", "자동차부품", ["금호타이어", "Kumho"]),
    "002350.KS": ("넥센타이어", "Nexen Tire", "KOSPI", "자동차부품", ["넥센타이어", "Nexen"]),
    # ── KOSPI 건설기계 ──────────────────────────────────────────────────────────
    "042670.KS": ("HD현대인프라코어", "HD Hyundai Infracore", "KOSPI", "건설기계", ["HD현대인프라코어", "현대두산인프라코어"]),
    "267270.KS": ("HD현대건설기계", "HD Hyundai CE", "KOSPI", "건설기계", ["HD현대건설기계"]),
    # ── KOSPI 소비재/생활 ──────────────────────────────────────────────────────
    "021240.KS": ("코웨이", "Coway", "KOSPI", "생활가전", ["코웨이", "Coway"]),
    "009240.KS": ("한샘", "Hanssem", "KOSPI", "인테리어/가구", ["한샘", "Hanssem"]),
    "010780.KS": ("아이에스동서", "IS Dongseo", "KOSPI", "환경/폐기물", ["아이에스동서"]),
    "383220.KS": ("F&F", "F&F Holdings", "KOSPI", "패션", ["F&F", "MLB패딩"]),
    "037270.KQ": ("YG플러스", "YG Plus", "KOSDAQ", "엔터/유통", ["YG플러스"]),
    # ── KOSPI 유통/식품 추가 ────────────────────────────────────────────────────
    "007070.KS": ("GS리테일", "GS Retail", "KOSPI", "유통", ["GS리테일", "GS25", "GS편의점"]),
    "069960.KS": ("현대백화점", "Hyundai Department Store", "KOSPI", "유통/백화점", ["현대백화점"]),
    "028090.KS": ("BGF리테일", "BGF Retail", "KOSPI", "유통", ["BGF리테일", "CU편의점"]),
    "282330.KS": ("BGF", "BGF", "KOSPI", "유통지주", ["BGF"]),
    "057050.KS": ("현대홈쇼핑", "Hyundai Home Shopping", "KOSPI", "유통/홈쇼핑", ["현대홈쇼핑"]),
    "023530.KS": ("롯데쇼핑", "Lotte Shopping", "KOSPI", "유통", ["롯데쇼핑", "롯데백화점", "롯데마트"]),
    # ── KOSPI 에너지 추가 ────────────────────────────────────────────────────────
    "267260.KS": ("HD현대일렉트릭", "HD Hyundai Electric", "KOSPI", "전력기기", ["HD현대일렉트릭", "현대일렉트릭"]),
    "298040.KS": ("효성중공업", "Hyosung Heavy Industries", "KOSPI", "전력기기", ["효성중공업"]),
    # ── KOSDAQ 반도체/장비 ────────────────────────────────────────────────────
    "056190.KQ": ("에스에프에이", "SFA", "KOSDAQ", "반도체FA/물류자동화", ["에스에프에이", "SFA"]),
    "090460.KQ": ("비에이치", "BH", "KOSDAQ", "FPCB", ["비에이치"]),
    "272290.KQ": ("이녹스첨단소재", "INNOX Advanced Materials", "KOSDAQ", "반도체소재", ["이녹스첨단소재", "이녹스"]),
    "140860.KQ": ("파크시스템스", "Park Systems", "KOSDAQ", "반도체장비/AFM", ["파크시스템스"]),
    "074600.KQ": ("원익QnC", "Wonik QnC", "KOSDAQ", "반도체소재/석영", ["원익QnC", "원익큐앤씨"]),
    "046890.KQ": ("서울반도체", "Seoul Semiconductor", "KOSDAQ", "LED", ["서울반도체"]),
    "098460.KQ": ("고영", "Koh Young Technology", "KOSDAQ", "3D검사장비", ["고영", "Koh Young"]),
    "039980.KQ": ("에스티아이", "STI", "KOSDAQ", "반도체장비", ["에스티아이", "STI"]),
    "091990.KQ": ("셀트리온헬스케어", "Celltrion Healthcare", "KOSDAQ", "바이오", ["셀트리온헬스케어"]),
    "050960.KQ": ("수산아이앤티", "Susan I&T", "KOSDAQ", "반도체장비", ["수산아이앤티"]),
    "131970.KQ": ("두산테스나", "Doosan Tesna", "KOSDAQ", "반도체테스트", ["두산테스나"]),
    "107640.KQ": ("한국전자인증", "Korea Electronic Certificate", "KOSDAQ", "보안", ["한국전자인증"]),
    # ── KOSDAQ 2차전지 ────────────────────────────────────────────────────────
    "336260.KQ": ("두산퓨얼셀", "Doosan Fuel Cell", "KOSDAQ", "수소/연료전지", ["두산퓨얼셀"]),
    "317400.KQ": ("엔에스", "NS", "KOSDAQ", "2차전지장비", ["엔에스"]),
    "402490.KQ": ("빅솔론", "Bixolon", "KOSDAQ", "2차전지장비", ["빅솔론"]),
    "299660.KQ": ("케어사인", "CareSign", "KOSDAQ", "2차전지소재", []),
    # ── KOSDAQ 의료기기/바이오 ─────────────────────────────────────────────────
    "048260.KQ": ("오스템임플란트", "Osstem Implant", "KOSDAQ", "치과임플란트", ["오스템임플란트", "Osstem"]),
    "145720.KQ": ("덴티움", "Dentium", "KOSDAQ", "치과임플란트", ["덴티움"]),
    "009420.KQ": ("한올바이오파마", "HanAll BioPharma", "KOSDAQ", "바이오", ["한올바이오파마", "한올"]),
    "068760.KQ": ("셀트리온제약", "Celltrion Pharm", "KOSDAQ", "제약", ["셀트리온제약"]),
    "338220.KQ": ("뷰노", "VUNO", "KOSDAQ", "의료AI", ["뷰노", "VUNO"]),
    "328130.KQ": ("루닛", "Lunit", "KOSDAQ", "의료AI", ["루닛", "Lunit"]),
    "287410.KQ": ("제이시스메디칼", "Jeisys Medical", "KOSDAQ", "의료기기", ["제이시스메디칼"]),
    "149980.KQ": ("하이로닉", "Hironic", "KOSDAQ", "의료기기", ["하이로닉"]),
    "228670.KQ": ("레이", "Ray", "KOSDAQ", "치과장비", ["레이"]),
    "045180.KQ": ("삼양식품", "Samyang Foods", "KOSPI", "식품", ["삼양식품", "불닭볶음면"]),
    "015570.KS": ("LIG넥스원", "LIG Nex1", "KOSPI", "방산", ["LIG넥스원"]),  # 079550 already there
    # ── KOSPI 금융 추가 ───────────────────────────────────────────────────────
    "175330.KS": ("JB금융지주", "JB Financial Group", "KOSPI", "금융", ["JB금융지주", "JB금융", "전북은행"]),
    "005940.KS": ("NH투자증권", "NH Investment & Securities", "KOSPI", "증권", ["NH투자증권", "NH증권"]),
    "001500.KS": ("현대차증권", "Hyundai Motor Securities", "KOSPI", "증권", ["현대차증권"]),
    "030610.KQ": ("교보증권", "Kyobo Securities", "KOSDAQ", "증권", ["교보증권"]),
    "001720.KS": ("신영증권", "Shinyoung Securities", "KOSPI", "증권", ["신영증권"]),
    "001450.KS": ("현대해상", "Hyundai Marine & Fire Insurance", "KOSPI", "보험", ["현대해상"]),
    "000370.KS": ("한화손해보험", "Hanwha General Insurance", "KOSPI", "보험", ["한화손해보험"]),
    # ── KOSPI 엔터/게임 추가 ────────────────────────────────────────────────────
    "263750.KQ": ("펄어비스", "Pearl Abyss", "KOSDAQ", "게임", ["펄어비스", "검은사막"]),
    "112040.KQ": ("위메이드", "Wemade", "KOSDAQ", "게임", ["위메이드", "미르"]),
    "036290.KQ": ("골프존", "Golfzon", "KOSDAQ", "스크린골프", ["골프존"]),
    "191410.KQ": ("육일씨엔에쓰", "Yooksam CNS", "KOSDAQ", "게임", []),
    "293490.KQ": ("카카오게임즈", "Kakao Games", "KOSDAQ", "게임", ["카카오게임즈"]),
    # ── KOSPI 건설 추가 ───────────────────────────────────────────────────────
    "047040.KS": ("대우건설", "Daewoo E&C", "KOSPI", "건설", ["대우건설"]),
    "028050.KS": ("삼성엔지니어링", "Samsung Engineering", "KOSPI", "건설/EPC", ["삼성엔지니어링", "삼성엔지"]),
    "006360.KS": ("GS건설", "GS Engineering & Construction", "KOSPI", "건설", ["GS건설", "자이"]),
    "000720.KS": ("현대건설", "Hyundai Engineering & Construction", "KOSPI", "건설", ["현대건설"]),
    # ── KOSPI 물류/해운 ───────────────────────────────────────────────────────
    "011200.KS": ("HMM", "HMM", "KOSPI", "해운", ["HMM", "현대상선"]),
    "028670.KS": ("팬오션", "Pan Ocean", "KOSPI", "해운", ["팬오션"]),
    "086280.KS": ("현대글로비스", "Hyundai Glovis", "KOSPI", "물류", ["현대글로비스", "글로비스"]),
    "002320.KS": ("한진", "Hanjin", "KOSPI", "물류", ["한진", "한진택배"]),
    # ── KOSDAQ 플랫폼/소프트웨어 ─────────────────────────────────────────────
    "248070.KQ": ("솔루엠", "SoluM", "KOSDAQ", "전자가격표시기", ["솔루엠"]),
    "317920.KQ": ("엔비티", "NBT", "KOSDAQ", "광고테크", ["엔비티"]),
    "216050.KQ": ("인크로스", "Incross", "KOSDAQ", "디지털광고", ["인크로스"]),
    "053300.KQ": ("피에스케이홀딩스", "PSK Holdings", "KOSDAQ", "반도체지주", ["피에스케이홀딩스"]),
    # ── KOSPI 항공/여행 ───────────────────────────────────────────────────────
    "003490.KS": ("대한항공", "Korean Air", "KOSPI", "항공", ["대한항공", "Korean Air"]),
    "020560.KS": ("아시아나항공", "Asiana Airlines", "KOSPI", "항공", ["아시아나항공", "Asiana"]),
    "089590.KQ": ("제주항공", "Jeju Air", "KOSDAQ", "저가항공", ["제주항공", "Jeju Air"]),
    "272450.KQ": ("진에어", "Jin Air", "KOSDAQ", "저가항공", ["진에어"]),
    "142510.KQ": ("에어부산", "Air Busan", "KOSDAQ", "저가항공", ["에어부산"]),
    "430900.KQ": ("에어인천", "Air Incheon", "KOSDAQ", "화물항공", ["에어인천"]),
    "008770.KS": ("호텔신라", "Hotel Shilla", "KOSPI", "호텔/면세", ["호텔신라", "신라면세점"]),
    "034230.KS": ("파라다이스", "Paradise", "KOSPI", "카지노/호텔", ["파라다이스"]),
    "114090.KS": ("GKL", "Grand Korea Leisure", "KOSPI", "카지노", ["GKL", "그랜드코리아레저"]),
    # ── KOSPI 식음료 ─────────────────────────────────────────────────────────
    "000080.KS": ("하이트진로", "Hite Jinro", "KOSPI", "식음료", ["하이트진로", "하이트", "진로"]),
    "005300.KS": ("롯데칠성", "Lotte Chilsung", "KOSPI", "음료", ["롯데칠성"]),
    "005180.KS": ("빙그레", "Binggrae", "KOSPI", "유제품/아이스크림", ["빙그레"]),
    "004370.KS": ("농심", "Nongshim", "KOSPI", "식품", ["농심", "신라면"]),
    "097950.KS": ("CJ제일제당", "CJ CheilJedang", "KOSPI", "식품", ["CJ제일제당", "CJ제당"]),
    "045080.KS": ("CJ CGV", "CJ CGV", "KOSPI", "영화관", ["CJ CGV", "CGV"]),
    "000640.KS": ("동아쏘시오홀딩스", "Dong-A Socio Holdings", "KOSPI", "제약지주", ["동아쏘시오홀딩스"]),
    "271560.KS": ("오리온", "Orion", "KOSPI", "식품", ["오리온", "초코파이"]),
    "004020.KS": ("현대제철", "Hyundai Steel", "KOSPI", "철강", ["현대제철"]),
    "010130.KS": ("고려아연", "Korea Zinc", "KOSPI", "비철금속/아연", ["고려아연"]),
    "001430.KS": ("세아베스틸지주", "SeAH Besteel Holdings", "KOSPI", "철강", ["세아베스틸지주", "세아베스틸"]),
    "010120.KS": ("LS ELECTRIC", "LS Electric", "KOSPI", "전력기기", ["LS일렉트릭", "LS ELECTRIC", "LS전기"]),
    "006260.KS": ("LS", "LS Corporation", "KOSPI", "지주", ["LS지주"]),
    "000880.KS": ("한화", "Hanwha Corporation", "KOSPI", "지주", ["한화지주"]),
    "112610.KS": ("씨에스윈드", "CS Wind", "KOSPI", "풍력타워", ["씨에스윈드", "CS Wind"]),
    "036460.KS": ("한국가스공사", "KOGAS", "KOSPI", "가스/에너지", ["한국가스공사", "KOGAS", "가스공사"]),
    "322000.KQ": ("현대에너지솔루션", "Hyundai Energy Solutions", "KOSDAQ", "태양광", ["현대에너지솔루션"]),
    "139480.KS": ("이마트", "E-Mart", "KOSPI", "유통", ["이마트", "E마트"]),
    "004170.KS": ("신세계", "Shinsegae", "KOSPI", "유통", ["신세계"]),
    "352820.KS": ("하이브", "HYBE", "KOSPI", "엔터", ["하이브", "HYBE", "빅히트"]),
    "041510.KQ": ("SM엔터테인먼트", "SM Entertainment", "KOSDAQ", "엔터", ["SM엔터", "에스엠"]),
    "035900.KQ": ("JYP엔터테인먼트", "JYP Entertainment", "KOSDAQ", "엔터", ["JYP엔터", "제이와이피"]),
    "122870.KQ": ("YG엔터테인먼트", "YG Entertainment", "KOSDAQ", "엔터", ["YG엔터", "와이지"]),
    "035760.KQ": ("CJ ENM", "CJ ENM", "KOSDAQ", "엔터/미디어", ["CJ ENM"]),
    "253450.KQ": ("스튜디오드래곤", "Studio Dragon", "KOSDAQ", "드라마제작", ["스튜디오드래곤"]),
    "017670.KS": ("SK텔레콤", "SK Telecom", "KOSPI", "통신", ["SK텔레콤", "SKT"]),
    "030200.KS": ("KT", "KT Corporation", "KOSPI", "통신", ["KT", "케이티"]),
    "032640.KS": ("LG유플러스", "LG Uplus", "KOSPI", "통신", ["LG유플러스", "유플러스"]),
    "000120.KS": ("CJ대한통운", "CJ Logistics", "KOSPI", "물류", ["CJ대한통운", "대한통운"]),
    "034730.KS": ("SK", "SK Holdings", "KOSPI", "지주", ["SK지주", "SK홀딩스"]),
    "096770.KS": ("SK이노베이션", "SK Innovation", "KOSPI", "에너지/배터리", ["SK이노베이션", "SK이노"]),
    "009830.KS": ("한화솔루션", "Hanwha Solutions", "KOSPI", "태양광/화학", ["한화솔루션", "큐셀"]),
    "010950.KS": ("S-Oil", "S-Oil Corporation", "KOSPI", "정유", ["S-OIL", "에쓰오일", "에스오일"]),
    "015760.KS": ("한국전력", "KEPCO", "KOSPI", "전력", ["한국전력", "한전", "KEPCO"]),
    "034020.KS": ("두산에너빌리티", "Doosan Enerbility", "KOSPI", "원전/발전설비", ["두산에너빌리티", "두산중공업"]),
    "005490.KS": ("포스코홀딩스", "POSCO Holdings", "KOSPI", "철강/소재", ["포스코홀딩스", "POSCO", "포스코"]),
    "051910.KS": ("LG화학", "LG Chem", "KOSPI", "화학/2차전지", ["LG화학", "엘지화학"]),
    "011170.KS": ("롯데케미칼", "Lotte Chemical", "KOSPI", "화학", ["롯데케미칼"]),
    "005930.KS": ("삼성전자", "Samsung Electronics", "KOSPI", "반도체/전자", ["삼성전자", "삼성", "Samsung"]),
    "000660.KS": ("SK하이닉스", "SK Hynix", "KOSPI", "메모리반도체", ["SK하이닉스", "하이닉스"]),
    "005380.KS": ("현대차", "Hyundai Motor", "KOSPI", "자동차", ["현대차", "현대자동차", "Hyundai"]),
    "000270.KS": ("기아", "Kia Corporation", "KOSPI", "자동차", ["기아", "기아차", "KIA"]),
    "012330.KS": ("현대모비스", "Hyundai Mobis", "KOSPI", "자동차부품", ["현대모비스", "모비스"]),
    "005390.KS": ("신성이엔지", "Shinsung E&G", "KOSPI", "태양광모듈", ["신성이엔지"]),
    "207940.KS": ("삼성바이오로직스", "Samsung Biologics", "KOSPI", "바이오CDMO", ["삼성바이오로직스", "삼성바이오"]),
    "068270.KS": ("셀트리온", "Celltrion", "KOSPI", "바이오시밀러", ["셀트리온"]),
    "196170.KQ": ("알테오젠", "Alteogen", "KOSDAQ", "바이오", ["알테오젠"]),
    "006400.KS": ("삼성SDI", "Samsung SDI", "KOSPI", "배터리/소재", ["삼성SDI"]),
    "373220.KS": ("LG에너지솔루션", "LG Energy Solution", "KOSPI", "배터리", ["LG에너지솔루션", "LG엔솔"]),
    "247540.KQ": ("에코프로비엠", "EcoPro BM", "KOSDAQ", "양극재", ["에코프로비엠"]),
    "086520.KQ": ("에코프로", "EcoPro", "KOSDAQ", "배터리소재", ["에코프로"]),
    "012450.KS": ("한화에어로스페이스", "Hanwha Aerospace", "KOSPI", "방산/항공", ["한화에어로스페이스", "한화에어로"]),
    "329180.KS": ("HD현대중공업", "HD Hyundai Heavy Industries", "KOSPI", "조선", ["HD현대중공업", "현대중공업"]),
    "042660.KS": ("한화오션", "Hanwha Ocean", "KOSPI", "조선", ["한화오션", "대우조선해양"]),
    "010140.KS": ("삼성중공업", "Samsung Heavy Industries", "KOSPI", "조선", ["삼성중공업"]),
    "047810.KS": ("한국항공우주", "KAI", "KOSPI", "방산/항공기", ["한국항공우주", "KAI"]),
    "079550.KS": ("LIG넥스원", "LIG Nex1", "KOSPI", "방산", ["LIG넥스원"]),
    "064350.KS": ("현대로템", "Hyundai Rotem", "KOSPI", "방산/K2전차", ["현대로템"]),
    "241560.KS": ("두산밥캣", "Doosan Bobcat", "KOSPI", "건설장비", ["두산밥캣", "Bobcat"]),
    "035420.KS": ("NAVER", "NAVER Corporation", "KOSPI", "포털/IT", ["NAVER", "네이버"]),
    "035720.KS": ("카카오", "Kakao", "KOSPI", "플랫폼", ["카카오"]),
    "105560.KS": ("KB금융", "KB Financial Group", "KOSPI", "금융지주", ["KB금융", "국민은행"]),
    "055550.KS": ("신한지주", "Shinhan Financial Group", "KOSPI", "금융지주", ["신한지주", "신한은행"]),
    "086790.KS": ("하나금융지주", "Hana Financial Group", "KOSPI", "금융지주", ["하나금융지주", "하나은행"]),
    "316140.KS": ("우리금융지주", "Woori Financial Group", "KOSPI", "금융지주", ["우리금융지주", "우리은행"]),
}

# ─── US master: symbol → (name, sector, extra_aliases) ─────────────────────
US_MASTER: dict[str, tuple[str, str, list[str]]] = {
    # ─ 빠진 S&P 500 대형주 ───────────────────────────────────────────────────
    "BRK-B": ("Berkshire Hathaway", "금융/보험", ["버크셔해서웨이", "워렌버핏", "Berkshire"]),
    "LOW": ("Lowe's Companies", "소비재/홈개선", ["로우스", "Lowes"]),
    "TJX": ("TJX Companies", "소비재/의류", ["TJX", "TJ맥스"]),
    "ROST": ("Ross Stores", "소비재/의류", ["로스스토어즈", "Ross"]),
    "CMG": ("Chipotle Mexican Grill", "식음료", ["치폴레", "Chipotle"]),
    "MDLZ": ("Mondelez International", "식품", ["몬덜리즈", "오레오"]),
    "GIS": ("General Mills", "식품", ["제너럴밀스"]),
    "STZ": ("Constellation Brands", "주류", ["컨스텔레이션브랜즈"]),
    "MNST": ("Monster Beverage", "음료", ["몬스터에너지", "Monster"]),
    "EL": ("Estée Lauder", "화장품", ["에스테로더", "Estee Lauder"]),
    "ULTA": ("Ulta Beauty", "화장품/소매", ["울타뷰티", "Ulta"]),
    "LULU": ("Lululemon Athletica", "의류/스포츠웨어", ["룰루레몬", "Lululemon"]),
    "RL": ("Ralph Lauren", "명품의류", ["랄프로렌", "Ralph Lauren"]),
    "TPR": ("Tapestry", "명품", ["타페스트리", "Coach", "코치"]),
    "T": ("AT&T", "통신", ["AT&T", "에이티앤티"]),
    "VZ": ("Verizon Communications", "통신", ["버라이즌", "Verizon"]),
    "TMUS": ("T-Mobile US", "통신", ["T모바일", "T-Mobile"]),
    "CMCSA": ("Comcast", "미디어/케이블", ["컴캐스트", "Comcast"]),
    "CHTR": ("Charter Communications", "케이블", ["차터커뮤니케이션즈"]),
    "WBD": ("Warner Bros. Discovery", "미디어", ["워너브라더스", "Warner Bros", "Warner Bros. Discovery"]),
    "TTWO": ("Take-Two Interactive", "게임", ["테이크투", "Take-Two", "GTA"]),
    "EA": ("Electronic Arts", "게임", ["EA게임즈", "Electronic Arts"]),
    "U": ("Unity Software", "게임엔진", ["유니티", "Unity"]),
    "DASH": ("DoorDash", "배달/배송", ["도어대시", "DoorDash"]),
    "LYFT": ("Lyft", "모빌리티", ["리프트", "Lyft"]),
    "GRAB": ("Grab Holdings", "동남아플랫폼", ["그랩", "Grab"]),
    "SE": ("Sea Limited", "동남아이커머스", ["시아리미티드", "Sea", "Shopee"]),
    "MELI": ("MercadoLibre", "중남미이커머스", ["메르카도리브레", "MercadoLibre"]),
    "ETSY": ("Etsy", "이커머스", ["엣시", "Etsy"]),
    "W": ("Wayfair", "가구이커머스", ["웨이페어", "Wayfair"]),
    "SHOP": ("Shopify", "이커머스플랫폼", ["쇼피파이", "Shopify"]),
    "SQ": ("Block", "핀테크/암호화폐", ["블록", "Block", "Square", "스퀘어"]),
    "SOFI": ("SoFi Technologies", "핀테크", ["소파이", "SoFi"]),
    "HOOD": ("Robinhood Markets", "핀테크/브로커", ["로빈후드", "Robinhood"]),
    "AFRM": ("Affirm Holdings", "BNPL", ["어펌", "Affirm"]),
    "UPST": ("Upstart Holdings", "AI대출", ["업스타트", "Upstart"]),
    "FISV": ("Fiserv", "금융IT", ["파이저브", "Fiserv"]),
    "ICE": ("Intercontinental Exchange", "거래소", ["ICE거래소", "Intercontinental Exchange"]),
    "CME": ("CME Group", "파생상품거래소", ["CME그룹", "시카고상품거래소"]),
    "NDAQ": ("Nasdaq", "거래소/데이터", ["나스닥거래소"]),
    "CBOE": ("Cboe Global Markets", "옵션거래소", ["시카고옵션거래소", "CBOE"]),
    "MSCI": ("MSCI", "금융서비스/지수", ["MSCI", "엠에스씨아이"]),
    "MCO": ("Moody's Corporation", "신용평가", ["무디스", "Moodys"]),
    "SPGI": ("S&P Global", "금융서비스", ["S&P글로벌"]),  # already there, skip handled by script
    "EQIX": ("Equinix", "데이터센터REIT", ["이퀴닉스", "Equinix"]),
    "AMT": ("American Tower", "통신타워REIT", ["아메리칸타워", "American Tower"]),
    "PLD": ("Prologis", "물류REIT", ["프롤로지스", "Prologis"]),
    "DLR": ("Digital Realty", "데이터센터REIT", ["디지털리얼티", "Digital Realty"]),
    "O": ("Realty Income", "리테일REIT", ["리얼티인컴", "Realty Income"]),
    "SPG": ("Simon Property Group", "쇼핑몰REIT", ["사이먼프로퍼티", "Simon Property"]),
    "PSA": ("Public Storage", "창고REIT", ["퍼블릭스토리지", "Public Storage"]),
    "ALB": ("Albemarle", "리튬/화학", ["알베말", "Albemarle"]),
    "CCJ": ("Cameco", "우라늄", ["카메코", "Cameco"]),
    "MP": ("MP Materials", "희토류", ["MP머티리얼즈", "희토류"]),
    "ENPH": ("Enphase Energy", "태양광인버터", ["엔페이즈", "Enphase"]),
    "FSLR": ("First Solar", "태양광모듈", ["퍼스트솔라", "First Solar"]),
    "SEDG": ("SolarEdge Technologies", "태양광", ["솔라엣지", "SolarEdge"]),
    "PLUG": ("Plug Power", "수소연료전지", ["플러그파워", "Plug Power"]),
    "BE": ("Bloom Energy", "연료전지", ["블룸에너지", "Bloom Energy"]),
    "PWR": ("Quanta Services", "전력인프라", ["퀀타서비스", "Quanta"]),
    "ETN": ("Eaton Corporation", "전력기기", ["이튼", "Eaton"]),
    "EMR": ("Emerson Electric", "산업자동화", ["에머슨", "Emerson"]),
    "HON": ("Honeywell", "산업재/항공", ["허니웰", "Honeywell"]),
    "CAT": ("Caterpillar", "건설기계", ["캐터필러", "Caterpillar"]),
    "DE": ("Deere & Company", "농기계", ["존디어", "Deere", "John Deere"]),
    "GE": ("GE Aerospace", "항공엔진/방산", ["GE에어로스페이스", "GE Aerospace"]),
    "LHX": ("L3Harris Technologies", "방산", ["엘쓰리해리스", "L3Harris"]),
    "NOC": ("Northrop Grumman", "방산/스텔스", ["노스롭그루먼", "Northrop"]),
    "GD": ("General Dynamics", "방산/IT", ["제너럴다이나믹스", "General Dynamics"]),
    "BA": ("Boeing", "항공우주", ["보잉", "Boeing"]),
    "HII": ("Huntington Ingalls", "조선/방산", ["헌팅턴잉걸스"]),
    "TDY": ("Teledyne Technologies", "방산기술", ["텔레다인", "Teledyne"]),
    "BWA": ("BorgWarner", "자동차부품", ["보그워너", "BorgWarner"]),
    "APTV": ("Aptiv", "자동차기술", ["앱티브", "Aptiv"]),
    "MGA": ("Magna International", "자동차부품", ["마그나", "Magna"]),
    "MBLY": ("Mobileye", "자율주행", ["모빌아이", "Mobileye"]),
    "LAZR": ("Luminar Technologies", "라이다", ["루미나", "Luminar"]),
    "LCID": ("Lucid Group", "전기차", ["루시드", "Lucid"]),
    "RIVN": ("Rivian Automotive", "전기차", ["리비안", "Rivian"]),
    "TSLA": ("Tesla", "전기차/AI", ["테슬라", "Tesla"]),  # already there
    "MRVL": ("Marvell Technology", "AI반도체/데이터센터칩", ["마벨", "Marvell"]),
    "SWKS": ("Skyworks Solutions", "RF반도체", ["스카이웍스", "Skyworks"]),
    "QRVO": ("Qorvo", "RF반도체", ["코르보", "Qorvo"]),
    "LSCC": ("Lattice Semiconductor", "FPGA", ["래티스반도체", "Lattice"]),
    "IPGP": ("IPG Photonics", "파이버레이저", ["IPG포토닉스"]),
    "COHR": ("Coherent", "광통신부품", ["코히런트", "Coherent"]),
    "CIEN": ("Ciena", "광통신장비", ["시에나", "Ciena"]),
    "NTAP": ("NetApp", "스토리지/클라우드", ["넷앱", "NetApp"]),
    "PSTG": ("Pure Storage", "올플래시스토리지", ["퓨어스토리지", "Pure Storage"]),
    "HUBS": ("HubSpot", "마케팅SaaS", ["허브스팟", "HubSpot"]),
    "TEAM": ("Atlassian", "개발협업SaaS", ["아틀라시안", "Atlassian", "Jira", "Confluence"]),
    "TWLO": ("Twilio", "통신API", ["트윌리오", "Twilio"]),
    "BILL": ("Bill.com", "결제SaaS", ["빌닷컴"]),
    "PAYC": ("Paycom Software", "HR소프트웨어", ["페이컴", "Paycom"]),
    "VEEV": ("Veeva Systems", "헬스케어SaaS", ["비바시스템즈", "Veeva"]),
    "ALGN": ("Align Technology", "치아교정/투명교정기", ["얼라인테크", "Invisalign"]),
    "DXCM": ("Dexcom", "연속혈당측정기", ["덱스컴", "Dexcom"]),
    "EW": ("Edwards Lifesciences", "심장의료기기", ["에드워즈", "Edwards"]),
    "ILMN": ("Illumina", "유전체분석", ["일루미나", "Illumina"]),
    "RXRX": ("Recursion Pharmaceuticals", "AI신약개발", ["리커션", "Recursion"]),
    "IONQ": ("IonQ", "양자컴퓨팅", ["아이온큐", "IonQ"]),
    "RGTI": ("Rigetti Computing", "양자컴퓨팅", ["리게티", "Rigetti"]),
    "QBTS": ("D-Wave Quantum", "양자컴퓨팅", ["디웨이브", "D-Wave"]),
    "ASTS": ("AST SpaceMobile", "위성광대역인터넷", ["AST스페이스모바일", "AST SpaceMobile"]),
    "RKLB": ("Rocket Lab", "소형발사체", ["로켓랩", "Rocket Lab"]),
    "SOUN": ("SoundHound AI", "음성AI", ["사운드하운드", "SoundHound"]),
    "MSTR": ("MicroStrategy", "비트코인기업", ["마이크로스트래티지", "MicroStrategy"]),
    "RIOT": ("Riot Platforms", "비트코인채굴", ["라이엇플랫폼스", "Riot"]),
    "MARA": ("MARA Holdings", "비트코인채굴", ["마라홀딩스", "MARA"]),
    "CLSK": ("CleanSpark", "비트코인채굴", ["클린스파크"]),
    "CORZ": ("Core Scientific", "비트코인채굴", ["코어사이언티픽"]),
    "MGM": ("MGM Resorts International", "카지노/리조트", ["MGM리조트", "MGM"]),
    "WYNN": ("Wynn Resorts", "카지노/럭셔리", ["윈리조트", "Wynn"]),
    "LVS": ("Las Vegas Sands", "카지노/마카오", ["라스베이거스샌즈", "Las Vegas Sands"]),
    "DKNG": ("DraftKings", "스포츠베팅", ["드래프트킹스", "DraftKings"]),
    "LYV": ("Live Nation Entertainment", "공연/티켓", ["라이브네이션", "Live Nation", "Ticketmaster"]),
    "CCL": ("Carnival Corporation", "크루즈", ["카니발크루즈", "Carnival"]),
    "NCLH": ("Norwegian Cruise Line", "크루즈", ["노르웨지안크루즈", "Norwegian"]),
    "MAR": ("Marriott International", "호텔체인", ["메리어트", "Marriott"]),
    "HLT": ("Hilton Worldwide", "호텔체인", ["힐튼", "Hilton"]),
    "EXPE": ("Expedia Group", "온라인여행", ["익스피디아", "Expedia"]),
    "LUV": ("Southwest Airlines", "저가항공", ["사우스웨스트항공", "Southwest"]),
    "FDX": ("FedEx", "특송/물류", ["페덱스", "FedEx"]),
    "UPS": ("United Parcel Service", "물류", ["UPS", "유피에스"]),
    "MPC": ("Marathon Petroleum", "정유", ["마라톤페트롤리엄", "Marathon Petroleum"]),
    "PSX": ("Phillips 66", "정유/화학", ["필립스66", "Phillips 66"]),
    "VLO": ("Valero Energy", "정유", ["발레로에너지", "Valero"]),
    "EOG": ("EOG Resources", "셰일오일", ["EOG리소시즈"]),
    "DVN": ("Devon Energy", "셰일오일", ["데본에너지", "Devon"]),
    "FANG": ("Diamondback Energy", "셰일오일", ["다이아몬드백에너지"]),
    "HAL": ("Halliburton", "유전서비스", ["할리버튼", "Halliburton"]),
    "BKR": ("Baker Hughes", "유전서비스", ["베이커휴즈", "Baker Hughes"]),
    "MO": ("Altria Group", "담배", ["알트리아", "Altria", "Marlboro"]),
    "PM": ("Philip Morris International", "담배/전자담배", ["필립모리스", "Philip Morris", "IQOS"]),
    "ELV": ("Elevance Health", "건강보험", ["엘리번스헬스", "Elevance"]),
    "CI": ("The Cigna Group", "건강보험", ["시그나", "Cigna"]),
    "HCA": ("HCA Healthcare", "병원체인", ["HCA헬스케어", "HCA"]),
    "MCK": ("McKesson Corporation", "의약품유통", ["맥케슨", "McKesson"]),
    "EQT": ("EQT Corporation", "천연가스", ["EQT"]),
    "TTD": ("The Trade Desk", "프로그래매틱광고", ["트레이드데스크", "Trade Desk"]),
    "PINS": ("Pinterest", "소셜미디어/이미지", ["핀터레스트", "Pinterest"]),
    "RDDT": ("Reddit", "소셜미디어", ["레딧", "Reddit"]),
    "BILI": ("Bilibili", "중국동영상플랫폼", ["빌리빌리", "Bilibili", "B站"]),
    "IQ": ("iQIYI", "중국OTT", ["아이치이", "iQIYI"]),
    "KWEB": ("KraneShares CSI China Internet ETF", "중국인터넷ETF", ["KWEB", "중국인터넷ETF"]),
    "FXI": ("iShares China Large-Cap ETF", "중국대형주ETF", ["FXI", "중국ETF"]),
    "EEM": ("iShares MSCI Emerging Markets ETF", "신흥국ETF", ["EEM", "신흥국ETF"]),
    "EWY": ("iShares MSCI South Korea ETF", "한국ETF", ["EWY", "한국ETF"]),
    "EWJ": ("iShares MSCI Japan ETF", "일본ETF", ["EWJ", "일본ETF"]),
    "EWT": ("iShares MSCI Taiwan ETF", "대만ETF", ["EWT", "대만ETF"]),
    "INDA": ("iShares MSCI India ETF", "인도ETF", ["INDA", "인도ETF"]),
    "TQQQ": ("ProShares UltraPro QQQ 3X", "레버리지ETF", ["TQQQ", "나스닥3배레버리지"]),
    "SQQQ": ("ProShares UltraPro Short QQQ 3X", "인버스ETF", ["SQQQ", "나스닥3배인버스"]),
    "SOXL": ("Direxion Daily Semiconductor Bull 3X", "반도체3배레버리지", ["SOXL", "반도체3배"]),
    "SOXS": ("Direxion Daily Semiconductor Bear 3X", "반도체3배인버스", ["SOXS"]),
    "UPRO": ("ProShares UltraPro S&P500 3X", "S&P500레버리지", ["UPRO", "S&P500 3배"]),
    "LABU": ("Direxion Daily S&P Biotech Bull 3X", "바이오3배레버리지", ["LABU"]),
    "FNGU": ("MicroSectors FANG+ Index 3X", "빅테크3배레버리지", ["FNGU", "FANG플러스3배"]),
    "TECL": ("Direxion Daily Technology Bull 3X", "기술주3배레버리지", ["TECL"]),
    "TSLL": ("Direxion Daily TSLA Bull 2X", "테슬라2배레버리지", ["TSLL"]),
    "NVDL": ("GraniteShares 2x Long NVDA Daily ETF", "엔비디아2배레버리지", ["NVDL"]),
    "MSFO": ("YieldMax MSFT Option Income ETF", "마이크로소프트옵션ETF", []),
    "JEPI": ("JPMorgan Equity Premium Income ETF", "커버드콜ETF", ["JEPI", "배당ETF"]),
    "JEPQ": ("JPMorgan Nasdaq Equity Premium Income ETF", "나스닥커버드콜", ["JEPQ"]),
    "SCHD": ("Schwab U.S. Dividend Equity ETF", "배당ETF", ["SCHD", "배당주ETF"]),
    "VIG": ("Vanguard Dividend Appreciation ETF", "배당성장ETF", ["VIG"]),
    "DGRO": ("iShares Core Dividend Growth ETF", "배당성장ETF", ["DGRO"]),
    "HDV": ("iShares Core High Dividend ETF", "고배당ETF", ["HDV"]),
}


def load_existing_symbols(yaml_path: Path) -> set[str]:
    with open(yaml_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return {s["symbol"] for s in data.get("stocks", [])}


def make_kr_entry(symbol: str, info: tuple) -> str:
    korean_name, english_name, board, sector, extra_aliases = info
    market = "KR"
    aliases = [korean_name]
    if english_name and english_name not in aliases:
        aliases.append(english_name)
    for a in extra_aliases:
        if a and a not in aliases:
            aliases.append(a)
    aliases_yaml = "[" + ", ".join(f'"{a}"' for a in aliases) + "]"
    board_line = f'    board: {board}\n' if board else ''
    return (
        f'  - symbol: "{symbol}"\n'
        f'    name: "{korean_name}"\n'
        f'    market: {market}\n'
        f'{board_line}'
        f'    sector: {sector}\n'
        f'    aliases: {aliases_yaml}\n\n'
    )


def make_us_entry(symbol: str, name: str, sector: str, extra_aliases: list[str]) -> str:
    aliases = [symbol, name]
    for a in extra_aliases:
        if a and a not in aliases:
            aliases.append(a)
    aliases_yaml = "[" + ", ".join(f'"{a}"' for a in aliases) + "]"
    # Short all-caps tickers need context
    needs_ctx = symbol.isascii() and symbol.isupper() and 1 <= len(symbol) <= 4
    ctx_line = f'    require_context_aliases: ["{symbol}"]\n' if needs_ctx else ''
    return (
        f'  - symbol: "{symbol}"\n'
        f'    name: "{name}"\n'
        f'    market: US\n'
        f'    sector: {sector}\n'
        f'    aliases: {aliases_yaml}\n'
        f'{ctx_line}\n'
    )


def main() -> None:
    root = Path(__file__).parent.parent
    yaml_path = root / "config" / "ticker_aliases.yml"

    print(f"Loading existing symbols from {yaml_path} …", file=sys.stderr)
    existing = load_existing_symbols(yaml_path)
    print(f"  → {len(existing)} symbols already registered", file=sys.stderr)

    kr_new = {s: info for s, info in KR_MASTER.items() if s not in existing}
    us_new = {s: info for s, info in US_MASTER.items() if s not in existing}
    print(f"  → KR new: {len(kr_new)}, US new: {len(us_new)}", file=sys.stderr)

    lines: list[str] = []

    if kr_new:
        lines.append("\n  # ─── 한국 종목 자동 확장 (expand_tickers.py) ──────────────────────────────\n")
        # Group by board/sector roughly
        for symbol, info in sorted(kr_new.items()):
            lines.append(make_kr_entry(symbol, info))

    if us_new:
        lines.append("\n  # ─── 미국 종목 자동 확장 (expand_tickers.py) ──────────────────────────────\n")
        for symbol, info in sorted(us_new.items()):
            name, sector, extra_aliases = info
            lines.append(make_us_entry(symbol, name, sector, extra_aliases))

    if not lines:
        print("Nothing new to add.", file=sys.stderr)
        return

    # Insert before "themes:" section
    content = yaml_path.read_text(encoding="utf-8")
    insert_marker = "\nthemes:"
    idx = content.find(insert_marker)
    if idx < 0:
        print("ERROR: could not find 'themes:' marker in YAML", file=sys.stderr)
        sys.exit(1)

    new_content = content[:idx] + "".join(lines) + content[idx:]
    yaml_path.write_text(new_content, encoding="utf-8")
    print(f"Done. Added {len(kr_new)} KR + {len(us_new)} US entries.", file=sys.stderr)

    # Quick validation
    try:
        import tele_quant.analysis.aliases as _al
        _al._CACHE.clear()
        from tele_quant.analysis.aliases import load_alias_config
        book = load_alias_config(yaml_path)
        syms = book.all_symbols
        from collections import Counter
        c = Counter(s.market for s in syms)
        print(f"Validation OK — total {len(syms)} symbols: {dict(c)}", file=sys.stderr)
    except Exception as e:
        print(f"Validation error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
