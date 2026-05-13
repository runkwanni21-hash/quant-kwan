import os
import argparse
from datetime import datetime

def merge_code_files(target_dirs=None, output_filename=None, extensions=None, ignore_files=None):
    """
    지정된 디렉토리 리스트와 하위 디렉토리를 순회하며 코드를 하나의 파일로 합칩니다.
    """
    
    # 1. 대상 폴더 리스트가 지정되지 않았거나 비어있으면 현재 실행 경로(.)로 설정
    if target_dirs is None or len(target_dirs) == 0:
        target_dirs = [os.getcwd()]
        
    # 2. 결과물 파일 이름이 없으면 현재 시간 조합하여 자동 생성
    if output_filename is None:
        now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"mergedcode_{now_str}.txt"
        
    # 결과물 저장 경로는 현재 실행 경로
    output_filepath = os.path.join(os.getcwd(), output_filename)

    # 3. 합칠 기본 확장자 목록
    if extensions is None:
        extensions = ['.py', '.js', '.ts', '.html', '.css', '.java', '.cpp', '.c', '.h', '.md', '.json']

    # 4. 무시할 파일(블랙리스트) 기본값 설정
    if ignore_files is None:
        ignore_files = ['__init__.py', '.DS_Store'] # 맥OS 숨김파일도 센스있게 추가

    merged_count = 0
    ignored_count = 0

    print(f"탐색 대상 폴더들: {target_dirs}")
    print(f"제외할 파일들: {ignore_files}")
    print(f"결과 저장 파일: {output_filepath}\n")

    # 결과를 저장할 파일 열기 (utf-8 인코딩)
    with open(output_filepath, 'w', encoding='utf-8') as outfile:
        # 전달받은 폴더 리스트를 하나씩 순회
        for target_dir in target_dirs:
            # 폴더가 실제로 존재하는지 체크
            if not os.path.isdir(target_dir):
                print(f"⚠️ [경고] 존재하지 않거나 폴더가 아닙니다. 건너뜁니다: {target_dir}")
                continue

            # os.walk를 사용하여 해당 폴더의 하위 폴더까지 순회
            for dirpath, dirnames, filenames in os.walk(target_dir):
                for filename in filenames:
                    # 1. 결과물 파일 자신이 다시 병합되는 것을 방지
                    if filename == output_filename:
                        continue
                    
                    # 2. 블랙리스트에 있는 파일명인지 체크하여 건너뛰기
                    if filename in ignore_files:
                        ignored_count += 1
                        continue
                        
                    # 3. 파일 확장자 확인 (화이트리스트)
                    if any(filename.endswith(ext) for ext in extensions):
                        file_path = os.path.join(dirpath, filename)
                        
                        # 현재 작업 디렉토리를 기준으로 상대 경로 계산하여 보기 좋게 만들기
                        rel_path = os.path.relpath(file_path, os.getcwd())

                        try:
                            # 파일 읽기
                            with open(file_path, 'r', encoding='utf-8') as infile:
                                content = infile.read()
                            
                            # 구분선 및 경로/파일명 명시
                            outfile.write(f"\n\n{'='*80}\n")
                            outfile.write(f"File Path : {rel_path}\n")
                            outfile.write(f"{'='*80}\n\n")
                            
                            # 코드 내용 쓰기
                            outfile.write(content)
                            outfile.write("\n")
                            
                            print(f"[성공] 병합 완료: {rel_path}")
                            merged_count += 1
                            
                        except UnicodeDecodeError:
                            print(f"[건너뜀] 텍스트 파일이 아니거나 인코딩 에러: {rel_path}")
                        except Exception as e:
                            print(f"[에러] {rel_path} 읽기 실패: {e}")

    print(f"\n✅ 총 {merged_count}개의 파일 병합 완료. (제외된 파일 수: {ignored_count}개)")


if __name__ == "__main__":
    # 터미널 명령어 파서를 생성합니다.
    parser = argparse.ArgumentParser(description="지정된 폴더의 코드를 하나로 병합합니다.")
    
    # 여러 개의 폴더 이름을 받을 수 있도록 설정
    parser.add_argument(
        'folders', 
        metavar='FOLDER', 
        type=str, 
        nargs='*', 
        help='병합할 폴더 이름들을 띄어쓰기로 구분하여 입력하세요. (예: core manager)'
    )
    
    # 제외할 파일명 옵션 추가 (--ignore)
    parser.add_argument(
        '--ignore', 
        type=str, 
        nargs='*', 
        default=['__init__.py', '.DS_Store'],
        help='병합에서 제외할 정확한 파일명들을 띄어쓰기로 구분하여 입력하세요. (기본값: __init__.py .DS_Store)'
    )
    
    # 사용자가 입력한 명령어 파라미터들을 분석
    args = parser.parse_args()
    
    # 파라미터 전달 (폴더 리스트와 제외할 파일 리스트)
    merge_code_files(target_dirs=args.folders, ignore_files=args.ignore)