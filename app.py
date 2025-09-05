import streamlit as st
import pandas as pd
from datetime import datetime
from io import BytesIO
import gspread
from google.oauth2.service_account import Credentials

# ================== Streamlit 기본 설정 ==================
st.set_page_config(page_title="롯데ON 주문건 변환기", page_icon="🛒")

# ================== Google Sheets 인증 ==================
@st.cache_resource
def get_gspread_client():
    creds = Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"]
    )
    return gspread.authorize(creds)

@st.cache_data(ttl=600)
def load_mapping():
    try:
        gc = get_gspread_client()
        sheet_id = st.secrets["GSHEETS_ID"]
        ws_name = st.secrets.get("GSHEETS_WORKSHEET", "Sheet1")
        sh = gc.open_by_key(sheet_id)
        ws = sh.worksheet(ws_name)

        records = ws.get_all_records()
        mapping = {
            str(row.get("상품번호", "")).strip(): str(row.get("상품명", "")).strip()
            for row in records if row.get("상품번호")
        }
        return mapping, ws
    except Exception as e:
        st.error("❌ 구글 시트 로드 실패")
        st.exception(e)
        return {}, None

# 매핑 현황은 동적으로 로드 (매핑 추가 후 실시간 업데이트를 위해)

# ================== 롯데ON 모음딜 하드코딩 ==================
lotteon_map = {
    '바닐라시럽1000ml': 'LO1506416845_1',
    '카라멜시럽1000ml': 'LO1506416845_2',
    '헤이즐넛시럽1000ml': 'LO1506416845_3',
    '그린민트시럽1000ml': 'LO1506416845_4',
    '블루큐라소시럽1000ml': 'LO1506416845_5',
    '레몬시럽1000ml': 'LO1506416845_6',
    '모히또시럽1000ml': 'LO1506416845_7',
    '초콜릿시럽1000ml': 'LO1506416845_8',
    '아이스티피치시럽1000ml': 'LO1506416845_9',
    '스트로베리시럽1000ml': 'LO1506416845_10',
    '오렌지시럽1000ml': 'LO1506416845_11',
    '키위시럽1000ml': 'LO1506416845_12',
    '자몽시럽1000ml': 'LO1506416845_13',
    '핑크자몽시럽1000ml': 'LO1506416845_14',
    '패션프릇시럽1000ml': 'LO1506416845_15',
    '망고시럽1000ml': 'LO1506416845_16',
    '라임시럽1000ml': 'LO1506416845_17',
    '로즈시럽1000ml': 'LO1506416845_18',
    '애플시럽1000ml': 'LO1506416845_19',
    '바나나시럽1000ml': 'LO1506416845_20',
    '블루베리시럽1000ml': 'LO1506416845_21',
    '체리시럽1000ml': 'LO1506416845_22',
    '케인슈가시럽1000ml': 'LO1506416845_23',
    '피치시럽1000ml': 'LO1506416845_24',
    '차이티시럽1000ml': 'LO1506416845_25',
    '솔티드카라멜시럽1000ml': 'LO1506416845_26',
    '시나몬시럽1000ml': 'LO1506416845_27',
    '라벤더시럽1000ml': 'LO1506416845_28',
    '화이트초코시럽1000ml': 'LO1506416845_29',
    '석류시럽1000ml': 'LO1506416845_30',
    '라즈베리시럽1000ml': 'LO1506416845_31',
    '파인애플시럽1000ml': 'LO1506416845_32',
    '아이리쉬크림시럽1000ml': 'LO1506416845_33',
    '그린애플시럽1000ml': 'LO1506416845_34',
    '돌체드레체시럽1000ml': 'LO1506416845_35',
    '엘더플라워시럽1000ml': 'LO1506416845_36',
    '1883시럽펌프': 'LO1506416845_37',
    '리치시럽1000ml': 'LO1506416845_38',
    '화이트피치시럽1000ml': 'LO1506416845_39',
    '아몬드시럽1000ml': 'LO1506416845_40',
    '마카다미아넛시럽1000ml': 'LO1506416845_41'
    # 빈 키 매핑 제거 (예상치 못한 매핑 방지)
}

# ================== 유틸리티 함수 ==================
def _safe_postal_code(postal_code):
    """우편번호를 안전하게 처리하는 함수 (5자리 고정)"""
    try:
        if not postal_code or str(postal_code).strip() == "":
            return "00000"
        
        # 문자열로 변환하고 공백 제거
        postal_str = str(postal_code).strip()
        
        # 소수점이 있으면 앞부분만 사용
        if "." in postal_str:
            postal_str = postal_str.split(".")[0]
        
        # 숫자만 추출
        import re
        numbers = re.findall(r'\d', postal_str)
        if not numbers:
            return "00000"
        
        # 숫자를 합쳐서 5자리로 맞추기 (앞에 0 채우기)
        result = ''.join(numbers)
        if len(result) == 0:
            return "00000"
        elif len(result) >= 5:
            return result[:5]  # 5자리 이상이면 앞 5자리만
        else:
            return result.zfill(5)  # 5자리 미만이면 앞에 0 채우기
        
    except Exception:
        return "00000"

# ================== 변환 함수 ==================
def convert_to_eplex(order_df: pd.DataFrame):
    # 주문일자를 2025-09-25 형식으로 설정 (월, 일에 0 패딩)
    today = datetime.today().strftime("%Y-%m-%d")
    rows = []

    for _, row in order_df.iterrows():
        # 안전한 데이터 추출 (None 값 처리)
        수집처 = str(row.get("수집처", "") or "").strip()
        옵션 = str(row.get("주문옵션", "") or "").replace(" ", "")
        쇼핑몰상품코드 = str(row.get("쇼핑몰상품코드", "") or "").strip()
        erp = str(row.get("품목코드(ERP)", "") or "").strip()

        code = erp
        if 수집처 == "롯데ON":
            # 전처리에서 이미 쇼핑몰상품코드가 시럽 코드로 변환되었으므로 그대로 사용
            if 쇼핑몰상품코드:
                code = 쇼핑몰상품코드

        rows.append({
            "* F/C": "NS001",
            "* 주문유형": "7",
            "* 배송처": "17",
            "* 고객ID": "90015746",
            "판매채널": 수집처,
            "* 묶음배송번호": str(row.get("주문번호", "") or ""),
            "* 품목코드": code,
            "품목명": str(row.get("품목명(ERP)", "") or ""),
            "옵션": 옵션,
            "가격": str(row.get("주문금액", "") or ""),
            "* 품목수량": str(row.get("수량", "") or ""),
            "주문자": str(row.get("주문자", "") or ""),
            "* 받는사람명": str(row.get("수취인", "") or ""),
            "주문자 전화번호": str(row.get("주문자연락처", "") or ""),
            "* 받는사람 전화번호": str(row.get("수취인연락처1", "") or ""),
            "* 받는사람 우편번호": _safe_postal_code(row.get("우편번호", "")),
            "* 받는사람 주소": str(row.get("주소", "") or ""),
            "배송메세지": str(row.get("배송요청사항", "") or ""),
            "* 주문일자": today,  # 2025-09-25 형식
            "상품주문번호": "",
            "주문번호(참조)": "",
            "주문중개채널(상세)": "",
            "박스구분": "",
            "상세배송유형": "",
            "새벽배송 SMS 전송": "",
            "새벽배송 현관비밀번호": "",
            "위험물 구분": "",
            "* 주문중개채널": "SELF",
            "API 연동용 판매자ID": "",
            "* 주문시간": "09:00:00",
            "받는사람 핸드폰": "",
        })
    return pd.DataFrame(rows)

# ================== UI ==================
st.title("롯데ON 주문건 변환기")
# 매핑 현황 동적 로드
mapping_dict, worksheet = load_mapping()

ecount_file = st.file_uploader("① 이카운트 양식 업로드", type=["xlsx"])

if ecount_file:
    try:
        df = pd.read_excel(ecount_file, skiprows=1, dtype=str).fillna("")
        
        # 컬럼 존재 여부 확인
        if df.empty:
            st.error("❌ 업로드된 파일이 비어있습니다.")
            st.stop()
        
        # 첫 번째 컬럼이 존재하는지 확인
        if len(df.columns) == 0:
            st.error("❌ 파일에 컬럼이 없습니다.")
            st.stop()
            
        # 시간행 제거 (안전하게 처리)
        try:
            df = df[~df.iloc[:, 0].astype(str).str.contains("오전|오후", na=False)]
        except:
            st.warning("⚠️ 시간행 제거 중 오류가 발생했지만 계속 진행합니다.")
        
        # 필수 컬럼 존재 여부 확인
        required_columns = ["쇼핑몰상품코드", "수집처", "주문옵션", "품목코드(ERP)"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            st.error(f"❌ 필수 컬럼이 없습니다: {', '.join(missing_columns)}")
            st.info("📋 파일에 다음 컬럼들이 포함되어 있는지 확인해주세요.")
            st.dataframe(df.columns.tolist(), use_container_width=True)
            st.stop()
        
        # 매핑 처리 (안전하게)
        if "쇼핑몰상품코드" in df.columns:
            # 1단계: 모음딜 전처리 - 쇼핑몰상품코드를 시럽 코드로 변환
            df_processed = df.copy()
            
            # 쇼핑몰품목key 컬럼 찾기 (대소문자 구분 없이)
            product_key_col = None
            for col in df.columns:
                if "쇼핑몰품목" in col and "key" in col.lower():
                    product_key_col = col
                    break
            
            if product_key_col:
                for idx, row in df_processed.iterrows():
                    쇼핑몰상품Key = str(row.get(product_key_col, "") or "").strip()
                    if 쇼핑몰상품Key.startswith("LO1506416845"):
                        # 시럽이름 추출하여 시럽 코드로 변환
                        시럽이름 = 쇼핑몰상품Key.replace("LO1506416845", "").replace(" ", "")
                        if 시럽이름 and 시럽이름 in lotteon_map:
                            시럽코드 = lotteon_map[시럽이름]
                            # 쇼핑몰상품코드를 시럽 코드로 변경
                            df_processed.at[idx, "쇼핑몰상품코드"] = 시럽코드
                        elif 시럽이름:
                            # 시럽이름이 있지만 lotteon_map에 없는 경우 기본 시럽 코드 사용
                            df_processed.at[idx, "쇼핑몰상품코드"] = "LO1506416845_1"
            
            # 2단계: 변환된 코드로 분류
            # 다잇쏘 주문건: 쇼핑몰상품코드가 Google Sheets 매핑에 있는 경우
            daitsso_df = df_processed[df_processed["쇼핑몰상품코드"].isin(mapping_dict.keys())].copy()
            
            # 이플렉스 주문건: 쇼핑몰상품코드가 Google Sheets 매핑에 없는 경우
            other_df = df_processed[~df_processed["쇼핑몰상품코드"].isin(mapping_dict.keys())].copy()
            
        else:
            st.error("❌ '쇼핑몰상품코드' 컬럼을 찾을 수 없습니다.")
            st.stop()
            
    except Exception as e:
        st.error("❌ 파일 읽기 중 오류가 발생했습니다.")
        st.error(f"오류 내용: {str(e)}")
        st.stop()

    # 파일 업로드 시 자동 변환 실행
    if ecount_file:
        # 빈 DataFrame 체크
        if other_df.empty:
            st.warning("⚠️ 이플렉스로 변환할 주문건이 없습니다.")
            eplex_df = pd.DataFrame()
        else:
            eplex_df = convert_to_eplex(other_df)
        
        if daitsso_df.empty and eplex_df.empty:
            st.error("❌ 처리할 주문건이 없습니다.")
        else:
            st.success("✅ 변환 완료!")
            
            # 세션 상태에 변환된 데이터 저장
            st.session_state['daitsso_df'] = daitsso_df
            st.session_state['eplex_df'] = eplex_df
            st.session_state['conversion_completed'] = True

    # 변환 완료 후 다운로드 버튼 표시 (세션 상태 사용)
    if st.session_state.get('conversion_completed', False):
        st.markdown("---")
        st.subheader("📥 다운로드")
        
        daitsso_df = st.session_state.get('daitsso_df', pd.DataFrame())
        eplex_df = st.session_state.get('eplex_df', pd.DataFrame())
        
        c1, c2 = st.columns(2)
        
        if not daitsso_df.empty:
            # XLSX 형식으로 다운로드
            excel_data = BytesIO()
            with pd.ExcelWriter(excel_data, engine='openpyxl') as writer:
                daitsso_df.to_excel(writer, index=False, sheet_name='Sheet1')
            excel_data.seek(0)
            
            c1.download_button(
                "다잇쏘 주문건 다운로드",
                data=excel_data.getvalue(),
                file_name="다잇쏘주문건.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            c1.info("📋 다잇쏘 주문건이 없습니다.")
        
        if not eplex_df.empty:
            # XLSX 형식으로 다운로드
            excel_data = BytesIO()
            with pd.ExcelWriter(excel_data, engine='openpyxl') as writer:
                eplex_df.to_excel(writer, index=False, sheet_name='Sheet1')
            excel_data.seek(0)
            
            c2.download_button(
                "이플렉스 주문건 다운로드",
                data=excel_data.getvalue(),
                file_name="이플렉스수기주문건.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            c2.info("📋 이플렉스 주문건이 없습니다.")

# ================== 매핑 현황 ==================
st.markdown("---")
st.subheader("📋 매핑 현황")
# 매핑 현황 실시간 로드
current_mapping_dict, _ = load_mapping()
if current_mapping_dict:
    st.dataframe(pd.DataFrame(list(current_mapping_dict.items()), columns=["상품번호", "상품명"]), use_container_width=True, height=200)

# ================== 매핑 추가 입력창 ==================
st.markdown("---")
st.subheader("➕ 매핑 추가")
with st.form("add_mapping"):
    new_number = st.text_input("상품번호 (필수)")
    new_name = st.text_input("상품명 (선택)", "")
    submitted = st.form_submit_button("매핑 추가하기")
    if submitted:
        # 입력 검증
        if not new_number or not new_number.strip():
            st.error("❌ 상품번호는 필수입니다.")
        elif len(new_number.strip()) < 3:
            st.error("❌ 상품번호는 3자리 이상이어야 합니다.")
        elif new_number.strip() in mapping_dict:
            st.warning("⚠️ 이미 존재하는 상품번호입니다.")
        else:
            try:
                # 쿠팡 코드 방식: 매핑 추가할 때마다 새로운 클라이언트 생성
                gc = get_gspread_client()
                sheet_id = st.secrets["GSHEETS_ID"]
                ws_name = st.secrets.get("GSHEETS_WORKSHEET", "Sheet1")
                sh = gc.open_by_key(sheet_id)
                ws = sh.worksheet(ws_name)
                
                # append_row 사용 (쿠팡 코드와 동일)
                ws.append_row([new_number.strip(), new_name.strip()])
                st.success(f"✅ 매핑 추가됨: {new_number.strip()} - {new_name.strip()}")
                
                # 매핑 현황 캐시 클리어하여 실시간 업데이트
                load_mapping.clear()
                
            except Exception as e:
                st.error("❌ 매핑 추가 중 오류 발생")
                st.error(f"오류 내용: {str(e)}")
