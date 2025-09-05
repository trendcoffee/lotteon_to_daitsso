import streamlit as st
import pandas as pd
from datetime import datetime
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
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

mapping_dict, worksheet = load_mapping()
st.subheader("📋 매핑 현황 (상위 10개)")
if mapping_dict:
    st.dataframe(pd.DataFrame(list(mapping_dict.items()), columns=["상품번호", "상품명"]).head(10))

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
    '마카다미아넛시럽1000ml': 'LO1506416845_41',
    '': 'LO1506416845_42'
}

# ================== 변환 함수 ==================
def convert_to_eplex(order_df: pd.DataFrame, bom_df: pd.DataFrame):
    today = datetime.today().strftime("%Y-%m-%d")
    rows = []

    for _, row in order_df.iterrows():
        수집처 = str(row.get("수집처", "")).strip()
        옵션 = str(row.get("주문옵션", "")).replace(" ", "")
        쇼핑몰상품코드 = str(row.get("쇼핑몰상품코드", "")).strip()
        erp = str(row.get("품목코드(ERP)", "")).strip()

        # 기본 코드 결정
        code = erp
        if 수집처 == "롯데ON":
            if 옵션 in lotteon_map:
                code = lotteon_map[옵션]
            elif 쇼핑몰상품코드:
                code = 쇼핑몰상품코드

        rows.append({
            "* F/C": "NS001",
            "* 주문유형": "7",
            "* 배송처": "17",
            "* 고객ID": "90015746",
            "판매채널": 수집처,
            "* 묶음배송번호": str(row.get("주문번호", "")),
            "* 품목코드": code,
            "품목명": row.get("품목명(ERP)", ""),
            "옵션": 옵션,
            "가격": row.get("주문금액", ""),
            "* 품목수량": row.get("수량", ""),
            "주문자": row.get("주문자", ""),
            "* 받는사람명": row.get("수취인", ""),
            "주문자 전화번호": row.get("주문자연락처", ""),
            "* 받는사람 전화번호": row.get("수취인연락처1", ""),
            "* 받는사람 우편번호": str(row.get("우편번호", "")).split(".")[0].zfill(5),
            "* 받는사람 주소": row.get("주소", ""),
            "배송메세지": row.get("배송요청사항", ""),
            "* 주문일자": today,
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
ecount_file = st.file_uploader("① 이카운트 양식 업로드", type=["xlsx"])
bom_file = st.file_uploader("② CJ이플렉스 BOM 등록리스트 업로드", type=["csv"])

if ecount_file and bom_file:
    bom_df = pd.read_csv(bom_file)
    df = pd.read_excel(ecount_file, skiprows=1, dtype=str).fillna("")
    df = df[~df.iloc[:, 0].astype(str).str.contains("오전|오후", na=False)]  # 마지막 시간행 제거

    # 다잇쏘 분리 (구글시트 상품번호 기준)
    daitsso_df = df[df["쇼핑몰상품코드"].isin(mapping_dict.keys())].copy()
    other_df = df[~df["쇼핑몰상품코드"].isin(mapping_dict.keys())].copy()

    if st.button("변환 실행"):
        # 이플렉스 변환
        eplex_df = convert_to_eplex(other_df, bom_df)

        st.success("✅ 변환 완료!")
        st.subheader("📥 다운로드")
        c1, c2 = st.columns(2)

        c1.download_button(
            "다잇쏘 주문건 다운로드",
            data=daitsso_df.to_csv(index=False).encode("utf-8-sig"),
            file_name="다잇쏘주문건.csv",
            mime="text/csv"
        )
        c2.download_button(
            "이플렉스 주문건 다운로드",
            data=eplex_df.to_csv(index=False).encode("utf-8-sig"),
            file_name="이플렉스수기주문건.csv",
            mime="text/csv"
        )

        st.subheader("📊 미리보기")
        st.write("👉 다잇쏘 주문건")
        st.dataframe(daitsso_df.head(10), use_container_width=True, height=250)
        st.write("👉 이플렉스 주문건")
        st.dataframe(eplex_df.head(10), use_container_width=True, height=250)

# ================== 매핑 추가 입력창 ==================
st.markdown("---")
st.subheader("➕ 매핑 추가")
with st.form("add_mapping"):
    new_number = st.text_input("상품번호 (필수)")
    new_name = st.text_input("상품명 (선택)", "")
    submitted = st.form_submit_button("매핑 추가하기")
    if submitted and new_number.strip():
        try:
            worksheet.append_rows([[new_number.strip(), new_name.strip()]], value_input_option="USER_ENTERED")
            st.success(f"✅ 매핑 추가됨: {new_number} - {new_name}")
        except Exception as e:
            st.error("❌ 매핑 추가 중 오류 발생")
            st.exception(e)
