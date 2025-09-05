import streamlit as st
import pandas as pd
from io import BytesIO
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

st.set_page_config(page_title="롯데ON → 이플렉스 변환기", page_icon="🛒")

# ------------------ 1. Google Sheets 매핑 불러오기 ------------------
@st.cache_resource
def get_gspread_client():
    creds = Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)

@st.cache_data(ttl=600)
def load_mapping():
    try:
        gc = get_gspread_client()
        sheet_id = st.secrets["GSHEETS_ID"]
        worksheet_name = st.secrets.get("GSHEETS_WORKSHEET", "Sheet1")

        sh = gc.open_by_key(sheet_id)
        ws = sh.worksheet(worksheet_name)

        records = ws.get_all_records()
        mapping = {str(row.get("상품번호", "")).strip(): str(row.get("상품명", "")).strip()
                   for row in records if row.get("상품번호")}
        return mapping, ws
    except Exception as e:
        st.error("❌ 구글 시트 로드 실패")
        st.exception(e)
        return {}, None

# ------------------ 2. 변환 함수 ------------------
def convert_to_eplex(df: pd.DataFrame) -> pd.DataFrame:
    today = datetime.today().strftime('%Y-%m-%d')

    rows = []
    for _, row in df.iterrows():
        주문번호 = str(row.get("주문번호", "")).split('.')[0]
        우편번호 = str(row.get("우편번호", "")).split('.')[0].zfill(5)

        rows.append({
            '* F/C': 'NS001',
            '* 주문유형': '7',
            '* 배송처': '17',
            '* 고객ID': '90015746',
            '판매채널': "롯데ON",
            '* 묶음배송번호': 주문번호,
            '* 품목코드': row.get("쇼핑몰상품코드", ""),
            '품목명': row.get("품목명(ERP)", ""),
            '옵션': row.get("주문옵션", ""),
            '가격': row.get("주문금액", ""),
            '* 품목수량': row.get("수량", ""),
            '주문자': row.get("주문자", ""),
            '* 받는사람명': row.get("수취인", ""),
            '주문자 전화번호': row.get("주문자연락처", ""),
            '* 받는사람 전화번호': row.get("수취인연락처1", ""),
            '* 받는사람 우편번호': 우편번호,
            '* 받는사람 주소': row.get("주소", ""),
            '배송메세지': row.get("배송요청사항", ""),
            '* 주문일자': today,
            '상품주문번호': '',
            '주문번호(참조)': '',
            '주문중개채널(상세)': '',
            '박스구분': '',
            '상세배송유형': '',
            '새벽배송 SMS 전송': '',
            '새벽배송 현관비밀번호': '',
            '위험물 구분': '',
            '* 주문중개채널': 'SELF',
            'API 연동용 판매자ID': '',
            '* 주문시간': '09:00:00',
            '받는사람 핸드폰': ''
        })
    return pd.DataFrame(rows)

# ------------------ 3. Excel 다운로드 ------------------
def to_excel(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    return buf.getvalue()

# ------------------ 4. UI ------------------
st.title("🛒 롯데ON 주문건 변환기")
st.markdown("Google Sheets 매핑을 이용해 다잇쏘 주문건을 분리하고, 나머지는 이플렉스 수기 주문으로 변환합니다.")
st.markdown("---")

mapping_dict, ws = load_mapping()
st.subheader("📋 현재 매핑 현황")
if mapping_dict:
    mapping_df = pd.DataFrame(list(mapping_dict.items()), columns=["상품번호", "상품명"])
    st.dataframe(mapping_df, height=200)

# ➕ 매핑 추가 입력
st.subheader("➕ 매핑 추가")
with st.form("add_mapping"):
    new_number = st.text_input("상품번호 (필수)")
    new_name = st.text_input("상품명 (선택)", "")
    submitted = st.form_submit_button("추가하기")
    if submitted and ws:
        if new_number.strip():
            ws.append_row([new_number.strip(), new_name.strip()])
            st.success(f"✅ 매핑 추가 완료: {new_number} → {new_name if new_name else '(상품명 없음)'}")
            st.cache_data.clear()
        else:
            st.warning("⚠️ 상품번호는 반드시 입력해야 합니다.")

# 주문 파일 업로드
uploaded = st.file_uploader("📂 롯데ON 주문건 Excel 업로드 (.xlsx)", type=["xlsx"])
if uploaded:
    try:
        # 2번째 행부터 컬럼 → 마지막행(빈 행) 제거
        df = pd.read_excel(uploaded, dtype=str, header=1).fillna("")
        df = df[df["주문번호"].notna() & df["주문번호"].str.strip().ne("")]

        st.success("✅ 파일 업로드 완료!")

        # 다잇쏘 vs 이플렉스 분리
        daitsso_df = df[df["쇼핑몰상품코드"].isin(mapping_dict.keys())].copy()
        eplex_df = df[~df["쇼핑몰상품코드"].isin(mapping_dict.keys())].copy()

        if not daitsso_df.empty:
            st.subheader("📦 다잇쏘 주문건")
            st.dataframe(daitsso_df, height=200)
            st.download_button("📥 다잇쏘 주문건 다운로드",
                               data=to_excel(daitsso_df),
                               file_name="다잇쏘_주문건.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        if not eplex_df.empty:
            st.subheader("📦 이플렉스 수기주문 변환")
            result = convert_to_eplex(eplex_df)
            st.dataframe(result, height=200)
            st.download_button("📥 이플렉스 수기주문 다운로드",
                               data=to_excel(result),
                               file_name="이플렉스_주문건.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    except Exception as e:
        st.error("❌ 변환 중 오류 발생")
        st.exception(e)
