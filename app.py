import streamlit as st
import pandas as pd
from io import BytesIO
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="롯데ON 주문건 변환기", page_icon="🛒")

# ------------------ 1. Google Sheets 매핑 불러오기 ------------------
@st.cache_resource
def get_gspread_client():
    creds = Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
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
        mapping = {
            str(row.get("상품번호", "")).strip(): str(row.get("상품명", "")).strip()
            for row in records if row.get("상품번호")
        }
        return mapping
    except Exception as e:
        st.error("❌ 구글 시트 로드 실패")
        st.exception(e)
        return {}

# ------------------ 2. 이플렉스 수기 주문건 변환 함수 ------------------
def build_iflex_orders(df: pd.DataFrame) -> pd.DataFrame:
    today = pd.Timestamp.today().strftime("%Y-%m-%d")

    res = pd.DataFrame({
        "* F/C": "NS001",
        "* 주문유형": "7",
        "* 배송처": "17",
        "* 고객ID": "90015746",
        "판매채널": "롯데ON",
        "* 묶음배송번호": df["주문번호"],
        "* 품목코드": df["쇼핑몰상품코드"],
        "품목명": df.get("품목명(ERP)", ""),
        "옵션": df.get("주문옵션", ""),
        "가격": df["주문금액"],
        "* 품목수량": df["수량"],
        "주문자": df["주문자"],
        "* 받는사람명": df["수취인"],
        "주문자 전화번호": df["주문자연락처"],
        "* 받는사람 전화번호": df["수취인연락처1"],
        "* 받는사람 우편번호": df["우편번호"],
        "* 받는사람 주소": df["주소"],
        "배송메세지": df["배송요청사항"],
        "* 주문일자": today,
        "상품주문번호": "",
        "주문번호(참조)": "",
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
    return res

# ------------------ 3. Excel 변환 함수 ------------------
def to_excel(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    return buf.getvalue()

# ------------------ 4. Streamlit UI ------------------
st.title("🛒 롯데ON 주문건 변환기")
st.markdown("업로드된 주문건을 Google Sheets 매핑을 기준으로 **다잇쏘 주문건**과 **이플렉스 수기 주문건**으로 분리합니다.")
st.markdown("---")

# 매핑 불러오기
mapping_dict = load_mapping()
st.write("불러온 매핑 데이터 (상위 5개):", dict(list(mapping_dict.items())[:5]))

uploaded = st.file_uploader("📂 롯데ON 주문건 Excel 업로드 (.xlsx)", type=["xlsx"])
if uploaded:
    try:
        df = pd.read_excel(uploaded, dtype=str).fillna("")

        # 매핑된 상품번호 = 다잇쏘 주문건
        daitsso_df = df[df["쇼핑몰상품코드"].isin(mapping_dict.keys())].copy()
        # 매핑되지 않은 상품번호 = 이플렉스 주문건
        iflex_df = df[~df["쇼핑몰상품코드"].isin(mapping_dict.keys())].copy()
        if not iflex_df.empty:
            iflex_df = build_iflex_orders(iflex_df)

        c1, c2 = st.columns(2)

        if not daitsso_df.empty:
            c1.download_button(
                "📥 다잇쏘 주문건 다운로드",
                data=to_excel(daitsso_df),
                file_name="다잇쏘_주문건.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            st.markdown("**다잇쏘 주문건 미리보기**")
            st.dataframe(daitsso_df.head(6), use_container_width=True, height=200)

        if not iflex_df.empty:
            c2.download_button(
                "📥 이플렉스 수기주문건 다운로드",
                data=to_excel(iflex_df),
                file_name="이플렉스_수기주문건.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            st.markdown("**이플렉스 수기주문건 미리보기**")
            st.dataframe(iflex_df.head(6), use_container_width=True, height=200)

        if daitsso_df.empty and iflex_df.empty:
            st.warning("❗ 변환 결과가 없습니다. 업로드된 파일을 확인해주세요.")

    except Exception as e:
        st.error("❌ 변환 중 오류 발생")
        st.exception(e)
