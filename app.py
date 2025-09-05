import streamlit as st
import pandas as pd
from datetime import datetime
from io import BytesIO
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="롯데온 주문건 변환기", page_icon="🛒")

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
            for row in records
            if row.get("상품번호")
        }
        return mapping
    except Exception as e:
        st.error("❌ 구글 시트 로드 실패")
        st.exception(e)
        return {}

# ------------------ 2. 변환 함수 ------------------
def build_iflex_order(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    today = datetime.today().strftime('%Y-%m-%d')

    rows = []
    for _, row in df.iterrows():
        mall_code = str(row.get("쇼핑몰상품코드", "")).strip()
        mapped_code = mapping.get(mall_code, "")  # 매핑된 코드 (없으면 "")

        rows.append({
            "* F/C": "NS001",
            "* 주문유형": "7",
            "* 배송처": "17",
            "* 고객ID": "90015746",
            "판매채널": "롯데ON",
            "* 묶음배송번호": str(row.get("주문번호", "")),
            "* 품목코드": mapped_code,
            "품목명": row.get("품목명(ERP)", ""),
            "옵션": row.get("주문옵션", ""),
            "가격": row.get("주문금액", ""),
            "* 품목수량": row.get("수량", ""),
            "주문자": row.get("주문자", ""),
            "* 받는사람명": row.get("수취인", ""),
            "주문자 전화번호": row.get("주문자연락처", ""),
            "* 받는사람 전화번호": row.get("수취인연락처1", ""),
            "* 받는사람 우편번호": str(row.get("우편번호", "")).zfill(5),
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

# ------------------ 3. Excel 변환 함수 ------------------
def to_excel(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    return buf.getvalue()

# ------------------ 4. Streamlit UI ------------------
st.title("🛒 롯데ON 주문건 변환기")
st.markdown("Google Sheets 매핑을 사용하여 롯데ON 주문건을 **이플렉스 수기주문등록 양식**으로 변환합니다.")
st.markdown("---")

mapping_dict = load_mapping()
st.write("📋 불러온 매핑 데이터 (앞 5개)", dict(list(mapping_dict.items())[:5]))

uploaded = st.file_uploader("📂 롯데ON 주문건 Excel 업로드 (.xlsx)", type=["xlsx"])
if uploaded:
    try:
        df = pd.read_excel(uploaded, dtype=str)
        st.success("✅ 파일 업로드 완료!")

        result = build_iflex_order(df, mapping_dict)

        st.success("🎉 변환 완료! 결과를 다운로드하거나 아래에서 미리보기 확인하세요.")
        st.dataframe(result, height=300)

        st.download_button(
            "📥 이플렉스 수기주문등록 다운로드",
            data=to_excel(result),
            file_name="롯데ON_이플렉스주문등록.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except Exception as e:
        st.error("❌ 변환 중 오류 발생")
        st.exception(e)
