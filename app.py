import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="롯데온 → 이플렉스 변환기", page_icon="📦")

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
            str(row.get("상품번호", "")).strip(): str(row.get("상품명") or "").strip()
            for row in records if row.get("상품번호")
        }
        return mapping, ws
    except Exception as e:
        st.error("❌ 구글 시트 로드 실패")
        st.exception(e)
        return {}, None

# ------------------ 2. 이플렉스 수기주문 변환 함수 ------------------
def build_eplex_order(df: pd.DataFrame, mapping: dict) -> (pd.DataFrame, pd.DataFrame):
    df = df.copy().fillna("")

    # 맨 마지막 "시간행" 제거
    df = df[~df.iloc[:, 0].astype(str).str.contains("오전|오후", na=False)]

    # 다잇쏘 주문건 분리 (상품번호 기준)
    daitsso_df = df[df["쇼핑몰상품코드"].isin(mapping.keys())].copy()
    eplex_df = df[~df["쇼핑몰상품코드"].isin(mapping.keys())].copy()

    today = datetime.today().strftime("%Y-%m-%d")

    rows = []
    for _, row in eplex_df.iterrows():
        우편번호 = str(row.get("우편번호", "")).split(".")[0].zfill(5)

        rows.append({
            "* F/C": "NS001",
            "* 주문유형": "7",
            "* 배송처": "17",
            "* 고객ID": "90015746",
            "판매채널": "롯데ON",
            "* 묶음배송번호": row.get("주문번호", ""),
            "* 품목코드": row.get("품목코드(ERP)", ""),
            "품목명": row.get("품목명(ERP)", ""),
            "옵션": row.get("주문옵션", ""),
            "가격": row.get("주문금액", ""),
            "* 품목수량": row.get("수량", ""),
            "주문자": row.get("주문자", ""),
            "* 받는사람명": row.get("수취인", ""),
            "주문자 전화번호": row.get("주문자연락처", ""),
            "* 받는사람 전화번호": row.get("수취인연락처1", ""),
            "* 받는사람 우편번호": 우편번호,
            "* 받는사람 주소": row.get("주소", ""),
            "배송메세지": row.get("배송요청사항", ""),
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

    return daitsso_df, pd.DataFrame(rows)

# ------------------ 3. Excel 변환 함수 ------------------
def to_excel(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    return buf.getvalue()

# ------------------ 4. Streamlit UI ------------------
st.title("📦 롯데온 주문건 → 이플렉스 변환기")
st.markdown("Google Sheets 매핑을 사용하여 다잇쏘 주문건 분리 + 이플렉스 수기주문 변환")

mapping_dict, worksheet = load_mapping()
st.write("📋 현재 매핑 현황 (상위 6개 표시, 전체는 스크롤로 확인)", mapping_dict)

uploaded = st.file_uploader("📂 롯데온 주문건 Excel 업로드 (.xlsx)", type=["xlsx"])
if uploaded:
    try:
        df = pd.read_excel(uploaded, dtype=str, skiprows=1).fillna("")
        st.success("✅ 파일 업로드 완료!")

        daitsso_df, eplex_df = build_eplex_order(df, mapping_dict)

        c1, c2 = st.columns(2)
        if not daitsso_df.empty:
            c1.download_button(
                "📁 다잇쏘 주문건 다운로드",
                data=to_excel(daitsso_df),
                file_name="다잇쏘_롯데ON_주문건.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        if not eplex_df.empty:
            c2.download_button(
                "✅ 이플렉스 업로드 파일 다운로드",
                data=to_excel(eplex_df),
                file_name="이플렉스_롯데ON_수기주문등록.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        if not daitsso_df.empty or not eplex_df.empty:
            st.success("🎉 변환 완료! 아래에서 결과 미리보기 확인하세요.")
            st.dataframe(pd.concat([daitsso_df, eplex_df], axis=0), height=250)

    except Exception as e:
        st.error("❌ 변환 중 오류 발생")
        st.exception(e)

# ------------------ 5. 매핑 추가 입력 ------------------
st.markdown("---")
st.subheader("➕ 매핑 추가하기")
with st.form("add_mapping_form"):
    new_number = st.text_input("상품번호 (필수)", "")
    new_name = st.text_input("상품명 (선택)", "")
    submitted = st.form_submit_button("추가하기")

    if submitted:
        if not new_number.strip():
            st.warning("⚠️ 상품번호는 필수입니다.")
        else:
            try:
                last_row = len(worksheet.get_all_values()) + 1
                worksheet.update(
                    f"A{last_row}:B{last_row}",
                    [[new_number.strip(), new_name.strip()]]
                )
                st.success(f"✅ 구글 시트에 추가 완료: {new_number} / {new_name}")
            except Exception as e:
                st.error("❌ 매핑 추가 중 오류 발생")
                st.exception(e)
