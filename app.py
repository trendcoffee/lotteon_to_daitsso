import streamlit as st
import pandas as pd
from io import BytesIO
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

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
    gc = get_gspread_client()
    sheet_id = st.secrets["GSHEETS_ID"]
    worksheet_name = st.secrets.get("GSHEETS_WORKSHEET", "Sheet1")

    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name)

    records = ws.get_all_records()
    mapping = {str(r.get("상품번호", "")).strip(): str(r.get("상품명", "")).strip()
               for r in records if r.get("상품번호")}
    return mapping, ws

# ------------------ 2. 변환 함수 ------------------
def build_eplex_orders(df: pd.DataFrame, mapping_dict: dict) -> (pd.DataFrame, pd.DataFrame):
    """원본 DataFrame을 이플렉스용과 다잇쏘 주문건으로 분리"""
    df = df.copy().fillna("")

    # 마지막 "시간 행" 제거
    df = df[~df["수집처"].str.contains("오전|오후", na=False)]

    # 다잇쏘 주문건: 쇼핑몰상품코드가 매핑 시트에 있는 것
    daitsso_df = df[df["쇼핑몰상품코드"].isin(mapping_dict.keys())].copy()

    # 이플렉스 수기 주문건 (전체 구조 유지)
    today = datetime.today().strftime("%Y-%m-%d")
    rows = []
    for _, row in df.iterrows():
        주문번호 = str(row.get("주문번호", "")).split(".")[0]
        우편번호 = str(row.get("우편번호", "")).split(".")[0].zfill(5)

        rows.append({
            "* F/C": "NS001",
            "* 주문유형": "7",
            "* 배송처": "17",
            "* 고객ID": "90015746",
            "판매채널": row.get("수집처", ""),
            "* 묶음배송번호": 주문번호,
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
    eplex_df = pd.DataFrame(rows)

    return eplex_df, daitsso_df

# ------------------ 3. Excel 변환 함수 ------------------
def to_excel(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    return buf.getvalue()

# ------------------ 4. UI ------------------
st.title("🛒 롯데ON 주문건 변환기")
st.markdown("롯데ON 주문건을 이플렉스 수기주문등록 + 다잇쏘 주문건으로 분리 변환합니다.")
st.markdown("---")

# 매핑 불러오기
mapping_dict, worksheet = load_mapping()
mapping_df = pd.DataFrame(list(mapping_dict.items()), columns=["상품번호", "상품명"])

st.subheader("📋 현재 매핑 현황")
st.dataframe(mapping_df, use_container_width=True, height=200)

# 파일 업로드
uploaded = st.file_uploader("📂 롯데ON 주문건 Excel 업로드 (.xlsx)", type=["xlsx"])
if uploaded:
    try:
        df = pd.read_excel(uploaded, header=1, dtype=str).fillna("")
        st.success("✅ 파일 업로드 완료!")

        eplex_df, daitsso_df = build_eplex_orders(df, mapping_dict)

        # 다운로드 버튼
        c1, c2 = st.columns(2)
        c1.download_button(
            "📥 이플렉스 수기주문등록 다운로드",
            data=to_excel(eplex_df),
            file_name="이플렉스_주문건.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        c2.download_button(
            "📥 다잇쏘 주문건 다운로드",
            data=to_excel(daitsso_df),
            file_name="다잇쏘_주문건.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        st.success("🎉 변환 완료! 결과 미리보기")
        st.subheader("이플렉스 수기 주문등록 (앞 6개)")
        st.dataframe(eplex_df.head(6), use_container_width=True)
        st.subheader("다잇쏘 주문건 (앞 6개)")
        st.dataframe(daitsso_df.head(6), use_container_width=True)

    except Exception as e:
        st.error("❌ 변환 중 오류 발생")
        st.exception(e)

# ------------------ 5. 매핑 추가 입력 ------------------
st.markdown("---")
st.subheader("➕ 매핑 추가")
with st.form("add_mapping_form"):
    new_number = st.text_input("상품번호 (필수)", "")
    new_name = st.text_input("상품명 (선택)", "")
    submitted = st.form_submit_button("추가하기")
    if submitted:
        if not new_number.strip():
            st.error("상품번호는 필수입니다.")
        elif worksheet:
            try:
                worksheet.append_row([new_number.strip(), new_name.strip()])
                st.success(f"✅ '{new_number}' 이(가) 시트에 추가되었습니다.")
                st.cache_data.clear()  # 캐시된 매핑 즉시 무효화
                st.rerun()  # 👉 UI 즉시 갱신
            except Exception as e:
                st.error("❌ 매핑 추가 중 오류 발생")
                st.exception(e)
        else:
            st.error("❌ Worksheet 객체를 찾을 수 없습니다. 구글시트 설정을 확인하세요.")
