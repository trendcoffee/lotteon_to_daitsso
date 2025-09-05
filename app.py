import streamlit as st
import pandas as pd
from io import BytesIO
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="롯데ON 주문 변환기", page_icon="🛒")

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
        worksheet_name = st.secrets["GSHEETS_WORKSHEET"]

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

# ------------------ 2. 이카운트 변환 함수 ------------------
def build_ecount_upload(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    df = df.copy().fillna("")

    # 롯데ON 주문만 필터링
    df = df[df["수집처"] == "롯데ON"].copy()

    rows = []
    for _, row in df.iterrows():
        상품번호 = str(row.get("쇼핑몰품목key", "")).strip()
        is_daitsso = 상품번호 in mapping.keys()

        rows.append({
            "* F/C": "NS001",
            "* 주문유형": "7",
            "* 배송처": "17",
            "* 고객ID": "90015746",
            "판매채널": "롯데ON",
            "* 묶음배송번호": str(row.get("주문번호", "")),
            "* 품목코드": 상품번호,
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
            "* 주문일자": row.get("주문일자", ""),
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
            "다잇쏘주문건": "Y" if is_daitsso else "N"
        })

    return pd.DataFrame(rows)

# ------------------ 3. Excel 변환 함수 ------------------
def to_excel(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    return buf.getvalue()

# ------------------ 4. Streamlit UI ------------------
st.title("🛒 롯데ON 주문건 → 이플렉스 수기 주문등록 변환기")
st.markdown("Google Sheets 매핑을 사용해 **롯데ON 주문건**을 ERP 업로드용으로 변환합니다.")
st.markdown("---")

# 매핑 불러오기
mapping_dict = load_mapping()
st.write("📋 불러온 매핑 현황 (최신 6개):")
if mapping_dict:
    st.dataframe(
        pd.DataFrame(list(mapping_dict.items()), columns=["상품번호", "상품명"]).tail(6),
        height=200
    )
else:
    st.warning("⚠️ 매핑 데이터를 불러오지 못했습니다.")

# ------------------ 5. 매핑 추가 입력 ------------------
st.subheader("➕ 매핑 추가 입력")
with st.form("add_mapping_form"):
    new_num = st.text_input("상품번호 (필수)")
    new_name = st.text_input("상품명 (선택, 공백 가능)")
    submitted = st.form_submit_button("추가하기")

    if submitted:
        if not new_num.strip():
            st.error("❌ 상품번호는 필수 입력값입니다.")
        else:
            try:
                gc = get_gspread_client()
                sheet_id = st.secrets["GSHEETS_ID"]
                worksheet_name = st.secrets["GSHEETS_WORKSHEET"]

                sh = gc.open_by_key(sheet_id)
                ws = sh.worksheet(worksheet_name)
                ws.append_row([new_num.strip(), new_name.strip()])

                st.success(f"✅ 매핑 추가 완료: {new_num} → {new_name}")
                st.cache_data.clear()
            except Exception as e:
                st.error("❌ 매핑 추가 중 오류 발생")
                st.exception(e)

# ------------------ 6. 파일 업로드 ------------------
uploaded = st.file_uploader("📂 롯데ON 주문건 Excel 업로드 (.xlsx)", type=["xlsx"])
if uploaded:
    try:
        df = pd.read_excel(uploaded, dtype=str)
        st.success("✅ 파일 업로드 완료!")

        result = build_ecount_upload(df, mapping_dict)

        c1, c2 = st.columns(2)
        c1.download_button(
            "✅ 이카운트 업로드 파일 다운로드",
            data=to_excel(result),
            file_name="롯데ON_이플렉스_주문등록.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        c2.download_button(
            "📁 원본 주문건 다운로드",
            data=to_excel(df),
            file_name="롯데ON_원본주문건.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        st.success("🎉 변환 완료! 아래에서 결과 미리보기 확인하세요.")
        st.dataframe(result)

    except Exception as e:
        st.error("❌ 변환 중 오류 발생")
        st.exception(e)
