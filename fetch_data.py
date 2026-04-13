"""
PUBGM KPI Dashboard — Tableau Cloud 자동 데이터 수집기
매일 GitHub Actions에서 실행되어 data/kpi_data.json 을 업데이트합니다.
"""
import os, json, csv, requests
from io import StringIO
from datetime import datetime, timezone

import tableauserverclient as TSC

# ── 설정 ──────────────────────────────────────────────────────────────────────
SERVER_URL    = 'https://prod-useast-a.online.tableau.com'
SITE_ID       = 'kraftonbi'
WORKBOOK_NAME = 'KRJPKPITLOG_Temp'
TRAFFIC_SHEET = 'Daily Traffic KPI (T-log Only)'
REVENUE_SHEET = 'Daily Revenue KPI (Billing Log Only)'

PAT_NAME   = os.environ['TABLEAU_PAT_NAME']
PAT_SECRET = os.environ['TABLEAU_PAT_SECRET']

# ── 유틸 ──────────────────────────────────────────────────────────────────────
def safe_num(v):
    """숫자 변환 (실패 시 None 반환)"""
    if v is None or str(v).strip() in ('', '-', 'N/A', 'null', 'Null'):
        return None
    try:
        return float(str(v).replace(',', '').replace('%', '').strip())
    except:
        return None

def find_col(headers, *candidates):
    """대소문자 무시하고 컬럼 이름 매칭"""
    for c in candidates:
        for h in headers:
            if c.lower() in h.lower():
                return h
    return None

def csv_to_rows(text):
    return list(csv.DictReader(StringIO(text)))

# ── 데이터 파싱 ────────────────────────────────────────────────────────────────
def process_traffic(rows):
    if not rows:
        print("⚠️  Traffic 데이터가 비어있습니다")
        return {}

    h = list(rows[0].keys())
    print(f"\n[Traffic] 컬럼 목록: {h}")

    date_c   = find_col(h, 'date', '날짜', 'day', 'Date')
    region_c = find_col(h, 'region', '지역', 'country', 'market', 'server', 'Region', 'Country')
    nru_c    = find_col(h, 'NRU', 'new reg', '신규', 'New Register')
    dau_c    = find_col(h, 'DAU', 'daily active', 'Daily Active')
    pcu_c    = find_col(h, 'PCU', 'peak', 'Peak')
    ret_c    = find_col(h, 'return', 'RET', '복귀', 'Return')

    print(f"[Traffic] 매핑: date={date_c}, region={region_c}, NRU={nru_c}, DAU={dau_c}, PCU={pcu_c}, RET={ret_c}")

    out = {}
    for r in rows:
        d   = (r.get(date_c) or '').strip()
        reg = (r.get(region_c) or '').strip().upper()
        if not d:
            continue
        # KR/JP 외 지역도 포함할 수 있으므로 유연하게 처리
        reg_key = None
        if 'KR' in reg or 'KOREA' in reg:
            reg_key = 'kr'
        elif 'JP' in reg or 'JAPAN' in reg:
            reg_key = 'jp'
        if not reg_key:
            continue

        out.setdefault(d, {})
        out[d][reg_key] = {
            'nru': safe_num(r.get(nru_c)),
            'dau': safe_num(r.get(dau_c)),
            'pcu': safe_num(r.get(pcu_c)),
            'ret': safe_num(r.get(ret_c)),
        }
    return out

def process_revenue(rows):
    if not rows:
        print("⚠️  Revenue 데이터가 비어있습니다")
        return {}

    h = list(rows[0].keys())
    print(f"\n[Revenue] 컬럼 목록: {h}")

    date_c   = find_col(h, 'date', '날짜', 'day', 'Date')
    region_c = find_col(h, 'region', '지역', 'country', 'market', 'server', 'Region', 'Country')
    pu_c     = find_col(h, 'PU', 'paying user', '결제자', 'Paying')
    npu_c    = find_col(h, 'NPU', 'new paying', 'New Paying')
    pur_c    = find_col(h, 'PUR', 'paying rate', '결제율', 'Rate')
    arpu_c   = find_col(h, 'ARPU')
    arppu_c  = find_col(h, 'ARPPU')
    rev_c    = find_col(h, 'revenue', 'Revenue', 'rev', '매출', '수익', 'billing', 'Billing', 'amount')

    print(f"[Revenue] 매핑: date={date_c}, region={region_c}, PU={pu_c}, NPU={npu_c}, PUR={pur_c}, ARPU={arpu_c}, ARPPU={arppu_c}, REV={rev_c}")

    out = {}
    for r in rows:
        d   = (r.get(date_c) or '').strip()
        reg = (r.get(region_c) or '').strip().upper()
        if not d:
            continue
        reg_key = None
        if 'KR' in reg or 'KOREA' in reg:
            reg_key = 'kr'
        elif 'JP' in reg or 'JAPAN' in reg:
            reg_key = 'jp'
        if not reg_key:
            continue

        out.setdefault(d, {})
        out[d][reg_key] = {
            'pu':    safe_num(r.get(pu_c)),
            'npu':   safe_num(r.get(npu_c)),
            'pur':   safe_num(r.get(pur_c)),
            'arpu':  safe_num(r.get(arpu_c)),
            'arppu': safe_num(r.get(arppu_c)),
            'rev':   safe_num(r.get(rev_c)),
        }
    return out

# ── 메인 ──────────────────────────────────────────────────────────────────────
def main():
    auth   = TSC.PersonalAccessTokenAuth(PAT_NAME, PAT_SECRET, SITE_ID)
    server = TSC.Server(SERVER_URL, use_server_version=True)

    with server.auth.sign_in(auth):
        print(f"✅ Tableau Cloud 로그인 성공 (site: {SITE_ID})")

        # ── REST API 직접 호출로 워크북 탐색 ──────────────────────────────────
        api_ver = server.version
        site_id = server.site_id
        base    = f"{SERVER_URL}/api/{api_ver}/sites/{site_id}"
        headers = {'x-tableau-auth': server.auth_token, 'Accept': 'application/json'}

        print(f"\nREST API로 워크북 검색: contentUrl={WORKBOOK_NAME}")
        resp = requests.get(f"{base}/workbooks",
                           params={'filter': f'contentUrl:eq:{WORKBOOK_NAME}'},
                           headers=headers)
        resp.raise_for_status()
        wbs_data = resp.json().get('workbooks', {}).get('workbook', [])
        print(f"검색 결과: {[w.get('name') for w in wbs_data]}")

        if not wbs_data:
            raise RuntimeError(
                f"contentUrl='{WORKBOOK_NAME}' 워크북을 찾을 수 없습니다.\n"
                f"응답: {resp.json()}"
            )

        wb_id   = wbs_data[0]['id']
        wb_name = wbs_data[0]['name']
        print(f"✅ 워크북 찾음: {wb_name} (id: {wb_id})")

        # ── Metadata API로 숨겨진 시트 포함 전체 시트 목록 가져오기 ────────────
        meta_query = """
        {
          workbooksConnection(filter: {luid: "%s"}) {
            nodes {
              name
              sheets {
                name
                luid
              }
            }
          }
        }
        """ % wb_id

        meta_resp = requests.post(
            f"{SERVER_URL}/api/metadata/graphql",
            headers={**headers, 'content-type': 'application/json'},
            json={'query': meta_query}
        )
        meta_resp.raise_for_status()
        meta_data = meta_resp.json()
        print(f"\nMetadata API 응답: {meta_data}")

        nodes = meta_data.get('data', {}).get('workbooksConnection', {}).get('nodes', [])
        all_sheets = nodes[0].get('sheets', []) if nodes else []
        print(f"\n전체 시트 목록 ({len(all_sheets)}개, 숨겨진 시트 포함):")
        for s in all_sheets:
            print(f"  - [{s.get('luid')}] {s.get('name')}")

        def get_view_id(sheet_name):
            for s in all_sheets:
                if sheet_name.lower() in s['name'].lower():
                    return s['luid'], s['name']
            available = [s['name'] for s in all_sheets]
            raise RuntimeError(f"시트 '{sheet_name}' 없음. 가능한 시트: {available}")

        def download_csv(view_id, view_name):
            """REST API로 뷰 데이터를 CSV로 다운로드"""
            print(f"⬇️  다운로드: {view_name}")
            r = requests.get(f"{base}/views/{view_id}/data",
                            headers={**headers, 'Accept': 'text/csv'})
            r.raise_for_status()
            return r.content.decode('utf-8-sig')

        # 다운로드
        trf_id, trf_name = get_view_id(TRAFFIC_SHEET)
        rev_id, rev_name = get_view_id(REVENUE_SHEET)

        trf_csv = download_csv(trf_id, trf_name)
        rev_csv = download_csv(rev_id, rev_name)

        # 파싱
        trf_data = process_traffic(csv_to_rows(trf_csv))
        rev_data = process_revenue(csv_to_rows(rev_csv))

        print(f"\n✅ 파싱 완료 — Traffic: {len(trf_data)}일, Revenue: {len(rev_data)}일")

        # 저장
        os.makedirs('data', exist_ok=True)

        # 원본 CSV (디버깅용, _접두사)
        with open('data/_traffic_raw.csv', 'w', encoding='utf-8') as f:
            f.write(trf_csv)
        with open('data/_revenue_raw.csv', 'w', encoding='utf-8') as f:
            f.write(rev_csv)

        # 대시보드용 JSON
        payload = {
            'updated': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'traffic': trf_data,
            'revenue': rev_data,
        }
        with open('data/kpi_data.json', 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))

        print("✅ data/kpi_data.json 저장 완료")

if __name__ == '__main__':
    main()
