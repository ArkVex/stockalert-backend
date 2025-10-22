from app import create_app
import json
import requests


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, text=''):
        self.status_code = status_code
        self._json = json_data or {}
        self.text = text

    def json(self):
        return self._json


def test_firecrawl_proxy_success(monkeypatch):
    # Mock requests.post to return a predictable JSON with table HTML
    html_with_table = '''
    <!DOCTYPE html>
    <html>
    <body>
        <table>
            <tr>
                <th>SYMBOL</th>
                <th>COMPANY NAME</th>
                <th>SUBJECT</th>
                <th>DETAILS</th>
                <th>ATTACHMENT</th>
                <th>XBRL</th>
                <th>BROADCAST DATE/TIME</th>
            </tr>
            <tr>
                <td><a href="/get-quotes/equity?symbol=INFY">INFY</a></td>
                <td>Infosys Limited</td>
                <td>Updates</td>
                <td>Infosys Limited has informed the Exchange regarding 'Earnings Call Transcript'.</td>
                <td><a href="https://nsearchives.nseindia.com/corporate/Infosys_21102025214121_SEfiling_Earningscalltranscript_q2.pdf">PDF</a>(597.28 KB)</td>
                <td><a href="https://www.nseindia.com/api/xbrl/106413674">XBRL</a></td>
                <td>21-Oct-2025 21:41:32</td>
            </tr>
        </table>
    </body>
    </html>
    '''

    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse(status_code=200, json_data={
            'data': {'html': html_with_table},
            'crawledAt': '2025-10-22T10:00:00Z'
        })

    monkeypatch.setattr('requests.post', fake_post)

    app = create_app({'TESTING': True})
    client = app.test_client()

    resp = client.get('/stock')
    assert resp.status_code == 200
    data = resp.get_json()
    
    assert data['source'] == 'firecrawl'
    assert 'filings' in data
    assert len(data['filings']) == 1
    assert data['filings'][0]['symbol'] == 'INFY'
    assert data['filings'][0]['company_name'] == 'Infosys Limited'


def test_firecrawl_upstream_error(monkeypatch):
    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse(status_code=500, json_data={'error': 'upstream failure'})

    monkeypatch.setattr('requests.post', fake_post)

    app = create_app({'TESTING': True})
    client = app.test_client()

    resp = client.get('/stock')
    assert resp.status_code == 500 or resp.status_code == 500
    data = resp.get_json()
    assert data['error'] == 'upstream_error'
