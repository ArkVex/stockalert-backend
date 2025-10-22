import json
from app import create_app


def test_index():
    app = create_app({'TESTING': True})
    client = app.test_client()
    resp = client.get('/')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['status'] == 'ok'
    assert 'message' in data
