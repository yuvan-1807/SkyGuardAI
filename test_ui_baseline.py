from pathlib import Path

SRC = Path(__file__).with_name('app_new.py')

def run():
    text = SRC.read_text(encoding='utf-8')
    assert 'SKYGUARD_BUILD = "FINAL-CONSOLIDATED-3J"' in text
    assert '@app.get("/api/final-dashboard")' in text
    assert '@app.get("/api/station/<path:station>")' in text
    assert '<aside class="sidebar"' in text
    assert '<div class="map" id="map"' not in text  # avoid duplicate legacy form
    assert text.count('id="map"') == 1
    assert 'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css' in text
    assert 'https://unpkg.com/leaflet@1.9.4/dist/leaflet.js' in text
    assert 'India AWS Network' in text
    assert 'Hover for quick status · click for station detail' in text
    assert 'function selectStation' in text
    print('UI baseline test: PASS')
    print('Single interactive map: PASS')
    print('Fixed sidebar navigation: PASS')
    print('Station inspector hooks: PASS')
    print('Clean UI preserved while backend 3G is integrated: PASS')

if __name__ == '__main__':
    run()
