# Complete Project Codebase
Generated on: Fri Aug 14 08:51:02 UTC 2026

## File: main.py
````py
import requests
import re
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from opencc import OpenCC
from stream_check import check_url as playback_check_url

# 初始化繁簡轉換器
cc = OpenCC('s2t')

# --- 設定區 ---

# 1. 來源列表
SOURCE_URLS = [
    "https://iptv.clbug.com/download.php?type=ipv4&category=%E9%A6%99%E6%B8%AF%E9%A2%91%E9%81%93&format=m3u",
    # 香港專用源 (優先)
    "https://raw.githubusercontent.com/s14685/tv/main/iptvhk.txt",
    "https://raw.githubusercontent.com/iptv-org/iptv/refs/heads/master/streams/hk.m3u",
    "https://raw.githubusercontent.com/hujingguang/ChinaIPTV/main/HongKong.m3u8",
    "https://raw.githubusercontent.com/iptv-js/iptv/main/txt/ew_hk.txt",
    "https://raw.githubusercontent.com/chingithub1/iptv/main/Original",
    "https://raw.githubusercontent.com/LiuYi0526/IPTVnew/main/IPTVnews.txt",
    # 其他綜合源
    "https://raw.githubusercontent.com/Supprise0901/TVBox_live/refs/heads/main/live.txt",
    "https://raw.githubusercontent.com/Guovin/iptv-api/refs/heads/gd/output/result.m3u",
    "https://raw.githubusercontent.com/imDazui/Tvlist-awesome-m3u-m3u8/refs/heads/master/m3u/%E5%8F%B0%E6%B9%BE%E9%A6%99%E6%B8%AF%E6%BE%B3%E9%97%A8202506.m3u",
    "https://raw.githubusercontent.com/imDazui/Tvlist-awesome-m3u-m3u8/refs/heads/master/m3u/%E5%8F%B0%E6%B9%BE%E9%A6%99%E6%B8%AF%E6%BE%B3%E9%97%A82023.m3u",
    "https://raw.githubusercontent.com/imDazui/Tvlist-awesome-m3u-m3u8/refs/heads/master/m3u/%E5%8F%B0%E6%B9%BE%E9%A6%99%E6%B8%AF%E6%BE%B3%E9%97%A82022-7.m3u",
    "https://raw.githubusercontent.com/imDazui/Tvlist-awesome-m3u-m3u8/refs/heads/master/m3u/%E5%8F%B0%E6%B9%BE%E9%A6%99%E6%B8%AF%E6%BE%B3%E9%97%A82022-11.m3u",
    "https://raw.githubusercontent.com/imDazui/Tvlist-awesome-m3u-m3u8/refs/heads/master/m3u/%E5%8F%B0%E6%B9%BE%E9%A6%99%E6%B8%AF%E6%B5%B7%E5%A4%96202005.m3u",
    "https://raw.githubusercontent.com/imDazui/Tvlist-awesome-m3u-m3u8/refs/heads/master/m3u/%E5%8F%B0%E6%B9%BE%E9%A6%99%E6%B8%AF%E6%B5%B7%E5%A4%96202003.m3u",
    "https://raw.githubusercontent.com/imDazui/Tvlist-awesome-m3u-m3u8/refs/heads/master/m3u/%E5%8F%B0%E6%B9%BE%E9%A6%99%E6%B8%AF%E6%B5%B7%E5%A4%96.m3u",
    "https://raw.githubusercontent.com/imDazui/Tvlist-awesome-m3u-m3u8/refs/heads/master/m3u/1300%E4%B8%AA%E7%9B%B4%E6%92%AD%E6%BA%90%E5%85%A8%E9%83%A8%E6%9C%89%E6%95%88%E3%80%90%E5%85%A8%E9%83%A84k%E8%80%81%E7%94%B5%E8%84%91%E5%88%AB%E7%94%A8%E3%80%91.m3u8",
    "https://raw.githubusercontent.com/imDazui/Tvlist-awesome-m3u-m3u8/refs/heads/master/m3u/5000%E4%B8%AA%E7%9B%B4%E6%92%AD%E6%BA%90%E5%85%A8%E9%83%A8%E6%9C%89%E6%95%88.m3u",
    "https://raw.githubusercontent.com/imDazui/Tvlist-awesome-m3u-m3u8/refs/heads/master/m3u/%E6%88%91%E7%9A%84%E6%92%AD%E6%94%BE%E6%BA%90.m3u8",
    "https://raw.githubusercontent.com/suxuang/myIPTV/refs/heads/main/ipv4.m3u",
    "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/ipv6.m3u",
    "https://raw.githubusercontent.com/YueChan/Live/main/IPTV.m3u",
    "https://raw.githubusercontent.com/Kimentanm/aptv/master/m3u/iptv.m3u",
    "https://raw.githubusercontent.com/yuanzl77/IPTV/main/live.m3u",
    "https://iptv-org.github.io/iptv/index.m3u",
    "https://iptv-org.github.io/iptv/countries/hk.m3u",
    "https://raw.githubusercontent.com/joevess/IPTV/main/home.m3u8",
    "https://raw.githubusercontent.com/YanG-1989/m3u/main/Gather.m3u",
    "https://raw.githubusercontent.com/Free-TV/IPTV/refs/heads/master/playlists/playlist_hong_kong.m3u8",
    "https://raw.githubusercontent.com/vbskycn/iptv/refs/heads/master/tv/iptv4.m3u",
    "https://epg.pw/test_channels_hong_kong.m3u",
    "https://raw.githubusercontent.com/hujingguang/ChinaIPTV/main/cnTV_AutoUpdate.m3u8",
    "https://raw.githubusercontent.com/MercuryZz/IPTVN/refs/heads/Files/GAT.m3u"
]

# 2. 包含關鍵字 (必須包含這些字才抓取)
KEYWORDS = [
    "ViuTV", "HOY", "RTHK", "Jade", "Pearl", "J2", "J5", "Now", "NOW",
    "无线", "無線", "有线", "有線", "翡翠", "明珠", "港台", 
]

# 3. 黑名單關鍵字 (包含這些字的一律丟棄)
BLOCK_KEYWORDS = [
    # 來自你的日誌分析 (美國/英語台)
    "FOX", "Pluto", "Local", "NBC", "CBS", "ABC", "AXS", "Snowy", 
    "Reuters", "Mirror", "ET Now", "The Now", "Right Now", "News Now",
    "Chopper", "Wow", "UHD", "8K", "Career", "Comics", "Movies",
    "CBTV","Pearl","AccuWeather","Jadeed","Curiosity","Electric",
    "Warfare","Knowledge","MagellanTV","70s","80s","90s","Rock",
    "Winnipeg","Edmonton","RightNow","Times","True","Mindanow",
    "Jade","70's","80's","Romedy","WSOC","NowMedia",
    
    # 來自你的日誌分析 (大陸/澳門台)
    "浙江", "杭州", "西湖", "廣東", "珠江", "大灣區", # 排除 "杭州西湖明珠"
    "澳門", "Macau", "有線 CH", "互動新聞",           # 排除澳門有線
    "CCTV", "CGTN", "鳳凰", "凤凰", "華麗", "星河", "延时", "測試", "iHOY", "福建"
]

# 4. 【已更新】頻道排序優先級 (越上面越靠前)
ORDER_KEYWORDS = [
    "翡翠", "無線新聞", "明珠", "J2", "J5", "財經",  # TVB系列
    "ViuTV", "Viutv", "VIUTV", "ViuTV 6", "ViuTVsix",  # Viu系列 (包含你加入的大小寫變體)
    "HOY", "奇妙", "有線",                         # HOY/Cable系列
    "港台電視31", "RTHK 31",                      # RTHK系列
    "港台電視32", "RTHK 32",
    "Now新聞", "Now直播"                          # Now系列
]

# 5. 必備的官方/穩定源
STATIC_CHANNELS = [
    {"name": "港台電視31 (官方)", "url": "https://rthklive1-lh.akamaihd.net/i/rthk31_1@167495/index_2052_av-b.m3u8"},
    {"name": "港台電視32 (官方)", "url": "https://rthklive2-lh.akamaihd.net/i/rthk32_1@168450/index_2052_av-b.m3u8"}
]

# --- 邏輯區 ---

def check_url_concurrent(channels, max_workers=10):
    """並行檢測多個頻道的有效性"""
    results = {}
    
    def check_single(ch):
        return ch['url'], playback_check_url(ch['url'])
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(check_single, ch): ch for ch in channels}
        
        for future in as_completed(futures):
            url, is_valid = future.result()
            results[url] = is_valid
    
    return results

def get_sort_key(item):
    """計算頻道的排序權重"""
    name = item["name"]
    for index, keyword in enumerate(ORDER_KEYWORDS):
        if keyword in name:
            return index
    return 999

def fetch_and_parse():
    found_channels = []
    
    print("🚀 任務開始！正在抓取網路源...", flush=True)
    
    for index, source in enumerate(SOURCE_URLS):
        print(f"  [{index+1}/{len(SOURCE_URLS)}] 正在讀取: {source}", flush=True)
        try:
            r = requests.get(source, timeout=15)
            r.encoding = 'utf-8'
            
            if r.status_code != 200:
                print(f"    ⚠️ 無法讀取 (Status: {r.status_code})", flush=True)
                continue
            
            lines = r.text.split('\n')
            current_name = ""
            count_added = 0
            is_m3u = any(line.startswith('#EXTM3U') or line.startswith('#EXTINF') for line in lines[:10])
            
            for line in lines:
                line = line.strip()
                if not line: continue
                
                if is_m3u:
                    # M3U 格式解析
                    if line.startswith("#EXTINF"):
                        match = re.search(r',(.+)$', line)
                        if match:
                            raw_name = match.group(1).strip()
                            converted_name = cc.convert(raw_name)
                            current_name = converted_name.replace('臺', '台')
                    elif line.startswith("http") and current_name:
                        if any(b.lower() in current_name.lower() for b in BLOCK_KEYWORDS):
                            current_name = ""
                            continue
                        if any(cc.convert(k).replace('臺', '台').lower() in current_name.lower() for k in KEYWORDS):
                            if not any(c['url'] == line for c in found_channels):
                                found_channels.append({"name": current_name, "url": line})
                                count_added += 1
                        current_name = ""
                else:
                    # TXT 格式解析 (名稱,URL 或 名稱@URL)
                    if ',' in line and line.startswith('http') == False:
                        parts = line.split(',', 1)
                        if len(parts) == 2:
                            name_part = parts[0].strip()
                            url_part = parts[1].strip()
                            if url_part.startswith('http'):
                                converted_name = cc.convert(name_part).replace('臺', '台')
                                if any(b.lower() in converted_name.lower() for b in BLOCK_KEYWORDS):
                                    continue
                                if any(cc.convert(k).replace('臺', '台').lower() in converted_name.lower() for k in KEYWORDS):
                                    if not any(c['url'] == url_part for c in found_channels):
                                        found_channels.append({"name": converted_name, "url": url_part})
                                        count_added += 1
                    elif line.startswith('http'):
                        # 純 URL，嘗試從 URL 推斷名稱
                        url_part = line.strip()
                        if url_part.startswith('http'):
                            for kw in KEYWORDS:
                                if cc.convert(kw).replace('臺', '台').lower() in url_part.lower():
                                    converted_name = cc.convert(kw).replace('臺', '台')
                                    if not any(c['url'] == url_part for c in found_channels):
                                        found_channels.append({"name": converted_name, "url": url_part})
                                        count_added += 1
                                    break
            
            print(f"    ✅ 抓取成功，新增 {count_added} 個頻道", flush=True)
            
        except Exception as e:
            print(f"    ❌ 抓取錯誤: {e}", flush=True)

    return found_channels

def generate_m3u(channels):
    total = len(channels) + len(STATIC_CHANNELS)
    print(f"\n🔍 共找到 {total} 個潛在頻道，開始並行檢測有效性...", flush=True)
    
    final_list = []
    
    # 1. 並行檢測靜態源與網路源
    validity_map = check_url_concurrent(STATIC_CHANNELS + channels, max_workers=10)
    
    for ch in STATIC_CHANNELS + channels:
        if validity_map.get(ch['url'], False):
            final_list.append(ch)
            print(f"  🟢 {ch['name']} - 有效", flush=True)
        else:
            print(f"  🔴 {ch['name']} - 失效", flush=True)

    # 3. 排序
    print("\n🔄 正在進行排序...", flush=True)
    final_list.sort(key=get_sort_key)

    # 4. 寫入文件
    content = '#EXTM3U x-tvg-url="https://epg.112114.xyz/pp.xml"\n'
    content += f'# Update: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n'
    
    for item in final_list:
        final_name = item["name"].replace('臺', '台')
        content += f'#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/{final_name}.png",{final_name}\n'
        content += f'{item["url"]}\n'

    with open("hk_live.m3u", "w", encoding="utf-8") as f:
        f.write(content)

    print(f"\n🎉 全部完成！共收錄 {len(final_list)} 個有效頻道。", flush=True)

if __name__ == "__main__":
    candidates = fetch_and_parse()
    generate_m3u(candidates)

````

## File: stream_check.py
````py
"""Lightweight playback probes for IPTV URLs.

An HTTP 200 response is not enough for a live stream: an HLS playlist can be
present while every media segment has already expired.  This module follows
the first few HLS entries and validates that at least one media payload can be
downloaded.
"""

import re
import time
from urllib.parse import urljoin

import requests


USER_AGENT = "hk-iptv-auto/1.0 (+https://github.com/linwengjian-gmail/hk-iptv-auto)"
MAX_PROBE_BYTES = 256 * 1024
MAX_PLAYLIST_DEPTH = 2
MAX_HLS_ENTRIES = 3


def _strip_m3u_suffix(url):
    """Remove the optional IPTV `$label` suffix before making an HTTP request."""
    return url.split("$", 1)[0].strip()


def _download_probe(url, timeout):
    """Download a bounded prefix and return data, final URL, and headers."""
    data = bytearray()
    try:
        with requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
            timeout=(timeout, timeout),
            stream=True,
            allow_redirects=True,
        ) as response:
            if response.status_code not in (200, 206):
                return None

            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    data.extend(chunk)
                if len(data) >= MAX_PROBE_BYTES:
                    break

            if not data:
                return None
            return bytes(data), response.url, dict(response.headers)
    except requests.exceptions.RequestException:
        return None


def _text_prefix(data):
    return data[:4096].decode("utf-8-sig", errors="ignore").lstrip()


def _is_playlist(data, headers):
    text = _text_prefix(data)
    content_type = headers.get("Content-Type", "").lower()
    return (
        text.startswith(("#EXTM3U", "#EXT-X-", "#EXTINF"))
        or "mpegurl" in content_type
        or "dash+xml" in content_type
    )


def _playlist_entries(text, base_url):
    """Resolve a small set of media/variant URLs from an HLS playlist."""
    entries = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # Low-latency HLS may put a playable partial segment in an attribute.
        if line.startswith(("#EXT-X-PART:", "#EXT-X-PRELOAD-HINT:")):
            match = re.search(r'URI="([^"]+)"', line)
            if match:
                line = match.group(1).strip()
            else:
                continue
        elif line.startswith("#"):
            continue

        line = _strip_m3u_suffix(line)
        child_url = urljoin(base_url, line)
        if child_url.startswith(("http://", "https://")) and child_url not in entries:
            entries.append(child_url)
    return entries


def _has_mpeg_ts_signature(data):
    if len(data) < 188:
        return False
    for offset in range(min(4, len(data))):
        if data[offset] != 0x47:
            continue
        if len(data) < offset + 376 or data[offset + 188] == 0x47:
            return True
    return False


def _looks_like_media_payload(data, headers, url):
    """Reject HTML/JSON error pages and accept common stream payloads."""
    if not data:
        return False

    prefix = data[:256].lstrip().lower()
    if prefix.startswith((b"<!doctype html", b"<html", b"{", b"[", b"error", b"not found")):
        return False

    if _has_mpeg_ts_signature(data):
        return True
    if data.startswith((b"FLV", b"\x1a\x45\xdf\xa3", b"ID3")):
        return True
    if b"ftyp" in data[:64] or b"moof" in data[:64] or b"mdat" in data[:64]:
        return True
    if len(data) >= 2 and data[0] == 0xFF and (data[1] & 0xE0) == 0xE0:
        return True

    content_type = headers.get("Content-Type", "").lower()
    media_type = (
        content_type.startswith("video/")
        or content_type.startswith("audio/")
        or "octet-stream" in content_type
    )
    if media_type and len(data) >= 512:
        return True

    # Some providers return text/plain for binary .ts/.mp4 responses.
    path = url.lower().split("?", 1)[0]
    return len(data) >= 512 and path.endswith((".ts", ".mp4", ".m4s", ".mkv", ".webm", ".flv"))


def _check_hls_playlist(url, data, headers, timeout, depth=0):
    if depth > MAX_PLAYLIST_DEPTH or not _is_playlist(data, headers):
        return False

    entries = _playlist_entries(_text_prefix(data), url)
    if len(entries) > MAX_HLS_ENTRIES:
        entries_to_check = entries[:1] + entries[-(MAX_HLS_ENTRIES - 1):]
    else:
        entries_to_check = entries

    for entry in entries_to_check:
        result = _download_probe(entry, timeout)
        if not result:
            continue

        child_data, child_url, child_headers = result
        if _is_playlist(child_data, child_headers):
            if _check_hls_playlist(child_url, child_data, child_headers, timeout, depth + 1):
                return True
        elif _looks_like_media_payload(child_data, child_headers, child_url):
            return True
    return False


def check_url(url, retries=2, timeout=5):
    """Return True only when the URL exposes a playable media payload."""
    url = _strip_m3u_suffix(url)
    if not url.startswith(("http://", "https://")):
        return False

    for attempt in range(retries + 1):
        result = _download_probe(url, timeout)
        if result:
            data, final_url, headers = result
            if _is_playlist(data, headers):
                valid = _check_hls_playlist(urljoin(url, final_url), data, headers, timeout)
            else:
                valid = _looks_like_media_payload(data, headers, final_url)
            if valid:
                return True

        if attempt < retries:
            time.sleep(1)
    return False

````

## File: .github/workflows/combine-code.yml
````yml
name: Generate All Codebase to MD

on:
  push:
    branches:
      - main
    paths-ignore:
      - 'combined_project_code.md' # 避免此檔案自身更新引發無限循環
  workflow_dispatch: # 支援在 GitHub 網頁上手動觸發執行

permissions:
  contents: write

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Combine All Files into MD
        run: |
          OUT_FILE="combined_project_code.md"
          echo "# Complete Project Codebase" > "$OUT_FILE"
          echo "Generated on: $(date)" >> "$OUT_FILE"
          echo "" >> "$OUT_FILE"

          # 遍歷專案內的所有檔案，排除依賴、Git 歷史、打包產物及二進位檔案
          find . -type f \
            -not -path "*/node_modules/*" \
            -not -path "*/.git/*" \
            -not -path "*/dist/*" \
            -not -name "package-lock.json" \
            -not -name "yarn.lock" \
            -not -name "pnpm-lock.yaml" \
            -not -name "$OUT_FILE" \
            -not -name "*.png" \
            -not -name "*.jpg" \
            -not -name "*.jpeg" \
            -not -name "*.gif" \
            -not -name "*.ico" \
            -not -name "*.woff*" \
            -not -name "*.ttf" | while read -r file; do
              
              # 取得相對路徑與副檔名
              rel_path="${file#./}"
              ext="${file##*.}"
              
              # 如果無副檔名，清除變數避免格式混亂
              if [ "$ext" = "$rel_path" ]; then
                ext=""
              fi
              
              # 寫入檔案標題
              echo "## File: $rel_path" >> "$OUT_FILE"
              # 使用四個反單引號（````）包裹，防止內部程式碼的三個反單引號造成排版衝突
              echo "\`\`\`\`$ext" >> "$OUT_FILE"
              cat "$file" >> "$OUT_FILE"
              echo "" >> "$OUT_FILE"
              echo "\`\`\`\`" >> "$OUT_FILE"
              echo "" >> "$OUT_FILE"
          done

      - name: Commit and Push changes
        run: |
          git config --local user.email "github-actions[bot]@users.noreply.github.com"
          git config --local user.name "github-actions[bot]"
          git add combined_project_code.md
          
          if git diff --staged --quiet; then
            echo "No changes in codebase."
          else
            git commit -m "docs: auto-generate complete codebase [skip ci]"
            git push origin main
          fi

````

## File: .github/workflows/main.yml
````yml
name: Update IPTV Source

on:
  schedule:
    # 每天香港時間 08:00 和 20:00 運行 (UTC 00:00, 12:00)
    - cron: '0 0,12 * * *'
  workflow_dispatch: # 允許手動點擊按鈕

permissions:
  contents: write

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
    - name: Checkout
      uses: actions/checkout@v3

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'

    - name: Install dependencies
      run: pip install -r requirements.txt

    - name: Run script
      run: python main.py

    - name: Commit and push
      run: |
        git config --local user.email "action@github.com"
        git config --local user.name "GitHub Action"
        git add hk_live.m3u
        git commit -m "Auto-update channel list" || echo "No changes to commit"
        git push

````

## File: README.md
````md
# 📺 HK IPTV Auto Updater | 香港電視台直播源自動更新

![Update Status](https://github.com/sammy0101/hk-iptv-auto/actions/workflows/main.yml/badge.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)

這是一個基於 **GitHub Actions** 的自動化 IPTV 聚合項目。
它每天定時從網路上抓取公開的直播源，自動過濾、檢測有效性、進行繁簡轉換與名稱修正，最終生成一份乾淨、可用的香港電視頻道列表 (`.m3u`)。

---

## 🚀 訂閱地址 (Subscription URL)

請在您的播放器 (TiviMate, TVBox, Kodi, PotPlayer 等) 中輸入以下鏈接：

| 線路 | 鏈接 (URL) | 推薦度 |
| :--- | :--- | :--- |
| **CDN 加速 (推薦)** | `https://raw.gh.registry.cyou/sammy0101/hk-iptv-auto/refs/heads/main/hk_live.m3u` | ⭐⭐⭐⭐⭐ |
| **GitHub Raw** | `https://raw.githubusercontent.com/sammy0101/hk-iptv-auto/refs/heads/main/hk_live.m3u` | ⭐⭐⭐ |

> ⚡ **CDN 加速服務由 [cmliussss](https://blog.cmliussss.com/) 提供，特此感謝！**
> 
> 💡 **提示**：推薦使用上方 **CDN 加速** 鏈接，在部分網路環境下更新速度會更快、更穩定。

---

## ❤️ 特別鳴謝 (Credits)

本項目的數據來源主要基於以下開源項目與維護者的大力奉獻，在此致以最誠摯的謝意：

*   **imDazui**: [Tvlist-awesome-m3u-m3u8](https://github.com/imDazui/Tvlist-awesome-m3u-m3u8)
*   **fanmingming**: [live](https://github.com/fanmingming/live)
*   **Guovin**: [TV](https://github.com/Guovin/TV)
*   **YueChan**: [Live](https://github.com/YueChan/Live)
*   **Kimentanm**: [APTV](https://github.com/Kimentanm/aptv)
*   **yuanzl77**: [IPTV](https://github.com/yuanzl77/IPTV)
*   **iptv-org**: [IPTV Collection](https://github.com/iptv-org/iptv)
*   **joevess**: [IPTV](https://github.com/joevess/IPTV)
*   **YanG-1989**: [m3u](https://github.com/YanG-1989/m3u)
*   **hujingguang**: [ChinaIPTV](https://github.com/hujingguang/ChinaIPTV)
*   **MercuryZz**: [IPTVN](https://github.com/MercuryZz/IPTVN)
*   **vbskycn**: [iptv](https://github.com/vbskycn/iptv)
*   **suxuang**: [myIPTV](https://github.com/suxuang/myIPTV)
*   **Free-TV**, **epg.pw** 以及所有無私維護直播源的開發者們。

---

## 📺 收錄頻道 (Supported Channels)

本項目專注於香港本地頻道，並根據習慣進行了排序：

1.  **TVB 系列**: 翡翠台 (Jade), 明珠台 (Pearl), 無線新聞台 (News), J2, 財經體育資訊台
2.  **ViuTV 系列**: ViuTV (99台), ViuTVsix (96台)
3.  **HOY TV 系列**: HOY TV (77台), HOY 資訊台 (78台), 76台
4.  **RTHK 系列**: 港台電視 31, 32, 33
5.  **Now TV 系列**: Now 新聞台, Now 直播台

---

## ✨ 項目特點 (Features)

*   **🤖 全自動維護**: 利用 GitHub Actions 每天定時抓取最新源。
*   **🔍 智能過濾**: 白名單保留香港頻道，黑名單攔截無效內容。
*   **✅ 有效性檢測**: 自動測試直播源連接，剔除失效鏈接。
*   **📝 名稱標準化**: 集成 `OpenCC` 繁簡轉換，並統一修正「台」字。
*   **🔄 智能排序**: 依照香港觀眾習慣自動排列頻道順序。

---

## 🛠️ 給 Fork 用戶的修改指南 (For Developers)

如果你 Fork 了本項目，並希望自定義抓取來源或過濾邏輯，請參考以下步驟：

### 1. 增加/刪除直播源 (Sources)
直接編輯 `main.py`，找到 `SOURCE_URLS` 列表。你可以加入任何公開的 `.m3u` 或 `.m3u8` 鏈接。

### 2. 修改過濾規則 (Filters)
*   **白名單 (`KEYWORDS`)**: 頻道名稱**必須包含**這些關鍵字才會被抓取。
*   **黑名單 (`BLOCK_KEYWORDS`)**: 頻道名稱若包含這些字，會被**強制丟棄**。

### 3. 調整頻道排序 (Sorting)
編輯 `main.py` 中的 `ORDER_KEYWORDS` 列表。越上面的關鍵字，優先級越高。

### 4. 修改訂閱鏈接 (Update Subscription URL)
Fork 之後，`README.md` 顯示的訂閱鏈接仍然指向原作者 (`sammy0101`) 的倉庫。
請務必編輯 `README.md`，將訂閱鏈接中的 `sammy0101` 替換為你的 GitHub 用戶名，否則你的用戶將無法獲取你更新的列表。

*   **CDN 格式範例**:
    `https://raw.gh.registry.cyou/<你的用戶名>/<倉庫名稱>/refs/heads/main/hk_live.m3u`

### ⚠️ 重要：Fork 後如何啟用自動更新
Fork 本項目後，GitHub Actions 默認是關閉的。你需要：
1.  進入你倉庫的 **Actions** 頁面。
2.  點擊綠色按鈕 **"I understand my workflows, go ahead and enable them"**。
3.  左側選擇 **Update IPTV Source** -> **Enable workflow**。

---

## ⚠️ 免責聲明 (Disclaimer)

1.  **僅供學習交流**: 本項目僅是一個技術研究項目。
2.  **不存儲視頻**: 所有直播源鏈接均來自網際網路上的公開渠道，本倉庫不存儲任何視頻流文件。
3.  **版權聲明**: 頻道版權歸相關電視台所有。
4.  **地區限制**: 部分源可能僅限香港 IP 播放。

**Last Update:** 每天自動更新

````

## File: requirements.txt
````txt
requests
opencc-python-reimplemented

````

## File: hk_live.m3u
````m3u
#EXTM3U x-tvg-url="https://epg.112114.xyz/pp.xml"
# Update: 2026-08-14 08:38:11
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/[BD]翡翠.png",[BD]翡翠
https://stream1.freetv.fun/2d6d5de01dfd6fdcaaf4fe4b5ab0188eb1849a91fee22cc73276e6737ce055a8.ctv
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/翡翠台.png",翡翠台
http://php.jdshipin.com:8880/TVOD/iptv.php?id=fct
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/翡翠台北美版.png",翡翠台北美版
http://php.jdshipin.com:8880/TVOD/iptv.php?id=j1
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/翡翠台.png",翡翠台
http://r.jdshipin.com/GeWKr?id=fct720
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/翡翠台.png",翡翠台
http://122.152.202.33/s/81a8a44f/index.m3u8?id=53
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/翡翠台.png",翡翠台
http://r.jdshipin.com/qClQf
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/翡翠台.png",翡翠台
http://r.jdshipin.com/qrfbg
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/翡翠台.png",翡翠台
http://r.jdshipin.com/62WM7
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/翡翠台.png",翡翠台
http://r.jdshipin.com/n90gt
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/翡翠台.png",翡翠台
http://r.jdshipin.com/GeWKr
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/翡翠.png",翡翠
http://php.jdshipin.com/TVOD/iptv.php?id=fct2
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/翡翠.png",翡翠
http://php.jdshipin.com/TVOD/iptv.php?id=fct3
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/TVB翡翠台 1080P.png",TVB翡翠台 1080P
http://php.jdshipin.com:8880/TVOD/iptv.php?id=fct3
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/翡翠台.png",翡翠台
http://php.jdshipin.com:8880/TVOD/iptv.php?id=fct4
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/翡翠台.png",翡翠台
http://27.39.122.2:9901/tsfile/live/1034_1.m3u8?key=txiptv&playlive=1&authid=0$LR—IPV4【线路1】
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/翡翠台.png",翡翠台
http://r.jdshipin.com/qClQf$LR—IPV4【线路2】
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/翡翠台.png",翡翠台
http://r.jdshipin.com/qrfbg$LR—IPV4【线路3】
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/翡翠台.png",翡翠台
http://r.jdshipin.com/62WM7$LR—IPV4【线路4】
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/翡翠台.png",翡翠台
http://r.jdshipin.com/n90gt$LR—IPV4【线路5】
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/翡翠台.png",翡翠台
http://r.jdshipin.com/GeWKr$LR—IPV4【线路6】
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/[BD]翡翠.png",[BD]翡翠
https://stream1.freetv.fun/b6805e649e0c7b8193b4fd6d6b407675b3254a0eca4b30ef8ed033b5202b285e.ctv
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/[SD]翡翠.png",[SD]翡翠
https://stream1.freetv.fun/b1128a129ebb9fd5ddde711ab565c82831400d80e5aa6aa9822e6c1b33ed95c5.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/[SD]翡翠.png",[SD]翡翠
https://stream1.freetv.fun/07b8f89d6c2bf405852bd8eb695e2389afdb45dfaf30d5273f8d2b819720681f.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/[BD]翡翠.png",[BD]翡翠
https://stream1.freetv.fun/7626cd1e0830deb6cc37d51b751648e27e25c8136a22f61f6bb6ce5a85dcb0c4.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/[BD]翡翠台[geo-blocked].png",[BD]翡翠台[geo-blocked]
https://stream1.freetv.fun/f47d16542d51e01285a1b9ce86acd63ff3f5522b3a256f392628327f068a63fa.ctv
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/[HD]無線新聞.png",[HD]無線新聞
https://stream1.freetv.fun/b9fc79702a33d75d0fa4c22a19341be1a7120d111c47d9e109566b9d42020383.ctv
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/TVB無線新聞.png",TVB無線新聞
http://122.152.202.33/s/81a8a44f/index.m3u8?id=21
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/無線新聞.png",無線新聞
http://r.jdshipin.com/CkuBd
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/無線新聞.png",無線新聞
http://php.jdshipin.com/TVOD/iptv.php?id=wxxw
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/無線新聞.png",無線新聞
http://r.jdshipin.com/CkuBd$LR—IPV4【线路1】
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/無線新聞.png",無線新聞
http://php.jdshipin.com/TVOD/iptv.php?id=wxxw$LR—IPV4【线路2】
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/[BD]明珠台.png",[BD]明珠台
https://stream1.freetv.fun/d1da08cd35d96f944f0ab0c0cc5ed0ee74e753f8fd9cbcb4c56e15d46ce3993a.ctv
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/[BD]明珠.png",[BD]明珠
https://stream1.freetv.fun/e56abfec74a8d135ddfc2c074a81751b6fc41d0272d5f5519c218b2e56d3265f.ctv
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/明珠台.png",明珠台
http://r.jdshipin.com/GeWKr?id=mzt720
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/TVB明珠.png",TVB明珠
http://122.152.202.33/s/81a8a44f/index.m3u8?id=23
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/明珠台.png",明珠台
http://r.jdshipin.com/ZQ4kN
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/明珠台.png",明珠台
http://r.jdshipin.com/jUx8K
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/明珠台.png",明珠台
http://php.jdshipin.com/TVOD/iptv.php?id=mzt2
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/明珠台.png",明珠台
http://27.39.122.2:9901/tsfile/live/1035_1.m3u8?key=txiptv&playlive=1&authid=0$LR—IPV4【线路1】
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/明珠台.png",明珠台
http://r.jdshipin.com/ZQ4kN$LR—IPV4【线路2】
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/明珠台.png",明珠台
http://r.jdshipin.com/jUx8K$LR—IPV4【线路3】
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/明珠台.png",明珠台
http://php.jdshipin.com/TVOD/iptv.php?id=mzt2$LR—IPV4【线路4】
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/[BD]明珠.png",[BD]明珠
https://stream1.freetv.fun/4db377997e788111dc3a7a47fec67132e36bd8221c27f01f0d14ae82d86bc4ff.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/[BD]明珠.png",[BD]明珠
https://stream1.freetv.fun/b49f11a7e68a76202daba76fb5c90eb8ef1c34d977c5e9eac0825112f74f87d3.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/有線財經信息台.png",有線財經信息台
https://epg.pw/stream/0fddc0600d3868b05ad741d46294410aebca0fdc4fada5028dc54e624b7b17ca.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/ViuTV.png",ViuTV
http://r.jdshipin.com/vSJvl
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/ViuTV.png",ViuTV
http://r.jdshipin.com/TcKr2
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/Viutv.png",Viutv
http://php.jdshipin.com:8880/PLTV/iptv.php?id=viutv
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/Viutv.png",Viutv
http://php.jdshipin.com:8880/PLTV/iptv.php?id=viutv$LR—IPV4
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/VIUTV.png",VIUTV
http://php.jdshipin.com/TVOD/iptv.php?id=viutv
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/VIUTV.png",VIUTV
http://php.jdshipin.com/TVOD/iptv.php?id=viutv2
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/HOY TV.png",HOY TV
http://uc6.i-cable.com/live_freedirect/opentvhd001_h.live/playlist.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/HOY TV.png",HOY TV
http://r.jdshipin.com/sFw4S
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/HOY TV.png",HOY TV
http://php.jdshipin.com/TVOD/iptv.php?id=hoytv
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/有線新聞.png",有線新聞
http://61.10.2.140/live_freedirect/freehd209_h.live/chunklist_w135209556.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/香港有線新聞.png",香港有線新聞
https://epg.pw/stream/4c65fc12a950810e9f068c55b2abf43cf7937762e9c5d4d44381205743c731bf.ctv
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/有線新聞.png",有線新聞
http://61.10.2.141/live_freedirect/freehd209_h.live/playlist.m3u
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/有線新聞台.png",有線新聞台
http://cm61-10-2-143.hkcable.com.hk/live_freedirect/freehd209_h.live/playlist.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/港台電視31 (官方).png",港台電視31 (官方)
https://rthklive1-lh.akamaihd.net/i/rthk31_1@167495/index_2052_av-b.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/港台電視31.png",港台電視31
http://php.jdshipin.com/TVOD/iptv.php?id=rthk31
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/RTHK 31.png",RTHK 31
https://www.rthk.hk/feeds/dtt/rthktv31_https.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/港台電視32.png",港台電視32
http://php.jdshipin.com/TVOD/iptv.php?id=rthk32
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/RTHK31.png",RTHK31
http://rthklive1-lh.akamaihd.net/i/rthk31_1@167495/index_810_av-b.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/RTHK31.png",RTHK31
http://rthklive1-lh.akamaihd.net/i/rthk31_1@167495/index_2052_av-b.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/RTHK31.png",RTHK31
https://rthktv31-live.akamaized.net/hls/live/2036818/RTHKTV31/master.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/RTHK31.png",RTHK31
https://rthklive1-lh.akamaihd.net/i/rthk31_1@167495/master.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/[BD]rthk33.png",[BD]rthk33
https://stream1.freetv.fun/3d32ed5e32eeae179d2bc42f25cd452226dad0093f2c6a64bf1f3c11f6bd430d.ctv
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/無線 TVB Plus.png",無線 TVB Plus
http://php.jdshipin.com/TVOD/iptv.php?id=tvbp
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/RTHK31.png",RTHK31
http://php.jdshipin.com:8880/TVOD/iptv.php?id=rthk31
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/RTHK32.png",RTHK32
http://php.jdshipin.com:8880/TVOD/iptv.php?id=rthk32
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/Anthony Bourdain: Parts Unknown.png",Anthony Bourdain: Parts Unknown
https://jmp2.uk/plu-69173ce8abd4703b27f71d44.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/HEi Now (1080p).png",HEi Now (1080p)
https://copacogen.desdeparaguay.net/heitv/heitv_py_alta/playlist.m3u8?admin=nacion
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/Now 14 (1080p).png",Now 14 (1080p)
https://r.il.cdn-redge.media/livehls/oil/ch14/live/ch14/live.livx/playlist.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/NOW News Arabic (720p).png",NOW News Arabic (720p)
https://live.nowtelly.com/now_arabic/live/playlist.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/NOW News Global (720p).png",NOW News Global (720p)
https://nowhls.wns.live/hls/stream.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/NOW News Spanish (1080p).png",NOW News Spanish (1080p)
https://live.nowtelly.com/now_spanish/live/playlist.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/NOW News USA (1080p).png",NOW News USA (1080p)
https://live.nowtelly.com/now_usa/live_usa/playlist.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/NOW TV (720p).png",NOW TV (720p)
https://uycyyuuzyh.turknet.ercdn.net/nphindgytw/nowtv/nowtv.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/[HD]now爆谷.png",[HD]now爆谷
https://stream1.freetv.fun/3f31f2dce2add4c7542daf68797a0bf424ceff7fb57654cbcf47f6b095189809.m3u8
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/[BD]rthk31.png",[BD]rthk31
https://stream1.freetv.fun/bcd3e759b0c9f8807280e9cb8fc50c6919ffad577b1d5d21f392f601d93301bc.ctv
#EXTINF:-1 group-title="Hong Kong" logo="https://epg.112114.xyz/logo/[BD]rthk32.png",[BD]rthk32
https://stream1.freetv.fun/e42fb110283fcb1befbda834ed93046b948ab8ecf4fa35c482c0296e220b7fc0.ctv

````

