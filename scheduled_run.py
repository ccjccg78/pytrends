"""
定时抓取 Google Trends 数据并推送通知

使用方式:
  1. 复制 config_example.json 为 config.json，填入你的配置
  2. 手动运行: python scheduled_run.py
  3. 定时任务 (北京时间):
     crontab -e 添加:
     0 21 * * * /opt/pytrends-git/.venv/bin/python /opt/pytrends-git/scheduled_run.py --mode trending
     0 22,9 * * * /opt/pytrends-git/.venv/bin/python /opt/pytrends-git/scheduled_run.py --mode sitemap
     0 0 * * * /opt/pytrends-git/.venv/bin/python /opt/pytrends-git/scheduled_run.py --mode rising

支持模式:
  --mode trending  采集所有地区时下流行 (默认)
  --mode rising    采集关键词爆增词
  --mode sitemap   监控竞品 Sitemap 变化
"""

import json
import time
import random
import os
import sys
import argparse
import xml.etree.ElementTree as ET
import requests as http_requests
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError, ResponseError

# 北京时间
BEIJING_TZ = timezone(timedelta(hours=8))

# 所有采集地区
ALL_REGIONS = [
    ("美国", "US"),
    ("英国", "GB"),
    ("日本", "JP"),
    ("德国", "DE"),
    ("法国", "FR"),
    ("加拿大", "CA"),
    ("澳大利亚", "AU"),
    ("印度", "IN"),
    ("巴西", "BR"),
    ("韩国", "KR"),
    ("新加坡", "SG"),
    ("中国台湾", "TW"),
    ("中国香港", "HK"),
]

# 流量阈值（注意：RSS 的 approx_traffic 是短时搜索热度，非严格日搜索量）
# 单次采集阈值：单次 approx_traffic > 1000 直接入选
TRAFFIC_THRESHOLD = 1000
# 累计阈值：同一话题连续3天累计 approx_traffic > 1000 也入选
CUMULATIVE_DAYS = 3
CUMULATIVE_THRESHOLD = 1000

# 历史记录文件（用于跨天累计判断）
HISTORY_PATH = Path(__file__).parent / "output" / "trending_history.json"

# 默认过滤词库（可通过 config.json 的 exclude_categories 和 exclude_words 覆盖）
DEFAULT_EXCLUDE_CATEGORIES = {
    "赌博": ["casino", "gambling", "gamble", "bet ", "betting", "slot machine", "poker", "roulette",
            "blackjack", "lottery", "jackpot", "wager", "sportsbook"],
    "人名/明星": ["wife", "husband", "boyfriend", "girlfriend", "married", "dating",
                 "net worth", "birthday", "born", "died", "death", "funeral", "obituary",
                 "son of", "daughter of", "who is", "how old"],
    "体育": ["nba", "nfl", "nhl", "mlb", "fifa", "ufc", "boxing", "wrestling",
            "playoff", "championship", "score", "highlights", "roster", "standings",
            "draft pick", "super bowl", "world cup", "premier league", "la liga",
            "serie a", "bundesliga", "vs ", " vs"],
    "娱乐/影视": ["movie", "trailer", "episode", "season finale", "netflix", "hulu",
                 "disney+", "box office", "premiere", "concert", "tour dates",
                 "album release", "grammy", "oscar", "emmy"],
    "新闻/时事": ["shooting", "earthquake", "hurricane", "tornado", "flood", "crash",
                 "explosion", "protest", "riot", "scandal", "arrested", "convicted",
                 "sentenced", "indicted", "breaking news", "election", "vote"],
    "成人内容": ["porn", "xxx", "nude", "naked", "onlyfans", "nsfw", "adult video"],
    "不相关": ["weather", "horoscope", "zodiac", "astrology", "recipe",
             "lyrics", "chords", "tab ", "mugshot"],
}


def get_all_exclude_words(config=None):
    """从 config 加载过滤词，config 未配置时使用默认值"""
    categories = DEFAULT_EXCLUDE_CATEGORIES
    if config and "exclude_categories" in config:
        categories = config["exclude_categories"]
    all_words = []
    for words in categories.values():
        all_words.extend(words)
    # 额外排除词
    if config and "exclude_words" in config:
        all_words.extend([w.strip().lower() for w in config["exclude_words"] if w.strip()])
    return all_words


def is_excluded(text, config=None):
    text_lower = text.lower()
    for ex in get_all_exclude_words(config):
        if ex in text_lower:
            return True
    return False


def parse_traffic(t):
    """将 '200K+' '5,000+' 等转为数字"""
    if not t:
        return 0
    t = t.replace('+', '').replace(',', '').strip()
    if 'M' in t.upper():
        return int(float(t.upper().replace('M', '')) * 1000000)
    elif 'K' in t.upper():
        return int(float(t.upper().replace('K', '')) * 1000)
    try:
        return int(t)
    except ValueError:
        return 0


# ── 配置 ──────────────────────────────────────────────────────
CONFIG_PATH = Path(__file__).parent / "config.json"


def load_config():
    if not CONFIG_PATH.exists():
        print(f"配置文件不存在: {CONFIG_PATH}")
        print("请复制 config_example.json 为 config.json 并填入配置")
        sys.exit(1)
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# ── 历史记录（用于跨天累计）──────────────────────────────────────
def load_history():
    """加载历史记录，格式: { "话题|地区代码": [{"date": "2026-04-12", "traffic": 200}, ...] }"""
    if HISTORY_PATH.exists():
        with open(HISTORY_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_history(history):
    """保存历史记录，自动清理超过 CUMULATIVE_DAYS 天的旧数据"""
    HISTORY_PATH.parent.mkdir(exist_ok=True)
    cutoff = (datetime.now(BEIJING_TZ) - timedelta(days=CUMULATIVE_DAYS)).strftime("%Y-%m-%d")
    # 清理过期记录
    cleaned = {}
    for key, records in history.items():
        valid = [r for r in records if r["date"] >= cutoff]
        if valid:
            cleaned[key] = valid
    with open(HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)


def get_cumulative_traffic(history, key):
    """计算某个话题在 CUMULATIVE_DAYS 天内的累计流量"""
    records = history.get(key, [])
    return sum(r["traffic"] for r in records)


# ── 时下流行采集（所有地区）─────────────────────────────────────
def fetch_all_trending(config=None):
    """采集所有地区的时下流行，返回汇总数据

    入选条件（满足任一即可）：
      1. 单次 approx_traffic > TRAFFIC_THRESHOLD (1000)
      2. 同一话题近 CUMULATIVE_DAYS (3) 天累计 approx_traffic > CUMULATIVE_THRESHOLD (1000)
    """
    all_results = []
    history = load_history()
    today = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")

    for name, code in ALL_REGIONS:
        print(f"  📡 采集 {name} ({code})...")
        try:
            rss_url = f"https://trends.google.com/trending/rss?geo={code}"
            resp = http_requests.get(rss_url, timeout=15, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })

            if resp.status_code == 200:
                root = ET.fromstring(resp.content)
                ns = {'ht': 'https://trends.google.com/trending/rss'}
                count = 0

                for item in root.iter('item'):
                    title_el = item.find('title')
                    traffic_el = item.find('ht:approx_traffic', ns)
                    title = title_el.text if title_el is not None else ''
                    traffic = traffic_el.text if traffic_el is not None else ''

                    # 关键词过滤（从 config.json 读取过滤规则）
                    if is_excluded(title, config):
                        continue

                    traffic_num = parse_traffic(traffic)
                    history_key = f"{title.lower()}|{code}"

                    # 记录到历史（不论是否入选，都记录以便累计）
                    if history_key not in history:
                        history[history_key] = []
                    # 同一天同一话题只记录一次（取较大值）
                    today_records = [r for r in history[history_key] if r["date"] == today]
                    if today_records:
                        today_records[0]["traffic"] = max(today_records[0]["traffic"], traffic_num)
                    else:
                        history[history_key].append({"date": today, "traffic": traffic_num})

                    cumulative = get_cumulative_traffic(history, history_key)

                    # 入选条件：单次 > 1000 或 3天累计 > 1000
                    if traffic_num <= TRAFFIC_THRESHOLD and cumulative <= CUMULATIVE_THRESHOLD:
                        continue

                    reason = "单次" if traffic_num > TRAFFIC_THRESHOLD else f"累计{CUMULATIVE_DAYS}天"
                    all_results.append({
                        'title': title,
                        'traffic': traffic,
                        'traffic_num': traffic_num,
                        'cumulative_traffic': cumulative,
                        'reason': reason,  # 备注入选原因
                        'region': name,
                        'region_code': code,
                    })
                    count += 1

                print(f"    -> {count} 条有效数据")
            else:
                print(f"    -> HTTP {resp.status_code}")

        except Exception as e:
            print(f"    -> 失败: {e}")

        time.sleep(2)  # 地区之间间隔2秒

    # 保存历史记录
    save_history(history)

    return all_results


# ── 飞书通知（统一发送）────────────────────────────────────────
def send_trending_feishu(webhook_url, all_results):
    """所有地区汇总后统一发一条飞书消息"""
    now = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")

    # 按搜索量排序
    all_results.sort(key=lambda x: x['traffic_num'], reverse=True)

    # 汇总信息
    content_lines = [
        [{"tag": "text", "text": f"📅 {now}\n共采集 {len(ALL_REGIONS)} 个地区，找到 {len(all_results)} 条热搜（单次>{TRAFFIC_THRESHOLD} 或 {CUMULATIVE_DAYS}天累计>{CUMULATIVE_THRESHOLD}）"}],
        [{"tag": "text", "text": "\n📊 全球热搜:"}],
    ]

    # 所有结果列表，每行: 话题 (搜索量) [入选原因] ← 地区
    for item in all_results:
        tag = f"[{item.get('reason', '')}]" if item.get('reason') else ""
        line = f"{item['title']}  ({item['traffic']}) {tag}  ← {item['region']}"
        content_lines.append([{"tag": "text", "text": line}])

    payload = {
        "msg_type": "post",
        "content": {"post": {"zh_cn": {
            "title": "🔥 全球时下流行趋势报告",
            "content": content_lines,
        }}}
    }

    try:
        resp = http_requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code == 200:
            print("✅ 飞书汇总通知发送成功")
        else:
            print(f"❌ 飞书通知失败: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"❌ 飞书通知异常: {e}")


# ── 爆增词采集 ────────────────────────────────────────────────
def fetch_rising_queries(config):
    """抓取所有关键词的 rising queries"""
    kw_list = [kw.strip() for kw in config["keywords"] if kw.strip()]
    geo = config.get("geo", "")
    cat = config.get("category", 0)
    timeframe = config.get("timeframe", "now 7-d")
    interval = config.get("request_interval", 8)

    all_rising = []
    failed = []
    effective_interval = interval

    pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)

    for i, kw in enumerate(kw_list):
        print(f"[{i+1}/{len(kw_list)}] 查询: {kw}")

        retry_count = 0
        while retry_count < 3:
            try:
                pytrend.build_payload(kw_list=[kw], cat=cat, timeframe=timeframe, geo=geo)
                result = pytrend.related_queries()

                if kw in result and result[kw]['rising'] is not None:
                    rising_df = result[kw]['rising'].copy()
                    rising_df['keyword'] = kw
                    all_rising.append(rising_df)
                    print(f"  -> 找到 {len(rising_df)} 个上升词")
                else:
                    print(f"  -> 无上升词")
                break

            except TooManyRequestsError:
                retry_count += 1
                wait = 60 + retry_count * 60 + random.randint(0, 10)
                old_interval = effective_interval
                effective_interval = min(effective_interval * 1.5, 300)
                print(f"  ⚠️ 429 限流，等待 {wait}s 后重试 ({retry_count}/3)，后续间隔 {old_interval:.0f}s → {effective_interval:.0f}s")
                time.sleep(wait)
                pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)

            except ResponseError as e:
                retry_count += 1
                if '429' in str(e):
                    wait = 60 + retry_count * 60 + random.randint(0, 10)
                    old_interval = effective_interval
                    effective_interval = min(effective_interval * 1.5, 300)
                    print(f"  ⚠️ 429 限流，等待 {wait}s 后重试 ({retry_count}/3)，后续间隔 {old_interval:.0f}s → {effective_interval:.0f}s")
                else:
                    wait = 30 + retry_count * 15
                    print(f"  ⚠️ 错误: {e}，等待 {wait}s ({retry_count}/3)")
                time.sleep(wait)

            except Exception as e:
                print(f"  ❌ 失败: {e}")
                failed.append(kw)
                break
        else:
            failed.append(kw)
            print(f"  ❌ 重试耗尽，跳过")

        if i < len(kw_list) - 1:
            time.sleep(effective_interval + random.uniform(0, 2))

    return all_rising, failed


def analyze_spikes(all_rising, config):
    """分析趋势，判断新词/近日飙升"""
    import numpy as np
    import pandas as pd

    if not all_rising:
        return pd.DataFrame(), {}

    combined = pd.concat(all_rising, ignore_index=True)
    combined['value_num'] = pd.to_numeric(combined['value'], errors='coerce')

    spike_top_n = config.get("spike_top_n", 20)
    temp = combined.copy()
    max_v = temp['value_num'].max()
    temp.loc[temp['value_num'].isna(), 'value_num'] = (max_v * 2) if pd.notna(max_v) else 999999
    top_queries = temp.nlargest(spike_top_n, 'value_num')['query'].unique().tolist()

    geo = config.get("geo", "")
    cat = config.get("category", 0)
    interval = config.get("request_interval", 8)
    spike_results = {}

    pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)
    batches = [top_queries[i:i+5] for i in range(0, len(top_queries), 5)]

    for bi, batch in enumerate(batches):
        print(f"验证趋势 [{bi+1}/{len(batches)}]: {', '.join(batch[:3])}...")

        try:
            pytrend.build_payload(kw_list=batch, cat=cat, timeframe='now 7-d', geo=geo)
            iot = pytrend.interest_over_time()

            if not iot.empty:
                for q in batch:
                    if q in iot.columns:
                        series = iot[q].values.astype(float)
                        if len(series) < 4:
                            continue
                        split = max(1, len(series) * 2 // 3)
                        early_avg = np.mean(series[:split])
                        late_avg = np.mean(series[split:])
                        peak_in_late = np.argmax(series) >= split

                        if early_avg < 1:
                            pattern = '新词飙升'
                        elif late_avg > early_avg * 2 and peak_in_late:
                            pattern = '近日飙升'
                        elif late_avg > early_avg * 1.3:
                            pattern = '持续上升'
                        else:
                            pattern = '平稳'

                        spike_results[q] = pattern

        except Exception as e:
            print(f"  验证失败: {e}")

        if bi < len(batches) - 1:
            time.sleep(interval + random.uniform(0, 2))

    combined['趋势'] = combined['query'].map(lambda q: spike_results.get(q, ''))
    return combined, spike_results


def send_rising_feishu(webhook_url, combined, spike_results, failed):
    """发送爆增词飞书通知"""
    import pandas as pd
    now = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")
    total = len(combined)
    sources = combined['keyword'].nunique()
    new_count = sum(1 for v in spike_results.values() if v == '新词飙升')
    spike_count = sum(1 for v in spike_results.values() if v == '近日飙升')

    combined['_sort'] = pd.to_numeric(combined['value'], errors='coerce')
    max_v = combined['_sort'].max()
    combined.loc[combined['_sort'].isna(), '_sort'] = (max_v * 2) if pd.notna(max_v) else 999999
    top = combined.nlargest(15, '_sort')

    content_lines = [
        [{"tag": "text", "text": f"📅 {now}\n共找到 {total} 个上升词，来自 {sources} 个词根"}],
    ]
    if new_count > 0 or spike_count > 0:
        summary = ""
        if new_count > 0:
            summary += f"✨ {new_count} 个新词飙升  "
        if spike_count > 0:
            summary += f"🔥 {spike_count} 个近日飙升"
        content_lines.append([{"tag": "text", "text": summary}])

    content_lines.append([{"tag": "text", "text": "\n📊 Top 15 爆增词:"}])

    for _, row in top.iterrows():
        val = row['value']
        growth = f'+{val}%' if str(val).isdigit() else '飙升'
        trend_tag = ''
        if row.get('趋势') == '新词飙升':
            trend_tag = ' ✨新词'
        elif row.get('趋势') == '近日飙升':
            trend_tag = ' 🔥飙升'
        content_lines.append([
            {"tag": "text", "text": f"{row['query']}  ({growth}){trend_tag}  ← {row['keyword']}"},
        ])

    combined.drop(columns=['_sort'], inplace=True)

    if failed:
        content_lines.append([{"tag": "text", "text": f"\n⚠️ {len(failed)} 个词查询失败"}])

    payload = {
        "msg_type": "post",
        "content": {"post": {"zh_cn": {
            "title": "🔍 爆增词追踪报告",
            "content": content_lines,
        }}}
    }
    resp = http_requests.post(webhook_url, json=payload, timeout=10)
    if resp.status_code == 200:
        print("✅ 飞书通知发送成功")
    else:
        print(f"❌ 飞书通知失败: {resp.status_code} {resp.text}")


# ── Sitemap 监控 ─────────────────────────────────────────────
SITEMAP_DIR = Path(__file__).parent / "output" / "sitemaps"


def fetch_sitemap(url):
    """下载 sitemap XML 内容"""
    resp = http_requests.get(url, timeout=(10, 30), headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    resp.raise_for_status()
    return resp.text


MAX_SUB_SITEMAPS = 20  # 最多展开20个子 sitemap，防止内存爆炸
# 大站优化：JSON 缓存超过此大小(字节)时，先对比索引再决定是否全量展开
LARGE_SITE_THRESHOLD = 1 * 1024 * 1024  # 1MB

import hashlib


def _get_sub_sitemap_locs(xml_content):
    """从 sitemapindex XML 提取子 sitemap URL 列表（不展开）"""
    root = ET.fromstring(xml_content)
    ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    if 'sitemapindex' not in root.tag:
        return None  # 不是 sitemapindex
    locs = []
    for loc in root.findall('.//ns:sitemap/ns:loc', ns):
        if loc.text:
            locs.append(loc.text.strip())
    return locs


def _parse_single_sitemap(xml_content):
    """解析单个 sitemap（不递归展开），返回 URL 集合"""
    root = ET.fromstring(xml_content)
    ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    urls = set()
    for loc in root.findall('.//ns:url/ns:loc', ns):
        if loc.text:
            urls.add(loc.text.strip())
    return urls


def parse_sitemap_urls(xml_content, follow_index=True):
    """从 sitemap XML 中提取所有 URL，支持 sitemapindex 自动展开子 sitemap（最多20个）"""
    root = ET.fromstring(xml_content)
    ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    urls = set()
    for loc in root.findall('.//ns:url/ns:loc', ns):
        if loc.text:
            urls.add(loc.text.strip())
    # sitemapindex: 展开子 sitemap（限制数量）
    if follow_index and 'sitemapindex' in root.tag:
        sub_locs = root.findall('.//ns:sitemap/ns:loc', ns)
        for loc in sub_locs[:MAX_SUB_SITEMAPS]:
            if loc.text:
                try:
                    sub_resp = http_requests.get(loc.text.strip(), timeout=(10, 30), headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    })
                    if sub_resp.status_code == 200:
                        sub_urls = _parse_single_sitemap(sub_resp.text)
                        urls.update(sub_urls)
                except Exception:
                    pass
    return urls


def _load_index_hash(domain):
    """加载上次保存的 sitemapindex 哈希"""
    meta_file = SITEMAP_DIR / f"{domain}.meta.json"
    if meta_file.exists():
        meta = json.loads(meta_file.read_text(encoding='utf-8'))
        return meta.get("index_hash", "")
    return ""


def _save_index_hash(domain, index_hash):
    """保存 sitemapindex 哈希"""
    meta_file = SITEMAP_DIR / f"{domain}.meta.json"
    meta_file.write_text(json.dumps({"index_hash": index_hash}), encoding='utf-8')


def check_sitemaps(config):
    """检查所有监控的 sitemap，返回各站点新增 URL

    大站优化：对于缓存 > 1MB 的 sitemapindex 站点，先对比索引页哈希。
    索引没变 = 子 sitemap 列表没变 = 跳过全量展开，节省大量时间和内存。
    """
    sitemap_urls = config.get("sitemap_urls", [])
    if not sitemap_urls:
        print("未配置 sitemap_urls，跳过")
        return {}

    SITEMAP_DIR.mkdir(parents=True, exist_ok=True)
    all_changes = {}

    for url in sitemap_urls:
        domain = url.split("//")[-1].split("/")[0]
        cache_file = SITEMAP_DIR / f"{domain}.json"
        is_large = cache_file.exists() and cache_file.stat().st_size > LARGE_SITE_THRESHOLD

        print(f"  📡 检查 {domain}{'（大站快速模式）' if is_large else ''}...")

        try:
            # 第一步：下载主 sitemap/sitemapindex
            new_content = fetch_sitemap(url)

            # 大站优化：如果是 sitemapindex 且缓存很大，先对比索引哈希
            if is_large:
                sub_locs = _get_sub_sitemap_locs(new_content)
                if sub_locs is not None:
                    # 是 sitemapindex，计算子 sitemap 列表的哈希
                    index_hash = hashlib.md5("\n".join(sorted(sub_locs)).encode()).hexdigest()
                    old_hash = _load_index_hash(domain)

                    if index_hash == old_hash:
                        # 索引没变，跳过全量展开
                        old_count = len(json.loads(cache_file.read_text(encoding='utf-8')))
                        print(f"    -> 索引未变化，跳过全量解析（缓存 {old_count} 个 URL）")
                        continue

                    # 索引有变化，做全量展开
                    print(f"    -> 索引有变化（{len(sub_locs)} 个子sitemap），全量解析...")
                    _save_index_hash(domain, index_hash)

            new_urls = parse_sitemap_urls(new_content)

            # 保存索引哈希（非大站首次也保存，为下次做准备）
            sub_locs = _get_sub_sitemap_locs(new_content)
            if sub_locs is not None:
                index_hash = hashlib.md5("\n".join(sorted(sub_locs)).encode()).hexdigest()
                _save_index_hash(domain, index_hash)

            # 读取上次保存的 URL 列表
            old_urls = set()
            if cache_file.exists():
                old_urls = set(json.loads(cache_file.read_text(encoding='utf-8')))

            # 对比差异
            added = new_urls - old_urls
            if added:
                all_changes[domain] = {
                    'url': url,
                    'new_urls': sorted(added),
                    'total': len(new_urls),
                    'old_total': len(old_urls),
                }
                print(f"    -> 发现 {len(added)} 个新 URL（总计 {len(old_urls)} -> {len(new_urls)}）")
            else:
                print(f"    -> 无变化（共 {len(new_urls)} 个 URL）")

            # 保存 URL 列表
            cache_file.write_text(json.dumps(sorted(new_urls), ensure_ascii=False), encoding='utf-8')

        except Exception as e:
            print(f"    -> 失败: {e}")

    return all_changes


def send_sitemap_feishu(webhook_url, all_changes):
    """推送 sitemap 变化到飞书"""
    now = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")

    total_new = sum(len(v['new_urls']) for v in all_changes.values())
    content_lines = [
        [{"tag": "text", "text": f"📅 {now}\n监控 {len(all_changes)} 个站点有更新，共 {total_new} 个新页面"}],
    ]

    for domain, info in all_changes.items():
        content_lines.append([{"tag": "text", "text": f"\n🌐 {domain}（{info['old_total']} -> {info['total']}）:"}])
        for u in info['new_urls'][:20]:
            content_lines.append([{"tag": "text", "text": f"  {u}"}])
        if len(info['new_urls']) > 20:
            content_lines.append([{"tag": "text", "text": f"  ...等共 {len(info['new_urls'])} 个新 URL"}])

    payload = {
        "msg_type": "post",
        "content": {"post": {"zh_cn": {
            "title": "🗺 Sitemap 监控报告",
            "content": content_lines,
        }}}
    }

    try:
        resp = http_requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code == 200:
            print("✅ 飞书 Sitemap 通知发送成功")
        else:
            print(f"❌ 飞书通知失败: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"❌ 飞书通知异常: {e}")


# ── 保存结果 ──────────────────────────────────────────────────
def save_trending_csv(all_results):
    import pandas as pd
    output_dir = Path(__file__).parent / "output"
    output_dir.mkdir(exist_ok=True)
    filename = f"trending_{datetime.now(BEIJING_TZ).strftime('%Y%m%d_%H%M')}.csv"
    filepath = output_dir / filename
    df = pd.DataFrame(all_results)
    df.to_csv(filepath, index=False, encoding='utf-8-sig')
    print(f"结果已保存: {filepath}")
    return filepath


# ── 主流程 ────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['trending', 'rising', 'sitemap'], default='trending',
                        help='trending=时下流行, rising=爆增词追踪, sitemap=Sitemap监控')
    args = parser.parse_args()

    now_bj = datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
    print("=" * 50)
    print(f"🔥 Google Trends 数据采集 - {now_bj} (北京时间)")
    print(f"   模式: {args.mode}")
    print("=" * 50)

    config = load_config()
    webhook = config.get("notify", {}).get("feishu_webhook", "")

    if args.mode == 'trending':
        # 时下流行 - 采集所有地区
        print(f"\n📡 开始采集 {len(ALL_REGIONS)} 个地区的时下流行...")
        all_results = fetch_all_trending(config)

        if all_results:
            save_trending_csv(all_results)
            if webhook:
                print("\n📮 发送飞书通知...")
                send_trending_feishu(webhook, all_results)
            print(f"\n✅ 完成! 共 {len(all_results)} 条有效热搜")
        else:
            print("❌ 未获取到任何数据")

    elif args.mode == 'rising':
        # 爆增词追踪
        print("\n📡 开始抓取 rising queries...")
        all_rising, failed = fetch_rising_queries(config)

        if not all_rising:
            print("❌ 未获取到任何数据")
            return

        print("\n📊 分析趋势...")
        combined, spike_results = analyze_spikes(all_rising, config)

        if webhook:
            print("\n📮 发送飞书通知...")
            send_rising_feishu(webhook, combined, spike_results, failed)

        print(f"\n✅ 完成! 共 {len(combined)} 个上升词")

    elif args.mode == 'sitemap':
        # Sitemap 监控
        sitemap_urls = config.get("sitemap_urls", [])
        print(f"\n🗺 开始检查 {len(sitemap_urls)} 个 Sitemap...")
        all_changes = check_sitemaps(config)

        if all_changes:
            total_new = sum(len(v['new_urls']) for v in all_changes.values())
            if webhook:
                print("\n📮 发送飞书通知...")
                send_sitemap_feishu(webhook, all_changes)
            print(f"\n✅ 完成! {len(all_changes)} 个站点有更新，共 {total_new} 个新 URL")
        else:
            print("✅ 所有站点无变化")


if __name__ == "__main__":
    main()
