"""
定时抓取 Google Trends 数据并推送通知

使用方式:
  1. 复制 config_example.json 为 config.json，填入你的配置
  2. 手动运行: python scheduled_run.py
  3. 定时任务 (北京时间):
     crontab -e 添加:
     0 4 * * *    /opt/pytrends-git/.venv/bin/python /opt/pytrends-git/scheduled_run.py --mode rising        # 04:00 爆增词追踪 ⚡Trends API ~06:30结束
     30 6 * * *   /opt/pytrends-git/.venv/bin/python /opt/pytrends-git/scheduled_run.py --mode trending      # 06:30 时下流行（RSS，不用Trends）
     0 7,17 * * * /opt/pytrends-git/.venv/bin/python /opt/pytrends-git/scheduled_run.py --mode sitemap       # 07:00/17:00 Sitemap 监控
     0 9 * * *    /opt/pytrends-git/.venv/bin/python /opt/pytrends-git/scheduled_run.py --mode twitter       # 09:00 Twitter 监控
     0 10,22 * * * /opt/pytrends-git/.venv/bin/python /opt/pytrends-git/scheduled_run.py --mode ai_monitor   # 10:00/22:00 AI 平台监控
     0 11 * * *   /opt/pytrends-git/.venv/bin/python /opt/pytrends-git/scheduled_run.py --mode domain        # 11:00 域名淘金 ⚡Trends API ~12:30结束

  ⚠️ 调度注意：rising(04:00) 和 domain(11:00) 都用 Google Trends API，不能同时运行！
     当前已错开 4.5 小时（rising ~06:30结束，domain 11:00开始），互不影响。
     其他模式（trending/sitemap/twitter/ai_monitor）不用 Trends API，可以随意并行。

支持模式:
  --mode trending    采集所有地区时下流行 (默认)
  --mode rising      采集关键词爆增词
  --mode sitemap     监控竞品 Sitemap 变化
  --mode twitter     监控 Twitter 账号动态
  --mode ai_monitor  监控 AI 平台动态 (HuggingFace/arXiv/ProductHunt/GitHub/HackerNews)
  --mode domain      域名淘金：拉取新注册域名 → 过滤 → Trends 验证 → 推飞书
"""

import json
import time
import random
import re
import os
import sys
import argparse
import xml.etree.ElementTree as ET
import requests as http_requests
import numpy as np
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
# 累计阈值：同一话题连续3天都出现且累计 approx_traffic > 2000 也入选
CUMULATIVE_DAYS = 3
CUMULATIVE_THRESHOLD = 2000

# 历史记录文件（用于跨天累计判断）
HISTORY_PATH = Path(__file__).parent / "output" / "trending_history.json"

# 默认过滤词库（可通过 config.json 的 exclude_categories 和 exclude_words 覆盖）
DEFAULT_EXCLUDE_CATEGORIES = {
    "赌博": ["casino", "gambling", "gamble", "bet ", "betting", "slot machine", "poker", "roulette",
            "blackjack", "lottery", "jackpot", "wager", "sportsbook", "ranking kasyn", "plcasino"],
    "人名/明星": ["wife", "husband", "boyfriend", "girlfriend", "married", "dating",
                 "net worth", "birthday", "born", "died", "death", "funeral", "obituary",
                 "son of", "daughter of", "who is", "how old"],
    "体育": ["nba", "nfl", "nhl", "mlb", "fifa", "ufc", "boxing", "wrestling",
            "playoff", "championship", "score", "highlights", "roster", "standings",
            "draft pick", "super bowl", "world cup", "premier league", "la liga",
            "serie a", "bundesliga", "vs ", " vs"],
    "娱乐/影视": ["movie", "trailer", "episode", "season finale", "netflix", "hulu",
                 "disney+", "box office", "premiere", "concert", "tour dates",
                 "album release", "grammy", "oscar", "emmy", "movie review",
                 "collection day", "box office collection"],
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
    """计算某个话题在 CUMULATIVE_DAYS 天内的累计流量，要求连续每天都出现"""
    records = history.get(key, [])
    if len(records) < CUMULATIVE_DAYS:
        return 0  # 不满足连续天数要求
    # 检查是否连续 CUMULATIVE_DAYS 天都有记录
    dates = sorted(set(r["date"] for r in records))
    if len(dates) < CUMULATIVE_DAYS:
        return 0
    # 取最近 CUMULATIVE_DAYS 个日期，检查是否连续
    recent_dates = dates[-CUMULATIVE_DAYS:]
    for i in range(1, len(recent_dates)):
        d1 = datetime.strptime(recent_dates[i - 1], "%Y-%m-%d")
        d2 = datetime.strptime(recent_dates[i], "%Y-%m-%d")
        if (d2 - d1).days != 1:
            return 0  # 日期不连续
    return sum(r["traffic"] for r in records)


# ── 时下流行采集（所有地区）─────────────────────────────────────
def fetch_all_trending(config=None):
    """采集所有地区的时下流行，返回汇总数据

    入选条件（满足任一即可）：
      1. 单次 approx_traffic > TRAFFIC_THRESHOLD (1000)
      2. 同一话题连续 CUMULATIVE_DAYS (3) 天出现且累计 approx_traffic > CUMULATIVE_THRESHOLD (2000)
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
        [{"tag": "text", "text": f"📅 {now}\n共采集 {len(ALL_REGIONS)} 个地区，找到 {len(all_results)} 条热搜（单次>{TRAFFIC_THRESHOLD} 或 连续{CUMULATIVE_DAYS}天>{CUMULATIVE_THRESHOLD}）"}],
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
    interval = config.get("request_interval", 50)

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
                if '429' in str(e):
                    retry_count += 1
                    wait = 60 + retry_count * 60 + random.randint(0, 10)
                    old_interval = effective_interval
                    effective_interval = min(effective_interval * 1.5, 300)
                    print(f"  ⚠️ 429 限流，等待 {wait}s 后重试 ({retry_count}/3)，后续间隔 {old_interval:.0f}s → {effective_interval:.0f}s")
                    time.sleep(wait)
                    pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)
                else:
                    print(f"  ❌ 失败: {e}")
                    failed.append(kw)
                    break
        else:
            failed.append(kw)
            print(f"  ❌ 重试耗尽，跳过")

        if i < len(kw_list) - 1:
            # 每10个词休息2分钟，避免触发限流
            if (i + 1) % 10 == 0:
                print(f"  ⏸ 已完成 {i+1}/{len(kw_list)}，休息2分钟避免限流...")
                time.sleep(2 * 60)
            else:
                time.sleep(effective_interval + random.uniform(0, 2))

    return all_rising, failed


def analyze_spikes(all_rising, config):
    """分析趋势，判断新词/近日飙升"""
    import numpy as np
    import pandas as pd

    if not all_rising:
        return pd.DataFrame(), {}

    combined = pd.concat(all_rising, ignore_index=True)
    # 过滤掉排除关键词（复用 trending 的过滤规则）
    combined = combined[~combined['query'].apply(lambda q: is_excluded(q, config))].reset_index(drop=True)
    combined['value_num'] = pd.to_numeric(combined['value'], errors='coerce')

    spike_top_n = config.get("spike_top_n", 20)
    temp = combined.copy()
    max_v = temp['value_num'].max()
    temp.loc[temp['value_num'].isna(), 'value_num'] = (max_v * 2) if pd.notna(max_v) else 999999
    top_queries = temp.nlargest(spike_top_n, 'value_num')['query'].unique().tolist()

    geo = config.get("geo", "")
    cat = config.get("category", 0)
    interval = config.get("request_interval", 50)
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
                if not old_urls:
                    # 首次检测该站点，只保存基准数据，不推送通知（避免全量推送噪音）
                    print(f"    -> 首次检测，保存基准 {len(new_urls)} 个 URL（不推送通知）")
                else:
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


# ── AI 平台监控 ─────────────────────────────────────────────────
AI_MONITOR_CACHE_DIR = Path(__file__).parent / "output" / "ai_monitor"

# 平台名称映射
AI_PLATFORM_NAMES = {
    "huggingface": "Hugging Face",
    "arxiv": "arXiv",
    "producthunt": "Product Hunt",
    "github": "GitHub Trending",
    "hackernews": "Hacker News",
}
AI_PLATFORM_ICONS = {
    "huggingface": "\U0001F917",
    "arxiv": "\U0001F4C4",
    "producthunt": "\U0001F680",
    "github": "\U0001F4BB",
    "hackernews": "\U0001F4F0",
}


def _load_ai_cache(platform):
    """加载平台缓存的已见 ID 集合"""
    cache_file = AI_MONITOR_CACHE_DIR / f"{platform}.json"
    if cache_file.exists():
        return set(json.loads(cache_file.read_text(encoding="utf-8")))
    return set()


def _save_ai_cache(platform, seen_ids):
    """保存已见 ID，保留最近 500 条"""
    AI_MONITOR_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = AI_MONITOR_CACHE_DIR / f"{platform}.json"
    trimmed = sorted(seen_ids)[-500:]
    cache_file.write_text(json.dumps(trimmed, ensure_ascii=False), encoding="utf-8")


def _ai_filter(text, keywords):
    """关键词过滤：text 中包含任一 keyword 则返回 True"""
    if not keywords:
        return True
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in keywords)


def _fetch_huggingface(ai_config):
    """采集 Hugging Face 热门模型/Spaces"""
    limit = ai_config.get("huggingface_limit", 30)
    print(f"  📡 Hugging Face（热门模型）...")
    try:
        resp = http_requests.get("https://huggingface.co/api/trending",
                                 timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        resp.raise_for_status()
        data = resp.json()

        # API 返回 recentlyTrending 列表
        items_raw = data if isinstance(data, list) else data.get("recentlyTrending", data.get("models", []))
        seen = _load_ai_cache("huggingface")
        new_items = []

        for item in items_raw[:limit]:
            repo_id = item.get("repoData", {}).get("id", "") or item.get("id", "")
            if not repo_id or repo_id in seen:
                continue
            author = item.get("repoData", {}).get("author", "") or repo_id.split("/")[0] if "/" in repo_id else ""
            likes = item.get("repoData", {}).get("likes", 0) or item.get("likes", 0)
            repo_type = item.get("repoType", "model")
            new_items.append({
                "id": repo_id,
                "title": repo_id,
                "author": author,
                "likes": likes,
                "type": repo_type,
            })
            seen.add(repo_id)

        _save_ai_cache("huggingface", seen)
        print(f"    -> {len(new_items)} 个新项目")
        return new_items
    except Exception as e:
        print(f"    -> 失败: {e}")
        return []


def _fetch_arxiv(ai_config):
    """采集 arXiv AI 相关论文"""
    categories = ai_config.get("arxiv_categories", ["cs.AI", "cs.CL", "cs.LG"])
    print(f"  📡 arXiv（{', '.join(categories)}）...")
    seen = _load_ai_cache("arxiv")
    new_items = []

    for cat in categories:
        try:
            rss_url = f"http://export.arxiv.org/rss/{cat}"
            resp = http_requests.get(rss_url, timeout=15,
                                     headers={'User-Agent': 'Mozilla/5.0'})
            resp.raise_for_status()
            root = ET.fromstring(resp.content)

            # RSS 2.0 格式
            ns = {'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                  'dc': 'http://purl.org/dc/elements/1.1/'}
            for item in root.iter('item'):
                title_el = item.find('title')
                link_el = item.find('link')
                creator_el = item.find('dc:creator', ns)

                title = title_el.text.strip() if title_el is not None and title_el.text else ""
                link = link_el.text.strip() if link_el is not None and link_el.text else ""
                authors = creator_el.text.strip() if creator_el is not None and creator_el.text else ""

                # 清理标题中的换行和多余空格
                title = " ".join(title.split())
                # 去掉 arXiv 标题末尾的 (arXiv:xxxx.xxxxx vN [cs.XX])
                if title.startswith("(") or not title:
                    continue

                paper_id = link if link else title
                if paper_id in seen:
                    continue

                new_items.append({
                    "id": paper_id,
                    "title": title,
                    "authors": authors[:80],  # 截断过长作者列表
                    "category": cat,
                    "url": link,
                })
                seen.add(paper_id)

        except Exception as e:
            print(f"    -> {cat} 失败: {e}")

        if cat != categories[-1]:
            time.sleep(3)  # arXiv 要求间隔 3 秒

    _save_ai_cache("arxiv", seen)
    print(f"    -> {len(new_items)} 篇新论文")
    return new_items


def _fetch_producthunt(ai_config):
    """采集 Product Hunt 最新产品（RSS）"""
    filter_kw = ai_config.get("filter_keywords", [])
    print(f"  📡 Product Hunt...")
    try:
        resp = http_requests.get("https://www.producthunt.com/feed",
                                 timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        resp.raise_for_status()
        root = ET.fromstring(resp.content)

        seen = _load_ai_cache("producthunt")
        new_items = []

        for item in root.iter('item'):
            title_el = item.find('title')
            link_el = item.find('link')
            desc_el = item.find('description')

            title = title_el.text.strip() if title_el is not None and title_el.text else ""
            link = link_el.text.strip() if link_el is not None and link_el.text else ""
            desc = desc_el.text.strip() if desc_el is not None and desc_el.text else ""

            if not title or link in seen:
                continue

            # 关键词过滤：标题或描述中包含 AI 相关词
            if not _ai_filter(f"{title} {desc}", filter_kw):
                continue

            new_items.append({
                "id": link,
                "title": title,
                "url": link,
                "tagline": desc[:120] if desc else "",
            })
            seen.add(link)

        _save_ai_cache("producthunt", seen)
        print(f"    -> {len(new_items)} 个新产品")
        return new_items
    except Exception as e:
        print(f"    -> 失败: {e}")
        return []


def _fetch_github_trending(ai_config):
    """采集 GitHub Trending 项目"""
    languages = ai_config.get("github_languages", [])
    filter_kw = ai_config.get("filter_keywords", [])
    print(f"  📡 GitHub Trending...")
    try:
        from lxml import html as lxml_html

        seen = _load_ai_cache("github")
        new_items = []

        # 采集总榜，如果配了语言再逐语言采集
        urls = [("", "https://github.com/trending?since=daily")]
        for lang in languages[:4]:
            urls.append((lang, f"https://github.com/trending/{lang.lower()}?since=daily"))

        collected_repos = set()
        for lang_label, url in urls:
            try:
                resp = http_requests.get(url, timeout=15, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })
                if resp.status_code != 200:
                    continue
                tree = lxml_html.fromstring(resp.text)
                articles = tree.cssselect('article.Box-row')

                for art in articles:
                    # 仓库名
                    h2 = art.cssselect('h2 a')
                    if not h2:
                        continue
                    repo_path = h2[0].get('href', '').strip('/')
                    if not repo_path or repo_path in collected_repos:
                        continue
                    collected_repos.add(repo_path)

                    # 描述
                    desc_el = art.cssselect('p')
                    desc = desc_el[0].text_content().strip() if desc_el else ""

                    # 语言
                    lang_el = art.cssselect('[itemprop="programmingLanguage"]')
                    language = lang_el[0].text_content().strip() if lang_el else ""

                    # 今日星标
                    star_els = art.cssselect('span.d-inline-block.float-sm-right')
                    today_stars = ""
                    if star_els:
                        today_stars = star_els[0].text_content().strip()

                    if repo_path in seen:
                        continue

                    # 关键词过滤
                    if not _ai_filter(f"{repo_path} {desc}", filter_kw):
                        continue

                    new_items.append({
                        "id": repo_path,
                        "repo": repo_path,
                        "description": desc[:150],
                        "language": language or lang_label,
                        "today_stars": today_stars,
                    })
                    seen.add(repo_path)

            except Exception as e:
                print(f"    -> {lang_label or '总榜'} 失败: {e}")

            time.sleep(2)

        _save_ai_cache("github", seen)
        print(f"    -> {len(new_items)} 个新项目")
        return new_items
    except ImportError:
        print(f"    -> 需要 lxml 库: pip install lxml")
        return []
    except Exception as e:
        print(f"    -> 失败: {e}")
        return []


def _fetch_hackernews(ai_config):
    """采集 Hacker News AI 相关热帖"""
    limit = ai_config.get("hackernews_limit", 50)
    filter_kw = ai_config.get("filter_keywords", [])
    print(f"  📡 Hacker News（Top {limit}）...")
    try:
        resp = http_requests.get("https://hacker-news.firebaseio.com/v0/topstories.json",
                                 timeout=15)
        resp.raise_for_status()
        story_ids = resp.json()[:limit]

        seen = _load_ai_cache("hackernews")
        new_items = []

        for i, sid in enumerate(story_ids):
            sid_str = str(sid)
            if sid_str in seen:
                continue
            try:
                item_resp = http_requests.get(
                    f"https://hacker-news.firebaseio.com/v0/item/{sid}.json",
                    timeout=10)
                item_resp.raise_for_status()
                item = item_resp.json()
                if not item:
                    continue

                title = item.get("title", "")
                url = item.get("url", "")
                score = item.get("score", 0)
                descendants = item.get("descendants", 0)

                # 关键词过滤
                if not _ai_filter(f"{title} {url}", filter_kw):
                    seen.add(sid_str)  # 标记已看过，避免反复请求
                    continue

                new_items.append({
                    "id": sid_str,
                    "title": title,
                    "url": url or f"https://news.ycombinator.com/item?id={sid}",
                    "score": score,
                    "comments": descendants,
                })
                seen.add(sid_str)

            except Exception:
                continue

            # 每 10 个请求休息 1 秒
            if (i + 1) % 10 == 0:
                time.sleep(1)

        _save_ai_cache("hackernews", seen)
        print(f"    -> {len(new_items)} 条新帖")
        return new_items
    except Exception as e:
        print(f"    -> 失败: {e}")
        return []


# 平台采集函数映射
_AI_FETCHERS = {
    "huggingface": _fetch_huggingface,
    "arxiv": _fetch_arxiv,
    "producthunt": _fetch_producthunt,
    "github": _fetch_github_trending,
    "hackernews": _fetch_hackernews,
}


def fetch_ai_monitor(config):
    """采集所有启用的 AI 平台，返回 {platform: [items]}"""
    ai_config = config.get("ai_monitor", {})
    enabled = ai_config.get("enabled_platforms",
                            ["huggingface", "arxiv", "producthunt", "github", "hackernews"])
    AI_MONITOR_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    all_results = {}

    for platform in enabled:
        fetcher = _AI_FETCHERS.get(platform)
        if not fetcher:
            print(f"  ⚠️ 未知平台: {platform}")
            continue
        items = fetcher(ai_config)
        if items:
            all_results[platform] = items
        time.sleep(2)  # 平台之间间隔

    return all_results


def send_ai_monitor_feishu(webhook_url, all_results):
    """推送 AI 平台监控结果到飞书"""
    now = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")
    total = sum(len(v) for v in all_results.values())
    platform_count = len(all_results)

    content_lines = [
        [{"tag": "text", "text": f"📅 {now}\n{platform_count} 个平台有新动态，共 {total} 条"}],
    ]

    for platform, items in all_results.items():
        icon = AI_PLATFORM_ICONS.get(platform, "")
        name = AI_PLATFORM_NAMES.get(platform, platform)
        content_lines.append([{"tag": "text", "text": f"\n{icon} {name}（{len(items)} 条新内容）:"}])

        for item in items[:15]:
            if platform == "huggingface":
                line = f"  {item['title']}  ({item['type']}, {item['likes']} likes)"
            elif platform == "arxiv":
                line = f"  {item['title'][:80]}  [{item['category']}]"
            elif platform == "producthunt":
                line = f"  {item['title']}  {item.get('tagline', '')[:60]}"
            elif platform == "github":
                line = f"  {item['repo']}  {item.get('today_stars', '')}  [{item.get('language', '')}]"
            elif platform == "hackernews":
                line = f"  {item['title'][:80]}  ({item['score']} pts, {item['comments']} comments)"
            else:
                line = f"  {item.get('title', '')}"
            content_lines.append([{"tag": "text", "text": line}])

        if len(items) > 15:
            content_lines.append([{"tag": "text", "text": f"  ...等共 {len(items)} 条"}])

    payload = {
        "msg_type": "post",
        "content": {"post": {"zh_cn": {
            "title": "🤖 AI 平台监控报告",
            "content": content_lines,
        }}}
    }

    try:
        resp = http_requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code == 200:
            print("✅ 飞书 AI 平台通知发送成功")
        else:
            print(f"❌ 飞书通知失败: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"❌ 飞书通知异常: {e}")


# ── Twitter 监控 ────────────────────────────────────────────────
TWITTER_API_HOST = "twitter241.p.rapidapi.com"
TWITTER_CACHE_DIR = Path(__file__).parent / "output" / "twitter"


def _twitter_headers(api_key):
    return {
        "x-rapidapi-host": TWITTER_API_HOST,
        "x-rapidapi-key": api_key,
    }


def _get_twitter_user_id(username, api_key):
    """通过 username 获取 Twitter 用户 rest_id"""
    url = f"https://{TWITTER_API_HOST}/user"
    resp = http_requests.get(url, params={"username": username},
                             headers=_twitter_headers(api_key), timeout=15)
    resp.raise_for_status()
    data = resp.json()
    # 尝试多种返回结构
    result = data.get("result", data)
    if "data" in result:
        result = result["data"]
    if "user" in result:
        result = result["user"]
    if "result" in result:
        result = result["result"]
    return result.get("rest_id", "")


def _get_twitter_user_tweets(user_id, api_key, count=20):
    """获取用户最新推文"""
    url = f"https://{TWITTER_API_HOST}/user-tweets"
    resp = http_requests.get(url, params={"user": user_id, "count": str(count)},
                             headers=_twitter_headers(api_key), timeout=15)
    if resp.status_code == 429:
        raise Exception("429 Too Many Requests")
    resp.raise_for_status()
    return resp.json()


def _extract_tweets(raw_data):
    """从 API 返回结构中提取推文列表 [{text, created_at, tweet_id}, ...]"""
    tweets = []
    # 遍历 timeline 指令找到推文条目
    instructions = (raw_data.get("result", raw_data)
                    .get("timeline", {})
                    .get("timeline", {})
                    .get("instructions", []))
    for inst in instructions:
        entries = inst.get("entries", [])
        for entry in entries:
            try:
                tweet_results = (entry.get("content", {})
                                 .get("itemContent", {})
                                 .get("tweet_results", {})
                                 .get("result", {}))
                if not tweet_results:
                    continue
                legacy = tweet_results.get("legacy", {})
                # 优先取 note_tweet（长推文），否则取 legacy.full_text
                note = (tweet_results.get("note_tweet", {})
                        .get("note_tweet_results", {})
                        .get("result", {})
                        .get("text", ""))
                text = note if note else legacy.get("full_text", "")
                created_at = legacy.get("created_at", "")
                tweet_id = legacy.get("id_str", entry.get("entryId", ""))
                if text:
                    tweets.append({
                        "text": text,
                        "created_at": created_at,
                        "tweet_id": tweet_id,
                    })
            except (KeyError, TypeError, AttributeError):
                continue
    return tweets


def _load_seen_tweets(username):
    """加载已推送过的推文 ID 集合"""
    cache_file = TWITTER_CACHE_DIR / f"{username}.json"
    if cache_file.exists():
        return set(json.loads(cache_file.read_text(encoding="utf-8")))
    return set()


def _save_seen_tweets(username, seen_ids):
    """保存已推送过的推文 ID（只保留最近 200 条）"""
    TWITTER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = TWITTER_CACHE_DIR / f"{username}.json"
    trimmed = sorted(seen_ids)[-200:]
    cache_file.write_text(json.dumps(trimmed, ensure_ascii=False), encoding="utf-8")


def fetch_twitter(config):
    """采集所有监控账号的最新推文，返回新推文汇总"""
    tw_config = config.get("twitter", {})
    api_key = tw_config.get("rapidapi_key", "")
    accounts = tw_config.get("accounts", [])
    max_tweets = tw_config.get("max_tweets_per_account", 20)
    filter_kw = [kw.lower() for kw in tw_config.get("filter_keywords", [])]

    if not api_key:
        print("❌ 未配置 twitter.rapidapi_key，跳过")
        return {}
    if not accounts:
        print("❌ 未配置 twitter.accounts，跳过")
        return {}

    TWITTER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    all_new_tweets = {}  # {username: [tweet, ...]}

    for username in accounts:
        print(f"  📡 采集 @{username}...")
        retry_count = 0
        success = False

        while retry_count < 3 and not success:
            try:
                user_id = _get_twitter_user_id(username, api_key)
                if not user_id:
                    print(f"    -> 未找到用户")
                    success = True
                    break

                time.sleep(3)  # user 和 tweets 之间间隔

                raw = _get_twitter_user_tweets(user_id, api_key, count=max_tweets)
                tweets = _extract_tweets(raw)
                seen = _load_seen_tweets(username)

                new_tweets = []
                for tw in tweets:
                    if tw["tweet_id"] in seen:
                        continue
                    if filter_kw:
                        text_lower = tw["text"].lower()
                        if not any(kw in text_lower for kw in filter_kw):
                            continue
                    new_tweets.append(tw)
                    seen.add(tw["tweet_id"])

                _save_seen_tweets(username, seen)

                if new_tweets:
                    all_new_tweets[username] = new_tweets
                    print(f"    -> {len(new_tweets)} 条新推文")
                else:
                    print(f"    -> 无新推文")
                success = True

            except Exception as e:
                if '429' in str(e):
                    retry_count += 1
                    wait = 15 + retry_count * 15
                    print(f"    ⚠️ 429 限流，等待 {wait}s 后重试 ({retry_count}/3)")
                    time.sleep(wait)
                else:
                    print(f"    -> 失败: {e}")
                    break

        if not success and retry_count >= 3:
            print(f"    ❌ 重试耗尽，跳过")

        time.sleep(120)  # 账号之间间隔 120 秒

    return all_new_tweets


def send_twitter_feishu(webhook_url, all_new_tweets):
    """推送 Twitter 监控结果到飞书"""
    now = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")
    total = sum(len(v) for v in all_new_tweets.values())

    content_lines = [
        [{"tag": "text", "text": f"📅 {now}\n监控 {len(all_new_tweets)} 个账号有新动态，共 {total} 条推文"}],
    ]

    for username, tweets in all_new_tweets.items():
        content_lines.append([{"tag": "text", "text": f"\n🐦 @{username}（{len(tweets)} 条新推文）:"}])
        for tw in tweets[:10]:
            # 截取前 120 字符，避免消息过长
            snippet = tw["text"].replace("\n", " ")
            if len(snippet) > 120:
                snippet = snippet[:120] + "..."
            time_str = ""
            if tw.get("created_at"):
                time_str = f" [{tw['created_at'][:16]}]"
            content_lines.append([{"tag": "text", "text": f"  {snippet}{time_str}"}])
        if len(tweets) > 10:
            content_lines.append([{"tag": "text", "text": f"  ...等共 {len(tweets)} 条"}])

    payload = {
        "msg_type": "post",
        "content": {"post": {"zh_cn": {
            "title": "🐦 Twitter 监控报告",
            "content": content_lines,
        }}}
    }

    try:
        resp = http_requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code == 200:
            print("✅ 飞书 Twitter 通知发送成功")
        else:
            print(f"❌ 飞书通知失败: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"❌ 飞书通知异常: {e}")


# ── 域名淘金 ─────────────────────────────────────────────────
DOMAIN_CACHE_DIR = Path(__file__).parent / "output" / "domains"

# 垃圾行业词黑名单
DOMAIN_BLACKLIST = [
    # 成人类
    "porn", "xxx", "sex", "sexy", "escort", "adult", "nude", "camgirl", "onlyfans",
    # 博彩类
    "casino", "bet", "bets", "betting", "poker", "slot", "slots", "gamble", "gambling", "lottery",
    # 贷款/灰产类
    "loan", "loans", "payday", "creditrepair", "cashadvance", "debt",
    # 仿牌/低质电商类
    "cheapnike", "cheapadidas", "replica", "fakebrand", "fakewatch", "discountbags",
    # 加密空投垃圾类
    "airdrop", "cryptoairdrop", "freecrypto", "claimtoken", "walletclaim",
    # 停车页/域名交易类
    "forsale", "buydomain", "premiumdomain", "domainmarket", "domainsale",
]


def _is_valid_tld(domain):
    """只保留 .com 和 .ai"""
    d = domain.lower().strip()
    return d.endswith(".com") or d.endswith(".ai")


def _extract_domain_body(domain):
    """提取域名主体（去 TLD）"""
    d = domain.lower().strip()
    if d.endswith(".com"):
        return d[:-4]
    elif d.endswith(".ai"):
        return d[:-3]
    return d


def _is_random_string(body):
    """判断是否为随机字符串"""
    clean = body.replace("-", "")
    if not clean:
        return True
    if len(clean) > 25 and "-" not in body:
        return True
    vowels = set("aeiou")
    consonant_run = 0
    for ch in clean:
        if ch.isalpha() and ch not in vowels:
            consonant_run += 1
            if consonant_run >= 6:
                return True
        else:
            consonant_run = 0
    digit_count = sum(1 for c in clean if c.isdigit())
    if len(clean) > 0 and digit_count / len(clean) > 0.4:
        return True
    if len(clean) > 8:
        alpha_chars = [c for c in clean if c.isalpha()]
        if alpha_chars:
            consonants = sum(1 for c in alpha_chars if c not in vowels)
            if consonants / len(alpha_chars) > 0.85:
                return True
    return False


def _filter_domains(domains):
    """完整域名过滤流水线，返回 (通过列表, 统计字典)"""
    stats = {"input": len(domains), "tld": 0, "digits": 0, "special": 0, "blacklist": 0, "random": 0}
    result = []

    for d in domains:
        d = d.strip()
        if not d:
            continue
        # 1. TLD
        if not _is_valid_tld(d):
            stats["tld"] += 1
            continue
        body = _extract_domain_body(d)
        # 2. 数字
        if re.search(r'\d', body):
            stats["digits"] += 1
            continue
        # 3. 特殊字符
        if re.search(r'[^a-z\-]', body):
            stats["special"] += 1
            continue
        # 4. 黑名单
        if any(word in body for word in DOMAIN_BLACKLIST):
            stats["blacklist"] += 1
            continue
        # 5. 随机串
        if _is_random_string(body):
            stats["random"] += 1
            continue
        result.append(d)

    stats["passed"] = len(result)
    return result, stats


def _download_whoisds(date_str=None):
    """从 WhoisDS 下载指定日期的新注册域名 ZIP，解压后返回域名列表

    WhoisDS 免费提供每日新注册域名列表:
      https://whoisds.com/newly-registered-domains/{date}/nrd
      日期格式: YYYY-MM-DD

    ZIP 里包含一个 .txt 文件，每行一个域名。
    数据通常在次日凌晨可用，所以默认拉昨天的。
    """
    import zipfile
    import io

    if not date_str:
        # 默认拉昨天的数据（WhoisDS 当天数据通常还没生成）
        yesterday = datetime.now(BEIJING_TZ) - timedelta(days=1)
        date_str = yesterday.strftime("%Y-%m-%d")

    # WhoisDS 的 URL 需要 base64 编码的日期
    import base64
    date_b64 = base64.b64encode(date_str.encode()).decode()
    url = f"https://whoisds.com/whois-database/newly-registered-domains/{date_b64}/nrd"

    print(f"  📡 从 WhoisDS 下载 {date_str} 新注册域名...")
    print(f"     URL: {url}")

    resp = http_requests.get(url, timeout=120, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/zip, application/octet-stream, */*',
        'Referer': 'https://whoisds.com/newly-registered-domains',
    }, allow_redirects=True)

    if resp.status_code != 200:
        raise Exception(f"HTTP {resp.status_code}")

    content_type = resp.headers.get('Content-Type', '')

    # 检查是否返回了 ZIP
    if b'PK' not in resp.content[:4] and 'zip' not in content_type.lower():
        # 可能返回了 HTML 页面（数据还没准备好）
        if b'<html' in resp.content[:200].lower():
            raise Exception(f"{date_str} 的数据暂未发布，WhoisDS 返回了 HTML 页面")
        raise Exception(f"返回非 ZIP 内容 (Content-Type: {content_type})")

    # 解压
    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    domains = []
    for name in zf.namelist():
        if name.endswith('.txt') or name.endswith('.csv'):
            text = zf.read(name).decode('utf-8', errors='ignore')
            for line in text.splitlines():
                d = line.strip()
                if d and not d.startswith('#'):
                    domains.append(d)

    print(f"  ✅ 下载完成: {len(domains)} 个域名（{date_str}）")

    # 保存原始备份
    DOMAIN_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    backup = DOMAIN_CACHE_DIR / f"whoisds_{date_str}.txt"
    backup.write_text("\n".join(domains), encoding="utf-8")
    print(f"  💾 备份: {backup}")

    return domains


def _fetch_domain_list(domain_config):
    """获取新注册域名列表

    优先级:
      1. auto_download: true → 自动从 WhoisDS 下载（免费，推荐）
      2. domain_file: 本地文件路径
      3. domain_url: 远程 URL 下载
      4. domain_dir: 目录，读取最新的 .txt 文件
    """
    domains = []

    # 来源0: 自动从 WhoisDS 下载（推荐）
    auto_download = domain_config.get("auto_download", True)
    if auto_download:
        try:
            domains = _download_whoisds()
        except Exception as e:
            print(f"  ⚠️ WhoisDS 自动下载失败: {e}")
            # 尝试前天的数据作为备选
            try:
                day_before = (datetime.now(BEIJING_TZ) - timedelta(days=2)).strftime("%Y-%m-%d")
                print(f"  🔄 尝试前天 ({day_before}) 的数据...")
                domains = _download_whoisds(day_before)
            except Exception as e2:
                print(f"  ⚠️ 前天数据也失败: {e2}")

        # 如果已有今天的本地备份，直接用缓存（避免重复下载）
        if not domains:
            yesterday = (datetime.now(BEIJING_TZ) - timedelta(days=1)).strftime("%Y-%m-%d")
            cache = DOMAIN_CACHE_DIR / f"whoisds_{yesterday}.txt"
            if cache.exists():
                text = cache.read_text(encoding="utf-8", errors="ignore")
                domains = [line.strip() for line in text.splitlines() if line.strip()]
                print(f"  📂 使用本地缓存: {cache.name}（{len(domains)} 个域名）")

    # 来源1: 本地文件
    domain_file = domain_config.get("domain_file", "")
    if domain_file and not domains:
        p = Path(domain_file)
        if p.exists():
            text = p.read_text(encoding="utf-8", errors="ignore")
            domains = [line.strip() for line in text.splitlines() if line.strip()]
            print(f"  📂 从文件加载: {p.name}（{len(domains)} 个域名）")
        else:
            print(f"  ⚠️ 文件不存在: {domain_file}")

    # 来源2: 远程 URL
    domain_url = domain_config.get("domain_url", "")
    if domain_url and not domains:
        try:
            print(f"  📡 从 URL 下载域名列表...")
            resp = http_requests.get(domain_url, timeout=60, headers={
                'User-Agent': 'Mozilla/5.0'})
            resp.raise_for_status()
            text = resp.text
            domains = [line.strip() for line in text.splitlines() if line.strip()]
            print(f"  ✅ 下载完成: {len(domains)} 个域名")

            DOMAIN_CACHE_DIR.mkdir(parents=True, exist_ok=True)
            today = datetime.now(BEIJING_TZ).strftime('%Y%m%d')
            backup = DOMAIN_CACHE_DIR / f"raw_{today}.txt"
            backup.write_text("\n".join(domains), encoding="utf-8")
        except Exception as e:
            print(f"  ❌ 下载失败: {e}")

    # 来源3: 目录中最新文件
    domain_dir = domain_config.get("domain_dir", "")
    if domain_dir and not domains:
        p = Path(domain_dir)
        if p.is_dir():
            txt_files = sorted(p.glob("*.txt"), key=lambda f: f.stat().st_mtime, reverse=True)
            if txt_files:
                latest = txt_files[0]
                text = latest.read_text(encoding="utf-8", errors="ignore")
                domains = [line.strip() for line in text.splitlines() if line.strip()]
                print(f"  📂 从目录加载最新文件: {latest.name}（{len(domains)} 个域名）")
            else:
                print(f"  ⚠️ 目录中没有 .txt 文件: {domain_dir}")

    return domains


def _trends_validate_domains(keywords, config):
    """用 Google Trends 验证域名关键词，返回 {keyword: {has_trend, avg, max, growth, trend}}"""
    domain_config = config.get("domain_mining", {})
    timeframe = domain_config.get("trends_timeframe", "today 15-d")
    interval = domain_config.get("trends_interval", 60)
    batch_size = min(5, domain_config.get("trends_batch_size", 5))

    batches = [keywords[i:i+batch_size] for i in range(0, len(keywords), batch_size)]
    total_batches = len(batches)
    effective_interval = interval

    pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)
    trends_data = {}

    for bi, batch in enumerate(batches):
        print(f"  📈 Trends [{bi+1}/{total_batches}]: {', '.join(batch[:3])}{'...' if len(batch)>3 else ''}")

        retry_count = 0
        while retry_count < 3:
            try:
                pytrend.build_payload(kw_list=batch, timeframe=timeframe, geo='')
                iot = pytrend.interest_over_time()

                if not iot.empty:
                    for kw in batch:
                        if kw in iot.columns:
                            series = iot[kw].values.astype(float)
                            avg_val = np.mean(series)
                            max_val = np.max(series)
                            if len(series) >= 3:
                                split = max(1, len(series) // 3)
                                early = np.mean(series[:split])
                                late = np.mean(series[-split:])
                                if early > 0:
                                    growth = (late - early) / early * 100
                                elif late > 0:
                                    growth = 999
                                else:
                                    growth = 0
                            else:
                                growth = 0
                            trends_data[kw] = {
                                "has_trend": avg_val > 0,
                                "avg": round(avg_val, 1),
                                "max": round(max_val, 1),
                                "growth": round(growth, 1),
                                "trend": series.tolist(),
                            }
                        else:
                            trends_data[kw] = {"has_trend": False, "avg": 0, "max": 0, "growth": 0, "trend": []}
                else:
                    for kw in batch:
                        trends_data[kw] = {"has_trend": False, "avg": 0, "max": 0, "growth": 0, "trend": []}
                break

            except TooManyRequestsError:
                retry_count += 1
                wait = 60 + retry_count * 60 + random.randint(0, 10)
                effective_interval = min(effective_interval * 1.5, 300)
                print(f"    ⚠️ 429 限流，等待 {wait}s ({retry_count}/3)，后续间隔→{effective_interval:.0f}s")
                time.sleep(wait)
                pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)
            except Exception as e:
                if '429' in str(e):
                    retry_count += 1
                    wait = 60 + retry_count * 60 + random.randint(0, 10)
                    effective_interval = min(effective_interval * 1.5, 300)
                    print(f"    ⚠️ 429 限流，等待 {wait}s ({retry_count}/3)")
                    time.sleep(wait)
                    pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)
                else:
                    print(f"    ❌ 查询失败: {e}")
                    for kw in batch:
                        if kw not in trends_data:
                            trends_data[kw] = {"has_trend": False, "avg": 0, "max": 0, "growth": 0, "trend": []}
                    break

        if bi < total_batches - 1:
            if (bi + 1) % 5 == 0:
                print(f"    ⏸ 已完成 {bi+1}/{total_batches} 批，休息3分钟...")
                time.sleep(3 * 60)
            else:
                time.sleep(effective_interval + random.uniform(0, 3))

    return trends_data


def fetch_and_filter_domains(config):
    """完整域名淘金流水线：拉取 → 过滤 → Trends 验证"""
    domain_config = config.get("domain_mining", {})
    if not domain_config:
        print("❌ 未配置 domain_mining，跳过")
        return [], [], {}

    # Step 1: 拉取域名
    print("\n📥 拉取新注册域名...")
    raw_domains = _fetch_domain_list(domain_config)
    if not raw_domains:
        print("❌ 未获取到域名数据")
        return [], [], {}

    # Step 2: 过滤
    print(f"\n🔍 过滤域名（{len(raw_domains)} 个输入）...")
    filtered, stats = _filter_domains(raw_domains)
    print(f"  过滤统计:")
    print(f"    输入: {stats['input']}")
    print(f"    非 .com/.ai: -{stats['tld']}")
    print(f"    含数字: -{stats['digits']}")
    print(f"    含特殊字符: -{stats['special']}")
    print(f"    垃圾词命中: -{stats['blacklist']}")
    print(f"    随机字符串: -{stats['random']}")
    print(f"    ✅ 通过: {stats['passed']}")

    if not filtered:
        print("❌ 过滤后无有效域名")
        return [], [], stats

    # Step 3: Trends 验证
    max_trends = domain_config.get("max_trends_check", 200)
    to_check = filtered[:max_trends]
    keywords = list(dict.fromkeys([_extract_domain_body(d) for d in to_check]))  # 去重保序
    print(f"\n📈 Google Trends 验证（{len(keywords)} 个关键词）...")
    trends_data = _trends_validate_domains(keywords, config)

    # 分类结果
    growing = []
    has_volume = []
    for domain in to_check:
        kw = _extract_domain_body(domain)
        info = trends_data.get(kw, {})
        item = {"domain": domain, "keyword": kw, **info}
        if info.get("growth", 0) > 20:
            growing.append(item)
        elif info.get("has_trend", False):
            has_volume.append(item)

    growing.sort(key=lambda x: x.get("growth", 0), reverse=True)
    has_volume.sort(key=lambda x: x.get("avg", 0), reverse=True)

    print(f"\n📊 结果:")
    print(f"  🚀 搜索量增长: {len(growing)}")
    print(f"  📊 有搜索量: {len(has_volume)}")
    print(f"  ❌ 无搜索量: {len(to_check) - len(growing) - len(has_volume)}")

    # 保存结果 CSV
    DOMAIN_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(BEIJING_TZ).strftime('%Y%m%d')
    if growing:
        import csv
        csv_path = DOMAIN_CACHE_DIR / f"growing_{today}.csv"
        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["domain", "keyword", "avg", "max", "growth"])
            writer.writeheader()
            for item in growing:
                writer.writerow({k: item.get(k, "") for k in ["domain", "keyword", "avg", "max", "growth"]})
        print(f"  💾 已保存: {csv_path}")

    return growing, has_volume, stats


def send_domain_feishu(webhook_url, growing, has_volume, stats):
    """推送域名淘金结果到飞书"""
    now = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")

    summary = (f"📅 {now}\n"
               f"输入 {stats.get('input', 0)} 个域名 → 过滤后 {stats.get('passed', 0)} 个\n"
               f"🚀 {len(growing)} 个搜索量增长  📊 {len(has_volume)} 个有搜索量")

    content_lines = [[{"tag": "text", "text": summary}]]

    if growing:
        content_lines.append([{"tag": "text", "text": "\n🚀 搜索量增长（重点关注）:"}])
        for item in growing[:30]:
            growth_str = f"+{item['growth']:.0f}%" if item.get('growth', 0) < 999 else "新词飙升"
            content_lines.append([{"tag": "text",
                                    "text": f"  {item['domain']}  ({growth_str}, 均值{item.get('avg', 0)})"}])

    if has_volume:
        content_lines.append([{"tag": "text", "text": f"\n📊 有搜索量（{len(has_volume)} 个，前10）:"}])
        for item in has_volume[:10]:
            content_lines.append([{"tag": "text",
                                    "text": f"  {item['domain']}  (均值{item.get('avg', 0)}, 增长{item.get('growth', 0):.0f}%)"}])

    payload = {
        "msg_type": "post",
        "content": {"post": {"zh_cn": {
            "title": "🌐 域名淘金报告",
            "content": content_lines,
        }}}
    }

    try:
        resp = http_requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code == 200:
            print("✅ 飞书域名淘金通知发送成功")
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
    parser.add_argument('--mode', choices=['trending', 'rising', 'sitemap', 'twitter', 'ai_monitor', 'domain'], default='trending',
                        help='trending=时下流行, rising=爆增词追踪, sitemap=Sitemap监控, twitter=Twitter监控, ai_monitor=AI平台监控, domain=域名淘金')
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

    elif args.mode == 'twitter':
        # Twitter 监控
        tw_accounts = config.get("twitter", {}).get("accounts", [])
        print(f"\n🐦 开始监控 {len(tw_accounts)} 个 Twitter 账号...")
        all_new_tweets = fetch_twitter(config)

        if all_new_tweets:
            total = sum(len(v) for v in all_new_tweets.values())
            if webhook:
                print("\n📮 发送飞书通知...")
                send_twitter_feishu(webhook, all_new_tweets)
            print(f"\n✅ 完成! {len(all_new_tweets)} 个账号有新动态，共 {total} 条推文")
        else:
            print("✅ 所有账号无新推文")

    elif args.mode == 'ai_monitor':
        # AI 平台监控
        enabled = config.get("ai_monitor", {}).get("enabled_platforms", [])
        print(f"\n🤖 开始监控 {len(enabled)} 个 AI 平台...")
        all_results = fetch_ai_monitor(config)

        if all_results:
            total = sum(len(v) for v in all_results.values())
            if webhook:
                print("\n📮 发送飞书通知...")
                send_ai_monitor_feishu(webhook, all_results)
            print(f"\n✅ 完成! {len(all_results)} 个平台有新内容，共 {total} 条")
        else:
            print("✅ 所有平台无新内容")

    elif args.mode == 'domain':
        # 域名淘金
        print("\n🌐 开始域名淘金...")
        growing, has_volume, stats = fetch_and_filter_domains(config)

        if growing or has_volume:
            if webhook:
                print("\n📮 发送飞书通知...")
                send_domain_feishu(webhook, growing, has_volume, stats)
            print(f"\n✅ 完成! {len(growing)} 个增长域名, {len(has_volume)} 个有搜索量")
        else:
            print("❌ 未找到有价值的域名")


if __name__ == "__main__":
    main()
