"""
定时抓取 Google Trends 数据并推送通知

使用方式:
  1. 复制 config_example.json 为 config.json，填入你的配置
  2. 手动运行: python scheduled_run.py
  3. 定时任务 (北京时间早上7点 = UTC 23:00):
     crontab -e 添加:
     0 23 * * * /opt/pytrends-git/.venv/bin/python /opt/pytrends-git/scheduled_run.py --mode trending
     0 1 * * * /opt/pytrends-git/.venv/bin/python /opt/pytrends-git/scheduled_run.py --mode rising

支持模式:
  --mode trending  采集所有地区时下流行 (默认)
  --mode rising    采集关键词爆增词
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

# 过滤词库
EXCLUDE_CATEGORIES = {
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


def get_all_exclude_words():
    all_words = []
    for words in EXCLUDE_CATEGORIES.values():
        all_words.extend(words)
    return all_words


def is_excluded(text):
    text_lower = text.lower()
    for ex in get_all_exclude_words():
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


# ── 时下流行采集（所有地区）─────────────────────────────────────
def fetch_all_trending():
    """采集所有地区的时下流行，返回汇总数据"""
    all_results = []

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

                    # 过滤
                    if is_excluded(title):
                        continue

                    # 搜索量 > 5000
                    traffic_num = parse_traffic(traffic)
                    if traffic_num <= 5000:
                        continue

                    all_results.append({
                        'title': title,
                        'traffic': traffic,
                        'traffic_num': traffic_num,
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

    return all_results


# ── 飞书通知（统一发送）────────────────────────────────────────
def send_trending_feishu(webhook_url, all_results):
    """所有地区汇总后统一发一条飞书消息"""
    now = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")

    # 按搜索量排序
    all_results.sort(key=lambda x: x['traffic_num'], reverse=True)

    # 按地区分组统计
    region_counts = {}
    for r in all_results:
        region_counts[r['region']] = region_counts.get(r['region'], 0) + 1

    # 汇总信息
    content_lines = [
        [{"tag": "text", "text": f"📅 {now}\n共采集 {len(ALL_REGIONS)} 个地区，找到 {len(all_results)} 条热搜（搜索量>5000）"}],
        [{"tag": "text", "text": "\n".join([f"  {k}: {v}条" for k, v in region_counts.items()])}],
        [{"tag": "text", "text": "\n📊 全球 Top 20 热搜:"}],
    ]

    # Top 20
    for item in all_results[:20]:
        line = f"{item['title']}  ({item['traffic']})  [{item['region']}]"
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

    # 逐条发送 Top 10 详情
    for item in all_results[:10]:
        lines = [
            "⚠ 监测到热门话题",
            "",
            f"话题: {item['title']}",
            f"搜索量: {item['traffic']}",
            f"地区: {item['region']}",
        ]
        detail_payload = {
            "msg_type": "post",
            "content": {"post": {"zh_cn": {
                "title": f"📊 {item['title'][:30]}",
                "content": [[{"tag": "text", "text": "\n".join(lines)}]],
            }}}
        }
        try:
            http_requests.post(webhook_url, json=detail_payload, timeout=10)
            time.sleep(0.5)
        except Exception:
            pass


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
                wait = 60 + retry_count * 30 + random.randint(0, 10)
                print(f"  ⚠️ 429 限流，等待 {wait}s 后重试 ({retry_count}/3)")
                time.sleep(wait)
                pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)

            except ResponseError as e:
                retry_count += 1
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
            time.sleep(interval + random.uniform(0, 2))

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
    parser.add_argument('--mode', choices=['trending', 'rising'], default='trending',
                        help='trending=时下流行, rising=爆增词追踪')
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
        all_results = fetch_all_trending()

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


if __name__ == "__main__":
    main()
