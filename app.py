import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import time
import random
import json
import re
import requests as http_requests
from datetime import datetime, timezone, timedelta

# 北京时间 UTC+8
BEIJING_TZ = timezone(timedelta(hours=8))
from pathlib import Path
from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError, ResponseError

# ── 加载配置 ─────────────────────────────────────────────────
def load_full_config():
    """加载完整配置文件"""
    config_path = Path(__file__).parent / "config.json"
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

CONFIG_PATH = Path(__file__).parent / "config.json"
APP_CONFIG = load_full_config()


def save_config(config):
    """保存配置到 config.json"""
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=4)

# 默认过滤词库（config.json 未配置时使用）
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

# 从 config.json 读取过滤分类，未配置则用默认值
EXCLUDE_CATEGORIES = APP_CONFIG.get("exclude_categories", DEFAULT_EXCLUDE_CATEGORIES)

def get_all_exclude_words(custom_excludes=""):
    """合并所有过滤词（config 分类 + 额外排除词）"""
    all_words = []
    for words in EXCLUDE_CATEGORIES.values():
        all_words.extend(words)
    # config.json 中的额外排除词
    for w in APP_CONFIG.get("exclude_words", []):
        if w.strip():
            all_words.append(w.strip().lower())
    # UI 输入的额外排除词
    if custom_excludes.strip():
        all_words.extend([w.strip().lower() for w in custom_excludes.split(",") if w.strip()])
    return all_words

def filter_results(df, query_col, custom_excludes=""):
    """过滤不适合的结果"""
    if df.empty:
        return df
    exclude_list = get_all_exclude_words(custom_excludes)
    mask = ~df[query_col].str.lower().apply(
        lambda q: any(ex in q for ex in exclude_list)
    )
    return df[mask].reset_index(drop=True)

# ── 飞书通知 ──────────────────────────────────────────────────
def load_notify_config():
    config_path = Path(__file__).parent / "config.json"
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        return cfg.get("notify", {})
    return {}

def send_feishu_notify(combined, spike_results=None, title="🔥 热点关键词趋势报告"):
    """查询完成后发送飞书通知，列表格式一条消息"""
    notify = load_notify_config()
    webhook = notify.get("feishu_webhook", "")
    if not webhook:
        return False

    now = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")
    total = len(combined)
    sources = combined['keyword'].nunique() if 'keyword' in combined.columns else 0

    # 排序
    df = combined.copy()
    if 'value' in df.columns:
        df['_sort'] = pd.to_numeric(df['value'], errors='coerce')
        max_v = df['_sort'].max()
        df.loc[df['_sort'].isna(), '_sort'] = (max_v * 2) if pd.notna(max_v) else 999999
        top = df.nlargest(30, '_sort')
    else:
        top = df.head(30)

    # 汇总头部
    summary = f"📅 {now}\n"
    if sources > 0:
        summary += f"共找到 {total} 个上升词，来自 {sources} 个词根"
    else:
        summary += f"共找到 {total} 条结果"

    if spike_results:
        new_count = sum(1 for v in spike_results.values() if v.get('pattern') == '新词飙升')
        spike_count = sum(1 for v in spike_results.values() if v.get('pattern') == '近日飙升')
        if new_count > 0:
            summary += f"\n✨ {new_count} 个新词飙升"
        if spike_count > 0:
            summary += f"  🔥 {spike_count} 个近日飙升"

    content_lines = [
        [{"tag": "text", "text": summary}],
    ]

    # 列表格式，每行一条
    for _, row in top.iterrows():
        parts = []

        # 查询词/话题
        query = str(row.get('query', row.get('title', '')))
        parts.append(query)

        # 增长率/搜索量
        if 'value' in row:
            val = row['value']
            growth = f'+{val}%' if str(val).isdigit() else '飙升'
            parts.append(f"({growth})")
        if 'formattedTraffic' in row and row['formattedTraffic']:
            parts.append(f"({row['formattedTraffic']})")

        # 趋势标记
        if '趋势' in row and row['趋势']:
            tag_map = {'新词飙升': '✨新词', '近日飙升': '🔥飙升', '持续上升': '📈上升'}
            tag = tag_map.get(row['趋势'], '')
            if tag:
                parts.append(tag)

        # 来源
        if 'keyword' in row:
            parts.append(f"← {row['keyword']}")

        content_lines.append([{"tag": "text", "text": "  ".join(parts)}])

    payload = {
        "msg_type": "post",
        "content": {"post": {"zh_cn": {
            "title": title,
            "content": content_lines,
        }}}
    }
    try:
        resp = http_requests.post(webhook, json=payload, timeout=10)
        return resp.status_code == 200
    except Exception:
        return False


# ════════════════════════════════════════════════════════════════
# 页面配置
# ════════════════════════════════════════════════════════════════
st.set_page_config(page_title="热点关键词趋势追踪", page_icon="🔥", layout="wide")

st.markdown("""
<style>
#MainMenu {visibility: hidden;}
header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ── 左侧边栏设置 ──────────────────────────────────────────────
with st.sidebar:
    st.header("⚙ 查询设置")

    track_freq = st.selectbox("追踪频率", [
        ("每天 1 次", "daily_1"),
        ("每天 2 次", "daily_2"),
        ("每周 1 次", "weekly_1"),
        ("每周 2 次", "weekly_2"),
        ("每周 3 次", "weekly_3"),
        ("仅手动执行", "manual"),
    ], format_func=lambda x: x[0], index=0)

    freq = track_freq[1]
    if freq != 'manual':
        if freq == 'daily_2':
            st.markdown("**执行时间**")
            run_time_1 = st.time_input("第 1 次", value=pd.Timestamp("08:00").time(), key="t1")
            run_time_2 = st.time_input("第 2 次", value=pd.Timestamp("20:00").time(), key="t2")
            st.caption("建议早晚各一次，如 8:00 和 20:00")
        elif freq == 'daily_1':
            run_time = st.selectbox("执行时间", [
                ("06:00", "06:00"),
                ("07:00", "07:00"),
                ("08:00", "08:00"),
                ("09:00", "09:00"),
                ("10:00", "10:00"),
            ], format_func=lambda x: x[0], index=1)
            st.caption("建议 6:00-8:00，数据更新及时")
        elif freq == 'weekly_1':
            run_day = st.selectbox("执行日", ["周一", "周二", "周三", "周四", "周五", "周六", "周日"], index=0)
            run_time = st.time_input("执行时间", value=pd.Timestamp("09:00").time())
        elif freq == 'weekly_2':
            st.markdown("**执行日**")
            run_day_1 = st.selectbox("第 1 天", ["周一", "周二", "周三", "周四", "周五", "周六", "周日"], index=0, key="d1")
            run_day_2 = st.selectbox("第 2 天", ["周一", "周二", "周三", "周四", "周五", "周六", "周日"], index=3, key="d2")
            run_time = st.time_input("执行时间", value=pd.Timestamp("09:00").time())
        elif freq == 'weekly_3':
            st.markdown("**执行日**")
            run_day_1 = st.selectbox("第 1 天", ["周一", "周二", "周三", "周四", "周五", "周六", "周日"], index=0, key="d1")
            run_day_2 = st.selectbox("第 2 天", ["周一", "周二", "周三", "周四", "周五", "周六", "周日"], index=2, key="d2")
            run_day_3 = st.selectbox("第 3 天", ["周一", "周二", "周三", "周四", "周五", "周六", "周日"], index=4, key="d3")
            run_time = st.time_input("执行时间", value=pd.Timestamp("09:00").time())

    st.divider()

    geo = st.selectbox("主要地区", [
        ("全球", ""),
        ("美国", "US"),
        ("中国", "CN"),
        ("日本", "JP"),
        ("英国", "GB"),
        ("德国", "DE"),
        ("法国", "FR"),
        ("韩国", "KR"),
        ("加拿大", "CA"),
        ("澳大利亚", "AU"),
        ("印度", "IN"),
        ("巴西", "BR"),
        ("新加坡", "SG"),
        ("中国香港", "HK"),
        ("中国台湾", "TW"),
    ], format_func=lambda x: x[0], index=0)

    category = st.selectbox("主要分类", [
        ("所有分类", 0),
        ("商业与工业", 12),
        ("计算机与电子产品", 5),
        ("互联网与电信", 13),
        ("购物", 18),
        ("健康", 45),
        ("新闻", 16),
        ("游戏", 8),
        ("金融", 7),
        ("食品与饮料", 71),
        ("旅行与交通", 67),
        ("体育", 20),
        ("娱乐", 3),
        ("科学", 174),
        ("教育", 958),
    ], format_func=lambda x: x[0], index=0)

    timeframe = st.selectbox("时间范围", [
        ("过去 7 天", "now 7-d"),
        ("过去 1 天", "now 1-d"),
        ("过去 30 天", "today 1-m"),
        ("过去 90 天", "today 3-m"),
        ("过去 12 个月", "today 12-m"),
    ], format_func=lambda x: x[0], index=0)

    request_interval = st.slider("请求间隔（秒）", min_value=2, max_value=300, value=60,
                                  help="每次请求之间的基础等待时间，触发429后自动加大")

    st.divider()

    # 额外排除词（config.json 中的 exclude_words 会自动加载）
    config_exclude_str = ", ".join(APP_CONFIG.get("exclude_words", []))
    exclude_words = st.text_input("额外排除词（逗号分隔）",
                                   value=config_exclude_str,
                                   help="除内置过滤分类外，额外排除的词")

    if st.button("💾 保存排除词", key="save_exclude"):
        new_words = [w.strip() for w in exclude_words.split(",") if w.strip()]
        APP_CONFIG["exclude_words"] = new_words
        save_config(APP_CONFIG)
        st.success("已保存")
        st.rerun()

    category_names = " / ".join(EXCLUDE_CATEGORIES.keys())
    st.markdown("**内置过滤分类：**")
    st.caption(f"{category_names}")

    st.divider()

    spike_check = st.checkbox("验证近日突然飙升", value=True,
                               help="对 Top 爆增词二次查询趋势曲线，判断是近几天突然飙升还是持续增长")
    spike_top_n = st.slider("验证词数量", min_value=5, max_value=50, value=20,
                             help="对增长最多的前 N 个词做趋势验证") if spike_check else 0

    st.divider()
    st.markdown("**限流说明**")
    st.markdown(
        "- 每次请求自动加入随机延迟\n"
        "- 触发 429 时自动指数退避重试\n"
        "- 超过频率上限时暂停等待\n\n"
        "如遇持续报错，请适当减小每分钟请求数或等待片刻后重试。"
    )

# ════════════════════════════════════════════════════════════════
# 主区域 - 标签页
# ════════════════════════════════════════════════════════════════
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["🔍 爆增词追踪", "🔥 时下流行", "🗺 Sitemap 监控", "🐦 Twitter 监控", "🤖 AI 平台监控", "🌐 域名淘金"])

# ════════════════════════════════════════════════════════════════
# Tab 1: 爆增词追踪
# ════════════════════════════════════════════════════════════════
with tab1:
    st.title("🔍 热点关键词趋势追踪")
    st.caption("输入一批关键词，自动查询每个词在 Google Trends 上近期增长最快的相关搜索词")

    # 从 config.json 读取关键词，未配置则用默认值
    config_keywords = APP_CONFIG.get("keywords", [
        "Translate", "Generator", "Example", "Convert", "Online", "Downloader",
        "Maker", "Creator", "Editor", "Processor", "Designer", "Compiler", "Analyzer",
        "Evaluator", "Sender", "Receiver", "Interpreter", "Uploader", "Calculator",
        "Sample", "Template", "Format"
    ])
    default_keywords = ", ".join(config_keywords)

    keywords_input = st.text_area(
        "输入关键词（用逗号或换行分隔）",
        value=default_keywords,
        height=100,
        label_visibility="collapsed",
        placeholder="输入关键词，用逗号或换行分隔..."
    )

    kw_list = [kw.strip() for kw in re.split(r'[,\n]+', keywords_input) if kw.strip()]
    total_kw = len(kw_list)

    col_save_kw, col_start = st.columns([1, 3])
    with col_save_kw:
        if st.button("💾 保存词根", key="save_keywords"):
            APP_CONFIG["keywords"] = kw_list
            save_config(APP_CONFIG)
            st.success("已保存")
            st.rerun()

    if total_kw > 0:
        effective_interval = request_interval
        est_time = total_kw * effective_interval / 60
        st.info(f"共 **{total_kw}** 个词根，预计用时约 **{est_time:.1f}** 分钟（间隔 {effective_interval:.0f} 秒）")

    with col_start:
        start = st.button("🔍 开始追踪", type="primary", use_container_width=True)

    if start and total_kw > 0:
        all_rising = []
        failed_kw = []
        effective_interval = request_interval

        progress_bar = st.progress(0, text="准备开始...")
        status_area = st.empty()

        pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)

        for i, kw in enumerate(kw_list):
            progress_bar.progress(i / total_kw, text=f"正在查询: **{kw}** ({i+1}/{total_kw})")

            retry_count = 0
            max_retries = 3

            while retry_count < max_retries:
                try:
                    pytrend.build_payload(
                        kw_list=[kw], cat=category[1],
                        timeframe=timeframe[1], geo=geo[1],
                    )
                    result = pytrend.related_queries()
                    if kw in result and result[kw]['rising'] is not None:
                        rising_df = result[kw]['rising'].copy()
                        rising_df['keyword'] = kw
                        all_rising.append(rising_df)
                    break

                except TooManyRequestsError:
                    retry_count += 1
                    wait = 60 + retry_count * 60 + random.randint(0, 10)
                    old_interval = effective_interval
                    effective_interval = min(effective_interval * 1.5, 300)
                    status_area.warning(f"⚠️ 触发限流 (429)，等待 {wait} 秒后重试... ({retry_count}/{max_retries})，后续间隔 {old_interval:.0f}s → {effective_interval:.0f}s")
                    time.sleep(wait)
                    pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)

                except ResponseError as e:
                    retry_count += 1
                    wait = 30 + retry_count * 15
                    if '429' in str(e):
                        old_interval = effective_interval
                        effective_interval = min(effective_interval * 1.5, 300)
                        wait = 60 + retry_count * 60 + random.randint(0, 10)
                        status_area.warning(f"⚠️ 触发限流 (429)，等待 {wait} 秒后重试... ({retry_count}/{max_retries})，后续间隔 {old_interval:.0f}s → {effective_interval:.0f}s")
                    else:
                        status_area.warning(f"⚠️ 请求出错: {e}，等待 {wait} 秒后重试... ({retry_count}/{max_retries})")
                    time.sleep(wait)

                except Exception as e:
                    if '429' in str(e):
                        retry_count += 1
                        wait = 60 + retry_count * 60 + random.randint(0, 10)
                        old_interval = effective_interval
                        effective_interval = min(effective_interval * 1.5, 300)
                        status_area.warning(f"⚠️ 触发限流 (429)，等待 {wait} 秒后重试... ({retry_count}/{max_retries})，后续间隔 {old_interval:.0f}s → {effective_interval:.0f}s")
                        time.sleep(wait)
                        pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)
                    else:
                        status_area.error(f"❌ 查询 '{kw}' 失败: {e}")
                        failed_kw.append(kw)
                        break
            else:
                failed_kw.append(kw)
                status_area.error(f"❌ '{kw}' 重试 {max_retries} 次后仍失败，跳过")

            if i < total_kw - 1:
                # 每10个词休息5分钟，避免触发限流
                if (i + 1) % 10 == 0:
                    rest_min = 5
                    status_area.info(f"⏸ 已完成 {i+1}/{total_kw}，休息 {rest_min} 分钟避免限流...")
                    time.sleep(rest_min * 60)
                    status_area.empty()
                else:
                    time.sleep(effective_interval + random.uniform(0, 2))

        progress_bar.progress(1.0, text="查询完成！")
        status_area.empty()

        # 趋势验证
        spike_results = {}
        if all_rising and spike_check and spike_top_n > 0:
            temp_combined = pd.concat(all_rising, ignore_index=True)
            temp_combined['value_num'] = pd.to_numeric(temp_combined['value'], errors='coerce')
            max_v = temp_combined['value_num'].max()
            temp_combined.loc[temp_combined['value_num'].isna(), 'value_num'] = (max_v * 2) if pd.notna(max_v) else 999999
            top_queries = temp_combined.nlargest(spike_top_n, 'value_num')['query'].unique().tolist()

            batches = [top_queries[i:i+5] for i in range(0, len(top_queries), 5)]
            total_batches = len(batches)
            progress_bar.progress(0, text="正在验证趋势曲线...")

            for bi, batch in enumerate(batches):
                progress_bar.progress(bi / total_batches, text=f"验证趋势: {', '.join(batch[:3])}... ({bi+1}/{total_batches})")

                retry_count = 0
                while retry_count < 3:
                    try:
                        pytrend.build_payload(kw_list=batch, cat=category[1], timeframe='now 7-d', geo=geo[1])
                        iot = pytrend.interest_over_time()

                        if not iot.empty:
                            for q in batch:
                                if q in iot.columns:
                                    series = iot[q].values.astype(float)
                                    if len(series) < 4:
                                        continue
                                    split = max(1, len(series) * 2 // 3)
                                    early = series[:split]
                                    late = series[split:]
                                    early_avg = np.mean(early) if len(early) > 0 else 0
                                    late_avg = np.mean(late) if len(late) > 0 else 0
                                    peak_pos = np.argmax(series)
                                    peak_in_late = peak_pos >= split
                                    is_new = early_avg < 1

                                    if is_new:
                                        pattern = '新词飙升'
                                    elif late_avg > early_avg * 2 and peak_in_late:
                                        pattern = '近日飙升'
                                    elif late_avg > early_avg * 1.3:
                                        pattern = '持续上升'
                                    else:
                                        pattern = '平稳'

                                    spike_results[q] = {
                                        'pattern': pattern, 'is_new': is_new,
                                        'trend': series.tolist(),
                                    }
                        break
                    except TooManyRequestsError:
                        retry_count += 1
                        wait = 60 + retry_count * 60 + random.randint(0, 10)
                        effective_interval = min(effective_interval * 1.5, 300)
                        status_area.warning(f"⚠️ 趋势验证触发限流，等待 {wait} 秒...后续间隔 → {effective_interval:.0f}s")
                        time.sleep(wait)
                        pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)
                    except Exception as e:
                        if '429' in str(e):
                            retry_count += 1
                            wait = 60 + retry_count * 60 + random.randint(0, 10)
                            effective_interval = min(effective_interval * 1.5, 300)
                            status_area.warning(f"⚠️ 趋势验证触发限流，等待 {wait} 秒...后续间隔 → {effective_interval:.0f}s")
                            time.sleep(wait)
                            pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)
                        else:
                            break

                if bi < total_batches - 1:
                    time.sleep(effective_interval + random.uniform(0, 2))

            progress_bar.progress(1.0, text="验证完成！")

        # 结果展示
        if all_rising:
            combined = pd.concat(all_rising, ignore_index=True)

            # 智能过滤
            combined = filter_results(combined, 'query', exclude_words)

            combined['value_num'] = pd.to_numeric(combined['value'], errors='coerce')
            total_rising = len(combined)
            total_sources = combined['keyword'].nunique()

            st.divider()
            st.subheader("🚀 相关爆增词汇总（近期增长最多）")
            st.markdown(f"**共找到 {total_rising} 个上升词，来自 {total_sources} 个词根**（已过滤不相关内容）")

            if spike_results:
                combined['趋势'] = combined['query'].map(
                    lambda q: spike_results[q]['pattern'] if q in spike_results else ''
                )
                new_count = sum(1 for v in spike_results.values() if v['pattern'] == '新词飙升')
                spike_count = sum(1 for v in spike_results.values() if v['pattern'] == '近日飙升')
                parts = []
                if new_count > 0:
                    parts.append(f"**{new_count}** 个 **新词飙升** ✨")
                if spike_count > 0:
                    parts.append(f"**{spike_count}** 个 **近日飙升** 🔥")
                if parts:
                    st.markdown("其中 " + "，".join(parts))
            else:
                combined['趋势'] = ''

            # 飞书通知
            if send_feishu_notify(combined, spike_results, "🔍 爆增词追踪报告"):
                st.success("✅ 飞书通知已发送")
            else:
                st.warning("⚠️ 飞书通知发送失败，请检查 config.json 中的 webhook 配置")

            col_chart, col_table = st.columns([3, 2])

            with col_chart:
                chart_df = combined.copy()
                max_val = chart_df['value_num'].max()
                breakout_placeholder = max_val * 1.2 if pd.notna(max_val) and max_val > 0 else 100000
                chart_df.loc[chart_df['value_num'].isna(), 'value_num'] = breakout_placeholder

                top_n = chart_df.nlargest(20, 'value_num').copy()
                def make_label(row):
                    name = row['query'][:25] + '...' if len(row['query']) > 25 else row['query']
                    tag = row.get('趋势', '')
                    if tag == '新词飙升':
                        return name + ' ✨'
                    elif tag == '近日飙升':
                        return name + ' 🔥'
                    return name
                top_n['label'] = top_n.apply(make_label, axis=1)

                fig = px.bar(
                    top_n.iloc[::-1], x='value_num', y='label', orientation='h',
                    labels={'value_num': '增长幅度 (%)', 'label': '', 'keyword': '词根'},
                    color='keyword', color_discrete_sequence=px.colors.qualitative.Pastel,
                )
                fig.update_layout(
                    height=max(450, len(top_n) * 32),
                    margin=dict(l=0, r=20, t=10, b=0),
                    yaxis=dict(tickfont=dict(size=12)),
                    legend=dict(title='词根', font=dict(size=11)),
                )
                st.plotly_chart(fig, use_container_width=True)

            with col_table:
                display_df = combined[['query', 'value', 'keyword', '趋势']].copy()
                display_df['增长量'] = display_df['value'].apply(
                    lambda v: f'+{v}%' if str(v).isdigit() else '飙升'
                )
                display_df = display_df.rename(columns={'query': '爆词', 'keyword': '来源词根'})
                display_df = display_df.sort_values(
                    by='value',
                    key=lambda s: pd.to_numeric(s, errors='coerce').fillna(float('inf')),
                    ascending=False
                )
                display_df = display_df[['爆词', '增长量', '趋势', '来源词根']]
                st.dataframe(display_df, use_container_width=True, hide_index=True, height=600)

                csv = display_df.to_csv(index=False).encode('utf-8-sig')
                st.download_button("📥 下载 CSV", csv, "trending_keywords.csv", "text/csv", use_container_width=True)

            # 近日飙升词趋势曲线
            if spike_results:
                new_queries = [q for q, v in spike_results.items() if v['pattern'] == '新词飙升']
                spike_queries = [q for q, v in spike_results.items() if v['pattern'] == '近日飙升']
                hot_queries = new_queries + spike_queries

                if hot_queries:
                    st.divider()
                    st.subheader("🔥 近日突然飙升的词 — 趋势曲线")
                    st.caption("✨ 新词飙升 = 之前几乎无搜索量  🔥 近日飙升 = 已有搜索量，近日突然大幅上升")

                    cols_per_row = 3
                    for row_start in range(0, len(hot_queries), cols_per_row):
                        cols = st.columns(cols_per_row)
                        for ci, col in enumerate(cols):
                            idx = row_start + ci
                            if idx >= len(hot_queries):
                                break
                            q = hot_queries[idx]
                            info = spike_results[q]
                            trend = info['trend']
                            tag = '✨ 新词' if info['pattern'] == '新词飙升' else '🔥 飙升'
                            line_color = '#f59e0b' if info['pattern'] == '新词飙升' else '#ff4b4b'
                            with col:
                                fig_mini = go.Figure()
                                fig_mini.add_trace(go.Scatter(
                                    y=trend, mode='lines+markers',
                                    line=dict(color=line_color, width=2),
                                    marker=dict(size=3), hoverinfo='y',
                                ))
                                fig_mini.update_layout(
                                    title=dict(text=f'{tag} {q[:25]}', font=dict(size=13)),
                                    height=180, margin=dict(l=10, r=10, t=35, b=10),
                                    xaxis=dict(showticklabels=False, showgrid=False),
                                    yaxis=dict(showticklabels=False, showgrid=True, gridcolor='#f0f0f0'),
                                    plot_bgcolor='white',
                                )
                                st.plotly_chart(fig_mini, use_container_width=True)
        else:
            st.warning("未查询到任何上升趋势词。可能是关键词太冷门，或遭遇限流。请调大请求间隔后重试。")

        if failed_kw:
            st.warning(f"以下 {len(failed_kw)} 个词查询失败: {', '.join(failed_kw)}")

    elif start and total_kw == 0:
        st.error("请输入至少一个关键词")


# ════════════════════════════════════════════════════════════════
# Tab 2: 时下流行
# ════════════════════════════════════════════════════════════════
with tab2:
    st.title("🔥 时下流行趋势")
    st.caption("获取 Google Trends 实时热门搜索话题，自动过滤不适合做工具站/小游戏的内容")

    trending_geo = st.selectbox("采集地区", [
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
    ], format_func=lambda x: x[0], index=0, key="trending_geo")

    start_trending = st.button("🔥 获取时下流行", type="primary", use_container_width=True)

    if start_trending:
        with st.spinner("正在获取时下流行数据..."):
            try:
                import xml.etree.ElementTree as ET

                # 使用 Google Trends RSS feed（最稳定）
                rss_url = f"https://trends.google.com/trending/rss?geo={trending_geo[1]}"
                resp = http_requests.get(rss_url, timeout=15, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })

                if resp.status_code == 200:
                    root = ET.fromstring(resp.content)
                    items = []
                    ns = {'ht': 'https://trends.google.com/trending/rss'}

                    for item in root.iter('item'):
                        title = item.find('title')
                        traffic = item.find('ht:approx_traffic', ns)
                        news_item = item.find('ht:news_item', ns)
                        news_title = ''
                        news_url = ''
                        if news_item is not None:
                            nt = news_item.find('ht:news_item_title', ns)
                            nu = news_item.find('ht:news_item_url', ns)
                            if nt is not None:
                                news_title = nt.text or ''
                            if nu is not None:
                                news_url = nu.text or ''

                        items.append({
                            'title': title.text if title is not None else '',
                            'traffic': traffic.text if traffic is not None else '',
                            'news_title': news_title,
                            'news_url': news_url,
                        })

                    if items:
                        trending_df = pd.DataFrame(items)

                        # 过滤
                        original_count = len(trending_df)
                        trending_df = filter_results(trending_df, 'title', exclude_words)
                        filtered_count = original_count - len(trending_df)

                        st.divider()
                        st.subheader(f"📊 {trending_geo[0]} 今日热搜")
                        st.markdown(f"**共 {len(trending_df)} 个话题**（已过滤 {filtered_count} 个不相关内容）")

                        # 飞书通知（只推送搜索量 > 1000 的）
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

                        if not trending_df.empty:
                            notify_df = trending_df.copy()
                            notify_df['_traffic_num'] = notify_df['traffic'].apply(parse_traffic)
                            notify_df = notify_df[notify_df['_traffic_num'] > 1000].copy()
                            if not notify_df.empty:
                                notify_df['keyword'] = trending_geo[0]
                                notify_df['query'] = notify_df['title']
                                notify_df['formattedTraffic'] = notify_df['traffic']
                                notify_df = notify_df.drop(columns=['_traffic_num'])
                                if send_feishu_notify(notify_df, title=f"🔥 {trending_geo[0]}今日热搜"):
                                    st.success(f"✅ 飞书通知已发送（{len(notify_df)} 条搜索量>1000）")
                            else:
                                st.info("没有搜索量超过 1000 的话题，跳过飞书推送")

                        # 展示
                        for i, row in trending_df.iterrows():
                            traffic_info = f"  ({row['traffic']})" if row['traffic'] else ""
                            st.markdown(f"**{i+1}.** {row['title']}{traffic_info}")
                            if row['news_title']:
                                st.caption(f"   相关新闻: {row['news_title']}")

                        # 下载
                        csv = trending_df.to_csv(index=False).encode('utf-8-sig')
                        st.download_button("📥 下载 CSV", csv, "trending_now.csv", "text/csv", use_container_width=True)
                    else:
                        st.warning("未获取到时下流行数据，请稍后重试。")
                else:
                    st.error(f"获取失败: HTTP {resp.status_code}")
                    st.info("提示：时下流行功能需要服务器能访问 Google Trends。")

            except Exception as e:
                st.error(f"获取失败: {e}")
                st.info("提示：时下流行功能需要服务器能访问 Google Trends。如遇网络问题，请检查服务器网络。")


# ════════════════════════════════════════════════════════════════
# Tab 3: Sitemap 监控
# ════════════════════════════════════════════════════════════════
with tab3:
    st.title("🗺 Sitemap 监控")
    st.caption("监控竞品网站 Sitemap 变化，发现新页面时推送飞书通知")

    import xml.etree.ElementTree as ET
    from urllib.parse import urlparse

    SITEMAP_DIR = Path(__file__).parent / "output" / "sitemaps"
    SM_NS = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    SM_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    MAX_SUB_SITEMAPS = 20  # 最多展开20个子 sitemap，防止内存爆炸

    def parse_sitemap_all_urls(content, follow_index=True):
        """解析 sitemap，支持 sitemapindex（自动展开子 sitemap，最多展开20个）"""
        root = ET.fromstring(content)
        urls = set()
        # 直接的 <url><loc>
        for loc in root.findall('.//ns:url/ns:loc', SM_NS):
            if loc.text:
                urls.add(loc.text.strip())
        # 如果是 sitemapindex，展开子 sitemap（限制数量）
        if follow_index and 'sitemapindex' in root.tag:
            sub_locs = root.findall('.//ns:sitemap/ns:loc', SM_NS)
            for loc in sub_locs[:MAX_SUB_SITEMAPS]:
                if loc.text:
                    try:
                        sub_resp = http_requests.get(loc.text.strip(), timeout=(10, 30), headers=SM_HEADERS)
                        if sub_resp.status_code == 200:
                            sub_urls = parse_sitemap_all_urls(sub_resp.text, follow_index=False)
                            urls.update(sub_urls)
                    except Exception:
                        pass
        return urls

    # ── 分组配置 ──
    # 兼容旧配置：如果只有 sitemap_urls（旧格式），自动迁移到 sitemap_groups
    if "sitemap_groups" not in APP_CONFIG and "sitemap_urls" in APP_CONFIG:
        APP_CONFIG["sitemap_groups"] = [{
            "name": "默认分组",
            "feishu_webhook": APP_CONFIG.get("notify", {}).get("feishu_webhook", ""),
            "urls": APP_CONFIG.get("sitemap_urls", []),
        }]
        save_config(APP_CONFIG)

    sitemap_groups = APP_CONFIG.get("sitemap_groups", [])

    # ── 分组管理 ──
    st.subheader("📂 监控分组")
    st.caption("不同品类分开监控，各自推送到独立的飞书群")

    # 新建分组
    with st.expander("➕ 新建分组"):
        new_group_name = st.text_input("分组名称", placeholder="如: 游戏、SaaS 工具", key="new_group_name")
        new_group_webhook = st.text_input("飞书 Webhook（留空则用全局配置）", placeholder="https://open.feishu.cn/open-apis/bot/v2/hook/xxx", key="new_group_webhook")
        if st.button("✅ 创建分组", key="create_group"):
            if new_group_name.strip():
                sitemap_groups.append({
                    "name": new_group_name.strip(),
                    "feishu_webhook": new_group_webhook.strip(),
                    "urls": [],
                })
                APP_CONFIG["sitemap_groups"] = sitemap_groups
                save_config(APP_CONFIG)
                st.success(f"已创建分组: {new_group_name.strip()}")
                st.rerun()
            else:
                st.warning("请输入分组名称")

    if not sitemap_groups:
        st.info("暂无分组，请先创建。")
    else:
        # 分组标签页
        group_tabs = st.tabs([f"{g['name']}（{len(g.get('urls', []))}）" for g in sitemap_groups])

        for gi, (group_tab, group) in enumerate(zip(group_tabs, sitemap_groups)):
            with group_tab:
                group_urls = group.get("urls", [])
                group_webhook = group.get("feishu_webhook", "")

                # 分组设置
                col_wh, col_del_group = st.columns([5, 1])
                with col_wh:
                    edited_webhook = st.text_input("飞书 Webhook", value=group_webhook,
                                                     key=f"grp_webhook_{gi}", type="password")
                    if edited_webhook != group_webhook:
                        sitemap_groups[gi]["feishu_webhook"] = edited_webhook
                        APP_CONFIG["sitemap_groups"] = sitemap_groups
                        save_config(APP_CONFIG)
                with col_del_group:
                    st.markdown("")
                    if st.button("🗑 删除分组", key=f"del_group_{gi}"):
                        sitemap_groups.pop(gi)
                        APP_CONFIG["sitemap_groups"] = sitemap_groups
                        save_config(APP_CONFIG)
                        st.rerun()

                # 站点列表
                if group_urls:
                    for i, url in enumerate(group_urls):
                        domain = urlparse(url).netloc
                        has_cache = (SITEMAP_DIR / f"{domain}.json").exists()
                        status = "✅" if has_cache else "🆕"
                        col_url, col_del = st.columns([5, 0.8])
                        with col_url:
                            st.markdown(f"{status} `{url}`")
                        with col_del:
                            if st.button("🗑", key=f"del_sm_{gi}_{i}"):
                                group_urls.pop(i)
                                sitemap_groups[gi]["urls"] = group_urls
                                APP_CONFIG["sitemap_groups"] = sitemap_groups
                                save_config(APP_CONFIG)
                                st.rerun()
                else:
                    st.caption("暂无站点")

                # 添加站点
                new_urls_input = st.text_area("添加 Sitemap URL（每行一个）",
                                               placeholder="https://example.com/sitemap.xml",
                                               height=80, key=f"add_sm_{gi}")
                if st.button("➕ 添加", key=f"btn_add_sm_{gi}"):
                    if new_urls_input.strip():
                        added = 0
                        for line in new_urls_input.strip().splitlines():
                            url = line.strip()
                            if not url:
                                continue
                            if not url.startswith("http"):
                                url = "https://" + url
                            if url not in group_urls:
                                group_urls.append(url)
                                added += 1
                        if added > 0:
                            sitemap_groups[gi]["urls"] = group_urls
                            APP_CONFIG["sitemap_groups"] = sitemap_groups
                            save_config(APP_CONFIG)
                            st.success(f"已添加 {added} 个站点")
                            st.rerun()

                st.divider()

                # 检查按钮
                if st.button(f"🔍 检查「{group['name']}」（{len(group_urls)} 个站点）",
                              type="primary", use_container_width=True,
                              disabled=len(group_urls) == 0, key=f"check_grp_{gi}"):

                    SITEMAP_DIR.mkdir(parents=True, exist_ok=True)
                    all_changes = {}
                    progress_bar = st.progress(0, text="开始检查...")
                    total = len(group_urls)

                    for i, url in enumerate(group_urls):
                        domain = urlparse(url).netloc
                        progress_bar.progress(i / total, text=f"检查 {domain}...")
                        try:
                            resp = http_requests.get(url, timeout=(10, 30), headers=SM_HEADERS)
                            resp.raise_for_status()
                            new_urls = parse_sitemap_all_urls(resp.text)
                            cache_file = SITEMAP_DIR / f"{domain}.json"
                            old_urls = set()
                            if cache_file.exists():
                                old_urls = set(json.loads(cache_file.read_text(encoding='utf-8')))
                            added_urls = new_urls - old_urls
                            if added_urls:
                                all_changes[domain] = {
                                    'new_urls': sorted(added_urls),
                                    'total': len(new_urls),
                                    'old_total': len(old_urls),
                                }
                            cache_file.write_text(json.dumps(sorted(new_urls), ensure_ascii=False), encoding='utf-8')
                            st.caption(f"✅ {domain} — {len(new_urls)} 个 URL")
                        except Exception as e:
                            st.warning(f"⚠️ {domain} 跳过: {e}")

                    progress_bar.progress(1.0, text="检查完成！")
                    st.divider()

                    if all_changes:
                        total_new = sum(len(v['new_urls']) for v in all_changes.values())
                        st.subheader(f"🆕 发现 {total_new} 个新页面")
                        for domain, info in all_changes.items():
                            with st.expander(f"🌐 {domain}（{info['old_total']} → {info['total']}，+{len(info['new_urls'])}）", expanded=True):
                                for u in info['new_urls']:
                                    st.markdown(f"- {u}")

                        # 飞书通知（用分组自己的 webhook，没有则用全局的）
                        wh = edited_webhook or APP_CONFIG.get("notify", {}).get("feishu_webhook", "")
                        if wh:
                            now = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")
                            content_lines = [
                                [{"tag": "text", "text": f"📅 {now}\n【{group['name']}】{len(all_changes)} 个站点有更新，共 {total_new} 个新页面"}],
                            ]
                            for domain, info in all_changes.items():
                                content_lines.append([{"tag": "text", "text": f"\n🌐 {domain}（{info['old_total']} → {info['total']}）:"}])
                                for u in info['new_urls'][:20]:
                                    content_lines.append([{"tag": "text", "text": f"  {u}"}])
                                if len(info['new_urls']) > 20:
                                    content_lines.append([{"tag": "text", "text": f"  ...等共 {len(info['new_urls'])} 个新 URL"}])
                            payload = {
                                "msg_type": "post",
                                "content": {"post": {"zh_cn": {
                                    "title": f"🗺 {group['name']} Sitemap 报告",
                                    "content": content_lines,
                                }}}
                            }
                            try:
                                feishu_resp = http_requests.post(wh, json=payload, timeout=10)
                                if feishu_resp.status_code == 200:
                                    st.success("✅ 飞书通知已发送")
                                else:
                                    st.warning(f"⚠️ 飞书通知失败: {feishu_resp.status_code}")
                            except Exception:
                                st.warning("⚠️ 飞书通知发送异常")
                    else:
                        st.success("✅ 所有站点无变化")


# ════════════════════════════════════════════════════════════════
# Tab 4: Twitter 监控
# ════════════════════════════════════════════════════════════════
with tab4:
    st.title("🐦 Twitter 监控")
    st.caption("监控 Twitter 账号最新动态，通过 RapidAPI Twttr API 获取推文")

    TWITTER_API_HOST = "twitter241.p.rapidapi.com"
    TWITTER_CACHE_DIR = Path(__file__).parent / "output" / "twitter"

    tw_config = APP_CONFIG.get("twitter", {})
    tw_api_key = tw_config.get("rapidapi_key", "")
    tw_accounts = tw_config.get("accounts", ["rohanpaul_ai", "arrakis_ai", "testingcatalog"])
    tw_max_tweets = tw_config.get("max_tweets_per_account", 20)
    tw_filter_kw = tw_config.get("filter_keywords", [])

    # ── API Key 配置 ──
    st.subheader("🔑 API 配置")
    tw_key_input = st.text_input("RapidAPI Key", value=tw_api_key, type="password",
                                  placeholder="填入你的 X-RapidAPI-Key",
                                  key="tw_api_key_input")
    if st.button("💾 保存 Key", key="save_tw_key"):
        if "twitter" not in APP_CONFIG:
            APP_CONFIG["twitter"] = {}
        APP_CONFIG["twitter"]["rapidapi_key"] = tw_key_input.strip()
        save_config(APP_CONFIG)
        st.success("已保存")
        st.rerun()

    if not tw_key_input.strip():
        st.warning("⚠️ 未配置 RapidAPI Key，请在上方填入后点击保存")

    st.divider()

    # ── 配置区 ──
    st.subheader("📋 监控账号")

    # 账号列表
    if tw_accounts:
        for i, acct in enumerate(tw_accounts):
            has_cache = (TWITTER_CACHE_DIR / f"{acct}.json").exists()
            status = "✅ 已有缓存" if has_cache else "🆕 待首次采集"
            col_name, col_del = st.columns([5, 0.8])
            with col_name:
                st.markdown(f"**{i+1}.** `@{acct}`  {status}")
            with col_del:
                if st.button("🗑", key=f"del_tw_{i}", help="删除此账号"):
                    tw_accounts.pop(i)
                    if "twitter" not in APP_CONFIG:
                        APP_CONFIG["twitter"] = {}
                    APP_CONFIG["twitter"]["accounts"] = tw_accounts
                    save_config(APP_CONFIG)
                    st.rerun()
    else:
        st.info("暂无监控账号，请在下方添加。")

    new_accounts = st.text_input("添加账号（逗号分隔，不含 @）",
                                  placeholder="username1, username2",
                                  key="tw_new_accounts")
    if st.button("➕ 添加账号", key="add_tw_account"):
        if new_accounts.strip():
            added = 0
            for name in new_accounts.split(","):
                name = name.strip().lstrip("@")
                if name and name not in tw_accounts:
                    tw_accounts.append(name)
                    added += 1
            if added > 0:
                if "twitter" not in APP_CONFIG:
                    APP_CONFIG["twitter"] = {}
                APP_CONFIG["twitter"]["accounts"] = tw_accounts
                save_config(APP_CONFIG)
                st.success(f"已添加 {added} 个账号")
                st.rerun()
            else:
                st.warning("没有新账号可添加")

    # 过滤关键词
    st.divider()
    tw_kw_str = ", ".join(tw_filter_kw)
    tw_kw_input = st.text_input("过滤关键词（逗号分隔，留空=不过滤）",
                                 value=tw_kw_str,
                                 help="只保留包含这些关键词的推文；留空则保留全部",
                                 key="tw_filter_kw")
    if st.button("💾 保存过滤词", key="save_tw_filter"):
        new_kw = [w.strip() for w in tw_kw_input.split(",") if w.strip()]
        if "twitter" not in APP_CONFIG:
            APP_CONFIG["twitter"] = {}
        APP_CONFIG["twitter"]["filter_keywords"] = new_kw
        save_config(APP_CONFIG)
        st.success("已保存")
        st.rerun()

    st.divider()

    # ── 采集按钮 ──
    # 使用页面输入的 key（可能刚填还没保存，也能直接用）
    effective_tw_key = tw_key_input.strip() if tw_key_input.strip() else tw_api_key

    start_twitter = st.button("🐦 立即采集", type="primary", use_container_width=True,
                               disabled=not effective_tw_key or not tw_accounts)

    if start_twitter:
        def _tw_headers(api_key):
            return {"x-rapidapi-host": TWITTER_API_HOST, "x-rapidapi-key": api_key}

        TWITTER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        filter_kw_lower = [kw.lower() for kw in tw_filter_kw] if tw_filter_kw else []

        progress = st.progress(0, text="开始采集...")
        total = len(tw_accounts)
        all_account_tweets = {}  # {username: [tweet, ...]}

        status_area = st.empty()

        for idx, username in enumerate(tw_accounts):
            progress.progress(idx / total, text=f"采集 @{username}...")

            retry_count = 0
            max_retries = 3
            success = False

            while retry_count < max_retries and not success:
                try:
                    # 获取 user_id
                    user_resp = http_requests.get(
                        f"https://{TWITTER_API_HOST}/user",
                        params={"username": username},
                        headers=_tw_headers(effective_tw_key), timeout=15)
                    if user_resp.status_code == 429:
                        raise Exception("429 Too Many Requests (user lookup)")
                    user_resp.raise_for_status()
                    user_data = user_resp.json()
                    result = user_data.get("result", user_data)
                    if "data" in result:
                        result = result["data"]
                    if "user" in result:
                        result = result["user"]
                    if "result" in result:
                        result = result["result"]
                    user_id = result.get("rest_id", "")
                    if not user_id:
                        st.warning(f"⚠️ @{username} 未找到用户")
                        success = True  # 不重试，跳过
                        continue

                    time.sleep(3)  # user 和 tweets 之间间隔

                    # 获取推文
                    tweets_resp = http_requests.get(
                        f"https://{TWITTER_API_HOST}/user-tweets",
                        params={"user": user_id, "count": str(tw_max_tweets)},
                        headers=_tw_headers(effective_tw_key), timeout=15)
                    if tweets_resp.status_code == 429:
                        raise Exception("429 Too Many Requests (user-tweets)")
                    tweets_resp.raise_for_status()
                    raw = tweets_resp.json()

                    # 解析推文（兼容新旧两种 API 返回格式）
                    tweets = []
                    result_data = raw.get("result", raw)
                    timeline = result_data.get("timeline", {})
                    # 旧格式: result.timeline.timeline.instructions
                    # 新格式: result.timeline.instructions
                    instructions = timeline.get("timeline", timeline).get("instructions", [])

                    def _parse_entry(entry):
                        """从单个 entry 中提取推文"""
                        try:
                            content = entry.get("content", entry)
                            item_content = content.get("itemContent", {})
                            tweet_results = item_content.get("tweet_results", {}).get("result", {})
                            if not tweet_results:
                                return None
                            legacy = tweet_results.get("legacy", {})
                            note = (tweet_results.get("note_tweet", {})
                                    .get("note_tweet_results", {})
                                    .get("result", {})
                                    .get("text", ""))
                            text = note if note else legacy.get("full_text", "")
                            created_at = legacy.get("created_at", "")
                            tweet_id = legacy.get("id_str", entry.get("entryId", ""))
                            if text:
                                return {"text": text, "created_at": created_at, "tweet_id": tweet_id}
                        except (KeyError, TypeError, AttributeError):
                            pass
                        return None

                    for inst in instructions:
                        # 新格式: inst.entry（单个）
                        if "entry" in inst:
                            tw = _parse_entry(inst["entry"])
                            if tw:
                                tweets.append(tw)
                        # 旧格式: inst.entries[]（数组）
                        for entry in inst.get("entries", []):
                            tw = _parse_entry(entry)
                            if tw:
                                tweets.append(tw)

                    # 加载已推送缓存
                    cache_file = TWITTER_CACHE_DIR / f"{username}.json"
                    seen = set()
                    if cache_file.exists():
                        seen = set(json.loads(cache_file.read_text(encoding="utf-8")))

                    new_tweets = []
                    for tw in tweets:
                        if tw["tweet_id"] in seen:
                            continue
                        if filter_kw_lower:
                            text_lower = tw["text"].lower()
                            if not any(kw in text_lower for kw in filter_kw_lower):
                                continue
                        new_tweets.append(tw)
                        seen.add(tw["tweet_id"])

                    # 保存缓存
                    trimmed = sorted(seen)[-200:]
                    cache_file.write_text(json.dumps(trimmed, ensure_ascii=False), encoding="utf-8")

                    all_account_tweets[username] = {"new": new_tweets, "all": tweets}
                    st.caption(f"✅ @{username} — {len(new_tweets)} 条新推文 / {len(tweets)} 条总计")
                    success = True

                except Exception as e:
                    if '429' in str(e):
                        retry_count += 1
                        wait = 15 + retry_count * 15
                        status_area.warning(f"⚠️ @{username} 触发限流，等待 {wait}s 后重试 ({retry_count}/{max_retries})")
                        time.sleep(wait)
                    else:
                        st.warning(f"⚠️ @{username} 失败: {e}")
                        break

            if not success and retry_count >= max_retries:
                st.warning(f"⚠️ @{username} 重试 {max_retries} 次仍失败，跳过")

            if idx < total - 1:
                status_area.info(f"⏸ 等待 120 秒后采集下一个账号...")
                time.sleep(120)  # 账号之间间隔 120 秒

        status_area.empty()

        progress.progress(1.0, text="采集完成！")
        st.divider()

        # 汇总显示
        has_new = any(v["new"] for v in all_account_tweets.values())

        if has_new:
            total_new = sum(len(v["new"]) for v in all_account_tweets.values())
            st.subheader(f"🆕 发现 {total_new} 条新推文")

            # 飞书通知
            notify = load_notify_config()
            webhook = notify.get("feishu_webhook", "")
            if webhook:
                now = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")
                content_lines = [
                    [{"tag": "text", "text": f"📅 {now}\n{sum(1 for v in all_account_tweets.values() if v['new'])} 个账号有新动态，共 {total_new} 条推文"}],
                ]
                for uname, data in all_account_tweets.items():
                    if not data["new"]:
                        continue
                    content_lines.append([{"tag": "text", "text": f"\n🐦 @{uname}（{len(data['new'])} 条新推文）:"}])
                    for tw in data["new"][:10]:
                        snippet = tw["text"].replace("\n", " ")
                        if len(snippet) > 120:
                            snippet = snippet[:120] + "..."
                        content_lines.append([{"tag": "text", "text": f"  {snippet}"}])
                    if len(data["new"]) > 10:
                        content_lines.append([{"tag": "text", "text": f"  ...等共 {len(data['new'])} 条"}])

                payload = {
                    "msg_type": "post",
                    "content": {"post": {"zh_cn": {
                        "title": "🐦 Twitter 监控报告",
                        "content": content_lines,
                    }}}
                }
                try:
                    feishu_resp = http_requests.post(webhook, json=payload, timeout=10)
                    if feishu_resp.status_code == 200:
                        st.success("✅ 飞书通知已发送")
                    else:
                        st.warning(f"⚠️ 飞书通知失败: {feishu_resp.status_code}")
                except Exception:
                    st.warning("⚠️ 飞书通知发送异常")

        # 展示所有账号推文
        for uname, data in all_account_tweets.items():
            tweets_to_show = data["new"] if data["new"] else data["all"][:5]
            label_suffix = f"+{len(data['new'])} 条新推文" if data["new"] else "无新推文"
            with st.expander(f"🐦 @{uname}（{label_suffix}）", expanded=bool(data["new"])):
                for tw in tweets_to_show:
                    is_new = tw in data["new"]
                    prefix = "🆕 " if is_new else ""
                    st.markdown(f"{prefix}**{tw.get('created_at', '')[:16]}**")
                    st.text(tw["text"][:500])
                    st.markdown(f"[查看原文](https://x.com/{uname}/status/{tw['tweet_id']})")
                    st.markdown("---")

        if not has_new:
            st.success("✅ 所有账号无新推文")


# ════════════════════════════════════════════════════════════════
# Tab 5: AI 平台监控
# ════════════════════════════════════════════════════════════════
with tab5:
    st.title("🤖 AI 平台监控")
    st.caption("监控 Hugging Face / arXiv / Product Hunt / GitHub Trending / Hacker News 上的 AI 新动态")

    AI_MONITOR_CACHE_DIR = Path(__file__).parent / "output" / "ai_monitor"

    AI_PLATFORM_INFO = {
        "huggingface": {"name": "Hugging Face", "icon": "\U0001F917", "desc": "热门模型/Spaces"},
        "arxiv": {"name": "arXiv", "icon": "\U0001F4C4", "desc": "AI 新论文"},
        "producthunt": {"name": "Product Hunt", "icon": "\U0001F680", "desc": "新上线 AI 工具"},
        "github": {"name": "GitHub Trending", "icon": "\U0001F4BB", "desc": "热门 AI 开源项目"},
        "hackernews": {"name": "Hacker News", "icon": "\U0001F4F0", "desc": "AI 相关热帖"},
    }

    ai_config = APP_CONFIG.get("ai_monitor", {})
    enabled_platforms = ai_config.get("enabled_platforms",
                                       ["huggingface", "arxiv", "producthunt", "github", "hackernews"])

    # ── 平台选择 ──
    st.subheader("📋 监控平台")
    selected_platforms = []
    cols = st.columns(5)
    for i, (key, info) in enumerate(AI_PLATFORM_INFO.items()):
        with cols[i]:
            checked = st.checkbox(f"{info['icon']} {info['name']}",
                                   value=key in enabled_platforms,
                                   key=f"ai_plat_{key}")
            if checked:
                selected_platforms.append(key)
            st.caption(info["desc"])

    if st.button("💾 保存平台设置", key="save_ai_platforms"):
        if "ai_monitor" not in APP_CONFIG:
            APP_CONFIG["ai_monitor"] = {}
        APP_CONFIG["ai_monitor"]["enabled_platforms"] = selected_platforms
        save_config(APP_CONFIG)
        st.success("已保存")
        st.rerun()

    # ── 过滤关键词 ──
    st.divider()
    ai_filter_kw = ai_config.get("filter_keywords", [])
    ai_kw_str = ", ".join(ai_filter_kw)
    ai_kw_input = st.text_input("过滤关键词（逗号分隔，用于 GitHub/HN/ProductHunt 过滤非 AI 内容）",
                                 value=ai_kw_str,
                                 help="只保留包含这些关键词的条目；HuggingFace 和 arXiv 默认全部 AI 内容无需过滤",
                                 key="ai_filter_kw")
    if st.button("💾 保存过滤词", key="save_ai_filter"):
        new_kw = [w.strip() for w in ai_kw_input.split(",") if w.strip()]
        if "ai_monitor" not in APP_CONFIG:
            APP_CONFIG["ai_monitor"] = {}
        APP_CONFIG["ai_monitor"]["filter_keywords"] = new_kw
        save_config(APP_CONFIG)
        st.success("已保存")
        st.rerun()

    st.divider()

    # ── 采集按钮 ──
    start_ai = st.button("🤖 立即采集", type="primary", use_container_width=True,
                           disabled=not selected_platforms)

    if start_ai:
        import xml.etree.ElementTree as ET

        AI_MONITOR_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        filter_kw = [w.strip() for w in ai_kw_input.split(",") if w.strip()] if ai_kw_input.strip() else ai_filter_kw

        def _ai_filter_ui(text, keywords):
            if not keywords:
                return True
            text_lower = text.lower()
            return any(kw.lower() in text_lower for kw in keywords)

        def _load_cache(platform):
            cache_file = AI_MONITOR_CACHE_DIR / f"{platform}.json"
            if cache_file.exists():
                return set(json.loads(cache_file.read_text(encoding="utf-8")))
            return set()

        def _save_cache(platform, seen_ids):
            cache_file = AI_MONITOR_CACHE_DIR / f"{platform}.json"
            trimmed = sorted(seen_ids)[-500:]
            cache_file.write_text(json.dumps(trimmed, ensure_ascii=False), encoding="utf-8")

        progress = st.progress(0, text="开始采集...")
        total = len(selected_platforms)
        all_results = {}  # {platform: [items]}

        for idx, platform in enumerate(selected_platforms):
            info = AI_PLATFORM_INFO[platform]
            progress.progress(idx / total, text=f"采集 {info['icon']} {info['name']}...")

            try:
                if platform == "huggingface":
                    resp = http_requests.get("https://huggingface.co/api/trending",
                                             timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
                    resp.raise_for_status()
                    data = resp.json()
                    items_raw = data if isinstance(data, list) else data.get("recentlyTrending", data.get("models", []))
                    seen = _load_cache("huggingface")
                    items = []
                    limit = ai_config.get("huggingface_limit", 30)
                    for item in items_raw[:limit]:
                        repo_id = item.get("repoData", {}).get("id", "") or item.get("id", "")
                        if not repo_id or repo_id in seen:
                            continue
                        likes = item.get("repoData", {}).get("likes", 0) or item.get("likes", 0)
                        repo_type = item.get("repoType", "model")
                        items.append({"id": repo_id, "title": repo_id, "likes": likes, "type": repo_type})
                        seen.add(repo_id)
                    _save_cache("huggingface", seen)
                    if items:
                        all_results["huggingface"] = items
                    st.caption(f"✅ Hugging Face — {len(items)} 个新项目")

                elif platform == "arxiv":
                    categories = ai_config.get("arxiv_categories", ["cs.AI", "cs.CL", "cs.LG"])
                    seen = _load_cache("arxiv")
                    items = []
                    ns = {'dc': 'http://purl.org/dc/elements/1.1/'}
                    for cat in categories:
                        try:
                            resp = http_requests.get(f"http://export.arxiv.org/rss/{cat}",
                                                     timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
                            resp.raise_for_status()
                            root = ET.fromstring(resp.content)
                            for item in root.iter('item'):
                                title_el = item.find('title')
                                link_el = item.find('link')
                                title = title_el.text.strip() if title_el is not None and title_el.text else ""
                                link = link_el.text.strip() if link_el is not None and link_el.text else ""
                                title = " ".join(title.split())
                                if title.startswith("(") or not title:
                                    continue
                                paper_id = link or title
                                if paper_id in seen:
                                    continue
                                items.append({"id": paper_id, "title": title, "category": cat, "url": link})
                                seen.add(paper_id)
                        except Exception as e:
                            st.caption(f"  ⚠️ {cat}: {e}")
                        if cat != categories[-1]:
                            time.sleep(3)
                    _save_cache("arxiv", seen)
                    if items:
                        all_results["arxiv"] = items
                    st.caption(f"✅ arXiv — {len(items)} 篇新论文")

                elif platform == "producthunt":
                    resp = http_requests.get("https://www.producthunt.com/feed",
                                             timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
                    resp.raise_for_status()
                    root = ET.fromstring(resp.content)
                    seen = _load_cache("producthunt")
                    items = []
                    for item in root.iter('item'):
                        title_el = item.find('title')
                        link_el = item.find('link')
                        desc_el = item.find('description')
                        title = title_el.text.strip() if title_el is not None and title_el.text else ""
                        link = link_el.text.strip() if link_el is not None and link_el.text else ""
                        desc = desc_el.text.strip() if desc_el is not None and desc_el.text else ""
                        if not title or link in seen:
                            continue
                        if not _ai_filter_ui(f"{title} {desc}", filter_kw):
                            continue
                        items.append({"id": link, "title": title, "url": link, "tagline": desc[:120]})
                        seen.add(link)
                    _save_cache("producthunt", seen)
                    if items:
                        all_results["producthunt"] = items
                    st.caption(f"✅ Product Hunt — {len(items)} 个新产品")

                elif platform == "github":
                    from lxml import html as lxml_html
                    seen = _load_cache("github")
                    items = []
                    languages = ai_config.get("github_languages", [])
                    urls = [("", "https://github.com/trending?since=daily")]
                    for lang in languages[:4]:
                        urls.append((lang, f"https://github.com/trending/{lang.lower()}?since=daily"))
                    collected = set()
                    for lang_label, url in urls:
                        try:
                            resp = http_requests.get(url, timeout=15, headers={
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
                            if resp.status_code != 200:
                                continue
                            tree = lxml_html.fromstring(resp.text)
                            articles = tree.cssselect('article.Box-row')
                            for art in articles:
                                h2 = art.cssselect('h2 a')
                                if not h2:
                                    continue
                                repo_path = h2[0].get('href', '').strip('/')
                                if not repo_path or repo_path in collected or repo_path in seen:
                                    continue
                                collected.add(repo_path)
                                desc_el = art.cssselect('p')
                                desc = desc_el[0].text_content().strip() if desc_el else ""
                                lang_el = art.cssselect('[itemprop="programmingLanguage"]')
                                language = lang_el[0].text_content().strip() if lang_el else ""
                                star_els = art.cssselect('span.d-inline-block.float-sm-right')
                                today_stars = star_els[0].text_content().strip() if star_els else ""
                                if not _ai_filter_ui(f"{repo_path} {desc}", filter_kw):
                                    continue
                                items.append({"id": repo_path, "repo": repo_path, "description": desc[:150],
                                              "language": language or lang_label, "today_stars": today_stars})
                                seen.add(repo_path)
                        except Exception as e:
                            st.caption(f"  ⚠️ GitHub {lang_label or '总榜'}: {e}")
                        time.sleep(2)
                    _save_cache("github", seen)
                    if items:
                        all_results["github"] = items
                    st.caption(f"✅ GitHub Trending — {len(items)} 个新项目")

                elif platform == "hackernews":
                    hn_limit = ai_config.get("hackernews_limit", 50)
                    resp = http_requests.get("https://hacker-news.firebaseio.com/v0/topstories.json", timeout=15)
                    resp.raise_for_status()
                    story_ids = resp.json()[:hn_limit]
                    seen = _load_cache("hackernews")
                    items = []
                    for i, sid in enumerate(story_ids):
                        sid_str = str(sid)
                        if sid_str in seen:
                            continue
                        try:
                            item_resp = http_requests.get(
                                f"https://hacker-news.firebaseio.com/v0/item/{sid}.json", timeout=10)
                            item_resp.raise_for_status()
                            hn_item = item_resp.json()
                            if not hn_item:
                                continue
                            title = hn_item.get("title", "")
                            url = hn_item.get("url", "")
                            score = hn_item.get("score", 0)
                            descendants = hn_item.get("descendants", 0)
                            if not _ai_filter_ui(f"{title} {url}", filter_kw):
                                seen.add(sid_str)
                                continue
                            items.append({"id": sid_str, "title": title,
                                          "url": url or f"https://news.ycombinator.com/item?id={sid}",
                                          "score": score, "comments": descendants})
                            seen.add(sid_str)
                        except Exception:
                            continue
                        if (i + 1) % 10 == 0:
                            time.sleep(1)
                    _save_cache("hackernews", seen)
                    if items:
                        all_results["hackernews"] = items
                    st.caption(f"✅ Hacker News — {len(items)} 条新帖")

            except Exception as e:
                st.warning(f"⚠️ {info['name']} 失败: {e}")

            if idx < total - 1:
                time.sleep(2)

        progress.progress(1.0, text="采集完成！")
        st.divider()

        # 结果展示
        if all_results:
            total_items = sum(len(v) for v in all_results.values())
            st.subheader(f"🆕 发现 {total_items} 条新内容")

            # 飞书通知
            notify = load_notify_config()
            webhook = notify.get("feishu_webhook", "")
            if webhook:
                now = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")
                content_lines = [
                    [{"tag": "text", "text": f"📅 {now}\n{len(all_results)} 个平台有新动态，共 {total_items} 条"}],
                ]
                for plat, items in all_results.items():
                    pinfo = AI_PLATFORM_INFO[plat]
                    content_lines.append([{"tag": "text", "text": f"\n{pinfo['icon']} {pinfo['name']}（{len(items)} 条）:"}])
                    for item in items[:15]:
                        if plat == "huggingface":
                            line = f"  {item['title']}  ({item['type']}, {item['likes']} likes)"
                        elif plat == "arxiv":
                            line = f"  {item['title'][:80]}  [{item['category']}]"
                        elif plat == "producthunt":
                            line = f"  {item['title']}  {item.get('tagline', '')[:60]}"
                        elif plat == "github":
                            line = f"  {item['repo']}  {item.get('today_stars', '')}  [{item.get('language', '')}]"
                        elif plat == "hackernews":
                            line = f"  {item['title'][:80]}  ({item['score']} pts)"
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
                    feishu_resp = http_requests.post(webhook, json=payload, timeout=10)
                    if feishu_resp.status_code == 200:
                        st.success("✅ 飞书通知已发送")
                    else:
                        st.warning(f"⚠️ 飞书通知失败: {feishu_resp.status_code}")
                except Exception:
                    st.warning("⚠️ 飞书通知发送异常")

            # 按平台展示
            for plat, items in all_results.items():
                pinfo = AI_PLATFORM_INFO[plat]
                with st.expander(f"{pinfo['icon']} {pinfo['name']}（{len(items)} 条新内容）", expanded=True):
                    for item in items:
                        if plat == "huggingface":
                            st.markdown(f"- **{item['title']}** — {item['type']}, {item['likes']} likes "
                                        f"[查看](https://huggingface.co/{item['id']})")
                        elif plat == "arxiv":
                            st.markdown(f"- **{item['title'][:100]}** [{item['category']}] "
                                        f"[论文]({item.get('url', '')})")
                        elif plat == "producthunt":
                            st.markdown(f"- **{item['title']}** — {item.get('tagline', '')} "
                                        f"[查看]({item['url']})")
                        elif plat == "github":
                            st.markdown(f"- **{item['repo']}** [{item.get('language', '')}] "
                                        f"{item.get('today_stars', '')} — {item.get('description', '')[:80]} "
                                        f"[GitHub](https://github.com/{item['repo']})")
                        elif plat == "hackernews":
                            st.markdown(f"- **{item['title'][:80]}** ({item['score']} pts, "
                                        f"{item.get('comments', 0)} comments) [链接]({item['url']})")
        else:
            st.success("✅ 所有平台无新内容")


# ════════════════════════════════════════════════════════════════
# Tab 6: 域名淘金
# ════════════════════════════════════════════════════════════════
with tab6:
    st.title("🌐 域名淘金")
    st.caption("从新注册域名中筛选有价值的域名，通过 Google Trends 验证搜索量增长趋势")

    # ── 垃圾行业词黑名单 ──
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

    # 低质量前缀/后缀词（常见垃圾域名套路）
    JUNK_PREFIXES = [
        "best", "top", "free", "cheap", "buy", "get", "my", "the",
        "pro", "vip", "real", "fast", "easy", "super", "mega", "ultra",
        "shop", "store", "deals", "offer", "discount", "sale", "price",
        "online", "web", "site", "page", "link", "click", "visit",
        "info", "help", "support", "service", "services", "solution", "solutions",
    ]

    DOMAIN_MIN_LENGTH = 6
    DOMAIN_MAX_LENGTH = 20

    def is_valid_tld(domain):
        """只保留 .com 和 .ai 域名"""
        domain_lower = domain.lower().strip()
        return domain_lower.endswith(".com")

    def extract_domain_body(domain):
        """提取域名主体部分（去掉 TLD）"""
        domain_lower = domain.lower().strip()
        if domain_lower.endswith(".com"):
            return domain_lower[:-4]
        elif domain_lower.endswith(".ai"):  # 备用，暂不启用
            return domain_lower[:-3]
        return domain_lower

    def has_digits(body):
        """域名主体包含数字"""
        return bool(re.search(r'\d', body))

    def has_special_chars(body):
        """域名主体包含特殊字符（连字符除外的非字母字符）"""
        return bool(re.search(r'[^a-z\-]', body))

    def contains_blacklist(body):
        """域名主体命中垃圾词黑名单"""
        for word in DOMAIN_BLACKLIST:
            if word in body:
                return True
        return False

    try:
        import wordninja
        _HAS_WORDNINJA = True
    except ImportError:
        _HAS_WORDNINJA = False

    def split_domain_words(body):
        """用 wordninja 拆分域名主体为单词"""
        clean = body.replace("-", "")
        if not clean:
            return [], 0.0
        if _HAS_WORDNINJA:
            all_words = wordninja.split(clean)
        else:
            return [clean], 1.0 if len(clean) >= 4 else 0.0
        good_words = [w for w in all_words if len(w) >= 4]
        good_chars = sum(len(w) for w in good_words)
        quality = good_chars / len(clean) if clean else 0.0
        return good_words, quality

    def is_random_string(body):
        """用 wordninja 拆词判断是否为随机字符串"""
        clean = body.replace("-", "")
        if not clean:
            return True
        if len(clean) > 25 and "-" not in body:
            return True
        good_words, quality = split_domain_words(body)
        if not good_words:
            return True
        if quality < 0.7:
            return True
        # 至少要有一个 >= 5 字符的真实单词
        if not any(len(w) >= 5 for w in good_words):
            return True
        return False

    def get_trends_keyword(body):
        """从域名主体提取 Trends 搜索关键词（拆词后空格连接）"""
        good_words, quality = split_domain_words(body)
        if good_words and quality >= 0.7:
            return " ".join(good_words)
        return body

    def bad_length(body):
        """域名主体长度不在合理范围"""
        return len(body) < DOMAIN_MIN_LENGTH or len(body) > DOMAIN_MAX_LENGTH

    def has_junk_prefix(body):
        """域名主体以低质量前缀开头或结尾"""
        for junk in JUNK_PREFIXES:
            if body.startswith(junk) or body.endswith(junk):
                return True
        return False

    def filter_domains(domains_text):
        """完整的域名过滤流水线"""
        lines = [line.strip() for line in domains_text.strip().splitlines() if line.strip()]

        results = {
            "input_total": len(lines),
            "after_tld": [],
            "after_digits": [],
            "after_special": [],
            "after_blacklist": [],
            "after_length": [],
            "after_junk": [],
            "after_random": [],
            "filtered_out": {
                "tld": [],
                "digits": [],
                "special": [],
                "blacklist": [],
                "length": [],
                "junk": [],
                "random": [],
            }
        }

        # Step 1: 只保留 .com
        for d in lines:
            if is_valid_tld(d):
                results["after_tld"].append(d)
            else:
                results["filtered_out"]["tld"].append(d)

        # Step 2: 过滤含数字的域名
        for d in results["after_tld"]:
            body = extract_domain_body(d)
            if not has_digits(body):
                results["after_digits"].append(d)
            else:
                results["filtered_out"]["digits"].append(d)

        # Step 3: 过滤含特殊字符的域名
        for d in results["after_digits"]:
            body = extract_domain_body(d)
            if not has_special_chars(body):
                results["after_special"].append(d)
            else:
                results["filtered_out"]["special"].append(d)

        # Step 4: 垃圾行业词过滤
        for d in results["after_special"]:
            body = extract_domain_body(d)
            if not contains_blacklist(body):
                results["after_blacklist"].append(d)
            else:
                results["filtered_out"]["blacklist"].append(d)

        # Step 5: 域名长度过滤（<6 或 >20）
        for d in results["after_blacklist"]:
            body = extract_domain_body(d)
            if not bad_length(body):
                results["after_length"].append(d)
            else:
                results["filtered_out"]["length"].append(d)

        # Step 6: 低质量前缀/后缀过滤
        for d in results["after_length"]:
            body = extract_domain_body(d)
            if not has_junk_prefix(body):
                results["after_junk"].append(d)
            else:
                results["filtered_out"]["junk"].append(d)

        # Step 7: 随机字符串过滤（wordninja 拆词）
        for d in results["after_junk"]:
            body = extract_domain_body(d)
            if not is_random_string(body):
                results["after_random"].append(d)
            else:
                results["filtered_out"]["random"].append(d)

        return results

    # ── 输入区 ──
    st.subheader("📥 获取新注册域名")

    input_method = st.radio("数据来源", ["自动下载（免费）", "手动粘贴", "上传文件"],
                             horizontal=True, key="domain_source")

    domain_input = ""

    if input_method == "自动下载（免费）":
        st.caption("从 whoisdownload.com 自动下载新注册域名列表（免费，每日约 7 万域名，保留近 4 天）")
        download_date = st.date_input("选择日期（默认昨天）",
                                        value=datetime.now(BEIJING_TZ).date() - timedelta(days=1),
                                        key="whoisds_date")
        if st.button("📡 下载域名列表", key="download_whoisds"):
            import zipfile
            import io as _io
            import base64

            date_str = download_date.strftime("%Y-%m-%d")
            date_zip = f"{date_str}.zip"
            date_b64 = base64.b64encode(date_zip.encode()).decode()
            url = f"https://www.whoisdownload.com/download-panel/free-download-file/{date_b64}/nrd/home"

            with st.spinner(f"正在下载 {date_str} 新注册域名..."):
                try:
                    resp = http_requests.get(url, timeout=120, headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                        'Referer': 'https://www.whoisdownload.com/newly-registered-domains',
                    }, allow_redirects=True)

                    if resp.status_code != 200:
                        st.error(f"下载失败: HTTP {resp.status_code}")
                    elif resp.content[:4] != b'PK\x03\x04':
                        st.error(f"{date_str} 的数据暂未发布，请尝试更早的日期（免费仅保留近 4 天）")
                    else:
                        zf = zipfile.ZipFile(_io.BytesIO(resp.content))
                        domains_list = []
                        for name in zf.namelist():
                            text = zf.read(name).decode('utf-8', errors='ignore')
                            for line in text.splitlines():
                                d = line.strip()
                                if d and not d.startswith('#'):
                                    domains_list.append(d)
                        st.session_state["whoisds_domains"] = "\n".join(domains_list)
                        st.success(f"✅ 下载完成: {len(domains_list)} 个域名（{date_str}）")
                except Exception as e:
                    st.error(f"下载失败: {e}")

        # 从 session_state 读取已下载的数据
        domain_input = st.session_state.get("whoisds_domains", "")
        if domain_input:
            count = len(domain_input.strip().splitlines())
            st.info(f"已加载 {count} 个域名，可直接开始筛选")
            st.download_button("📥 下载原始域名列表", domain_input.encode('utf-8'),
                                f"nrd_{download_date.strftime('%Y-%m-%d')}.txt", "text/plain")

    elif input_method == "手动粘贴":
        domain_input = st.text_area(
            "域名列表（每行一个）",
            height=200,
            placeholder="example.com\ncoolapp.ai\ntest123.net\nxyzrandomstring.com\n...",
            key="domain_input"
        )

    elif input_method == "上传文件":
        uploaded_file = st.file_uploader("上传 TXT/CSV 文件", type=["txt", "csv"], key="domain_file")
        if uploaded_file is not None:
            file_content = uploaded_file.read().decode("utf-8")
            if uploaded_file.name.endswith(".csv"):
                import csv
                import io as _io
                reader = csv.reader(_io.StringIO(file_content))
                file_domains = []
                for row in reader:
                    if row:
                        d = row[0].strip()
                        if d and not d.startswith("#"):
                            file_domains.append(d)
                domain_input = "\n".join(file_domains)
            else:
                domain_input = file_content
            st.info(f"已从文件加载 {len(domain_input.strip().splitlines())} 个域名")

    st.divider()

    # ── 过滤设置 ──
    col_filter, col_trends = st.columns(2)

    with col_filter:
        st.subheader("🔧 过滤设置")
        filter_tld = st.checkbox("只保留 .com", value=True, key="filter_tld")
        filter_digits = st.checkbox("过滤含数字的域名", value=True, key="filter_digits")
        filter_special = st.checkbox("过滤含特殊字符的域名", value=True, key="filter_special")
        filter_blacklist = st.checkbox("垃圾行业词过滤", value=True, key="filter_blacklist")
        filter_random = st.checkbox("随机字符串过滤", value=True, key="filter_random")

    with col_trends:
        st.subheader("📈 Trends 验证设置")
        trends_enabled = st.checkbox("启用 Google Trends 二次验证", value=True, key="trends_enabled")
        trends_timeframe = st.selectbox("趋势时间范围", [
            ("近 15 天", "today 15-d"),
            ("近 7 天", "now 7-d"),
            ("近 30 天", "today 1-m"),
        ], format_func=lambda x: x[0], index=0, key="trends_timeframe")
        trends_batch_size = st.slider("每批查询数量", min_value=1, max_value=5, value=5,
                                       help="Google Trends 每次最多查 5 个关键词", key="trends_batch")
        trends_interval = st.slider("Trends 请求间隔（秒）", min_value=10, max_value=120, value=60,
                                     key="trends_interval")

    st.divider()

    # ── 开始筛选 ──
    col_filter_btn, col_trends_btn = st.columns(2)

    with col_filter_btn:
        start_filter = st.button("🔍 开始筛选", type="primary", use_container_width=True)

    with col_trends_btn:
        start_full = st.button("🚀 筛选 + Trends 验证", type="primary", use_container_width=True)

    if (start_filter or start_full) and domain_input.strip():
        # Step 1: 域名过滤
        filter_result = filter_domains(domain_input)

        # 最终列表
        final_domains = filter_result["after_random"]

        st.divider()
        st.subheader("📊 筛选结果")

        # 漏斗统计（两行展示）
        col_s1, col_s2, col_s3, col_s4 = st.columns(4)
        with col_s1:
            st.metric("输入总数", filter_result["input_total"])
        with col_s2:
            st.metric(".com", len(filter_result["after_tld"]),
                       delta=f"-{len(filter_result['filtered_out']['tld'])}")
        with col_s3:
            st.metric("去数字", len(filter_result["after_digits"]),
                       delta=f"-{len(filter_result['filtered_out']['digits'])}")
        with col_s4:
            st.metric("去特殊字符", len(filter_result["after_special"]),
                       delta=f"-{len(filter_result['filtered_out']['special'])}")

        col_s5, col_s6, col_s7, col_s8 = st.columns(4)
        with col_s5:
            st.metric("去垃圾词", len(filter_result["after_blacklist"]),
                       delta=f"-{len(filter_result['filtered_out']['blacklist'])}")
        with col_s6:
            st.metric("去长度异常", len(filter_result["after_length"]),
                       delta=f"-{len(filter_result['filtered_out']['length'])}")
        with col_s7:
            st.metric("去低质前缀", len(filter_result["after_junk"]),
                       delta=f"-{len(filter_result['filtered_out']['junk'])}")
        with col_s8:
            st.metric("去随机串", len(filter_result["after_random"]),
                       delta=f"-{len(filter_result['filtered_out']['random'])}")

        st.success(f"✅ 筛选完成！从 **{filter_result['input_total']}** 个域名中筛选出 **{len(final_domains)}** 个有效域名")

        # 展示筛选后的域名
        if final_domains:
            with st.expander(f"📋 筛选后域名列表（{len(final_domains)} 个）", expanded=True):
                domain_df = pd.DataFrame({
                    "域名": final_domains,
                    "主体": [extract_domain_body(d) for d in final_domains],
                    "拆词": [get_trends_keyword(extract_domain_body(d)) for d in final_domains],
                    "后缀": ["." + d.rsplit(".", 1)[-1] if "." in d else "" for d in final_domains],
                })
                st.dataframe(domain_df, use_container_width=True, hide_index=True, height=300)

        # 展示被过滤的域名（可折叠）
        for stage, label in [("tld", "非 .com"), ("digits", "含数字"),
                              ("special", "含特殊字符"), ("blacklist", "垃圾词命中"),
                              ("length", "长度异常"), ("junk", "低质前缀/后缀"),
                              ("random", "随机字符串")]:
            filtered = filter_result["filtered_out"][stage]
            if filtered:
                with st.expander(f"🗑 {label}（{len(filtered)} 个）"):
                    st.text("\n".join(filtered[:100]))
                    if len(filtered) > 100:
                        st.caption(f"...等共 {len(filtered)} 个")

        # Step 2: Google Trends 验证
        if start_full and trends_enabled and final_domains:
            st.divider()
            st.subheader("📈 Google Trends 分批验证")

            # 构建域名→关键词映射
            domain_kw_map = {}
            for d in final_domains:
                body = extract_domain_body(d)
                domain_kw_map[d] = get_trends_keyword(body)

            # 去重关键词 + 过滤无效关键词
            seen_kw = set()
            unique_keywords = []
            kw_to_domains = {}  # 关键词→域名列表
            for d, kw in domain_kw_map.items():
                # 跳过无效关键词：空、纯符号、太长（>50字符）
                if not kw or len(kw) > 50 or not re.search(r'[a-zA-Z]', kw):
                    continue
                if kw not in seen_kw:
                    unique_keywords.append(kw)
                    seen_kw.add(kw)
                    kw_to_domains[kw] = []
                kw_to_domains[kw].append(d)

            # 分成每轮 50 个关键词（10批×5个）
            ROUND_SIZE = 50
            rounds = [unique_keywords[i:i+ROUND_SIZE] for i in range(0, len(unique_keywords), ROUND_SIZE)]
            total_rounds = len(rounds)
            total_kw = len(unique_keywords)

            st.info(f"共 **{total_kw}** 个关键词，分 **{total_rounds}** 轮查询（每轮 {ROUND_SIZE} 个），每轮完成后即时推送飞书")

            # 停止按钮
            if "domain_stop" not in st.session_state:
                st.session_state.domain_stop = False
            stop_btn = st.button("⏹ 停止验证（推送已有结果）", key="stop_trends", type="secondary")
            if stop_btn:
                st.session_state.domain_stop = True

            progress = st.progress(0, text="准备开始...")
            status_area = st.empty()
            results_area = st.empty()
            effective_interval = trends_interval

            pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)

            all_growing = []
            all_has_volume = []
            all_no_volume = []
            trends_data = {}
            stopped = False

            def _send_round_feishu(spike_items, round_idx, total_r, checked_count, total_count):
                """每轮完成后推送新词爆发到飞书（只推单词格式）"""
                notify = load_notify_config()
                webhook = notify.get("feishu_webhook", "")
                if not webhook or not spike_items:
                    return
                now = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")
                content_lines = [
                    [{"tag": "text", "text": f"📅 {now}\n第 {round_idx}/{total_r} 轮（已查 {checked_count}/{total_count}），发现 {len(spike_items)} 个新词爆发"}],
                ]
                for item in spike_items[:20]:
                    kw = item.get("keyword", "")
                    late = item.get("late", 0)
                    max_val = item.get("max", 0)
                    content_lines.append([{"tag": "text",
                                            "text": f"  {kw}  (当前热度{late}, 峰值{max_val})"}])
                payload = {
                    "msg_type": "post",
                    "content": {"post": {"zh_cn": {
                        "title": f"🔥 新词爆发 第{round_idx}轮",
                        "content": content_lines,
                    }}}
                }
                try:
                    http_requests.post(webhook, json=payload, timeout=10)
                except Exception:
                    pass

            for ri, round_keywords in enumerate(rounds):
                if st.session_state.get("domain_stop", False):
                    stopped = True
                    status_area.warning(f"⏹ 已手动停止，共完成 {ri}/{total_rounds} 轮")
                    break

                round_num = ri + 1
                progress.progress(ri / total_rounds, text=f"第 {round_num}/{total_rounds} 轮...")

                # 每轮内分成 5 个一批
                round_batches = [round_keywords[i:i+trends_batch_size]
                                 for i in range(0, len(round_keywords), trends_batch_size)]
                round_growing = []

                for bi, batch in enumerate(round_batches):
                    status_area.info(f"第 {round_num}/{total_rounds} 轮 — 批次 {bi+1}/{len(round_batches)}: {', '.join(batch[:3])}{'...' if len(batch)>3 else ''}")

                    retry_count = 0
                    while retry_count < 3:
                        try:
                            pytrend.build_payload(kw_list=batch, timeframe=trends_timeframe[1], geo='')
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
                                            growth = ((late - early) / early * 100) if early > 0 else (999 if late > 0 else 0)
                                        else:
                                            growth = 0
                                        trends_data[kw] = {
                                            "has_trend": avg_val > 0,
                                            "avg": round(avg_val, 1), "max": round(max_val, 1),
                                            "growth": round(growth, 1), "trend": series.tolist(),
                                            "early": round(early, 1) if len(series) >= 3 else 0,
                                            "late": round(late, 1) if len(series) >= 3 else 0,
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
                            status_area.warning(f"⚠️ 429 限流，等待 {wait}s ({retry_count}/3)")
                            time.sleep(wait)
                            pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)
                        except Exception as e:
                            err_str = str(e)
                            if '429' in err_str:
                                retry_count += 1
                                wait = 60 + retry_count * 60 + random.randint(0, 10)
                                effective_interval = min(effective_interval * 1.5, 300)
                                status_area.warning(f"⚠️ 429 限流，等待 {wait}s ({retry_count}/3)")
                                time.sleep(wait)
                                pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)
                            elif '400' in err_str:
                                # 400 = 批次中有无效关键词，逐个重试
                                for single_kw in batch:
                                    if single_kw in trends_data:
                                        continue
                                    try:
                                        pytrend.build_payload(kw_list=[single_kw], timeframe=trends_timeframe[1], geo='')
                                        iot2 = pytrend.interest_over_time()
                                        if not iot2.empty and single_kw in iot2.columns:
                                            s = iot2[single_kw].values.astype(float)
                                            a, m = np.mean(s), np.max(s)
                                            if len(s) >= 3:
                                                sp = max(1, len(s) // 3)
                                                e_val, l_val = np.mean(s[:sp]), np.mean(s[sp:])
                                                g = ((l_val - e_val) / e_val * 100) if e_val > 0 else (999 if l_val > 0 else 0)
                                            else:
                                                e_val, l_val, g = 0, 0, 0
                                            trends_data[single_kw] = {"has_trend": a > 0, "avg": round(a, 1), "max": round(m, 1),
                                                                       "growth": round(g, 1), "trend": s.tolist(),
                                                                       "early": round(e_val, 1), "late": round(l_val, 1)}
                                        else:
                                            trends_data[single_kw] = {"has_trend": False, "avg": 0, "max": 0, "growth": 0, "trend": [], "early": 0, "late": 0}
                                        time.sleep(random.uniform(3, 8))
                                    except Exception:
                                        trends_data[single_kw] = {"has_trend": False, "avg": 0, "max": 0, "growth": 0, "trend": [], "early": 0, "late": 0}
                                break
                            else:
                                status_area.warning(f"⚠️ 查询出错，跳过: {e}")
                                for kw in batch:
                                    if kw not in trends_data:
                                        trends_data[kw] = {"has_trend": False, "avg": 0, "max": 0, "growth": 0, "trend": []}
                                break

                    if bi < len(round_batches) - 1:
                        time.sleep(effective_interval + random.uniform(0, 3))

                # 本轮结果汇总
                round_spikes = []  # 新词爆发（重点推送）
                for kw in round_keywords:
                    info = trends_data.get(kw, {})
                    for domain in kw_to_domains.get(kw, []):
                        item = {"domain": domain, "keyword": kw, **info}
                        early = info.get("early", 0)
                        late = info.get("late", 0)
                        growth = info.get("growth", 0)

                        # 新词爆发：之前几乎没流量(early<2)，现在有了(late>=5)
                        is_spike = early < 2 and late >= 5
                        item["is_spike"] = is_spike

                        if growth > 20:
                            round_growing.append(item)
                            all_growing.append(item)
                            if is_spike:
                                round_spikes.append(item)
                        elif info.get("has_trend", False):
                            all_has_volume.append(item)
                        else:
                            all_no_volume.append(item)

                checked = min((ri + 1) * ROUND_SIZE, total_kw)

                # 只推送新词爆发的到飞书（单词格式，不要域名格式）
                _send_round_feishu(round_spikes, round_num, total_rounds, checked, total_kw)
                if round_spikes:
                    status_area.success(f"✅ 第 {round_num} 轮完成！发现 {len(round_spikes)} 个新词爆发，已推飞书")
                elif round_growing:
                    status_area.info(f"第 {round_num} 轮完成，{len(round_growing)} 个增长但无新词爆发")
                else:
                    status_area.info(f"第 {round_num} 轮完成，无增长")

                # 实时更新结果展示
                with results_area.container():
                    st.markdown(f"**已完成 {checked}/{total_kw}** — 🚀 增长 {len(all_growing)} / 📊 有量 {len(all_has_volume)} / ❌ 无量 {len(all_no_volume)}")
                    if all_growing:
                        gdf = pd.DataFrame(all_growing).sort_values("growth", ascending=False)
                        st.dataframe(gdf[["domain", "keyword", "avg", "max", "growth"]].rename(
                            columns={"domain": "域名", "keyword": "关键词", "avg": "均值", "max": "最高", "growth": "增长%"}
                        ), use_container_width=True, hide_index=True)

                # 轮间休息 2-3 分钟
                if ri < total_rounds - 1 and not st.session_state.get("domain_stop", False):
                    rest = random.uniform(2 * 60, 3 * 60)
                    status_area.info(f"⏸ 轮间休息 {rest/60:.1f} 分钟...")
                    time.sleep(rest)

            # 最终结果
            if not stopped:
                progress.progress(1.0, text="Trends 验证完成！")
            st.session_state.domain_stop = False
            status_area.empty()

            st.divider()
            st.subheader("📊 最终结果")

            col_g, col_v, col_n = st.columns(3)
            with col_g:
                st.metric("🚀 搜索量增长", len(all_growing))
            with col_v:
                st.metric("📊 有搜索量", len(all_has_volume))
            with col_n:
                st.metric("❌ 无搜索量", len(all_no_volume))

            if all_growing:
                growing_df = pd.DataFrame(all_growing).sort_values("growth", ascending=False)
                display_growing = growing_df[["domain", "keyword", "avg", "max", "growth"]].copy()
                display_growing.columns = ["域名", "关键词", "平均搜索量", "最高搜索量", "增长率%"]
                st.dataframe(display_growing, use_container_width=True, hide_index=True)

                csv_data = display_growing.to_csv(index=False).encode('utf-8-sig')
                st.download_button("📥 下载增长域名 CSV", csv_data, "growing_domains.csv", "text/csv",
                                    use_container_width=True)

                # 最终汇总飞书 — 只推新词爆发
                all_spikes = [item for item in all_growing if item.get("is_spike", False)]
                notify = load_notify_config()
                webhook = notify.get("feishu_webhook", "")
                if webhook and all_spikes:
                    now = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")
                    status_text = "已停止" if stopped else "全部完成"
                    checked = len(all_growing) + len(all_has_volume) + len(all_no_volume)
                    content_lines = [
                        [{"tag": "text", "text": f"📅 {now}\n{status_text}！共查 {checked}/{total_kw}\n🔥 {len(all_spikes)} 个新词爆发（之前无流量，近期突然爆起）"}],
                    ]
                    for item in all_spikes[:30]:
                        kw = item.get("keyword", "")
                        late = item.get("late", 0)
                        max_val = item.get("max", 0)
                        content_lines.append([{"tag": "text", "text": f"  {kw}  (当前热度{late}, 峰值{max_val})"}])
                    payload = {
                        "msg_type": "post",
                        "content": {"post": {"zh_cn": {
                            "title": "🔥 新词爆发 最终汇总",
                            "content": content_lines,
                        }}}
                    }
                    try:
                        feishu_resp = http_requests.post(webhook, json=payload, timeout=10)
                        if feishu_resp.status_code == 200:
                            st.success("✅ 最终报告已推飞书")
                    except Exception:
                        pass

            if all_has_volume:
                with st.expander(f"📊 有搜索量但未明显增长（{len(all_has_volume)} 个）"):
                    vol_df = pd.DataFrame(all_has_volume).sort_values("avg", ascending=False)
                    st.dataframe(vol_df[["domain", "keyword", "avg", "max", "growth"]].rename(
                        columns={"domain": "域名", "keyword": "关键词", "avg": "均值", "max": "最高", "growth": "增长%"}
                    ), use_container_width=True, hide_index=True)

            if all_no_volume:
                with st.expander(f"❌ 无搜索量（{len(all_no_volume)} 个）"):
                    st.text("\n".join([d["domain"] for d in all_no_volume[:200]]))
                    if len(all_no_volume) > 200:
                        st.caption(f"...等共 {len(all_no_volume)} 个")

    elif (start_filter or start_full) and not domain_input.strip():
        st.error("请输入域名列表或上传文件")
