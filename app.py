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
                                  help="每次请求之间的基础等待时间，优先级最高")

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
tab1, tab2, tab3 = st.tabs(["🔍 爆增词追踪", "🔥 时下流行", "🗺 Sitemap 监控"])

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
                    wait = 60 + retry_count * 30 + random.randint(0, 10)
                    status_area.warning(f"⚠️ 触发限流 (429)，等待 {wait} 秒后重试... ({retry_count}/{max_retries})")
                    time.sleep(wait)
                    pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)

                except ResponseError as e:
                    retry_count += 1
                    wait = 30 + retry_count * 15
                    status_area.warning(f"⚠️ 请求出错: {e}，等待 {wait} 秒后重试... ({retry_count}/{max_retries})")
                    time.sleep(wait)

                except Exception as e:
                    status_area.error(f"❌ 查询 '{kw}' 失败: {e}")
                    failed_kw.append(kw)
                    break
            else:
                failed_kw.append(kw)
                status_area.error(f"❌ '{kw}' 重试 {max_retries} 次后仍失败，跳过")

            if i < total_kw - 1:
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
                        wait = 60 + retry_count * 30
                        status_area.warning(f"⚠️ 趋势验证触发限流，等待 {wait} 秒...")
                        time.sleep(wait)
                        pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)
                    except Exception:
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

    # 从 config.json 读取已配置的 sitemap URL
    config_sitemaps = APP_CONFIG.get("sitemap_urls", [])

    st.subheader("📋 监控列表")

    if config_sitemaps:
        for i, url in enumerate(config_sitemaps):
            domain = urlparse(url).netloc
            cache_file = SITEMAP_DIR / f"{domain}.xml"
            status = "✅ 已有快照" if cache_file.exists() else "🆕 待首次采集"
            col_url, col_del = st.columns([5, 1])
            with col_url:
                st.markdown(f"**{i+1}.** `{url}`  {status}")
            with col_del:
                if st.button("🗑", key=f"del_sitemap_{i}", help="删除此站点"):
                    config_sitemaps.pop(i)
                    APP_CONFIG["sitemap_urls"] = config_sitemaps
                    save_config(APP_CONFIG)
                    st.rerun()
    else:
        st.info("暂无监控站点，请在下方添加。")

    # 添加新 sitemap
    col_input, col_add = st.columns([4, 1])
    with col_input:
        new_sitemap_url = st.text_input("添加 Sitemap URL", placeholder="https://example.com/sitemap.xml",
                                         label_visibility="collapsed")
    with col_add:
        if st.button("➕ 添加", key="add_sitemap"):
            if new_sitemap_url.strip():
                url_to_add = new_sitemap_url.strip()
                if url_to_add not in config_sitemaps:
                    config_sitemaps.append(url_to_add)
                    APP_CONFIG["sitemap_urls"] = config_sitemaps
                    save_config(APP_CONFIG)
                    st.success(f"已添加")
                    st.rerun()
                else:
                    st.warning("该 URL 已存在")
            else:
                st.warning("请输入 URL")

    st.divider()

    start_sitemap = st.button("🔍 立即检查", type="primary", use_container_width=True,
                               disabled=len(config_sitemaps) == 0)

    if start_sitemap and config_sitemaps:
        SITEMAP_DIR.mkdir(parents=True, exist_ok=True)
        all_changes = {}

        progress_bar = st.progress(0, text="开始检查...")
        total = len(config_sitemaps)

        for i, url in enumerate(config_sitemaps):
            domain = urlparse(url).netloc
            progress_bar.progress(i / total, text=f"检查 {domain}...")

            try:
                resp = http_requests.get(url, timeout=15, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })
                resp.raise_for_status()

                root = ET.fromstring(resp.content)
                ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                new_urls = set()
                for loc in root.findall('.//ns:url/ns:loc', ns):
                    if loc.text:
                        new_urls.add(loc.text.strip())

                # 读取旧快照
                cache_file = SITEMAP_DIR / f"{domain}.xml"
                old_urls = set()
                if cache_file.exists():
                    old_root = ET.fromstring(cache_file.read_text(encoding='utf-8'))
                    for loc in old_root.findall('.//ns:url/ns:loc', ns):
                        if loc.text:
                            old_urls.add(loc.text.strip())

                added = new_urls - old_urls
                if added:
                    all_changes[domain] = {
                        'new_urls': sorted(added),
                        'total': len(new_urls),
                        'old_total': len(old_urls),
                    }

                # 保存最新版本
                cache_file.write_text(resp.text, encoding='utf-8')

            except Exception as e:
                st.error(f"❌ {domain} 检查失败: {e}")

        progress_bar.progress(1.0, text="检查完成！")

        st.divider()

        if all_changes:
            total_new = sum(len(v['new_urls']) for v in all_changes.values())
            st.subheader(f"🆕 发现 {total_new} 个新页面")

            for domain, info in all_changes.items():
                with st.expander(f"🌐 {domain}（{info['old_total']} → {info['total']}，+{len(info['new_urls'])}）", expanded=True):
                    for u in info['new_urls']:
                        st.markdown(f"- {u}")

            # 飞书通知
            notify = load_notify_config()
            webhook = notify.get("feishu_webhook", "")
            if webhook:
                from datetime import timezone
                BEIJING_TZ = timezone(timedelta(hours=8))
                now = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")
                content_lines = [
                    [{"tag": "text", "text": f"📅 {now}\n{len(all_changes)} 个站点有更新，共 {total_new} 个新页面"}],
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
                        "title": "🗺 Sitemap 监控报告",
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
        else:
            st.success("✅ 所有站点无变化")
