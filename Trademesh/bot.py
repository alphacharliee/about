from datetime import datetime

import datetime as dt
import pytz

from fastapi import FastAPI
import uvicorn
import threading
from event_classifier import classify_event
import requests  # DO NOT reassign 'requests'; ensure this remains the globally imported module
from bs4 import BeautifulSoup
import pandas as pd
import holidays
import numpy as np
import ta
from dotenv import load_dotenv
load_dotenv()
import discord
from finnhub_scraper import get_general_news, get_company_news
import os
from discord.ext import tasks
import datetime as dt
from collections import defaultdict
last_alert_time = defaultdict(lambda: dt.datetime.min.replace(tzinfo=dt.timezone.utc))
import asyncio

from analyzer import analyze_article
import yfinance as yf
import sqlite3
from event_classifier import classify_event

load_dotenv()

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)
print("[DEBUG] Discord client initialized with intents:", intents)
app = FastAPI()

@app.get("/")
async def root():
    return {"status": "✅ TradeMesh is running"}

conn = sqlite3.connect("trademesh_news.db")
cursor = conn.cursor()
cursor.execute('''
CREATE TABLE IF NOT EXISTS subscriptions (
    user_id TEXT,
    ticker TEXT
)
''')
conn.commit()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")  # Loaded from your .env file

pending_subscriptions = {}
user_subscriptions = {}

@client.event
async def on_ready():
    print("[DEBUG] on_ready() triggered")
    now = dt.datetime.now(dt.timezone.utc)
    print(f"[RECONNECTED at {now.strftime('%Y-%m-%d %H:%M:%S')}] ✅ TradeMesh bot is live as {client.user}")

    if not post_market_digest_loop.is_running():
        post_market_digest_loop.start()
    if not post_movers_digest.is_running():
        post_movers_digest.start()
    if not check_unusual_movements.is_running():
        check_unusual_movements.start()

@client.event
async def on_message(message):
    print(f"[DEBUG] Message received: {message.content} from {message.author}")
    if message.author == client.user:
        return

    if message.content.lower().startswith('!help'):
        embed = discord.Embed(
            title="📘 TradeMesh Bot Commands & Features",
            description="Here's what I can do for you:",
            color=0x1abc9c
        )

        embed.add_field(
            name="📰 !gm [TICKER] [COUNT]",
            value="Scan financial news and return sentiment-tagged headlines. Ex: `!gm TSLA 5`",
            inline=False
        )

        embed.add_field(
            name="📊 !analyze <TICKER>",
            value="Request a full analysis of the stock (with disclaimer, sentiment, RSI, VWAP, and more). Ex: `!analyze AAPL`",
            inline=False
        )

        embed.add_field(
            name="⏰ Automatic Market Digest",
            value="Posts news summaries daily at pre-market (8AM), mid-day (12PM), and after-market (5PM).",
            inline=False
        )

        embed.add_field(
            name="📊 Movers Digest (Auto)",
            value="Posts top gainers and losers at 9:45 AM and 3:00 PM ET in the movers channel.",
            inline=False
        )

        embed.add_field(
            name="🚨 Unusual Alerts (Auto)",
            value="Alerts you when stocks move ±2.5% in 5 min, spike in volume, or trigger options flow anomalies.",
            inline=False
        )

        embed.add_field(
            name="🧠 Built-in Intelligence",
            value="Sentiment analysis, plain-English tech indicators, time zones for New York + Tegucigalpa, and ticker mapping.",
            inline=False
        )

        embed.set_footer(text="TradeMesh is for informational use only. DYOR.")
        await message.channel.send(embed=embed)

    if message.content.lower().startswith('!watch'):
        parts = message.content.split()
        if len(parts) == 2:
            ticker = parts[1].upper()
            cursor.execute("SELECT * FROM subscriptions WHERE user_id=? AND ticker=?", (str(message.author.id), ticker))
            if cursor.fetchone():
                await message.channel.send(f"📌 You're already subscribed to `{ticker}`.")
            else:
                cursor.execute("INSERT INTO subscriptions (user_id, ticker) VALUES (?, ?)", (str(message.author.id), ticker))
                conn.commit()
                await message.channel.send(f"✅ You are now watching `{ticker}`.")
        else:
            await message.channel.send("Usage: `!watch TICKER`")

    if message.content.lower().startswith('!unwatch'):
        parts = message.content.split()
        if len(parts) == 2:
            ticker = parts[1].upper()
            cursor.execute("DELETE FROM subscriptions WHERE user_id=? AND ticker=?", (str(message.author.id), ticker))
            conn.commit()
            await message.channel.send(f"🗑️ You unsubscribed from `{ticker}`.")
        else:
            await message.channel.send("Usage: `!unwatch TICKER`")

    if message.content.lower().startswith('!digestnow'):
        await message.channel.send("📬 Running a manual digest check...")
        try:
            await post_market_digest()
        except Exception as e:
            await message.channel.send(f"⚠️ Digest failed: {e}")

    if message.content.lower().startswith('!moversnow'):
        await message.channel.send("📊 Running a manual movers check...")
        await post_movers_digest()

    if message.content.lower().startswith('!insidercheck'):
        parts = message.content.split()
        if len(parts) != 2:
            await message.channel.send("Usage: `!insidercheck <TICKER>`")
        else:
            ticker = parts[1].upper()
            await message.channel.send(f"🔎 Checking insider trades for `${ticker}`...")

            try:
                url = "https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions.json"
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                all_data = response.json()
            except Exception as e:
                await message.channel.send(f"⚠️ Failed to fetch trade data: {e}")
                return

            matches = [entry for entry in all_data if entry.get("ticker") == ticker]
            if not matches:
                await message.channel.send(f"ℹ️ No recent congressional trades found for `{ticker}`.")
                return

            trade_lines = []
            for entry in matches[:5]:  # Show top 5 recent trades
                name = entry.get("senator", "Unknown")
                action = entry.get("type", "N/A")
                amount = entry.get("amount", "N/A")
                date = entry.get("transaction_date", "N/A")
                trade_lines.append(f"• {name} — {action.upper()} — {date} — {amount}")

            output = "\n".join(trade_lines)
            await message.channel.send(
                f"🛡️ Insider Trade Log for `${ticker}`:\n{output}\n[Source](https://senatestockwatcher.com/latest-trades)"
            )

    if message.content.lower().startswith('!movers'):
        import datetime as dt
        import holidays
        us_holidays = holidays.US()
        today = dt.date.today()
        if today.weekday() >= 5 or today in us_holidays:
            await message.channel.send("📅 Markets are closed today (weekend or holiday). Try again on the next trading day.")
            return

        await message.channel.send("📊 Fetching today's top movers...")

        gainers_url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=day_gainers"
        losers_url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=day_losers"

        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            gainers_data = requests.get(gainers_url, headers=headers, timeout=10).json()
            losers_data = requests.get(losers_url, headers=headers, timeout=10).json()

            gainers = gainers_data['finance']['result'][0]['quotes'][:5]
            losers = losers_data['finance']['result'][0]['quotes'][:5]

            gainers_text = "\n".join([
                f"**{g['symbol']}** ({g.get('shortName', 'N/A')}) | ${g.get('regularMarketPrice', 'N/A')} | +{g.get('regularMarketChangePercent', 0):.2f}%"
                for g in gainers
            ])
            losers_text = "\n".join([
                f"**{l['symbol']}** ({l.get('shortName', 'N/A')}) | ${l.get('regularMarketPrice', 'N/A')} | {l.get('regularMarketChangePercent', 0):.2f}%"
                for l in losers
            ])

            await message.channel.send(
                f"🏆 **Top Gainers Today**:\n{gainers_text}\n\n🔻 **Top Losers Today**:\n{losers_text}"
            )
        except Exception as e:
            await message.channel.send(f"⚠️ Unable to fetch market movers: {e}")

    if message.content.lower().startswith('!watchlist'):
        parts = message.content.split()

        # Admin view of another user's watchlist
        if len(parts) == 2 and message.mentions and message.author.guild_permissions.administrator:
            target_user = message.mentions[0]
            cursor.execute("SELECT ticker FROM subscriptions WHERE user_id=?", (str(target_user.id),))
            rows = cursor.fetchall()
            if rows:
                tickers = ", ".join([r[0] for r in rows])
                await message.channel.send(f"📋 {target_user.display_name}'s watchlist: `{tickers}`")
            else:
                await message.channel.send(f"ℹ️ {target_user.display_name} is not watching any tickers.")
        elif len(parts) == 2 and message.mentions:
            await message.channel.send("🚫 You don’t have permission to view other users’ watchlists.")
        else:
            # Default self watchlist
            cursor.execute("SELECT ticker FROM subscriptions WHERE user_id=?", (str(message.author.id),))
            rows = cursor.fetchall()
            if rows:
                tickers = ", ".join([r[0] for r in rows])
                await message.channel.send(f"📈 {message.author.display_name}, your current watchlist is: `{tickers}`")
            else:
                await message.channel.send(f"🔍 {message.author.display_name}, you’re not watching any tickers.")

    if message.content.lower().startswith('!gm'):
        await message.channel.send("🧠 TradeMesh scanning top news...")

        parts = message.content.split()
        ticker_filter = None
        count = 3

        if len(parts) >= 2:
            if parts[1].isdigit():
                count = int(parts[1])
            else:
                ticker_filter = parts[1].upper()
                if len(parts) >= 3 and parts[2].isdigit():
                    count = int(parts[2])

        if ticker_filter:
            headlines = get_company_news(ticker_filter, limit=count)
        else:
            headlines = get_general_news(limit=count)

        if not headlines:
            await message.channel.send(f"❌ No recent news found for `{ticker_filter or 'general market'}`.")
            return

        for news in headlines:
            embed = discord.Embed(
                title=news['title'],
                url=news['link'],
                description=news['description'],
                color=0x3498db
            )
            embed.set_footer(text=f"Source: {news['source']} • Published: {news['published']}")
            await message.channel.send(embed=embed)

    if message.content.lower().startswith('!fundamentals'):
        parts = message.content.split()
        if len(parts) != 2:
            await message.channel.send("Usage: `!fundamentals <TICKER>`")
        else:
            ticker = parts[1].upper()
            await message.channel.send(f"🔍 Fetching fundamentals for `{ticker}`...")
            try:
                stock = yf.Ticker(ticker)
                info = stock.info

                name = info.get("shortName", "Unknown")
                market_cap = info.get("marketCap", 0)
                pe_ratio = info.get("trailingPE", "N/A")
                eps = info.get("trailingEps", "N/A")
                revenue = info.get("totalRevenue", 0)
                profit_margin = info.get("profitMargins", 0)
                debt_equity = info.get("debtToEquity", "N/A")
                recommendation = info.get("recommendationKey", "N/A")

                def format_money(val):
                    if val and val > 1e9:
                        return f"${val / 1e9:.2f}B"
                    elif val and val > 1e6:
                        return f"${val / 1e6:.1f}M"
                    else:
                        return f"${val:,.0f}" if val else "N/A"

                embed = discord.Embed(
                    title=f"📊 Fundamentals: {name} ({ticker})",
                    color=0x3498db
                )
                embed.add_field(name="Market Cap", value=format_money(market_cap), inline=True)
                embed.add_field(name="PE Ratio", value=pe_ratio, inline=True)
                embed.add_field(name="EPS (TTM)", value=eps, inline=True)
                embed.add_field(name="Revenue (TTM)", value=format_money(revenue), inline=True)
                embed.add_field(name="Profit Margin", value=f"{profit_margin*100:.2f}%" if profit_margin else "N/A", inline=True)
                embed.add_field(name="Debt/Equity", value=debt_equity, inline=True)
                embed.add_field(name="Analyst Rating", value=recommendation.capitalize(), inline=True)
                embed.set_footer(text="Source: Yahoo Finance")

                await message.channel.send(embed=embed)
            except Exception as e:
                await message.channel.send(f"⚠️ Could not retrieve fundamentals for `{ticker}`: {e}")

    if message.content.lower().startswith('!analyze'):
        parts = message.content.split()
        if len(parts) == 2:
            ticker = parts[1].upper()
            pending_subscriptions[message.author.id] = ticker
            await message.channel.send(
                "⚠️ DISCLAIMER | AVISO ⚠️\n"
                "🇺🇸 This tool is for informational and educational purposes only. It may contain errors or outdated data and should not be considered financial advice. "
                "Always double-check prices and calculations before making decisions. You are solely responsible for any actions taken. DYOR — I am not liable for any losses.\n\n"
                "🇪🇸 Esta herramienta es solo para fines informativos y educativos. Puede contener errores o datos desactualizados y no debe considerarse asesoramiento financiero. "
                "Siempre verifica precios y cálculos por tu cuenta antes de tomar decisiones. Tú eres el único responsable de tus acciones. DYOR — No soy responsable por pérdidas.\n\n"
                "Do you accept this? Reply with `Y` to confirm or `N` to cancel."
            )
        else:
            await message.channel.send("Usage: `!analyze <TICKER>`")

    elif message.content.strip().upper() in ['Y', 'N'] and message.author.id in pending_subscriptions:
        response = message.content.strip().upper()
        ticker = pending_subscriptions.pop(message.author.id)

        if response == 'Y':
            import datetime as dt
            try:
                stock = yf.Ticker(ticker)
                company_name = stock.info.get("shortName", "Unknown Company")
                data = stock.history(period="2d", interval="1m")
                if not data.empty and len(data) > 21:
                    last_price = data['Close'].iloc[-1]
                    past_price = data['Close'].iloc[-6]
                    pct_change = ((last_price - past_price) / past_price) * 100
                    direction = "📈 Up" if pct_change > 0 else "📉 Down"

                    # Recent sentiment from DB
                    cursor.execute("SELECT sentiment, score FROM news WHERE tickers LIKE ? ORDER BY id DESC LIMIT 1", (f"%{ticker}%",))
                    sentiment_data = cursor.fetchone()
                    if sentiment_data:
                        sentiment, score = sentiment_data
                        sentiment_line = f"🧠 Recent Sentiment: {sentiment} ({score:.2f})"
                    else:
                        sentiment_line = "🧠 Recent Sentiment: No data"

                    # Technical indicators
                    data = data.copy()
                    data['ema_10'] = ta.trend.ema_indicator(data['Close'], window=10)
                    data['sma_20'] = ta.trend.sma_indicator(data['Close'], window=20)
                    data['vwap'] = (data['Close'] * data['Volume']).cumsum() / data['Volume'].cumsum()
                    data['rsi'] = ta.momentum.RSIIndicator(data['Close'], window=14).rsi()

                    latest = data.iloc[-1]
                    from_zone = dt.timezone.utc
                    edt_zone = dt.timezone(dt.timedelta(hours=-4))  # New York time
                    import pytz
                    cst_zone = pytz.timezone("America/Tegucigalpa")  # Tegucigalpa, Honduras

                    data_timestamp = latest.name.to_pydatetime().replace(tzinfo=from_zone)
                    timestamp_edt = data_timestamp.astimezone(edt_zone)
                    timestamp_cst = data_timestamp.astimezone(cst_zone)

                    timestamp_str = (
                        f"{timestamp_edt.strftime('%Y-%m-%d %I:%M %p (%Z - New York)')} / "
                        f"{timestamp_cst.strftime('%I:%M %p (%Z - Tegucigalpa)')}"
                    )
                    insights = []

                    if latest['rsi'] > 70:
                        insights.append("📊 RSI suggests the stock may be overbought — a pullback could be near.")
                    elif latest['rsi'] < 30:
                        insights.append("📊 RSI suggests the stock may be oversold — a rebound could be coming.")

                    if latest['Close'] > latest['vwap']:
                        insights.append("📈 The price is strong, trading above its average level (VWAP).")
                    else:
                        insights.append("📉 The price is weak, trading below its average level (VWAP).")

                    if latest['ema_10'] > latest['sma_20']:
                        insights.append("📈 Short-term momentum is building up.")
                    else:
                        insights.append("📉 Momentum is cooling off compared to recent averages.")

                    tech_insight = "\n".join(insights)

                    await message.channel.send(
                    f"✅ You’ve requested analysis for {company_name} (${ticker}).\n"
                        f"{direction}: {pct_change:.2f}% in the last 5 minutes\n"
                        f"Current Price: ${last_price:.2f} (as of {timestamp_str})\n"
                        f"{sentiment_line}\n\n"
                        f"{tech_insight}"
                    )
                else:
                    await message.channel.send(f"✅ You’ve requested analysis for ${ticker}, but market data may be limited or closed.")
            except Exception as e:
                await message.channel.send(f"⚠️ Error retrieving data for ${ticker}: {e}")

    if message.content.lower().startswith('!options'):
        parts = message.content.split()
        if len(parts) != 2:
            await message.channel.send("Usage: `!options <TICKER>`")
            return

        ticker = parts[1].upper()
        pending_subscriptions[message.author.id] = {'ticker': ticker, 'step': 'awaiting_expiry'}
        stock = yf.Ticker(ticker)
        expiries = stock.options

        if not expiries:
            await message.channel.send(f"⚠️ No available options data for `{ticker}`.")
            pending_subscriptions.pop(message.author.id, None)
            return

        exp_list = "\n".join([f"{i + 1}. {d}" for i, d in enumerate(expiries)])
        await message.channel.send(
            f"🗓️ Available expiration dates for `{ticker}`:\n{exp_list}\n\nReply with the number of the expiration date you want.")

    # Handle replies for options expiration and strike range selection
    elif message.author.id in pending_subscriptions:
        user_state = pending_subscriptions[message.author.id]

        if user_state.get('step') == 'awaiting_expiry':
            try:
                selection = int(message.content.strip()) - 1
                ticker = user_state['ticker']
                stock = yf.Ticker(ticker)
                expiries = stock.options

                if 0 <= selection < len(expiries):
                    selected_expiry = expiries[selection]
                    user_state['expiry'] = selected_expiry
                    user_state['step'] = 'awaiting_strike_range'
                    await message.channel.send(
                        f"✅ Expiration date `{selected_expiry}` selected for `{ticker}`.\n"
                        f"Please enter the strike price range (e.g. `100-150`)."
                    )
                else:
                    await message.channel.send("❌ Invalid selection. Please enter a valid number from the list.")

            except ValueError:
                await message.channel.send("❌ Please enter a number corresponding to the expiration date.")

        elif user_state.get('step') == 'awaiting_strike_range':
            try:
                strike_range = message.content.strip().split('-')
                if len(strike_range) != 2:
                    raise ValueError("Invalid range format.")
                lower, upper = map(float, strike_range)
                ticker = user_state['ticker']
                expiry = user_state['expiry']
                stock = yf.Ticker(ticker)
                opt_chain = stock.option_chain(expiry)

                calls = opt_chain.calls
                puts = opt_chain.puts

                filtered_calls = calls[(calls['strike'] >= lower) & (calls['strike'] <= upper)]
                filtered_puts = puts[(puts['strike'] >= lower) & (puts['strike'] <= upper)]

                def summarize(df, label):
                    df = df[df['volume'] > 0].copy()
                    df['volume_to_oi'] = df['volume'] / (df['openInterest'] + 1)
                    unusual = df[df['volume_to_oi'] > 2.0]
                    if unusual.empty:
                        return f"No unusual {label.lower()} activity."
                    top = unusual.sort_values('volume_to_oi', ascending=False).head(3)
                    result = []
                    for _, row in top.iterrows():
                        result.append(
                            f"{label} @ ${row['strike']} | {int(row['volume'])} vol / {int(row['openInterest'])} OI | "
                            f"Ratio: {row['volume_to_oi']:.2f}"
                        )
                    return "\n".join(result)

                def list_all_strikes(df, label):
                    df = df[df['strike'].between(lower, upper)].copy()
                    if df.empty:
                        return f"No {label.lower()} contracts in this range."
                    top = df.sort_values('strike')
                    result = []
                    for _, row in top.iterrows():
                        result.append(
                            f"{label} @ ${row['strike']} | {int(row['volume'])} vol / {int(row['openInterest'])} OI"
                        )
                    return "\n".join(result)

                calls_summary = summarize(filtered_calls, "Calls")
                puts_summary = summarize(filtered_puts, "Puts")
                all_calls = list_all_strikes(filtered_calls, "Calls")
                all_puts = list_all_strikes(filtered_puts, "Puts")

                embed = discord.Embed(
                    title=f"📈 Options Activity for {ticker} ({expiry})",
                    color=0x8e44ad
                )
                embed.add_field(name="Unusual Calls", value=calls_summary, inline=False)
                embed.add_field(name="All Calls in Range", value=all_calls, inline=False)
                embed.add_field(name="Unusual Puts", value=puts_summary, inline=False)
                embed.add_field(name="All Puts in Range", value=all_puts, inline=False)
                await message.channel.send(embed=embed)
                pending_subscriptions.pop(message.author.id)

            except Exception as e:
                await message.channel.send(f"⚠️ Could not retrieve options data: {e}")

def summarize(df, label):
    df = df[df['volume'] > 0].copy()
    df['volume_to_oi'] = df['volume'] / (df['openInterest'] + 1)
    unusual = df[df['volume_to_oi'] > 2.0]
    if unusual.empty:
        return f"No unusual {label.lower()} activity."
    top = unusual.sort_values('volume_to_oi', ascending=False).head(3)
    result = []
    for _, row in top.iterrows():
        result.append(
            f"{label} @ ${row['strike']} | {int(row['volume'])} vol / {int(row['openInterest'])} OI | "
            f"Ratio: {row['volume_to_oi']:.2f}"
        )
    return "\n".join(result)

    # (No code here using await!)

@tasks.loop(minutes=1)
async def post_market_digest_loop():
    await post_market_digest()


# Extracted digest logic into a new function
async def post_market_digest():
    print("[DEBUG] Running post_market_digest loop")
    import holidays
    us_holidays = holidays.US()
    today = dt.date.today()
    print(f"[DEBUG] Digest check for {today} (weekday: {today.weekday()})")
    if today.weekday() >= 5 or today in us_holidays:
        print("[INFO] Digest skipped — market closed (weekend or holiday).")
        return

    import pytz
    now_utc = dt.datetime.now(dt.timezone.utc)
    ny_zone = pytz.timezone("America/New_York")
    now_nyc = now_utc.astimezone(ny_zone)

    if now_nyc.hour in [8, 12, 17] and now_nyc.minute == 0:
        channel = client.get_channel(1354123151130366042)  # #market-digest
        headlines = get_general_news(limit=10)

        cursor.execute("SELECT DISTINCT user_id FROM subscriptions")
        users = cursor.fetchall()

        for (user_id,) in users:
            cursor.execute("SELECT ticker FROM subscriptions WHERE user_id=?", (user_id,))
            tickers = [row[0] for row in cursor.fetchall()]
            user = await client.fetch_user(int(user_id))
            print(f"[DEBUG] Processing digest for user {user_id}")
            print(f"[DEBUG] User tickers: {tickers}")

            user_lines = []

            for news in headlines:
                combined_text = f"{news['title']}. {news['description']}"
                analysis = analyze_article(news['title'], combined_text)

                sentiment = analysis['sentiment_label']
                score = f"{analysis['sentiment_score']:.2f}"
                detected = [c['ticker'] for c in analysis['companies']]
                matched = [ticker for ticker in tickers if ticker in detected]

                if matched:
                    entry = (
                        f"🗓️ {news['published']}\n"
                        f"**{news['title']}**\n"
                        f"📊 Sentiment: {sentiment} ({score})\n"
                        f"🏷️ Matched: {', '.join(matched)}\n"
                        f"[Read more]({news['link']})\n"
                        f"---"
                    )
                    user_lines.append(entry)
                    save_article_to_db(news, sentiment, float(score), detected)

            if user_lines:
                print(f"[DEBUG] Digest prepared for user {user_id} — {len(user_lines)} items matched.")
                summary = "\n".join(user_lines[:5])
                await channel.send(f"📬 <@{user_id}>, here’s your market digest:\n{summary}")
        print("[INFO] Market digest loop completed.")
# Expanded list of tickers to monitor (top from S&P 500, Nasdaq, and key ETFs)
MONITORED_TICKERS = [
    # Mega-cap tech & growth
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "TSLA", "META", "NFLX", "AMD", "INTC", "AVGO", "QCOM",

    # Financials
    "JPM", "BAC", "WFC", "GS", "MS", "SCHW", "AXP",

    # Healthcare
    "JNJ", "PFE", "LLY", "MRK", "UNH", "ABBV", "BMY", "CVS",

    # Industrials
    "BA", "CAT", "GE", "DE", "LMT", "NOC", "UPS", "FDX",

    # Consumer Discretionary
    "HD", "LOW", "MCD", "SBUX", "NKE", "TGT", "COST", "BKNG", "ROST",

    # Energy
    "XOM", "CVX", "COP", "SLB", "PSX",

    # Materials
    "LIN", "ECL", "APD", "NEM",

    # Utilities & Real Estate
    "NEE", "DUK", "SO", "PLD", "AMT",

    # Communication & Media
    "DIS", "CMCSA", "T", "VZ", "TMUS",

    # ETFs
    "SPY", "QQQ", "DIA", "XLF", "XLE", "XLK", "XLV", "ARKK", "SOXL", "TQQQ"
]

@tasks.loop(minutes=5)
async def check_unusual_movements():
    print("[DEBUG] Running check_unusual_movements loop")
    import holidays
    us_holidays = holidays.US()
    today = dt.date.today()
    if today.weekday() >= 5 or today in us_holidays:
        return

    ALERT_CHANNEL_ID = 1361063053180928111  # trademesh-alerts
    channel = client.get_channel(ALERT_CHANNEL_ID)
    now = dt.datetime.now(dt.timezone.utc)
    for ticker in MONITORED_TICKERS:
        # Skip if no users are subscribed to this ticker
        cursor.execute("SELECT user_id FROM subscriptions WHERE ticker=?", (ticker,))
        subscribers = cursor.fetchall()
        if not subscribers:
            continue

        # Throttle: Skip if last alert was less than 1 hour ago
        if (now - last_alert_time[ticker]).total_seconds() < 3600:
            continue

        try:
            data = yf.Ticker(ticker).history(period="2d", interval="1m")
            if data.empty or len(data) < 21:
                continue

            last_price = data['Close'].iloc[-1]
            past_price = data['Close'].iloc[-6]
            recent_volume = data['Volume'].iloc[-1]
            avg_volume = data['Volume'].iloc[-21:-1].mean()

            if past_price == 0 or avg_volume == 0:
                continue

            pct_change = ((last_price - past_price) / past_price) * 100
            volume_spike = recent_volume / avg_volume

            THRESHOLD_PERCENT = 1.5
            VOLUME_SPIKE_MULTIPLIER = 2.0

            if abs(pct_change) >= THRESHOLD_PERCENT or volume_spike >= VOLUME_SPIKE_MULTIPLIER:
                # Throttle again before sending
                if (now - last_alert_time[ticker]).total_seconds() > 3600:
                    last_alert_time[ticker] = now
                    direction = "📈 Up" if pct_change > 0 else "📉 Down"

                    # Sentiment from DB
                    cursor.execute("SELECT sentiment, score FROM news WHERE tickers LIKE ? ORDER BY id DESC LIMIT 1", (f"%{ticker}%",))
                    sentiment_data = cursor.fetchone()
                    sentiment_line = f"\n🧠 Recent Sentiment: {sentiment_data[0]} ({sentiment_data[1]:.2f})" if sentiment_data else "\n🧠 Recent Sentiment: No data"
                    volume_line = f"\n📊 Volume Spike: {volume_spike:.2f}x avg"

                    # Technical indicators
                    data['ema_10'] = ta.trend.ema_indicator(data['Close'], window=10)
                    data['sma_20'] = ta.trend.sma_indicator(data['Close'], window=20)
                    data['vwap'] = (data['Close'] * data['Volume']).cumsum() / data['Volume'].cumsum()
                    data['rsi'] = ta.momentum.RSIIndicator(data['Close'], window=14).rsi()
                    latest = data.iloc[-1]

                    insights = []
                    if latest['rsi'] > 70:
                        insights.append("📊 RSI suggests the stock may be overbought — a pullback could be near.")
                    elif latest['rsi'] < 30:
                        insights.append("📊 RSI suggests the stock may be oversold — a rebound could be coming.")
                    if latest['Close'] > latest['vwap']:
                        insights.append("📈 The price is strong, trading above its average level (VWAP).")
                    else:
                        insights.append("📉 The price is weak, trading below its average level (VWAP).")
                    if latest['ema_10'] > latest['sma_20']:
                        insights.append("📈 Short-term momentum is building up.")
                    else:
                        insights.append("📉 Momentum is cooling off compared to recent averages.")
                    tech_insight = "\n".join(insights)

                    user_mentions = [f"<@{row[0]}>" for row in subscribers]
                    mention_block = "📣 " + ", ".join(user_mentions)

                    await channel.send(
                        f"{mention_block}\n🚨 Alert: ${ticker} Unusual Activity\n"
                        f"{direction}: {pct_change:.2f}% in 5 minutes\n"
                        f"Current Price: ${last_price:.2f}"
                        f"{volume_line}"
                        f"{sentiment_line}\n\n"
                        f"{tech_insight}"
                    )
        except Exception as e:
            print(f"[ERROR] check_unusual_movements ({ticker}): {e}")
def is_weekend_or_market_closed():
    now = dt.datetime.now(pytz.timezone('US/Eastern'))
    # Weekday: Monday = 0, Sunday = 6
    if now.weekday() >= 5:  # Saturday or Sunday
        return True
    return False

@tasks.loop(minutes=1)
async def post_movers_digest():
    if is_weekend_or_market_closed():
        print("[SKIP] Market closed or weekend — skipping top movers post.")
        return
    print("[DEBUG] Running post_movers_digest loop")
    now_utc = dt.datetime.now(dt.timezone.utc)
    ny_zone = pytz.timezone("America/New_York")
    now_nyc = now_utc.astimezone(ny_zone)

    if (now_nyc.hour == 9 and now_nyc.minute == 45) or (now_nyc.hour == 15 and now_nyc.minute == 0):
        gainers_url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=day_gainers"
        losers_url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=day_losers"
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            gainers_data = requests.get(gainers_url, headers=headers, timeout=10).json()
            losers_data = requests.get(losers_url, headers=headers, timeout=10).json()

            gainers = gainers_data['finance']['result'][0]['quotes'][:5]
            losers = losers_data['finance']['result'][0]['quotes'][:5]

            gainers_text = "\n".join([
                f"**{g['symbol']}** ({g.get('shortName', 'N/A')}) | ${g.get('regularMarketPrice', 'N/A')} | +{g.get('regularMarketChangePercent', 0):.2f}%"
                for g in gainers
            ])
            losers_text = "\n".join([
                f"**{l['symbol']}** ({l.get('shortName', 'N/A')}) | ${l.get('regularMarketPrice', 'N/A')} | {l.get('regularMarketChangePercent', 0):.2f}%"
                for l in losers
            ])

            channel = client.get_channel(1354122504616149066) #stock-market-talk
            digest_time = now_nyc.strftime('%I:%M %p').lstrip('0')  # e.g. 9:45 AM or 3:00 PM
            await channel.send(
                f"🏆 **Top Gainers at {digest_time} ET**:\n{gainers_text}\n\n🔻 **Top Losers at {digest_time} ET**:\n{losers_text}"
            )
        except Exception as e:
            print(f"[ERROR] post_movers_digest: {e}")

def save_article_to_db(news, sentiment_label, sentiment_score, tickers):
    cursor.execute('''
    INSERT INTO news (title, published, sentiment, score, tickers, source, link)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        news['title'],
        news['published'],
        sentiment_label,
        sentiment_score,
        ', '.join(tickers),
        news['source'],
        news['link']
    ))
    conn.commit()
def run_web():
    uvicorn.run(app, host="0.0.0.0", port=3000)

threading.Thread(target=run_web).start()
client.run(DISCORD_TOKEN)